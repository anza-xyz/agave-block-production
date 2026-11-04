#![cfg(feature = "agave-unstable-api")]

use {
    agave_block_engine_proto::{
        auth::{
            GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role, Token,
            auth_service_client::AuthServiceClient,
        },
        block_engine::{
            SubscribePacketsRequest, SubscribePacketsResponse,
            block_engine_validator_client::BlockEngineValidatorClient,
        },
        packet::{Meta as ProtoMeta, Packet},
    },
    crossbeam_channel::{Sender, TrySendError},
    log::{debug, info, warn},
    solana_keypair::Keypair,
    solana_perf::packet::{BytesPacket, BytesPacketBatch, Meta, PacketBatch, PacketFlags},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        convert::Infallible,
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::{Duration, SystemTime},
    },
    thiserror::Error,
    tokio::{runtime::Runtime, time},
    tonic::{
        Request, Status, Streaming,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        transport::{self, Channel, ClientTlsConfig, Endpoint},
    },
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(5)
};
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const RECONNECT_BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(5)
};
const TOKEN_REFRESH_SLACK: Duration = Duration::from_secs(60);
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
enum SessionError {
    #[error("transport: {0}")]
    Transport(#[from] transport::Error),
    #[error("rpc: {0}")]
    Status(#[from] Status),
    #[error("auth response has no access token")]
    MissingAccessToken,
    #[error("access token has no valid expiry")]
    MissingExpiry,
    #[error("access token already expired")]
    ExpiredAccessToken,
    #[error("invalid access token: {0}")]
    InvalidAccessToken(#[from] InvalidMetadataValue),
    #[error("packet stream closed")]
    StreamClosed,
    #[error("packet receiver disconnected")]
    ReceiverDisconnected,
}

/// Converts a block engine packet into a [`BytesPacket`].
/// Returns `None` for discarded packets and packets with missing or
/// inconsistent metadata.
fn packet_from_proto(packet: Packet) -> Option<BytesPacket> {
    let Packet { data, meta } = packet;
    let meta = meta?;

    // Nothing really blocks outside engine to just spam packets with flag set to discarded
    // so lets skip them early
    if meta.flags.as_ref().is_some_and(|flags| flags.discard) {
        return None;
    }
    let size = usize::try_from(meta.size)
        .ok()
        .filter(|&size| size != 0 && size <= data.len())?;
    Some(BytesPacket::new(
        data.slice(..size),
        meta_from_proto(&meta, size),
    ))
}

/// Maps the proto metadata onto a packet [`Meta`] of `size` bytes. An address
/// that fails to parse is left unspecified.
fn meta_from_proto(proto_meta: &ProtoMeta, size: usize) -> Meta {
    let flags = proto_meta.flags.unwrap_or_default();
    let mut meta = Meta::default();
    meta.size = size;
    if let Ok(addr) = proto_meta.addr.parse() {
        meta.addr = addr;
    }
    meta.port = u16::try_from(proto_meta.port).unwrap_or_default();
    meta.flags
        .set(PacketFlags::SIMPLE_VOTE_TX, flags.simple_vote_tx);
    meta.flags.set(PacketFlags::FORWARDED, flags.forwarded);
    meta.flags
        .set(PacketFlags::FROM_STAKED_NODE, flags.from_staked_node);
    meta
}

/// Authenticates with a block engine as a validator and forwards the packets it
/// streams. Reconnects with a fixed backoff after any failure.
pub struct BlockEngineClient {
    url: String,
    identity: Keypair,
    packet_sender: Sender<PacketBatch>,
}

impl BlockEngineClient {
    pub fn new(url: String, identity: Keypair, packet_sender: Sender<PacketBatch>) -> Self {
        Self {
            url,
            identity,
            packet_sender,
        }
    }

    /// Runs the client on its own thread until `exit` is set or the packet
    /// receiver is dropped.
    pub fn spawn(self, exit: Arc<AtomicBool>) -> io::Result<JoinHandle<()>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        thread::Builder::new()
            .name("solBlkEngClient".into())
            .spawn(move || self.run(&runtime, &exit))
    }

    fn run(&self, runtime: &Runtime, exit: &AtomicBool) {
        runtime.block_on(async {
            loop {
                let Err(err) = tokio::select! {
                    result = self.run_session() => result,
                    () = wait_for_exit(exit) => return,
                };
                if let SessionError::ReceiverDisconnected = err {
                    info!("Block engine packet receiver disconnected, stopping client");
                    return;
                }
                warn!("Block engine session with {} failed: {err}", self.url);
                tokio::select! {
                    () = time::sleep(RECONNECT_BACKOFF) => {}
                    () = wait_for_exit(exit) => return,
                }
            }
        });
    }

    async fn run_session(&self) -> Result<Infallible, SessionError> {
        let channel = connect(&self.url).await?;
        let mut auth = AuthServiceClient::new(channel.clone());
        let mut validator = BlockEngineValidatorClient::new(channel);
        let (mut packets, refresh_delay) = self.subscribe(&mut auth, &mut validator).await?;
        info!("Subscribed to block engine {} packets", self.url);
        let refresh = time::sleep(refresh_delay);
        tokio::pin!(refresh);

        loop {
            tokio::select! {
                message = packets.message() => match message? {
                    Some(response) => self.forward(response)?,
                    None => return Err(SessionError::StreamClosed),
                },
                () = &mut refresh => {
                    // The token is only checked when a subscription starts, so
                    // refreshing it means subscribing again. Keep the current
                    // stream if that fails.
                    let refresh_delay = match self.subscribe(&mut auth, &mut validator).await {
                        Ok((resubscribed, refresh_delay)) => {
                            debug!("Resubscribed to block engine packets with a new token");
                            packets = resubscribed;
                            refresh_delay
                        }
                        Err(err) => {
                            warn!("Failed to refresh block engine subscription: {err}");
                            RECONNECT_BACKOFF
                        }
                    };
                    refresh.set(time::sleep(refresh_delay));
                }
            }
        }
    }

    async fn subscribe(
        &self,
        auth: &mut AuthServiceClient<Channel>,
        validator: &mut BlockEngineValidatorClient<Channel>,
    ) -> Result<(Streaming<SubscribePacketsResponse>, Duration), SessionError> {
        let access_token = authenticate(auth, &self.identity).await?;
        let refresh_delay = access_token.refresh_delay()?;
        let mut request = Request::new(SubscribePacketsRequest {});
        request
            .metadata_mut()
            .insert("authorization", access_token.value);
        let packets = validator.subscribe_packets(request).await?.into_inner();
        Ok((packets, refresh_delay))
    }

    fn forward(&self, response: SubscribePacketsResponse) -> Result<(), SessionError> {
        let Some(batch) = response.batch else {
            return Ok(());
        };
        let packets: BytesPacketBatch = batch
            .packets
            .into_iter()
            .filter_map(packet_from_proto)
            .collect();
        if packets.is_empty() {
            return Ok(());
        }
        match self.packet_sender.try_send(packets.into()) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(packets)) => {
                debug!(
                    "Dropping {} block engine packets: receiver is full",
                    packets.len()
                );
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err(SessionError::ReceiverDisconnected),
        }
    }
}

struct AccessToken {
    value: AsciiMetadataValue,
    expires_at: Option<SystemTime>,
}

impl AccessToken {
    fn refresh_delay(&self) -> Result<Duration, SessionError> {
        let expires_at = self.expires_at.ok_or(SessionError::MissingExpiry)?;
        refresh_delay(expires_at, SystemTime::now()).ok_or(SessionError::ExpiredAccessToken)
    }
}

impl TryFrom<Token> for AccessToken {
    type Error = SessionError;

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        Ok(Self {
            value: format!("Bearer {}", token.value).parse()?,
            expires_at: token
                .expires_at_utc
                .and_then(|expires_at| SystemTime::try_from(expires_at).ok()),
        })
    }
}

async fn connect(url: &str) -> Result<Channel, SessionError> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .tcp_keepalive_interval(Some(TCP_KEEPALIVE_INTERVAL))
        .tcp_keepalive_retries(Some(TCP_KEEPALIVE_RETRIES));
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(endpoint.connect().await?)
}

async fn authenticate(
    auth: &mut AuthServiceClient<Channel>,
    identity: &Keypair,
) -> Result<AccessToken, SessionError> {
    let pubkey = identity.pubkey();
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator.into(),
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await?
        .into_inner()
        .challenge;
    let challenge = challenge_message(&pubkey, &challenge);
    let signed_challenge = identity.sign_message(challenge.as_bytes());
    let tokens = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed_challenge.as_ref().to_vec(),
        })
        .await?
        .into_inner();
    tokens
        .access_token
        .ok_or(SessionError::MissingAccessToken)?
        .try_into()
}

fn challenge_message(pubkey: &Pubkey, challenge: &str) -> String {
    format!("{pubkey}-{challenge}")
}

fn refresh_delay(expires_at: SystemTime, now: SystemTime) -> Option<Duration> {
    let remaining = expires_at
        .duration_since(now)
        .ok()
        .filter(|remaining| !remaining.is_zero())?;
    let half = remaining.checked_div(2).unwrap_or_default();
    Some(remaining.saturating_sub(TOKEN_REFRESH_SLACK).max(half))
}

async fn wait_for_exit(exit: &AtomicBool) {
    while !exit.load(Ordering::Relaxed) {
        time::sleep(EXIT_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod client_tests;

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_block_engine_proto::packet::{
            Meta as ProtoMeta, PacketBatch as ProtoPacketBatch, PacketFlags as ProtoPacketFlags,
        },
        crossbeam_channel::{Receiver, TryRecvError, bounded},
        solana_perf::packet::bytes::Bytes,
        std::{
            assert_matches,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
        },
    };

    fn client(capacity: usize) -> (BlockEngineClient, Receiver<PacketBatch>) {
        let (sender, receiver) = bounded(capacity);
        let client = BlockEngineClient::new("http://localhost".into(), Keypair::new(), sender);
        (client, receiver)
    }

    fn packet(data: &[u8], size: u64, addr: &str, flags: ProtoPacketFlags) -> Packet {
        Packet {
            data: Bytes::copy_from_slice(data),
            meta: Some(ProtoMeta {
                size,
                addr: addr.into(),
                port: 8001,
                flags: Some(flags),
                sender_stake: 0,
            }),
        }
    }

    fn valid_packet() -> Packet {
        packet(&[1, 2, 3], 3, "127.0.0.1", ProtoPacketFlags::default())
    }

    fn response(packets: usize) -> SubscribePacketsResponse {
        SubscribePacketsResponse {
            header: None,
            batch: Some(ProtoPacketBatch {
                packets: vec![valid_packet(); packets],
            }),
        }
    }

    fn proto_meta(addr: &str, port: u32, flags: Option<ProtoPacketFlags>) -> ProtoMeta {
        ProtoMeta {
            size: 0,
            addr: addr.into(),
            port,
            flags,
            sender_stake: 0,
        }
    }

    #[test]
    fn meta_from_proto_maps_fields() {
        let flags = ProtoPacketFlags {
            simple_vote_tx: true,
            forwarded: true,
            from_staked_node: true,
            ..ProtoPacketFlags::default()
        };
        let mut proto_meta = proto_meta("10.0.0.1", 8001, Some(flags));
        proto_meta.size = 100;
        let meta = meta_from_proto(&proto_meta, 3);
        assert_eq!(meta.size, 3);
        assert_eq!(meta.addr, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(meta.port, 8001);
        assert_eq!(
            meta.flags,
            PacketFlags::SIMPLE_VOTE_TX | PacketFlags::FORWARDED | PacketFlags::FROM_STAKED_NODE
        );
    }

    #[test]
    fn meta_from_proto_ignores_unmapped_flags() {
        let flags = ProtoPacketFlags {
            discard: true,
            repair: true,
            tracer_packet: true,
            ..ProtoPacketFlags::default()
        };
        let meta = meta_from_proto(&proto_meta("127.0.0.1", 0, Some(flags)), 1);
        assert_eq!(meta.flags, PacketFlags::empty());
        let meta = meta_from_proto(&proto_meta("127.0.0.1", 0, None), 1);
        assert_eq!(meta.flags, PacketFlags::empty());
    }

    #[test]
    fn meta_from_proto_parses_ipv6_and_defaults_invalid_addresses() {
        let addr_from = |addr| meta_from_proto(&proto_meta(addr, 0, None), 1).addr;
        let unspecified = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        assert_eq!(addr_from("::1"), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr_from("10.0.0.1:8001"), unspecified);
        assert_eq!(addr_from("not an address"), unspecified);
        assert_eq!(addr_from(""), unspecified);
    }

    #[test]
    fn meta_from_proto_defaults_out_of_range_ports() {
        let port_from = |port| meta_from_proto(&proto_meta("127.0.0.1", port, None), 1).port;
        assert_eq!(port_from(u32::from(u16::MAX)), u16::MAX);
        assert_eq!(port_from(u32::from(u16::MAX) + 1), 0);
        assert_eq!(port_from(u32::MAX), 0);
    }

    #[test]
    fn packet_from_proto_truncates_to_size_without_copying() {
        let packet = packet(&[1, 2, 3, 0, 0], 3, "10.0.0.1", ProtoPacketFlags::default());
        let data_ptr = packet.data.as_ptr();
        let expected_meta = meta_from_proto(packet.meta.as_ref().unwrap(), 3);
        let packet = packet_from_proto(packet).unwrap();
        assert_eq!(packet.data(..), Some(&[1, 2, 3][..]));
        assert_eq!(packet.buffer().as_ptr(), data_ptr);
        assert_eq!(packet.meta(), &expected_meta);
    }

    #[test]
    fn packet_from_proto_accepts_exact_size_and_missing_flags() {
        let mut packet = packet(&[1, 2], 2, "127.0.0.1", ProtoPacketFlags::default());
        packet.meta.as_mut().unwrap().flags = None;
        let packet = packet_from_proto(packet).unwrap();
        assert_eq!(packet.data(..), Some(&[1, 2][..]));
        assert_eq!(packet.meta().size, 2);
    }

    #[test]
    fn packet_from_proto_rejects_discarded_and_malformed_packets() {
        let discard = ProtoPacketFlags {
            discard: true,
            ..ProtoPacketFlags::default()
        };
        let no_meta = Packet {
            data: Bytes::from_static(&[1]),
            meta: None,
        };
        for packet in [
            no_meta,
            packet(&[1], 1, "127.0.0.1", discard),
            packet(&[1], 0, "127.0.0.1", ProtoPacketFlags::default()),
            packet(&[1], 2, "127.0.0.1", ProtoPacketFlags::default()),
            packet(&[1], u64::MAX, "127.0.0.1", ProtoPacketFlags::default()),
            packet(&[], 0, "127.0.0.1", ProtoPacketFlags::default()),
        ] {
            assert_eq!(packet_from_proto(packet), None);
        }
    }

    #[test]
    fn forwards_only_valid_packets() {
        let (client, receiver) = client(1);
        let mut response = response(2);
        response.batch.as_mut().unwrap().packets.push(Packet {
            data: Bytes::from_static(&[1]),
            meta: None,
        });
        client.forward(response).unwrap();
        assert_eq!(receiver.try_recv().unwrap().len(), 2);
    }

    #[test]
    fn signed_challenge_verifies_against_identity() {
        let identity = Keypair::new();
        let pubkey = identity.pubkey();
        let message = challenge_message(&pubkey, "challenge");
        assert_eq!(message, format!("{pubkey}-challenge"));
        let signature = identity.sign_message(message.as_bytes());
        assert!(signature.verify(pubkey.as_ref(), message.as_bytes()));
    }

    #[test]
    fn refresh_delay_leaves_slack_before_expiry() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let hour = Duration::from_secs(3600);
        assert_eq!(
            refresh_delay(now + hour, now),
            Some(hour - TOKEN_REFRESH_SLACK)
        );
    }

    #[test]
    fn refresh_delay_uses_half_of_short_lifetimes() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(
            refresh_delay(now + Duration::from_secs(30), now),
            Some(Duration::from_secs(15))
        );
    }

    #[test]
    fn refresh_delay_rejects_expired_tokens() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(refresh_delay(now, now), None);
        assert_eq!(refresh_delay(now - Duration::from_secs(1), now), None);
    }

    #[test]
    fn access_token_requires_expiry() {
        let token = AccessToken::try_from(Token {
            value: "token".into(),
            expires_at_utc: None,
        })
        .unwrap();
        assert_eq!(token.value, "Bearer token");
        assert_matches!(token.refresh_delay(), Err(SessionError::MissingExpiry));
    }

    #[test]
    fn forwards_packet_batches() {
        let (client, receiver) = client(1);
        client.forward(response(2)).unwrap();
        assert_eq!(receiver.try_recv().unwrap().len(), 2);
    }

    #[test]
    fn skips_responses_without_packets() {
        let (client, receiver) = client(1);
        client
            .forward(SubscribePacketsResponse {
                header: None,
                batch: None,
            })
            .unwrap();
        client.forward(response(0)).unwrap();
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn drops_batches_when_receiver_is_full() {
        let (client, receiver) = client(1);
        client.forward(response(1)).unwrap();
        client.forward(response(2)).unwrap();
        assert_eq!(receiver.try_recv().unwrap().len(), 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn stops_when_receiver_disconnects() {
        let (client, receiver) = client(1);
        drop(receiver);
        assert_matches!(
            client.forward(response(1)),
            Err(SessionError::ReceiverDisconnected)
        );
    }
}
