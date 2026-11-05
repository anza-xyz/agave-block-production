#![cfg(feature = "agave-unstable-api")]

use {
    agave_block_engine_proto::{
        auth::{
            GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
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
    solana_signer::Signer,
    std::{
        io,
        thread::{self, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tokio::time,
    tokio_stream::{Stream, StreamExt},
    tokio_util::sync::CancellationToken,
    tonic::{
        Request, Status, Streaming,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        transport::{self, Channel, ClientTlsConfig, Endpoint},
    },
};

/// Timeouts and intervals used by [`BlockEngineClient`].
#[derive(Clone, Debug)]
pub struct BlockEngineClientConfig {
    /// Timeout for establishing the connection.
    pub connect_timeout: Duration,
    /// Timeout for each request.
    pub request_timeout: Duration,
    /// Idle time before TCP keepalive probes start.
    pub tcp_keepalive: Duration,
    /// Interval between TCP keepalive probes.
    pub tcp_keepalive_interval: Duration,
    /// Unanswered TCP keepalive probes before the connection is dropped.
    pub tcp_keepalive_retries: u32,
    /// Delay before reconnecting after a failed session.
    pub reconnect_backoff: Duration,
}

impl Default for BlockEngineClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            tcp_keepalive: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(10),
            tcp_keepalive_retries: 3,
            reconnect_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Error)]
enum SessionError {
    #[error("transport: {0}")]
    Transport(#[from] transport::Error),
    #[error("rpc: {0}")]
    Status(#[from] Status),
    #[error("auth response has no access token")]
    MissingAccessToken,
    #[error("invalid access token: {0}")]
    InvalidAccessToken(#[from] InvalidMetadataValue),
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
    config: BlockEngineClientConfig,
}

impl BlockEngineClient {
    pub fn new(
        url: String,
        identity: Keypair,
        packet_sender: Sender<PacketBatch>,
        config: BlockEngineClientConfig,
    ) -> Self {
        Self {
            url,
            identity,
            packet_sender,
            config,
        }
    }

    /// Runs the client on its own thread until `cancel` is cancelled or the packet
    /// receiver is dropped.
    pub fn spawn(self, cancel: CancellationToken) -> io::Result<JoinHandle<()>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        thread::Builder::new()
            .name("solBlkEngClient".into())
            .spawn(move || {
                runtime.block_on(cancel.run_until_cancelled_owned(self.reconnect_loop(
                    async || {
                        let channel = connect(&self.url, &self.config).await?;
                        let packets = self.subscribe(channel).await?;
                        info!("Subscribed to block engine {} packets", self.url);
                        self.consume(packets).await
                    },
                )));
            })
    }

    /// Runs `session` repeatedly until it fails with `ReceiverDisconnected`,
    /// waiting `reconnect_backoff` after every other outcome.
    async fn reconnect_loop(&self, mut session: impl AsyncFnMut() -> Result<(), SessionError>) {
        loop {
            match session().await {
                Ok(()) => info!("Block engine {} closed the packet stream", self.url),
                Err(SessionError::ReceiverDisconnected) => {
                    info!("Block engine packet receiver disconnected, stopping client");
                    return;
                }
                Err(err) => warn!("Block engine session with {} failed: {err}", self.url),
            }
            time::sleep(self.config.reconnect_backoff).await;
        }
    }

    /// Forwards packets from `packets` until the stream ends (`Ok`) or fails.
    async fn consume(
        &self,
        mut packets: impl Stream<Item = Result<SubscribePacketsResponse, Status>> + Unpin,
    ) -> Result<(), SessionError> {
        while let Some(response) = packets.next().await {
            self.forward(response?)?;
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        channel: Channel,
    ) -> Result<Streaming<SubscribePacketsResponse>, SessionError> {
        let access_token =
            authenticate(&mut AuthServiceClient::new(channel.clone()), &self.identity).await?;
        let mut request = Request::new(SubscribePacketsRequest {});
        request.metadata_mut().insert("authorization", access_token);
        Ok(BlockEngineValidatorClient::new(channel)
            .subscribe_packets(request)
            .await?
            .into_inner())
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

async fn connect(url: &str, config: &BlockEngineClientConfig) -> Result<Channel, SessionError> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(config.connect_timeout)
        .timeout(config.request_timeout)
        .tcp_keepalive(Some(config.tcp_keepalive))
        .tcp_keepalive_interval(Some(config.tcp_keepalive_interval))
        .tcp_keepalive_retries(Some(config.tcp_keepalive_retries));
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(endpoint.connect().await?)
}

async fn authenticate(
    auth: &mut AuthServiceClient<Channel>,
    identity: &Keypair,
) -> Result<AsciiMetadataValue, SessionError> {
    let pubkey = identity.pubkey();
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator.into(),
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await?
        .into_inner()
        .challenge;
    let token = auth
        .generate_auth_tokens(auth_tokens_request(identity, &challenge))
        .await?
        .into_inner()
        .access_token
        .ok_or(SessionError::MissingAccessToken)?;
    Ok(format!("Bearer {}", token.value).parse()?)
}

/// Builds the token request answering `challenge`, signed by `identity`.
fn auth_tokens_request(identity: &Keypair, challenge: &str) -> GenerateAuthTokensRequest {
    let pubkey = identity.pubkey();
    let challenge = format!("{pubkey}-{challenge}");
    let signed_challenge = identity.sign_message(challenge.as_bytes());
    GenerateAuthTokensRequest {
        challenge,
        client_pubkey: pubkey.to_bytes().to_vec(),
        signed_challenge: signed_challenge.as_ref().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_block_engine_proto::packet::{
            Meta as ProtoMeta, PacketBatch as ProtoPacketBatch, PacketFlags as ProtoPacketFlags,
        },
        crossbeam_channel::{Receiver, TryRecvError, bounded},
        solana_perf::packet::bytes::Bytes,
        solana_signature::Signature,
        std::{
            assert_matches,
            collections::VecDeque,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
        },
        tokio::time::Instant,
        tokio_util::sync::CancellationToken,
    };

    const BACKOFF: Duration = Duration::from_secs(5);
    const HALF_BACKOFF: Duration = Duration::from_millis(2500);

    fn client(capacity: usize) -> (BlockEngineClient, Receiver<PacketBatch>) {
        let (sender, receiver) = bounded(capacity);
        let client = BlockEngineClient::new(
            "http://localhost".into(),
            Keypair::new(),
            sender,
            BlockEngineClientConfig::default(),
        );
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
    fn auth_tokens_request_signs_challenge_with_identity() {
        let identity = Keypair::new();
        let pubkey = identity.pubkey();
        let request = auth_tokens_request(&identity, "challenge");
        assert_eq!(request.challenge, format!("{pubkey}-challenge"));
        assert_eq!(request.client_pubkey, pubkey.to_bytes());
        let signature = Signature::try_from(request.signed_challenge.as_slice()).unwrap();
        assert!(signature.verify(pubkey.as_ref(), request.challenge.as_bytes()));
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

    fn stream(
        items: Vec<Result<SubscribePacketsResponse, Status>>,
    ) -> impl Stream<Item = Result<SubscribePacketsResponse, Status>> + Unpin {
        tokio_stream::iter(items)
    }

    #[tokio::test]
    async fn consume_forwards_until_stream_ends() {
        let (client, receiver) = client(2);
        let result = client
            .consume(stream(vec![Ok(response(1)), Ok(response(2))]))
            .await;
        assert_matches!(result, Ok(()));
        assert_eq!(receiver.try_recv().unwrap().len(), 1);
        assert_eq!(receiver.try_recv().unwrap().len(), 2);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn consume_stops_at_stream_error() {
        let (client, receiver) = client(2);
        let mut packets = stream(vec![
            Ok(response(1)),
            Err(Status::internal("stream failed")),
            Ok(response(2)),
        ]);
        let result = client.consume(&mut packets).await;
        assert_matches!(result, Err(SessionError::Status(_)));
        assert_eq!(receiver.try_recv().unwrap().len(), 1);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(packets.next().await.unwrap().unwrap(), response(2));
    }

    #[tokio::test]
    async fn consume_stops_when_receiver_disconnects() {
        let (client, receiver) = client(1);
        drop(receiver);
        let mut packets = stream(vec![Ok(response(1)), Ok(response(2))]);
        let result = client.consume(&mut packets).await;
        assert_matches!(result, Err(SessionError::ReceiverDisconnected));
        assert_eq!(packets.next().await.unwrap().unwrap(), response(2));
    }

    fn backoff_client() -> BlockEngineClient {
        let (sender, _receiver) = bounded(1);
        BlockEngineClient::new(
            "http://localhost".into(),
            Keypair::new(),
            sender,
            BlockEngineClientConfig {
                reconnect_backoff: BACKOFF,
                ..BlockEngineClientConfig::default()
            },
        )
    }

    async fn run_sessions(outcomes: Vec<Result<(), SessionError>>) -> Vec<Duration> {
        let client = backoff_client();
        let start = Instant::now();
        let mut outcomes = VecDeque::from(outcomes);
        let mut started = Vec::new();
        client
            .reconnect_loop(|| {
                started.push(start.elapsed());
                let outcome = outcomes.pop_front().expect("loop ran past its script");
                async move { outcome }
            })
            .await;
        started
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_loop_waits_backoff_after_closed_stream_and_errors() {
        let started = run_sessions(vec![
            Err(SessionError::Status(Status::unavailable("down"))),
            Ok(()),
            Err(SessionError::ReceiverDisconnected),
        ])
        .await;
        assert_eq!(started, [Duration::ZERO, BACKOFF, Duration::from_secs(10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_loop_stops_without_backoff_when_receiver_disconnects() {
        let start = Instant::now();
        let started = run_sessions(vec![Err(SessionError::ReceiverDisconnected)]).await;
        assert_eq!(started, [Duration::ZERO]);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_loop_during_backoff() {
        let client = backoff_client();
        let cancel = CancellationToken::new();
        let mut sessions = 0;
        let start = Instant::now();
        let run = cancel
            .clone()
            .run_until_cancelled_owned(client.reconnect_loop(|| {
                sessions += 1;
                async { Err(SessionError::Status(Status::unavailable("down"))) }
            }));
        let cancel_mid_backoff = async {
            time::sleep(HALF_BACKOFF).await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(run, cancel_mid_backoff);
        assert_eq!(result, None);
        assert_eq!(sessions, 1);
        assert_eq!(start.elapsed(), HALF_BACKOFF);
    }
}
