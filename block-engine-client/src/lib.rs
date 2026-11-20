#![cfg(feature = "agave-unstable-api")]

use {
    agave_block_engine_proto::{
        auth::{
            GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
            auth_service_client::AuthServiceClient,
        },
        block_engine::{
            SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
            SubscribePacketsResponse, block_engine_validator_client::BlockEngineValidatorClient,
        },
        bundle::BundleUuid,
        packet::{Meta as ProtoMeta, Packet},
    },
    log::{debug, info, warn},
    shaq::spsc::Producer,
    solana_keypair::Keypair,
    solana_perf::packet::{BytesPacket, BytesPacketBatch, Meta, PacketFlags},
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
        Request, Status,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        transport::{self, Channel, ClientTlsConfig, Endpoint},
    },
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);
const DEFAULT_TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_TCP_KEEPALIVE_RETRIES: u32 = 3;
const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
const SOL_BLOCK_ENGINE_CLIENT_THREAD_NAME: &str = "solBlkEngClient";

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
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            tcp_keepalive: DEFAULT_TCP_KEEPALIVE,
            tcp_keepalive_interval: DEFAULT_TCP_KEEPALIVE_INTERVAL,
            tcp_keepalive_retries: DEFAULT_TCP_KEEPALIVE_RETRIES,
            reconnect_backoff: DEFAULT_RECONNECT_BACKOFF,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockEngineMessage {
    Packets(BytesPacketBatch),
    Bundles(Vec<BlockEngineBundle>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockEngineBundle {
    pub uuid: String,
    pub packets: BytesPacketBatch,
}

impl TryFrom<BundleUuid> for BlockEngineBundle {
    type Error = BundleError;

    fn try_from(bundle: BundleUuid) -> Result<Self, Self::Error> {
        let BundleUuid { bundle, uuid } = bundle;
        let members = bundle.ok_or(BundleError::Missing)?.packets;
        if members.is_empty() {
            return Err(BundleError::Empty);
        }
        let mut packets = BytesPacketBatch::with_capacity(members.len());
        for member in members {
            packets.push(packet_from_proto(member).ok_or(BundleError::InvalidMember)?);
        }
        Ok(Self { uuid, packets })
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum BundleError {
    #[error("bundle is missing")]
    Missing,
    #[error("bundle has no packets")]
    Empty,
    #[error("bundle has a dropped member")]
    InvalidMember,
}

/// Authenticates with a block engine as a validator and forwards the packets
/// and bundles it streams. Reconnects with a fixed backoff after any failure.
pub struct BlockEngineClient {
    url: String,
    identity: Keypair,
    producer: Producer<BlockEngineMessage>,
    config: BlockEngineClientConfig,
}

impl BlockEngineClient {
    pub fn new(
        url: String,
        identity: Keypair,
        producer: Producer<BlockEngineMessage>,
        config: BlockEngineClientConfig,
    ) -> Self {
        Self {
            url,
            identity,
            producer,
            config,
        }
    }

    /// Runs the client on its own thread until `cancel` is cancelled.
    pub fn spawn(mut self, cancel: CancellationToken) -> io::Result<JoinHandle<()>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        thread::Builder::new()
            .name(SOL_BLOCK_ENGINE_CLIENT_THREAD_NAME.into())
            .spawn(move || {
                let run = self.reconnect_loop(async |client: &mut Self| {
                    let channel = connect(&client.url, &client.config).await?;
                    let access_token = authenticate(
                        &mut AuthServiceClient::new(channel.clone()),
                        &client.identity,
                    )
                    .await?;
                    let mut packets_request = Request::new(SubscribePacketsRequest {});
                    packets_request
                        .metadata_mut()
                        .insert("authorization", access_token.clone());
                    let mut bundles_request = Request::new(SubscribeBundlesRequest {});
                    bundles_request
                        .metadata_mut()
                        .insert("authorization", access_token);
                    let mut validator = BlockEngineValidatorClient::new(channel);
                    let packets = validator
                        .subscribe_packets(packets_request)
                        .await?
                        .into_inner();
                    let bundles = validator
                        .subscribe_bundles(bundles_request)
                        .await?
                        .into_inner();
                    info!(
                        "Subscribed to block engine {} packets and bundles",
                        client.url
                    );
                    client.consume(packets, bundles).await
                });
                runtime.block_on(cancel.run_until_cancelled_owned(run));
            })
    }

    /// Runs `session` forever, waiting `reconnect_backoff` after every outcome.
    async fn reconnect_loop(
        &mut self,
        mut session: impl AsyncFnMut(&mut Self) -> Result<(), SessionError>,
    ) {
        loop {
            match session(self).await {
                Ok(()) => info!("Block engine {} closed a stream", self.url),
                Err(err) => warn!("Block engine session with {} failed: {err}", self.url),
            }
            time::sleep(self.config.reconnect_backoff).await;
        }
    }

    /// Forwards packets and bundles until either stream ends (`Ok`) or fails.
    async fn consume(
        &mut self,
        mut packets: impl Stream<Item = Result<SubscribePacketsResponse, Status>> + Unpin,
        mut bundles: impl Stream<Item = Result<SubscribeBundlesResponse, Status>> + Unpin,
    ) -> Result<(), SessionError> {
        loop {
            tokio::select! {
                response = packets.next() => match response {
                    Some(response) => self.forward_packets(response?),
                    None => return Ok(()),
                },
                response = bundles.next() => match response {
                    Some(response) => self.forward_bundles(response?),
                    None => return Ok(()),
                },
            }
        }
    }

    fn forward_packets(&mut self, response: SubscribePacketsResponse) {
        let Some(batch) = response.batch else {
            return;
        };
        let mut packets = BytesPacketBatch::with_capacity(batch.packets.len());
        packets.extend(batch.packets.into_iter().filter_map(packet_from_proto));
        if packets.is_empty() {
            return;
        }
        let len = packets.len();
        if self
            .producer
            .try_write(BlockEngineMessage::Packets(packets))
            .is_err()
        {
            debug!("Dropping {len} block engine packets: receiver is full");
        }
    }

    fn forward_bundles(&mut self, response: SubscribeBundlesResponse) {
        let bundles: Vec<_> = response
            .bundles
            .into_iter()
            .filter_map(|bundle| BlockEngineBundle::try_from(bundle).ok())
            .collect();
        if bundles.is_empty() {
            return;
        }
        let len = bundles.len();
        if self
            .producer
            .try_write(BlockEngineMessage::Bundles(bundles))
            .is_err()
        {
            debug!("Dropping {len} block engine bundles: receiver is full");
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_block_engine_proto::{
            bundle::Bundle,
            packet::{
                Meta as ProtoMeta, PacketBatch as ProtoPacketBatch, PacketFlags as ProtoPacketFlags,
            },
        },
        shaq::spsc::{Consumer, pair},
        solana_perf::packet::bytes::Bytes,
        solana_signature::Signature,
        std::{
            assert_matches,
            collections::VecDeque,
            net::{IpAddr, Ipv4Addr, Ipv6Addr},
        },
        tokio::time::Instant,
        tokio_stream::pending,
        tokio_util::sync::CancellationToken,
    };

    const BACKOFF: Duration = Duration::from_secs(5);
    const HALF_BACKOFF: Duration = Duration::from_millis(2500);

    fn client(capacity: usize) -> (BlockEngineClient, Consumer<BlockEngineMessage>) {
        let (producer, receiver) = pair(capacity).unwrap();
        let client = BlockEngineClient::new(
            "http://localhost".into(),
            Keypair::new(),
            producer,
            BlockEngineClientConfig::default(),
        );
        (client, receiver)
    }

    fn read_packets(receiver: &mut Consumer<BlockEngineMessage>) -> BytesPacketBatch {
        let BlockEngineMessage::Packets(packets) = receiver.try_read().unwrap() else {
            panic!("expected packets");
        };
        packets
    }

    fn read_bundles(receiver: &mut Consumer<BlockEngineMessage>) -> Vec<BlockEngineBundle> {
        let BlockEngineMessage::Bundles(bundles) = receiver.try_read().unwrap() else {
            panic!("expected bundles");
        };
        bundles
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

    fn bundle(uuid: &str, packets: Vec<Packet>) -> BundleUuid {
        BundleUuid {
            bundle: Some(Bundle {
                header: None,
                packets,
            }),
            uuid: uuid.into(),
        }
    }

    fn bundles_response(uuids: &[&str]) -> SubscribeBundlesResponse {
        SubscribeBundlesResponse {
            bundles: uuids
                .iter()
                .map(|uuid| bundle(uuid, vec![valid_packet()]))
                .collect(),
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
        let (mut client, mut receiver) = client(1);
        let mut response = response(2);
        response.batch.as_mut().unwrap().packets.push(Packet {
            data: Bytes::from_static(&[1]),
            meta: None,
        });
        client.forward_packets(response);
        assert_eq!(read_packets(&mut receiver).len(), 2);
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
        let (mut client, mut receiver) = client(1);
        client.forward_packets(response(2));
        assert_eq!(read_packets(&mut receiver).len(), 2);
    }

    #[test]
    fn skips_responses_without_packets() {
        let (mut client, mut receiver) = client(1);
        client.forward_packets(SubscribePacketsResponse {
            header: None,
            batch: None,
        });
        client.forward_packets(response(0));
        assert!(receiver.try_read().is_none());
    }

    #[test]
    fn drops_batches_when_receiver_is_full() {
        let (mut client, mut receiver) = client(1);
        let capacity = receiver.capacity();
        for _ in 0..capacity {
            client.forward_packets(response(1));
        }
        client.forward_packets(response(2));
        for _ in 0..capacity {
            assert_eq!(read_packets(&mut receiver).len(), 1);
        }
        assert!(receiver.try_read().is_none());
    }

    fn stream<T>(items: Vec<Result<T, Status>>) -> impl Stream<Item = Result<T, Status>> + Unpin {
        tokio_stream::iter(items)
    }

    #[tokio::test]
    async fn consume_forwards_packets_until_stream_ends() {
        let (mut client, mut receiver) = client(2);
        let result = client
            .consume(stream(vec![Ok(response(1)), Ok(response(2))]), pending())
            .await;
        assert_matches!(result, Ok(()));
        assert_eq!(read_packets(&mut receiver).len(), 1);
        assert_eq!(read_packets(&mut receiver).len(), 2);
        assert!(receiver.try_read().is_none());
    }

    #[tokio::test]
    async fn consume_stops_at_stream_error() {
        let (mut client, mut receiver) = client(2);
        let mut packets = stream(vec![
            Ok(response(1)),
            Err(Status::internal("stream failed")),
            Ok(response(2)),
        ]);
        let result = client.consume(&mut packets, pending()).await;
        assert_matches!(result, Err(SessionError::Status(_)));
        assert_eq!(read_packets(&mut receiver).len(), 1);
        assert!(receiver.try_read().is_none());

        assert_eq!(packets.next().await.unwrap().unwrap(), response(2));
    }

    #[tokio::test]
    async fn consume_forwards_bundles_until_stream_ends() {
        let (mut client, mut receiver) = client(2);
        let bundles = stream(vec![
            Ok(bundles_response(&["first"])),
            Ok(bundles_response(&["second", "third"])),
        ]);
        let result = client.consume(pending(), bundles).await;
        assert_matches!(result, Ok(()));
        let uuids = |bundles: Vec<BlockEngineBundle>| {
            bundles
                .into_iter()
                .map(|bundle| bundle.uuid)
                .collect::<Vec<_>>()
        };
        assert_eq!(uuids(read_bundles(&mut receiver)), ["first"]);
        assert_eq!(uuids(read_bundles(&mut receiver)), ["second", "third"]);
        assert!(receiver.try_read().is_none());
    }

    #[tokio::test]
    async fn consume_stops_at_bundle_stream_error() {
        let (mut client, mut receiver) = client(2);
        let mut bundles = stream(vec![
            Ok(bundles_response(&["first"])),
            Err(Status::internal("stream failed")),
            Ok(bundles_response(&["second"])),
        ]);
        let result = client.consume(pending(), &mut bundles).await;
        assert_matches!(result, Err(SessionError::Status(_)));
        assert_eq!(read_bundles(&mut receiver).len(), 1);
        assert!(receiver.try_read().is_none());
        assert_eq!(
            bundles.next().await.unwrap().unwrap(),
            bundles_response(&["second"])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn consume_forwards_from_both_streams() {
        let (mut client, mut receiver) = client(4);
        let packets = stream(vec![Ok(response(1)), Ok(response(2))]).chain(pending());
        let bundles = stream(vec![Ok(bundles_response(&["bundle"]))]).chain(pending());
        let result = time::timeout(BACKOFF, client.consume(packets, bundles)).await;
        assert!(result.is_err());
        let (mut packets, mut bundles) = (Vec::new(), Vec::new());
        while let Some(message) = receiver.try_read() {
            match message {
                BlockEngineMessage::Packets(batch) => packets.push(batch.len()),
                BlockEngineMessage::Bundles(received) => bundles.extend(received),
            }
        }
        assert_eq!(packets, [1, 2]);
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].uuid, "bundle");
    }

    #[test]
    fn converts_bundle_members() {
        let bundle = BlockEngineBundle::try_from(bundle(
            "uuid",
            vec![
                valid_packet(),
                packet(&[4, 5, 0], 2, "::1", ProtoPacketFlags::default()),
            ],
        ))
        .unwrap();
        assert_eq!(bundle.uuid, "uuid");
        assert_eq!(
            bundle
                .packets
                .iter()
                .map(|packet| packet.data(..).unwrap())
                .collect::<Vec<_>>(),
            [&[1, 2, 3][..], &[4, 5]]
        );
    }

    #[test]
    fn rejects_bundle_with_discarded_packet() {
        let discard = ProtoPacketFlags {
            discard: true,
            ..ProtoPacketFlags::default()
        };
        let bundle = bundle(
            "discarded",
            vec![valid_packet(), packet(&[1], 1, "", discard)],
        );
        assert_eq!(
            BlockEngineBundle::try_from(bundle),
            Err(BundleError::InvalidMember)
        );
    }

    #[test]
    fn rejects_bundle_with_malformed_packet() {
        let bundle = bundle(
            "malformed",
            vec![
                valid_packet(),
                packet(&[1], 2, "", ProtoPacketFlags::default()),
            ],
        );
        assert_eq!(
            BlockEngineBundle::try_from(bundle),
            Err(BundleError::InvalidMember)
        );
    }

    #[test]
    fn rejects_bundle_without_packets() {
        assert_eq!(
            BlockEngineBundle::try_from(bundle("empty", vec![])),
            Err(BundleError::Empty)
        );
    }

    #[test]
    fn rejects_missing_bundle() {
        let bundle = BundleUuid {
            bundle: None,
            uuid: "missing".into(),
        };
        assert_eq!(
            BlockEngineBundle::try_from(bundle),
            Err(BundleError::Missing)
        );
    }

    #[test]
    fn forwards_only_complete_bundles() {
        let (mut client, mut receiver) = client(1);
        client.forward_bundles(SubscribeBundlesResponse {
            bundles: vec![
                bundle("complete", vec![valid_packet()]),
                bundle("empty", vec![]),
            ],
        });
        let bundles = read_bundles(&mut receiver);
        assert_eq!(
            bundles
                .iter()
                .map(|bundle| bundle.uuid.as_str())
                .collect::<Vec<_>>(),
            ["complete"]
        );

        client.forward_bundles(SubscribeBundlesResponse {
            bundles: vec![bundle("empty", vec![])],
        });
        assert!(receiver.try_read().is_none());
    }

    #[test]
    fn drops_bundles_when_receiver_is_full() {
        let (mut client, mut receiver) = client(1);
        let capacity = receiver.capacity();
        for _ in 0..capacity {
            client.forward_bundles(bundles_response(&["kept"]));
        }
        client.forward_bundles(bundles_response(&["dropped"]));
        for _ in 0..capacity {
            assert_eq!(read_bundles(&mut receiver)[0].uuid, "kept");
        }
        assert!(receiver.try_read().is_none());
    }

    fn backoff_client() -> BlockEngineClient {
        let (producer, _receiver) = pair(1).unwrap();
        BlockEngineClient::new(
            "http://localhost".into(),
            Keypair::new(),
            producer,
            BlockEngineClientConfig {
                reconnect_backoff: BACKOFF,
                ..BlockEngineClientConfig::default()
            },
        )
    }

    async fn run_sessions(outcomes: Vec<Result<(), SessionError>>) -> Vec<Duration> {
        let mut client = backoff_client();
        let cancel = CancellationToken::new();
        let start = Instant::now();
        let mut outcomes = VecDeque::from(outcomes);
        let mut started = Vec::new();
        cancel
            .clone()
            .run_until_cancelled_owned(client.reconnect_loop(|_: &mut BlockEngineClient| {
                started.push(start.elapsed());
                let outcome = outcomes.pop_front().expect("loop ran past its script");
                if outcomes.is_empty() {
                    cancel.cancel();
                }
                async move { outcome }
            }))
            .await;
        started
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_loop_waits_backoff_after_closed_stream_and_errors() {
        let started = run_sessions(vec![
            Err(SessionError::Status(Status::unavailable("down"))),
            Ok(()),
            Err(SessionError::Status(Status::unavailable("down"))),
        ])
        .await;
        assert_eq!(started, [Duration::ZERO, BACKOFF, Duration::from_secs(10)]);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_loop_during_backoff() {
        let mut client = backoff_client();
        let cancel = CancellationToken::new();
        let mut sessions = 0;
        let start = Instant::now();
        let run = cancel
            .clone()
            .run_until_cancelled_owned(client.reconnect_loop(|_: &mut BlockEngineClient| {
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
