use {
    super::*,
    agave_block_engine_proto::{
        auth::{
            GenerateAuthChallengeResponse, GenerateAuthTokensResponse, RefreshAccessTokenRequest,
            RefreshAccessTokenResponse,
            auth_service_server::{AuthService, AuthServiceServer},
        },
        block_engine::{
            BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, GetBlockEngineEndpointRequest,
            GetBlockEngineEndpointResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
            block_engine_validator_server::{BlockEngineValidator, BlockEngineValidatorServer},
        },
        packet::{
            Meta as ProtoMeta, PacketBatch as ProtoPacketBatch, PacketFlags as ProtoPacketFlags,
        },
    },
    crossbeam_channel::{Receiver, bounded},
    hyper_util::{
        rt::{TokioExecutor, TokioIo},
        server::conn::auto,
        service::TowerToHyperService,
    },
    solana_perf::packet::bytes::Bytes,
    solana_signature::Signature,
    std::{
        collections::HashSet,
        convert::Infallible,
        net::{IpAddr, Ipv4Addr, TcpListener},
        sync::{Mutex, atomic::AtomicUsize},
        time::Instant,
    },
    tonic::{
        Response,
        codegen::{
            Body, BoxFuture, BoxStream, Context, Poll, Service, StdError, http,
            tokio_stream::{self, StreamExt},
        },
    },
};

const CHALLENGE: &str = "challenge";
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum PacketStream {
    KeepOpen,
    Close,
    Repeat(Duration),
}

struct MockBlockEngine {
    token_ttl: Duration,
    packet_stream: PacketStream,
    batches: Vec<ProtoPacketBatch>,
    stall_auth: bool,
    fail_token_requests_from: Option<usize>,
    issued_tokens: Mutex<HashSet<String>>,
    challenge_requests: AtomicUsize,
    token_requests: AtomicUsize,
    subscription_tokens: Mutex<Vec<String>>,
}

impl Default for MockBlockEngine {
    fn default() -> Self {
        Self {
            token_ttl: Duration::from_secs(3600),
            packet_stream: PacketStream::KeepOpen,
            batches: Vec::new(),
            stall_auth: false,
            fail_token_requests_from: None,
            issued_tokens: Mutex::default(),
            challenge_requests: AtomicUsize::default(),
            token_requests: AtomicUsize::default(),
            subscription_tokens: Mutex::default(),
        }
    }
}

impl MockBlockEngine {
    fn challenge_requests(&self) -> usize {
        self.challenge_requests.load(Ordering::Relaxed)
    }

    fn token_requests(&self) -> usize {
        self.token_requests.load(Ordering::Relaxed)
    }

    fn subscription_tokens(&self) -> Vec<String> {
        self.subscription_tokens.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl AuthService for MockBlockEngine {
    async fn generate_auth_challenge(
        &self,
        request: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        self.challenge_requests.fetch_add(1, Ordering::Relaxed);
        if self.stall_auth {
            return std::future::pending().await;
        }
        let request = request.into_inner();
        if request.role != i32::from(Role::Validator) || request.pubkey.len() != 32 {
            return Err(Status::invalid_argument("bad challenge request"));
        }
        Ok(Response::new(GenerateAuthChallengeResponse {
            challenge: CHALLENGE.into(),
        }))
    }

    async fn generate_auth_tokens(
        &self,
        request: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let index = self.token_requests.fetch_add(1, Ordering::Relaxed);
        if self
            .fail_token_requests_from
            .is_some_and(|from| index >= from)
        {
            return Err(Status::unavailable("token requests are failing"));
        }
        let request = request.into_inner();
        let pubkey = Pubkey::try_from(request.client_pubkey.as_slice())
            .map_err(|_| Status::invalid_argument("bad pubkey"))?;
        let signature = Signature::try_from(request.signed_challenge.as_slice())
            .map_err(|_| Status::invalid_argument("bad signature"))?;
        if request.challenge != challenge_message(&pubkey, CHALLENGE)
            || !signature.verify(pubkey.as_ref(), request.challenge.as_bytes())
        {
            return Err(Status::unauthenticated("challenge not signed by pubkey"));
        }
        let value = format!("token-{index}");
        self.issued_tokens.lock().unwrap().insert(value.clone());
        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(Token {
                value,
                expires_at_utc: SystemTime::now()
                    .checked_add(self.token_ttl)
                    .map(Into::into),
            }),
            refresh_token: None,
        }))
    }

    async fn refresh_access_token(
        &self,
        _request: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }
}

fn packets_response(batch: ProtoPacketBatch) -> Result<SubscribePacketsResponse, Status> {
    Ok(SubscribePacketsResponse {
        header: None,
        batch: Some(batch),
    })
}

#[tonic::async_trait]
impl BlockEngineValidator for MockBlockEngine {
    type SubscribePacketsStream = BoxStream<SubscribePacketsResponse>;
    type SubscribeBundlesStream = BoxStream<SubscribeBundlesResponse>;

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        if !self.issued_tokens.lock().unwrap().contains(token) {
            return Err(Status::unauthenticated("unknown token"));
        }
        self.subscription_tokens
            .lock()
            .unwrap()
            .push(token.to_owned());
        let batches = tokio_stream::iter(self.batches.clone());
        let stream: Self::SubscribePacketsStream = match self.packet_stream {
            PacketStream::KeepOpen => {
                Box::pin(batches.map(packets_response).chain(tokio_stream::pending()))
            }
            PacketStream::Close => Box::pin(batches.map(packets_response)),
            PacketStream::Repeat(interval) => Box::pin(
                tokio_stream::iter(self.batches.clone().into_iter().cycle()).then(
                    move |batch| async move {
                        time::sleep(interval).await;
                        packets_response(batch)
                    },
                ),
            ),
        };
        Ok(Response::new(stream))
    }

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }

    async fn get_block_engine_endpoints(
        &self,
        _request: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        Err(Status::unimplemented("not used by the client"))
    }
}

#[derive(Clone)]
struct MockRoutes {
    auth: AuthServiceServer<MockBlockEngine>,
    validator: BlockEngineValidatorServer<MockBlockEngine>,
}

impl<B> Service<http::Request<B>> for MockRoutes
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path().starts_with("/auth.AuthService/") {
            self.auth.call(request)
        } else {
            self.validator.call(request)
        }
    }
}

fn serve(mock: Arc<MockBlockEngine>) -> String {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let routes = MockRoutes {
                auth: AuthServiceServer::from_arc(Arc::clone(&mock)),
                validator: BlockEngineValidatorServer::from_arc(mock),
            };
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service = TowerToHyperService::new(routes.clone());
                tokio::spawn(async move {
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .http2_only()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    });
    url
}

struct RunningClient {
    receiver: Receiver<PacketBatch>,
    exit: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

fn start(mock: &Arc<MockBlockEngine>) -> RunningClient {
    let url = serve(Arc::clone(mock));
    let (sender, receiver) = bounded(16);
    let exit = Arc::new(AtomicBool::new(false));
    let handle = BlockEngineClient::new(url, Keypair::new(), sender)
        .spawn(Arc::clone(&exit))
        .unwrap();
    RunningClient {
        receiver,
        exit,
        handle,
    }
}

impl RunningClient {
    fn recv(&self) -> PacketBatch {
        self.receiver.recv_timeout(TIMEOUT).unwrap()
    }

    fn stop(self) {
        self.exit.store(true, Ordering::Relaxed);
        wait_until(|| self.handle.is_finished());
        self.handle.join().unwrap();
    }
}

fn wait_until(condition: impl Fn() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < TIMEOUT,
            "condition not met in {TIMEOUT:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn batch(packets: Vec<Packet>) -> ProtoPacketBatch {
    ProtoPacketBatch { packets }
}

fn packet(data: &[u8], size: u64, flags: ProtoPacketFlags) -> Packet {
    Packet {
        data: Bytes::copy_from_slice(data),
        meta: Some(ProtoMeta {
            size,
            addr: "10.0.0.1".into(),
            port: 8001,
            flags: Some(flags),
            sender_stake: 0,
        }),
    }
}

fn single_packet_batch() -> ProtoPacketBatch {
    batch(vec![packet(&[1], 1, ProtoPacketFlags::default())])
}

/// Drops packets received so far, then waits for a new batch.
fn recv_fresh(client: &RunningClient) {
    while client.receiver.try_recv().is_ok() {}
    assert_eq!(client.recv().len(), 1);
}

#[test]
fn authenticates_and_forwards_packets() {
    let discard = ProtoPacketFlags {
        discard: true,
        ..ProtoPacketFlags::default()
    };
    let forwarded = ProtoPacketFlags {
        forwarded: true,
        ..ProtoPacketFlags::default()
    };
    let mock = Arc::new(MockBlockEngine {
        batches: vec![batch(vec![
            packet(&[1, 2, 3, 0], 3, forwarded),
            packet(&[4], 1, discard),
        ])],
        ..MockBlockEngine::default()
    });
    let client = start(&mock);

    let mut meta = Meta::default();
    meta.size = 3;
    meta.addr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    meta.port = 8001;
    meta.flags.set(PacketFlags::FORWARDED, true);
    assert_eq!(
        client.recv(),
        PacketBatch::from(vec![BytesPacket::new(Bytes::from_static(&[1, 2, 3]), meta)])
    );
    assert_eq!(mock.token_requests(), 1);
    assert_eq!(mock.subscription_tokens(), ["token-0"]);
    client.stop();
}

#[test]
fn resubscribes_with_new_token_before_expiry() {
    let mock = Arc::new(MockBlockEngine {
        token_ttl: Duration::from_secs(2),
        packet_stream: PacketStream::Repeat(Duration::from_millis(50)),
        batches: vec![single_packet_batch()],
        ..MockBlockEngine::default()
    });
    let client = start(&mock);

    wait_until(|| mock.subscription_tokens().len() == 2);
    assert_eq!(mock.subscription_tokens(), ["token-0", "token-1"]);
    recv_fresh(&client);
    client.stop();
}

#[test]
fn keeps_stream_when_refresh_fails() {
    let mock = Arc::new(MockBlockEngine {
        token_ttl: Duration::from_secs(2),
        packet_stream: PacketStream::Repeat(Duration::from_millis(50)),
        batches: vec![single_packet_batch()],
        fail_token_requests_from: Some(1),
        ..MockBlockEngine::default()
    });
    let client = start(&mock);

    wait_until(|| mock.token_requests() >= 3);
    assert_eq!(mock.subscription_tokens(), ["token-0"]);
    recv_fresh(&client);
    client.stop();
}

#[test]
fn reconnects_after_stream_closes() {
    let mock = Arc::new(MockBlockEngine {
        packet_stream: PacketStream::Close,
        batches: vec![single_packet_batch()],
        ..MockBlockEngine::default()
    });
    let client = start(&mock);

    assert_eq!(client.recv().len(), 1);
    assert_eq!(client.recv().len(), 1);
    assert!(mock.subscription_tokens().len() >= 2);
    client.stop();
}

#[test]
fn retries_when_auth_times_out() {
    let mock = Arc::new(MockBlockEngine {
        stall_auth: true,
        ..MockBlockEngine::default()
    });
    let client = start(&mock);

    wait_until(|| mock.challenge_requests() >= 2);
    client.stop();
}

#[test]
fn stops_on_exit_while_streaming() {
    let mock = Arc::new(MockBlockEngine::default());
    let client = start(&mock);

    wait_until(|| mock.subscription_tokens().len() == 1);
    client.stop();
}
