#![cfg(feature = "agave-unstable-api")]
#![cfg(unix)]

use {
    crate::{
        block_engine_verifier::VerifiedMessage, check_response::TransactionState,
        in_flight_tracker::InFlightTracker, leader_slot_metrics::LeaderSlotMetrics,
        progress_tracker::SchedulerState, schedule::SchedulerScratch,
        scheduling_details::SchedulingDetails, transaction_container::TransactionContainer,
    },
    agave_block_engine_client::{BlockEngineClient, BlockEngineClientConfig},
    agave_reserved_account_keys::ReservedAccountKeys,
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, PackToCheckWorkerMessage, ProgressMessage, TpuToPackMessage,
    },
    agave_scheduler_handshake::{
        ClientCheckWorkerSession, ClientHandshakeError, ClientSession, ClientWorkerSession, client,
    },
    agave_scheduling_utils::{
        cost_pacer::CostPacer, thread_aware_account_locks::ThreadAwareAccountLocks,
    },
    core::{
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    },
    crossbeam_channel::Receiver,
    log::{debug, info, warn},
    rts_alloc::Allocator,
    solana_clock::Slot,
    solana_keypair::{Keypair, read_keypair_file},
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::{
        collections::HashSet,
        io::{self, ErrorKind},
        path::PathBuf,
        thread::{self, JoinHandle},
        time::Instant,
    },
    thiserror::Error,
    tokio_util::sync::CancellationToken,
};

const CONNECTION_RETRY_INTERVAL: Duration = Duration::from_millis(100);

mod block_engine_verifier;
mod check_response;
mod config;
mod execution_response;
mod in_flight_tracker;
mod leader_slot_metrics;
mod progress_tracker;
mod resolved_transaction;
mod schedule;
mod scheduling_details;
mod tpu_ingress;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "additional container operations are used by upcoming execution handling"
    )
)]
mod transaction_container;

#[cfg(test)]
mod tests;

pub use config::{BlockEngineConfig, Config, SchedulerConfig, SessionConfig};

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Handshake(#[from] ClientHandshakeError),
    #[error("failed to read block engine keypair {}: {err}", path.display())]
    BlockEngineKeypair { path: PathBuf, err: String },
    #[error("failed to spawn block engine client: {0}")]
    BlockEngineSpawn(io::Error),
}

struct Scheduler {
    state: SchedulerState,
    leader_slot_metrics: LeaderSlotMetrics,
    scheduling_details: SchedulingDetails,
    scheduling_slot: Option<Slot>,
    cost_pacer: Option<CostPacer>,
    /// Estimated CUs dispatched in the current scheduling slot, used for pacing.
    scheduled_cost: u64,
    /// Transactions held out of the priority queue until the next scheduling slot.
    deferred_execution: Vec<usize>,
    execution_margin: Duration,
    allocator: Allocator,
    tpu_receiver: shaq::spsc::Consumer<TpuToPackMessage>,
    progress_receiver: shaq::spsc::Consumer<ProgressMessage>,
    check_sender: shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    outstanding_check_packets: usize,
    outstanding_check_batches: usize,
    /// Minimum response queue capacity requested during the handshake.
    check_response_capacity: usize,
    transactions: TransactionContainer<TransactionState>,
    scheduling_scratch: SchedulerScratch,
    filter_keys: HashSet<Pubkey, PubkeyHasherBuilder>,
    reserved_account_keys: HashSet<Pubkey, PubkeyHasherBuilder>,
    check_receiver: shaq::mpmc::Consumer<CheckWorkerToPackMessage>,
    workers: Vec<ClientWorkerSession>,
    in_flight: InFlightTracker,
    account_locks: ThreadAwareAccountLocks,
    max_cost_units_per_worker: u64,
    max_cost_units_per_batch: u64,
    target_entry_bytes_per_batch: u64,
    #[expect(
        dead_code,
        reason = "block engine messages are consumed by upcoming ingress handling"
    )]
    block_engine: Receiver<VerifiedMessage>,
}

impl Scheduler {
    fn new(
        session: ClientSession,
        check_response_capacity: usize,
        filter_keys: HashSet<Pubkey, PubkeyHasherBuilder>,
        transaction_state_capacity: usize,
        execution_margin: Duration,
        max_cost_units_per_worker: u64,
        max_cost_units_per_batch: u64,
        target_entry_bytes_per_batch: u64,
        block_engine: Receiver<VerifiedMessage>,
    ) -> Result<Self, ClientHandshakeError> {
        let ClientSession {
            allocator,
            tpu_to_pack,
            progress_tracker,
            workers,
            check_workers,
            ..
        } = session;
        let ClientCheckWorkerSession {
            pack_to_check_worker,
            check_worker_to_pack,
        } = check_workers.ok_or(ClientHandshakeError::ProtocolViolation)?;

        Ok(Self {
            state: SchedulerState::new(),
            leader_slot_metrics: LeaderSlotMetrics::default(),
            scheduling_details: SchedulingDetails::default(),
            scheduling_slot: None,
            cost_pacer: None,
            scheduled_cost: 0,
            deferred_execution: Vec::with_capacity(transaction_state_capacity),
            execution_margin,
            allocator,
            tpu_receiver: tpu_to_pack,
            progress_receiver: progress_tracker,
            check_sender: pack_to_check_worker,
            outstanding_check_packets: 0,
            outstanding_check_batches: 0,
            check_response_capacity,
            transactions: TransactionContainer::with_capacity(transaction_state_capacity),
            scheduling_scratch: SchedulerScratch::new(transaction_state_capacity),
            filter_keys,
            reserved_account_keys: ReservedAccountKeys::default().active.into_iter().collect(),
            check_receiver: check_worker_to_pack,
            in_flight: InFlightTracker::new(workers.len()),
            account_locks: ThreadAwareAccountLocks::new(workers.len()),
            max_cost_units_per_worker,
            max_cost_units_per_batch,
            target_entry_bytes_per_batch,
            workers,
            block_engine,
        })
    }

    fn run_iteration(&mut self) {
        self.handle_leader_progress();
        self.handle_execution_worker_responses();
        self.handle_leader_slot_metrics();
        self.handle_slot_change();
        self.handle_check_worker_responses();
        self.handle_tpu_ingress();
        let budget = self.pacing_budget(Instant::now(), self.scheduled_cost);
        self.schedule(budget);
        self.scheduling_details.maybe_report();
        core::hint::spin_loop();
    }

    fn handle_leader_progress(&mut self) {
        self.state.drain_progress(&mut self.progress_receiver);
    }

    fn handle_slot_change(&mut self) {
        let SchedulerState::LeaderReady {
            slot,
            remaining_cost_units,
            slot_start,
            slot_duration,
        } = self.state
        else {
            return;
        };
        // Seed the slot once. Later snapshots can include our own reservations and must not
        // replace the initial pacing budget.
        if self.scheduling_slot == Some(slot) || !self.in_flight.is_empty() {
            return;
        }

        self.scheduling_slot = Some(slot);
        self.scheduled_cost = 0;
        for id in self.deferred_execution.drain(..) {
            self.transactions.requeue(id);
        }
        self.cost_pacer = Some(CostPacer::new(
            remaining_cost_units,
            slot_start,
            slot_duration.saturating_sub(self.execution_margin),
        ));
    }

    fn pacing_budget(&self, now: Instant, consumed_cost: u64) -> u64 {
        if !matches!(self.state, SchedulerState::LeaderReady { slot, .. } if self.scheduling_slot == Some(slot))
        {
            return 0;
        }
        self.cost_pacer
            .as_ref()
            .map_or(0, |pacer| pacer.available_budget(now, consumed_cost))
    }
}

/// Connects to Agave and runs the scheduler loop on the calling thread until exit is set.
///
/// Missing or refused connections are retried until Agave is available or exit is set.
/// Other errors are returned to the caller. Shared resources remain alive until the loop exits.
/// The loop receives leader progress, checks and schedules transactions, and handles completion.
///
/// The exit flag cannot interrupt an in-progress handshake. The timeout has the syscall-level
/// semantics of [`client::connect`], rather than imposing a deadline on the entire handshake.
pub fn run(mut config: Config, exit: &AtomicBool) -> Result<(), SchedulerError> {
    let block_engine = load_block_engine(config.block_engine.take())?;

    info!("Waiting for Agave at {}", config.ipc_path.display());
    while !exit.load(Ordering::Relaxed) {
        if let Some(session) = try_connect(&config)? {
            info!("Connected to Agave at {}", config.ipc_path.display());
            let (receiver, client) = spawn_block_engine(block_engine)?;
            let result = run_scheduler(
                session,
                config.scheduler,
                config.session.check_worker_to_pack_capacity,
                receiver,
                exit,
            );
            client.stop();
            return result.map_err(Into::into);
        }
        thread::sleep(CONNECTION_RETRY_INTERVAL);
    }
    info!("Scheduler stopped while waiting for Agave");
    Ok(())
}

/// Reads the block engine identity up front so a bad keypair fails at startup.
fn load_block_engine(
    config: Option<BlockEngineConfig>,
) -> Result<Option<(BlockEngineConfig, Keypair)>, SchedulerError> {
    config
        .map(|config| {
            let identity = read_keypair_file(&config.keypair_path).map_err(|err| {
                SchedulerError::BlockEngineKeypair {
                    path: config.keypair_path.clone(),
                    err: err.to_string(),
                }
            })?;
            Ok((config, identity))
        })
        .transpose()
}

/// Spawns the block engine client and verifier, if configured, returning the receiver the
/// scheduler drains and the handle that stops both. Without a block engine the receiver never
/// yields.
fn spawn_block_engine(
    block_engine: Option<(BlockEngineConfig, Keypair)>,
) -> Result<(Receiver<VerifiedMessage>, BlockEngineHandle), SchedulerError> {
    let Some((config, identity)) = block_engine else {
        return Ok((crossbeam_channel::never(), BlockEngineHandle::Off));
    };
    let (client_sender, verifier_receiver) =
        crossbeam_channel::bounded(config.block_engine_to_pack_capacity);
    let (verifier_sender, receiver) =
        crossbeam_channel::bounded(config.block_engine_to_pack_capacity);
    let verifier = block_engine_verifier::spawn_verifier(verifier_receiver, verifier_sender)
        .map_err(SchedulerError::BlockEngineSpawn)?;
    let cancel = CancellationToken::new();
    let client = BlockEngineClient::new(
        config.url,
        identity,
        client_sender,
        BlockEngineClientConfig::default(),
    )
    .spawn(cancel.clone())
    .map_err(SchedulerError::BlockEngineSpawn)?;
    Ok((
        receiver,
        BlockEngineHandle::Running {
            cancel,
            client,
            verifier,
        },
    ))
}

#[derive(Debug)]
enum BlockEngineHandle {
    Off,
    Running {
        cancel: CancellationToken,
        client: JoinHandle<()>,
        verifier: JoinHandle<()>,
    },
}

impl BlockEngineHandle {
    fn stop(self) {
        let Self::Running {
            cancel,
            client,
            verifier,
        } = self
        else {
            return;
        };
        cancel.cancel();
        if client.join().is_err() {
            warn!("Block engine client thread panicked");
        }
        if verifier.join().is_err() {
            warn!("Block engine verifier thread panicked");
        }
    }
}

/// Runs the scheduler on the calling thread using an established local or external session.
pub fn run_session(
    session: ClientSession,
    config: SchedulerConfig,
    check_response_capacity: usize,
    exit: &AtomicBool,
) -> Result<(), ClientHandshakeError> {
    run_scheduler(
        session,
        config,
        check_response_capacity,
        crossbeam_channel::never(),
        exit,
    )
}

fn run_scheduler(
    session: ClientSession,
    config: SchedulerConfig,
    check_response_capacity: usize,
    block_engine: Receiver<VerifiedMessage>,
    exit: &AtomicBool,
) -> Result<(), ClientHandshakeError> {
    let worker_count = session.workers.len();
    let mut scheduler = Scheduler::new(
        session,
        check_response_capacity,
        config.filter_keys,
        config.transaction_state_capacity,
        config.execution_margin,
        config.max_cost_units_per_worker,
        config.max_cost_units_per_batch,
        config.target_entry_bytes_per_batch,
        block_engine,
    )?;

    info!("Scheduler running with {worker_count} execution workers");
    while !exit.load(Ordering::Relaxed) {
        scheduler.run_iteration();
    }
    scheduler.leader_slot_metrics.report();
    info!("Scheduler stopped");
    Ok(())
}

/// Returns `None` when Agave is not yet listening.
fn try_connect(config: &Config) -> Result<Option<ClientSession>, ClientHandshakeError> {
    // Resolve on every attempt: Agave replaces the ledger symlink when it starts.
    let session = config
        .ipc_path
        .canonicalize()
        .map_err(ClientHandshakeError::from)
        .and_then(|path| {
            client::connect(
                path,
                config.session.client_logon(),
                config.session.handshake_timeout,
            )
        });
    match session {
        Ok(session) => Ok(Some(session)),
        Err(ClientHandshakeError::Io(error))
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::ConnectionRefused
            ) =>
        {
            debug!("Agave is not available yet: {error}");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
