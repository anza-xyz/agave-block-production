use {
    crate::{
        config::SchedulerConfig,
        messages::{
            BlockVerificationOutcome, BlockVerificationToReplayMessage, VerificationFailureReason,
            replay_to_scheduler::{
                AbortMessage, AllEntriesSubmittedMessage, BeginMessage, EntryMessage,
                ReplayToBlockVerificationMessage,
            },
            worker::{
                EntryHashVerificationRequest, EntryHashVerificationResponse,
                SignatureVerificationRequest, SignatureVerificationResponse, TransactionCheckError,
                TransactionCheckRequest, TransactionCheckResponse, TransactionExecutionError,
                TransactionExecutionRequest, TransactionExecutionResponse,
            },
        },
        stage::BlockVerificationStage,
        utils::ShutdownOnDrop,
        workers::{
            VerificationQueue, VerificationWorkers, entry_hash_verification,
            signature_verification, transaction_check, transaction_execution,
        },
    },
    crossbeam_channel::{Receiver, RecvError, Sender, TrySendError, bounded, never, select},
    solana_accounts_db::bank_id::BankId,
    solana_clock::Slot,
    solana_entry::entry::EntryVerificationData,
    solana_hash::Hash,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::runtime_transaction::ReplayTransaction,
    std::{
        collections::VecDeque,
        iter, mem,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

type EntryHashVerificationQueue =
    VerificationQueue<EntryHashVerificationRequest, EntryHashVerificationResponse>;
type SignatureVerificationQueue =
    VerificationQueue<SignatureVerificationRequest, SignatureVerificationResponse>;
type TransactionCheckQueue = VerificationQueue<TransactionCheckRequest, TransactionCheckResponse>;
type TransactionExecutionQueue =
    VerificationQueue<TransactionExecutionRequest, TransactionExecutionResponse>;

/// The timeout used on waiting for messages before checking for a timeout
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(thiserror::Error, Debug)]
pub enum SchedulerExitReason {
    #[error("the block verification response receiver was dropped")]
    DisconnectedResponseChannel,
    #[error("the block verification request sender was dropped")]
    DisconnectedRequestChannel,
    #[error("every entry hash verification worker exited")]
    EntryHashVerificationWorkersExited,
    #[error("every signature verification worker exited")]
    SignatureVerificationWorkersExited,
    #[error("every transaction check worker exited")]
    TransactionCheckWorkersExited,
    #[error("every transaction execution worker exited")]
    TransactionExecutionWorkersExited,
}

impl From<DisconnectedResponseChannel> for SchedulerExitReason {
    fn from(_: DisconnectedResponseChannel) -> Self {
        SchedulerExitReason::DisconnectedResponseChannel
    }
}

pub(super) struct BlockVerificationScheduler {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    replay_message_sender: Sender<BlockVerificationToReplayMessage>,

    entry_hash_verification_queue: EntryHashVerificationQueue,
    signature_verification_queue: SignatureVerificationQueue,
    transaction_check_queue: TransactionCheckQueue,
    transaction_execution_queue: TransactionExecutionQueue,

    shutdown_signal: Arc<AtomicBool>,
    workers_shutdown_signal: ShutdownOnDrop,

    blocks_in_progress: Vec<BlockVerificationState>,

    entry_hash_verification_workers: VerificationWorkers,
    signature_verification_workers: VerificationWorkers,
    transaction_check_workers: VerificationWorkers,
    transaction_execution_workers: VerificationWorkers,

    buffered_replay_response_messages: VecDeque<BlockVerificationToReplayMessage>,
    buffered_signature_verification_requests: VecDeque<SignatureVerificationRequest>,
    buffered_transaction_check_requests: VecDeque<TransactionCheckRequest>,
}

impl BlockVerificationScheduler {
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        shutdown_signal: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> BlockVerificationStage {
        let (
            entry_hash_verification_queue,
            entry_hash_verification_request_consumer,
            entry_hash_verification_response_sender,
        ) = EntryHashVerificationQueue::new(
            scheduler_config.max_entry_hash_verifications_in_flight,
        );
        let (
            signature_verification_queue,
            signature_verification_request_consumer,
            signature_verification_response_sender,
        ) = SignatureVerificationQueue::new(scheduler_config.max_signature_verifications_in_flight);
        let (
            transaction_check_queue,
            transaction_check_request_consumer,
            transaction_check_response_sender,
        ) = TransactionCheckQueue::new(scheduler_config.max_transaction_checks_in_flight);
        let (
            transaction_execution_queue,
            transaction_execution_request_consumer,
            transaction_execution_response_sender,
        ) = TransactionExecutionQueue::new(scheduler_config.max_transaction_executions_in_flight);

        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);
        let block_verification_to_replay =
            bounded(scheduler_config.block_verification_to_replay_channel_size);

        let workers_shutdown_signal = ShutdownOnDrop::from(Arc::new(AtomicBool::new(false)));

        let entry_hash_verification_workers = VerificationWorkers::spawn(
            entry_hash_verification::THREAD_NAME_PREFIX,
            workers_shutdown_signal.signal().clone(),
            scheduler_config.num_entry_hash_workers,
            entry_hash_verification_request_consumer,
            entry_hash_verification_response_sender,
            entry_hash_verification::verify_entry_hash,
        );
        let signature_verification_workers = VerificationWorkers::spawn(
            signature_verification::THREAD_NAME_PREFIX,
            workers_shutdown_signal.signal().clone(),
            scheduler_config.num_signature_workers,
            signature_verification_request_consumer,
            signature_verification_response_sender,
            signature_verification::verify_signatures,
        );
        let transaction_check_workers = VerificationWorkers::spawn(
            transaction_check::THREAD_NAME_PREFIX,
            workers_shutdown_signal.signal().clone(),
            scheduler_config.num_transaction_check_workers,
            transaction_check_request_consumer,
            transaction_check_response_sender,
            {
                let bank_forks = bank_forks.clone();
                move |request| transaction_check::check_transaction(&bank_forks, request)
            },
        );
        let transaction_execution_workers = VerificationWorkers::spawn(
            transaction_execution::THREAD_NAME_PREFIX,
            workers_shutdown_signal.signal().clone(),
            scheduler_config.num_transaction_execution_workers,
            transaction_execution_request_consumer,
            transaction_execution_response_sender,
            move |request| transaction_execution::execute_transaction(&bank_forks, request),
        );

        let scheduler = Self::new(
            replay_to_block_verification.1,
            block_verification_to_replay.0,
            entry_hash_verification_queue,
            signature_verification_queue,
            transaction_check_queue,
            transaction_execution_queue,
            shutdown_signal,
            workers_shutdown_signal,
            entry_hash_verification_workers,
            signature_verification_workers,
            transaction_check_workers,
            transaction_execution_workers,
        );

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            block_verification_to_replay.1,
            scheduler_thread_join_handle,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn new(
        replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
        replay_message_sender: Sender<BlockVerificationToReplayMessage>,
        entry_hash_verification_queue: EntryHashVerificationQueue,
        signature_verification_queue: SignatureVerificationQueue,
        transaction_check_queue: TransactionCheckQueue,
        transaction_execution_queue: TransactionExecutionQueue,
        shutdown_signal: Arc<AtomicBool>,
        workers_shutdown_signal: ShutdownOnDrop,
        entry_hash_verification_workers: VerificationWorkers,
        signature_verification_workers: VerificationWorkers,
        transaction_check_workers: VerificationWorkers,
        transaction_execution_workers: VerificationWorkers,
    ) -> Self {
        Self {
            replay_message_receiver,
            replay_message_sender,
            entry_hash_verification_queue,
            signature_verification_queue,
            transaction_check_queue,
            transaction_execution_queue,
            shutdown_signal,
            workers_shutdown_signal,
            blocks_in_progress: Vec::new(),
            buffered_replay_response_messages: VecDeque::new(),
            buffered_signature_verification_requests: VecDeque::new(),
            buffered_transaction_check_requests: VecDeque::new(),
            entry_hash_verification_workers,
            signature_verification_workers,
            transaction_check_workers,
            transaction_execution_workers,
        }
    }

    /// Runs until shutdown is requested, or any of the communications channels disconnect.
    ///
    /// Blocks still in progress are dropped on exit without an outcome being sent for them.
    /// Dropping the scheduler drops its outcome sender, disconnecting the stage's receiver.
    fn run_scheduler_event_loop(mut self) -> Result<(), SchedulerExitReason> {
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            self.try_flush_completed_blocks()?;
            self.try_flush_buffered_signature_verification_requests();
            self.try_flush_buffered_transaction_check_requests();

            let replay_message_receiver = if self.workers_can_accept_request() {
                &self.replay_message_receiver
            }
            // back-pressure replay until workers can accept work
            else {
                &never()
            };

            select! {
                recv(self.entry_hash_verification_queue.response_receiver()) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::EntryHashVerificationWorkersExited)?;
                    let () = self.handle_entry_hash_verification_response(response);
                }
                recv(self.signature_verification_queue.response_receiver()) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::SignatureVerificationWorkersExited)?;
                    let () = self.handle_signature_verification_response(response);
                }
                recv(self.transaction_check_queue.response_receiver()) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::TransactionCheckWorkersExited)?;
                    let () = self.handle_transaction_check_response(response);
                }
                recv(self.transaction_execution_queue.response_receiver()) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::TransactionExecutionWorkersExited)?;
                    let () = self.handle_transaction_execution_response(response);
                }
                recv(replay_message_receiver) -> replay_message => {
                    let replay_message = replay_message.map_err(|_: RecvError| SchedulerExitReason::DisconnectedRequestChannel)?;
                    let () = self.handle_replay_message(replay_message);
                }
                default(SHUTDOWN_POLL_INTERVAL) => {},
            }
        }

        let Self {
            mut workers_shutdown_signal,
            entry_hash_verification_queue,
            signature_verification_queue,
            transaction_check_queue,
            transaction_execution_queue,
            entry_hash_verification_workers,
            signature_verification_workers,
            transaction_check_workers,
            transaction_execution_workers,
            ..
        } = self;

        // The response channels have room for every request in flight, so workers never block on
        // sending a response and will exit once they see the shutdown signal.
        workers_shutdown_signal.shutdown();
        drop(entry_hash_verification_queue);
        drop(signature_verification_queue);
        drop(transaction_check_queue);
        drop(transaction_execution_queue);
        let () = entry_hash_verification_workers.join_workers();
        let () = signature_verification_workers.join_workers();
        let () = transaction_check_workers.join_workers();
        let () = transaction_execution_workers.join_workers();

        Ok(())
    }

    fn workers_can_accept_request(&self) -> bool {
        self.entry_hash_verification_queue.can_send_request()
            && self.signature_verification_queue.can_send_request()
            && self.transaction_check_queue.can_send_request()
            && self.buffered_signature_verification_requests.is_empty()
            && self.buffered_transaction_check_requests.is_empty()
    }

    fn try_flush_buffered_signature_verification_requests(&mut self) {
        while let Some(request) = self.buffered_signature_verification_requests.pop_front() {
            if let Err(request) = self.signature_verification_queue.try_send(request) {
                self.buffered_signature_verification_requests
                    .push_front(request);
                break;
            }
        }
    }

    fn try_flush_buffered_transaction_check_requests(&mut self) {
        while let Some(request) = self.buffered_transaction_check_requests.pop_front() {
            if let Err(request) = self.transaction_check_queue.try_send(request) {
                self.buffered_transaction_check_requests.push_front(request);
                break;
            }
        }
    }

    /// Tries to send the [`BlockVerificationToReplayMessage`] for blocks that have a final outcome.
    fn try_flush_completed_blocks(&mut self) -> Result<(), DisconnectedResponseChannel> {
        self.blocks_in_progress.retain(|block_verification_state| {
            let Some(verification_status) = block_verification_state.release_outcome() else {
                return true;
            };
            self.buffered_replay_response_messages
                .push_back(BlockVerificationToReplayMessage {
                    slot: block_verification_state.slot,
                    bank_id: block_verification_state.bank_id,
                    verification_status,
                });
            false
        });

        while let Some(message) = self.buffered_replay_response_messages.pop_front() {
            match self.replay_message_sender.try_send(message) {
                Ok(()) => {}
                Err(TrySendError::Full(message)) => {
                    self.buffered_replay_response_messages.push_front(message);
                    break;
                }
                Err(TrySendError::Disconnected(_message)) => {
                    return Err(DisconnectedResponseChannel);
                }
            }
        }

        Ok(())
    }

    fn handle_replay_message(&mut self, replay_message: ReplayToBlockVerificationMessage) {
        match replay_message {
            ReplayToBlockVerificationMessage::Begin(begin_message) => {
                self.handle_begin_message(begin_message)
            }
            ReplayToBlockVerificationMessage::Entry(entry_message) => {
                self.handle_entry_message(entry_message)
            }
            ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                all_entries_submitted_message,
            ) => self.handle_all_entries_submitted_message(all_entries_submitted_message),
            ReplayToBlockVerificationMessage::Abort(abort_message) => {
                self.handle_abort_message(abort_message)
            }
        }
    }

    fn handle_begin_message(
        &mut self,
        BeginMessage {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        }: BeginMessage,
    ) {
        let bank_id_is_in_use = self.blocks_in_progress.iter().any(|e| e.bank_id == bank_id);
        if bank_id_is_in_use {
            panic!(
                "internal invariant violated. A duplicate bank id was requested to be scheduled"
            );
        }

        let slot_is_in_use = self.blocks_in_progress.iter().any(|e| e.slot == slot);
        if slot_is_in_use {
            panic!("internal invariant violated. A duplicate slot was requested to be scheduled");
        }

        self.blocks_in_progress.push(BlockVerificationState::new(
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        ));
    }

    fn handle_entry_message(
        &mut self,
        EntryMessage {
            bank_id,
            entry_view,
        }: EntryMessage,
    ) {
        let Some(block_verification_state) = self
            .blocks_in_progress
            .iter_mut()
            .find(|state| state.bank_id == bank_id)
        else {
            // The block already has its outcome
            return;
        };

        if block_verification_state.tracking_state != BlockTrackingState::InProgress {
            return;
        }

        let entry_verification_data = EntryVerificationData::from(&entry_view);

        // Every entry carries its own hash, which the next entry is verified against. So the
        // entries of a block are verified independently of each other.
        let start_hash = mem::replace(
            &mut block_verification_state.last_entry_hash,
            entry_verification_data.hash,
        );
        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.entry_hash_jobs_pending = progress_tracker
            .entry_hash_jobs_pending
            .checked_add(1)
            .expect("the jobs pending of a block are bounded by the request queue size");

        self.entry_hash_verification_queue
            .send(EntryHashVerificationRequest {
                bank_id,
                start_hash,
                entry_verification_data,
            });

        progress_tracker.signature_jobs_pending = progress_tracker
            .signature_jobs_pending
            .checked_add(entry_view.transactions.len())
            .expect(
                "the jobs pending of a block are bounded by the request queue size and the \
                 transactions of one entry",
            );

        let mut requests = entry_view.transactions.iter().cloned().map(|transaction| {
            SignatureVerificationRequest {
                bank_id,
                transaction,
            }
        });

        for request in requests.by_ref() {
            if let Err(request) = self.signature_verification_queue.try_send(request) {
                self.buffered_signature_verification_requests
                    .push_back(request);
                break;
            }
        }
        self.buffered_signature_verification_requests
            .extend(requests);

        progress_tracker.transaction_check_jobs_pending = progress_tracker
            .transaction_check_jobs_pending
            .checked_add(entry_view.transactions.len())
            .expect(
                "the jobs pending of a block are bounded by the request queue size and the \
                 transactions of one entry",
            );

        let slot = block_verification_state.slot;
        let transaction_execution_states =
            &mut block_verification_state.transaction_execution_states;
        let first_transaction_index = transaction_execution_states.len();
        transaction_execution_states.extend(
            iter::repeat_with(|| TransactionExecutionState::AwaitingCheck)
                .take(entry_view.transactions.len()),
        );

        let mut requests = entry_view
            .transactions
            .into_iter()
            .zip(first_transaction_index..)
            .map(|(transaction, transaction_index)| TransactionCheckRequest {
                bank_id,
                slot,
                transaction_index,
                transaction,
            });

        for request in requests.by_ref() {
            if let Err(request) = self.transaction_check_queue.try_send(request) {
                self.buffered_transaction_check_requests.push_back(request);
                break;
            }
        }
        self.buffered_transaction_check_requests.extend(requests);
    }

    fn handle_abort_message(&mut self, AbortMessage { bank_id }: AbortMessage) {
        let Some(block_verification_state) = self.block_verification_state_mut(bank_id) else {
            // The block completed or failed verification before the abort message arrived
            return;
        };

        // A block that already failed verification keeps the failure as its outcome
        if block_verification_state.tracking_state == BlockTrackingState::InProgress {
            block_verification_state.tracking_state = BlockTrackingState::Aborted;
        }
    }

    fn handle_all_entries_submitted_message(
        &mut self,
        AllEntriesSubmittedMessage { bank_id }: AllEntriesSubmittedMessage,
    ) {
        let Some(block_verification_state) = self.block_verification_state_mut(bank_id) else {
            // The block already failed verification
            return;
        };

        block_verification_state
            .progress_tracker
            .all_entries_are_submitted = true;
    }

    fn handle_entry_hash_verification_response(
        &mut self,
        EntryHashVerificationResponse { bank_id, verified }: EntryHashVerificationResponse,
    ) {
        self.entry_hash_verification_queue.complete_request();

        let block_verification_state = self
            .block_verification_state_mut(bank_id)
            .expect("a block is only released once none of its requests are in flight");

        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.entry_hash_jobs_pending = progress_tracker
            .entry_hash_jobs_pending
            .checked_sub(1)
            .expect("each response for a block answers a request counted as pending when sent");

        if !verified && block_verification_state.tracking_state == BlockTrackingState::InProgress {
            block_verification_state.tracking_state =
                BlockTrackingState::Invalidated(VerificationFailureReason::InvalidEntryHash);
        }
    }

    fn handle_signature_verification_response(
        &mut self,
        SignatureVerificationResponse { bank_id, verified }: SignatureVerificationResponse,
    ) {
        self.signature_verification_queue.complete_request();

        let block_verification_state = self
            .block_verification_state_mut(bank_id)
            .expect("a block is only released once none of its requests are in flight");

        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.signature_jobs_pending = progress_tracker
            .signature_jobs_pending
            .checked_sub(1)
            .expect("each response for a block answers a request counted as pending when sent");

        if !verified && block_verification_state.tracking_state == BlockTrackingState::InProgress {
            block_verification_state.tracking_state =
                BlockTrackingState::Invalidated(VerificationFailureReason::InvalidSignature);
        }
    }

    fn handle_transaction_check_response(
        &mut self,
        TransactionCheckResponse {
            bank_id,
            transaction_index,
            result,
        }: TransactionCheckResponse,
    ) {
        self.transaction_check_queue.complete_request();

        let block_verification_state = self
            .block_verification_state_mut(bank_id)
            .expect("a block is only released once none of its requests are in flight");

        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.transaction_check_jobs_pending = progress_tracker
            .transaction_check_jobs_pending
            .checked_sub(1)
            .expect("each response for a block answers a request counted as pending when sent");

        if block_verification_state.tracking_state == BlockTrackingState::InProgress {
            match result {
                Ok(transaction) => {
                    *block_verification_state
                        .transaction_execution_states
                        .get_mut(transaction_index)
                        .expect(
                            "every transaction has an index allocated for it before sending it to \
                             check workers",
                        ) = TransactionExecutionState::Checked(transaction);
                }
                Err(TransactionCheckError::BankNotFound) => {
                    block_verification_state.tracking_state = BlockTrackingState::BankNotFound;
                }
                Err(TransactionCheckError::InvalidTransaction(transaction_error)) => {
                    block_verification_state.tracking_state = BlockTrackingState::Invalidated(
                        VerificationFailureReason::InvalidTransaction(transaction_error),
                    );
                }
            }
        }

        self.dispatch_transaction_executions();
    }

    fn handle_transaction_execution_response(
        &mut self,
        TransactionExecutionResponse { bank_id, result }: TransactionExecutionResponse,
    ) {
        self.transaction_execution_queue.complete_request();

        let block_verification_state = self.block_verification_state_mut(bank_id).expect(
            "the block verification exists, as it's released only if there are no worker requests \
             in flight",
        );

        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.transaction_execution_jobs_pending = progress_tracker
            .transaction_execution_jobs_pending
            .checked_sub(1)
            .expect("each response for a block answers a request counted as pending when sent");

        if let Err(error) = result
            && block_verification_state.tracking_state == BlockTrackingState::InProgress
        {
            block_verification_state.tracking_state = match error {
                TransactionExecutionError::BankNotFound => BlockTrackingState::BankNotFound,
                TransactionExecutionError::ExecutionFailed(transaction_error) => {
                    BlockTrackingState::Invalidated(
                        VerificationFailureReason::TransactionExecutionFailed(transaction_error),
                    )
                }
            };
        }

        // The response frees room in the request queue, which any block may be waiting for.
        self.dispatch_transaction_executions();
    }

    fn dispatch_transaction_executions(&mut self) {
        for block_verification_state in &mut self.blocks_in_progress {
            if !self.transaction_execution_queue.can_send_request() {
                return;
            }

            let progress_tracker = &mut block_verification_state.progress_tracker;
            if block_verification_state.tracking_state != BlockTrackingState::InProgress
                || progress_tracker.transaction_execution_jobs_pending > 0
            {
                continue;
            }

            let Some(transaction) = block_verification_state
                .transaction_execution_states
                .get_mut(block_verification_state.next_transaction_index_to_execute)
                .and_then(TransactionExecutionState::take_for_execution)
            else {
                continue;
            };

            block_verification_state.next_transaction_index_to_execute = block_verification_state
                .next_transaction_index_to_execute
                .checked_add(1)
                .expect("the transaction indices of a block are bounded by the size of a block");

            progress_tracker.transaction_execution_jobs_pending = progress_tracker
                .transaction_execution_jobs_pending
                .checked_add(1)
                .expect("number of transactions executed in flight is bounded");

            self.transaction_execution_queue
                .send(TransactionExecutionRequest {
                    bank_id: block_verification_state.bank_id,
                    slot: block_verification_state.slot,
                    transaction,
                });
        }
    }

    fn block_verification_state_mut(
        &mut self,
        bank_id: BankId,
    ) -> Option<&mut BlockVerificationState> {
        self.blocks_in_progress
            .iter_mut()
            .find(|state| state.bank_id == bank_id)
    }
}

struct BlockVerificationState {
    bank_id: BankId,
    last_entry_hash: Hash,
    slot: Slot,
    tracking_state: BlockTrackingState,
    progress_tracker: BlockVerificationStatus,
    transaction_execution_states: Vec<TransactionExecutionState>,
    next_transaction_index_to_execute: usize,
}

impl BlockVerificationState {
    fn new(bank_id: BankId, parent_bank_last_entry_hash: Hash, slot: Slot) -> Self {
        Self {
            bank_id,
            last_entry_hash: parent_bank_last_entry_hash,
            slot,
            tracking_state: BlockTrackingState::InProgress,
            progress_tracker: BlockVerificationStatus::default(),
            transaction_execution_states: Vec::new(),
            next_transaction_index_to_execute: 0,
        }
    }

    /// returns a [`Some`] [`BlockVerificationOutcome`] if the block can be released
    /// by the scheduler
    fn release_outcome(&self) -> Option<BlockVerificationOutcome> {
        if self.progress_tracker.has_jobs_pending() {
            return None;
        }

        match &self.tracking_state {
            BlockTrackingState::InProgress => (self.progress_tracker.all_entries_are_submitted
                && self.next_transaction_index_to_execute
                    == self.transaction_execution_states.len())
            .then_some(BlockVerificationOutcome::Verified),
            BlockTrackingState::Aborted => Some(BlockVerificationOutcome::Aborted),
            BlockTrackingState::BankNotFound => Some(BlockVerificationOutcome::BankNotFound),
            BlockTrackingState::Invalidated(failure_reason) => Some(
                BlockVerificationOutcome::VerificationFailed(failure_reason.clone()),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockTrackingState {
    InProgress,
    Aborted,
    Invalidated(VerificationFailureReason),
    BankNotFound,
}

enum TransactionExecutionState {
    AwaitingCheck,
    Checked(Box<ReplayTransaction>),
    SentForExecution,
}

impl TransactionExecutionState {
    fn take_for_execution(&mut self) -> Option<Box<ReplayTransaction>> {
        match mem::replace(self, Self::SentForExecution) {
            Self::Checked(transaction) => Some(transaction),
            state => {
                *self = state;
                None
            }
        }
    }
}

#[derive(Debug)]
struct DisconnectedResponseChannel;

#[derive(Default)]
struct BlockVerificationStatus {
    all_entries_are_submitted: bool,
    entry_hash_jobs_pending: usize,
    signature_jobs_pending: usize,
    transaction_check_jobs_pending: usize,
    transaction_execution_jobs_pending: usize,
}

impl BlockVerificationStatus {
    fn has_jobs_pending(&self) -> bool {
        self.entry_hash_jobs_pending > 0
            || self.signature_jobs_pending > 0
            || self.transaction_check_jobs_pending > 0
            || self.transaction_execution_jobs_pending > 0
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        crossbeam_channel::TryRecvError,
        shaq::mpmc,
        solana_entry::entry::{Entry, EntryView, entry_views_for_tests, next_entry},
        solana_keypair::Keypair,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        solana_system_transaction as system_transaction,
        solana_transaction::TransactionVerificationMode,
        std::num::NonZeroUsize,
        test_case::test_case,
    };

    const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
    const BANK_ID: BankId = BankId::new(123);
    const SLOT: Slot = 120;
    const ENTRY_HASH_VERIFICATION_QUEUE_SIZE: usize = 8;
    const SIGNATURE_VERIFICATION_QUEUE_SIZE: usize = 4;
    const TRANSACTION_CHECK_QUEUE_SIZE: usize = 6;
    const TRANSACTION_EXECUTION_QUEUE_SIZE: usize = 2;
    const OUTCOME_CHANNEL_SIZE: usize = 64;

    /// Flushes the completed blocks after every handler call, like the event loop.
    struct ManuallyDrivenScheduler {
        scheduler: BlockVerificationScheduler,
        entry_hash_verification_request_consumer: mpmc::Consumer<EntryHashVerificationRequest>,
        signature_verification_request_consumer: mpmc::Consumer<SignatureVerificationRequest>,
        transaction_check_request_consumer: mpmc::Consumer<TransactionCheckRequest>,
        transaction_execution_request_consumer: mpmc::Consumer<TransactionExecutionRequest>,
        outcome_receiver: Receiver<BlockVerificationToReplayMessage>,
    }

    impl ManuallyDrivenScheduler {
        fn new() -> Self {
            // The handlers are called directly, so nothing is ever received on the replay message
            // channel, and the response senders are not needed.
            let (entry_hash_verification_queue, entry_hash_verification_request_consumer, _) =
                EntryHashVerificationQueue::new(
                    NonZeroUsize::new(ENTRY_HASH_VERIFICATION_QUEUE_SIZE).unwrap(),
                );
            let (signature_verification_queue, signature_verification_request_consumer, _) =
                VerificationQueue::new(
                    NonZeroUsize::new(SIGNATURE_VERIFICATION_QUEUE_SIZE).unwrap(),
                );
            let (transaction_check_queue, transaction_check_request_consumer, _) =
                VerificationQueue::new(NonZeroUsize::new(TRANSACTION_CHECK_QUEUE_SIZE).unwrap());
            let (transaction_execution_queue, transaction_execution_request_consumer, _) =
                VerificationQueue::new(
                    NonZeroUsize::new(TRANSACTION_EXECUTION_QUEUE_SIZE).unwrap(),
                );
            let (outcome_sender, outcome_receiver) = bounded(OUTCOME_CHANNEL_SIZE);
            let message_receiver = never();

            let scheduler = BlockVerificationScheduler::new(
                message_receiver,
                outcome_sender,
                entry_hash_verification_queue,
                signature_verification_queue,
                transaction_check_queue,
                transaction_execution_queue,
                Arc::new(AtomicBool::new(false)),
                ShutdownOnDrop::from(Arc::new(AtomicBool::new(false))),
                VerificationWorkers::none(),
                VerificationWorkers::none(),
                VerificationWorkers::none(),
                VerificationWorkers::none(),
            );

            Self {
                scheduler,
                entry_hash_verification_request_consumer,
                signature_verification_request_consumer,
                transaction_check_request_consumer,
                transaction_execution_request_consumer,
                outcome_receiver,
            }
        }

        fn begin_block(&mut self, bank_id: BankId, slot: Slot, parent_bank_last_entry_hash: Hash) {
            self.handle_message(ReplayToBlockVerificationMessage::Begin(BeginMessage {
                bank_id,
                parent_bank_last_entry_hash,
                slot,
            }));
        }

        fn submit_entries(&mut self, bank_id: BankId, entry_views: Vec<EntryView<Bytes>>) {
            for entry_view in entry_views {
                self.handle_message(ReplayToBlockVerificationMessage::Entry(EntryMessage {
                    bank_id,
                    entry_view,
                }));
            }
        }

        fn notify_all_entries_submitted(&mut self, bank_id: BankId) {
            self.handle_message(ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                AllEntriesSubmittedMessage { bank_id },
            ));
        }

        fn abort_block(&mut self, bank_id: BankId) {
            self.handle_message(ReplayToBlockVerificationMessage::Abort(AbortMessage {
                bank_id,
            }));
        }

        fn handle_message(&mut self, message: ReplayToBlockVerificationMessage) {
            assert!(
                self.scheduler.workers_can_accept_request(),
                "the event loop does not receive replay messages while a request queue is full"
            );
            self.scheduler.handle_replay_message(message);
            self.scheduler.try_flush_completed_blocks().unwrap();
        }

        /// Answers the oldest request in the queue
        fn respond_entry_hash(&mut self, verified: bool) {
            let request = self
                .entry_hash_verification_request_consumer
                .try_read()
                .expect("tests only respond to requests the scheduler sent");
            self.scheduler
                .handle_entry_hash_verification_response(EntryHashVerificationResponse {
                    bank_id: request.bank_id,
                    verified,
                });
            self.scheduler.try_flush_completed_blocks().unwrap();
        }

        /// Answers the oldest request in the queue
        fn respond_signatures(&mut self, verified: bool) {
            let request = self
                .signature_verification_request_consumer
                .try_read()
                .expect("tests only respond to requests the scheduler sent");
            self.scheduler
                .handle_signature_verification_response(SignatureVerificationResponse {
                    bank_id: request.bank_id,
                    verified,
                });
            self.scheduler.try_flush_completed_blocks().unwrap();
        }

        /// Answers the oldest request in the queue as passing the check
        fn respond_transaction_check(&mut self) {
            let request = self
                .transaction_check_request_consumer
                .try_read()
                .expect("tests only respond to requests the scheduler sent");
            let transaction = Bank::default_for_tests()
                .verify_transaction(request.transaction, TransactionVerificationMode::HashOnly)
                .expect("the tests only submit transactions that pass the check");
            self.scheduler
                .handle_transaction_check_response(TransactionCheckResponse {
                    bank_id: request.bank_id,
                    transaction_index: request.transaction_index,
                    result: Ok(Box::new(transaction)),
                });
            self.scheduler.try_flush_completed_blocks().unwrap();
        }

        /// Answers the oldest request in the queue
        fn respond_transaction_execution(&mut self, result: Result<(), TransactionExecutionError>) {
            let request = self
                .transaction_execution_request_consumer
                .try_read()
                .expect("tests only respond to requests the scheduler sent");
            self.scheduler
                .handle_transaction_execution_response(TransactionExecutionResponse {
                    bank_id: request.bank_id,
                    result,
                });
            self.scheduler.try_flush_completed_blocks().unwrap();
        }

        fn try_recv_outcome(&self) -> Option<BlockVerificationToReplayMessage> {
            match self.outcome_receiver.try_recv() {
                Ok(outcome) => Some(outcome),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    panic!("the outcome sender is owned by the scheduler")
                }
            }
        }

        fn entry_hash_requests_in_flight(&self) -> usize {
            self.scheduler
                .entry_hash_verification_queue
                .requests_in_flight()
        }

        fn signature_requests_in_flight(&self) -> usize {
            self.scheduler
                .signature_verification_queue
                .requests_in_flight()
        }

        fn transaction_check_requests_in_flight(&self) -> usize {
            self.scheduler.transaction_check_queue.requests_in_flight()
        }
    }

    /// The tests choose the verdict of every request, so the entries need not form a valid chain.
    fn arbitrary_entries(num_entries: usize) -> Vec<EntryView<Bytes>> {
        entry_views_for_tests(vec![Entry::default(); num_entries])
    }

    /// Like [`arbitrary_entries`], but every entry has a transaction, so it is not a tick.
    fn arbitrary_entries_with_a_transaction(num_entries: usize) -> Vec<EntryView<Bytes>> {
        let payer = Keypair::new();
        let transaction = system_transaction::transfer(&payer, &payer.pubkey(), 1, Hash::default());
        entry_views_for_tests(vec![
            next_entry(&Hash::default(), 1, vec![transaction]);
            num_entries
        ])
    }

    #[test]
    fn all_entries_submitted_after_last_response_verifies_block() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries(3));
        assert_eq!(scheduler.entry_hash_requests_in_flight(), 3);
        for _ in 0..3 {
            scheduler.respond_entry_hash(true);
        }
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete before all its entries are marked as submitted"
        );

        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::Verified,
            })
        );
    }

    #[test]
    fn last_response_after_all_entries_submitted_verifies_block() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries(3));
        scheduler.respond_entry_hash(true);
        scheduler.respond_entry_hash(true);
        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete while verifications are in flight"
        );

        scheduler.respond_entry_hash(true);
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::Verified,
            })
        );
    }

    #[derive(Debug)]
    enum FinishReason {
        Abort,
        FailVerification,
    }

    #[test_case(FinishReason::Abort)]
    #[test_case(FinishReason::FailVerification)]
    fn aborted_or_failed_block_is_released_once_its_requests_in_flight_are_answered(
        finish_reason: FinishReason,
    ) {
        let mut scheduler = ManuallyDrivenScheduler::new();

        let mut entries = arbitrary_entries(4);
        let entry_submitted_after_finish = entries.pop().unwrap();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, entries);

        let finished_block_outcome = match finish_reason {
            FinishReason::Abort => {
                scheduler.abort_block(BANK_ID);
                BlockVerificationOutcome::Aborted
            }
            FinishReason::FailVerification => {
                scheduler.respond_entry_hash(false);
                BlockVerificationOutcome::VerificationFailed(
                    VerificationFailureReason::InvalidEntryHash,
                )
            }
        };

        let existing_messages_in_flight = scheduler.entry_hash_requests_in_flight();
        scheduler.submit_entries(BANK_ID, vec![entry_submitted_after_finish]);
        let messages_in_flight_after_sending_finished_block =
            scheduler.entry_hash_requests_in_flight();

        assert_eq!(
            existing_messages_in_flight, messages_in_flight_after_sending_finished_block,
            "new entries for a block that already got aborted or failed verification should be \
             discarded"
        );

        while scheduler.entry_hash_requests_in_flight() > 1 {
            scheduler.respond_entry_hash(true);
        }
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not be released while requests for it are in flight"
        );

        // The last request in flight is answered as invalid, which must not change the outcome.
        scheduler.respond_entry_hash(false);
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: finished_block_outcome,
            }),
            "a block must be released once the last request for it is answered"
        );
    }

    #[test]
    fn aborting_block_that_failed_verification_keeps_the_failure_as_its_outcome() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries(2));
        scheduler.respond_entry_hash(false);
        scheduler.abort_block(BANK_ID);
        assert_eq!(scheduler.try_recv_outcome(), None);

        scheduler.respond_entry_hash(true);
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::VerificationFailed(
                    VerificationFailureReason::InvalidEntryHash
                ),
            })
        );
    }

    #[test]
    fn replay_messages_are_not_received_while_requests_in_flight_fill_the_request_queue() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(
            BANK_ID,
            arbitrary_entries(ENTRY_HASH_VERIFICATION_QUEUE_SIZE),
        );
        assert!(
            !scheduler.scheduler.workers_can_accept_request(),
            "replay messages must not be received while the request queue is full"
        );

        scheduler.respond_entry_hash(true);
        assert!(
            scheduler.scheduler.workers_can_accept_request(),
            "a response must free up room for a request"
        );

        // The freed room can be used, so the next entry is sent without the queue overflowing.
        scheduler.submit_entries(BANK_ID, arbitrary_entries(1));
    }

    #[test]
    fn last_signature_response_after_all_entries_submitted_verifies_block() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries(1));

        assert_eq!(
            scheduler.signature_requests_in_flight(),
            0,
            "a tick entry should schedule signature verification work"
        );
        scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));

        scheduler.respond_entry_hash(true);
        scheduler.respond_entry_hash(true);
        scheduler.respond_transaction_check();
        scheduler.respond_transaction_execution(Ok(()));

        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete while signature verifications are in flight"
        );

        scheduler.respond_signatures(true);
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::Verified,
            })
        );
    }

    #[test]
    fn block_failing_signature_verification() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));
        scheduler.respond_signatures(false);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not be released while requests for it are in flight"
        );

        scheduler.respond_entry_hash(true);
        scheduler.respond_transaction_check();
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::VerificationFailed(
                    VerificationFailureReason::InvalidSignature
                ),
            })
        );
    }

    #[test]
    fn signature_workers_cause_backpressure_to_replay_channel() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(
            BANK_ID,
            arbitrary_entries_with_a_transaction(SIGNATURE_VERIFICATION_QUEUE_SIZE),
        );
        assert!(
            scheduler
                .scheduler
                .entry_hash_verification_queue
                .can_send_request(),
            "the entry hash request queue is larger than the signature request queue"
        );
        assert!(
            !scheduler.scheduler.workers_can_accept_request(),
            "replay messages must not be received while the signature request queue is full"
        );

        scheduler.respond_signatures(true);
        assert!(
            scheduler.scheduler.workers_can_accept_request(),
            "a response must free up room for a request"
        );

        // The freed room can be used, so the next entry is sent without the queue overflowing.
        scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));
    }

    #[test]
    fn last_transaction_execution_response_after_all_entries_submitted_verifies_block() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, arbitrary_entries(1));

        assert_eq!(
            scheduler.transaction_check_requests_in_flight(),
            0,
            "a tick entry should not schedule transaction check work"
        );
        scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));

        scheduler.respond_entry_hash(true);
        scheduler.respond_entry_hash(true);
        scheduler.respond_signatures(true);

        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete while transaction checks are in flight"
        );

        scheduler.respond_transaction_check();
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete while its transactions are executing"
        );

        scheduler.respond_transaction_execution(Ok(()));
        assert_eq!(
            scheduler.try_recv_outcome(),
            Some(BlockVerificationToReplayMessage {
                slot: SLOT,
                bank_id: BANK_ID,
                verification_status: BlockVerificationOutcome::Verified,
            })
        );
    }

    #[test]
    fn transaction_check_workers_cause_backpressure_to_replay_channel() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        // The signature request queue is smaller, so its requests are answered to keep it from
        // filling up first.
        for _ in 0..TRANSACTION_CHECK_QUEUE_SIZE {
            scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));
            scheduler.respond_signatures(true);
        }
        assert!(
            scheduler
                .scheduler
                .entry_hash_verification_queue
                .can_send_request(),
            "the entry hash request queue is larger than the transaction check request queue"
        );
        assert!(
            scheduler
                .scheduler
                .signature_verification_queue
                .can_send_request(),
            "every signature request was answered"
        );
        assert!(
            !scheduler.scheduler.workers_can_accept_request(),
            "replay messages must not be received while the transaction check request queue is \
             full"
        );

        scheduler.respond_transaction_check();
        assert!(
            scheduler.scheduler.workers_can_accept_request(),
            "a response must free up room for a request"
        );

        // The freed room can be used, so the next entry is sent without the queue overflowing.
        scheduler.submit_entries(BANK_ID, arbitrary_entries_with_a_transaction(1));
    }
}
