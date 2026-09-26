use {
    crate::{
        config::SchedulerConfig,
        messages::{
            AbortMessage, BeginMessage, BeginMessageResponse, BlockVerificationOutcome,
            CompleteMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        stage::BlockVerificationStage,
        utils::{
            cancellation_token::{CancellationToken, CancellationTokenRef},
            oneshot,
        },
        verification_components::{
            entry_hash_verification::VerifyEntryHash, signature_verification::VerifySignature,
        },
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, bounded},
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::time::Duration,
};

/// How long the event loop waits for a replay message before checking for shutdown.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(super) struct BlockVerificationScheduler<S, E> {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    shutdown_token_ref: CancellationTokenRef,
    blocks_in_progress: Vec<BlockVerificationState>,

    #[expect(dead_code)]
    signature_verifier: S,
    #[expect(dead_code)]
    entry_hash_verifier: E,
}

impl<S, E> BlockVerificationScheduler<S, E>
where
    S: VerifySignature + Send + 'static,
    E: VerifyEntryHash + Send + 'static,
{
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        signature_verifier: S,
        entry_hash_verifier: E,
    ) -> BlockVerificationStage {
        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);

        let shutdown_token = CancellationToken::new();

        let scheduler = Self {
            replay_message_receiver: replay_to_block_verification.1,
            shutdown_token_ref: shutdown_token.token_ref(),
            blocks_in_progress: Vec::new(),
            signature_verifier,
            entry_hash_verifier,
        };

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            shutdown_token,
            scheduler_thread_join_handle,
        )
    }
}

impl<S, E> BlockVerificationScheduler<S, E>
where
    S: VerifySignature,
    E: VerifyEntryHash,
{
    /// Runs until shutdown is requested or every replay message sender is dropped.
    ///
    /// Blocks still in progress are dropped on exit, so their outcome receivers see the
    /// scheduler as shut down.
    fn run_scheduler_event_loop(mut self) {
        while !self.shutdown_token_ref.is_cancelled() {
            match self
                .replay_message_receiver
                .recv_timeout(SHUTDOWN_POLL_INTERVAL)
            {
                Ok(replay_message) => self.handle_replay_message(replay_message),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn handle_replay_message(&mut self, replay_message: ReplayToBlockVerificationMessage) {
        match replay_message {
            ReplayToBlockVerificationMessage::Begin(begin_message) => {
                self.handle_begin_message(begin_message)
            }
            ReplayToBlockVerificationMessage::Entry(entry_message) => {
                self.handle_entry_message(entry_message)
            }
            ReplayToBlockVerificationMessage::Complete(complete_message) => {
                self.handle_complete_message(complete_message);
            }
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
            begin_message_response_sender,
        }: BeginMessage,
    ) {
        let bank_id_is_in_use = self.blocks_in_progress.iter().any(|e| e.bank_id == bank_id);
        if bank_id_is_in_use {
            let _ = begin_message_response_sender.send(BeginMessageResponse::BankIdOccupied);
            return;
        }

        let slot_is_in_use = self.blocks_in_progress.iter().any(|e| e.slot == slot);
        if slot_is_in_use {
            let _ = begin_message_response_sender.send(BeginMessageResponse::SlotIdOccupied);
            return;
        }

        let (verification_sender, verification_receiver) = oneshot::channel();

        let Ok(_) = begin_message_response_sender.send(BeginMessageResponse::Success {
            verification_receiver,
        }) else {
            return;
        };

        self.blocks_in_progress.push(BlockVerificationState::new(
            bank_id,
            parent_bank_last_entry_hash,
            slot,
            verification_sender,
        ));
    }

    fn handle_entry_message(&mut self, _entry_message: EntryMessage) {}

    fn handle_abort_message(&mut self, abort_message: AbortMessage) {
        let Some(index) = self
            .blocks_in_progress
            .iter()
            .position(|state| state.bank_id == abort_message.bank_id)
        else {
            // this can happen if `try_finish_completed_bank` was triggered before abort_message was handled
            return;
        };

        let state = self.blocks_in_progress.remove(index);

        let _ = state
            .oneshot_response_sender
            .send(BlockVerificationOutcome::Aborted);
    }

    fn handle_complete_message(&mut self, CompleteMessage { bank_id }: CompleteMessage) {
        let Some(block_verification_state_index) = self
            .blocks_in_progress
            .iter()
            .position(|state| state.bank_id == bank_id)
        else {
            // this can happen if it was removed due to failing verification
            return;
        };

        self.blocks_in_progress[block_verification_state_index]
            .progress_tracker
            .replay_marked_as_completed = true;

        self.try_finish_completed_bank(block_verification_state_index);
    }

    /// Checks if block verification is completed for the bank at `block_verification_state_index`.
    /// If it is completed, the bank_id is removed the scheduler state and a response is sent back
    /// to replay.
    ///
    /// Panics if `block_verification_state_index` >= self.blocks_in_progress.len();
    fn try_finish_completed_bank(&mut self, block_verification_state_index: usize) {
        let state = self
            .blocks_in_progress
            .get(block_verification_state_index)
            .expect("caller guarantees index is valid");

        if !state.progress_tracker.is_completed() {
            return;
        }

        let state = self
            .blocks_in_progress
            .remove(block_verification_state_index);

        let _ = state
            .oneshot_response_sender
            .send(BlockVerificationOutcome::Completed);
    }
}

struct BlockVerificationState {
    bank_id: BankId,
    #[expect(dead_code)]
    parent_bank_last_entry_hash: Hash,
    slot: Slot,
    oneshot_response_sender: oneshot::Sender<BlockVerificationOutcome>,
    progress_tracker: BlockVerificationStatus,
    #[expect(dead_code)]
    cancellation_token: CancellationToken,
}

impl BlockVerificationState {
    fn new(
        bank_id: BankId,
        parent_bank_last_entry_hash: Hash,
        slot: Slot,
        oneshot_response_sender: oneshot::Sender<BlockVerificationOutcome>,
    ) -> Self {
        Self {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
            oneshot_response_sender,
            progress_tracker: BlockVerificationStatus::default(),
            cancellation_token: CancellationToken::new(),
        }
    }
}

#[derive(Default)]
struct BlockVerificationStatus {
    replay_marked_as_completed: bool,
    sig_verify_operations_in_progress: u64,
    entry_hash_verification_operations_in_progress: u64,
}

impl BlockVerificationStatus {
    fn is_completed(&self) -> bool {
        self.sig_verify_operations_in_progress == 0
            && self.entry_hash_verification_operations_in_progress == 0
            && self.replay_marked_as_completed
    }
}
