use {
    crate::{
        config::SchedulerConfig,
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage, BlockVerificationOutcome,
            BlockVerificationToReplayMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        stage::BlockVerificationStage,
    },
    crossbeam_channel::{Receiver, RecvError, Sender, TrySendError, bounded, select},
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

/// The timeout used on waiting for messages before checking for a timeout
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(thiserror::Error, Debug)]
pub enum SchedulerExitReason {
    #[error("the block verification response receiver was dropped")]
    DisconnectedResponseChannel,
    #[error("the block verification request sender was dropped")]
    DisconnectedRequestChannel,
}

impl From<DisconnectedResponseChannel> for SchedulerExitReason {
    fn from(_: DisconnectedResponseChannel) -> Self {
        SchedulerExitReason::DisconnectedResponseChannel
    }
}

pub(super) struct BlockVerificationScheduler {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    replay_message_sender: Sender<BlockVerificationToReplayMessage>,

    shutdown_signal: Arc<AtomicBool>,

    blocks_in_progress: Vec<BlockVerificationState>,
    buffered_replay_response_messages: VecDeque<BlockVerificationToReplayMessage>,
}

impl BlockVerificationScheduler {
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        shutdown_signal: Arc<AtomicBool>,
    ) -> BlockVerificationStage {
        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);
        let block_verification_to_replay =
            bounded(scheduler_config.block_verification_to_replay_channel_size);

        let scheduler = Self {
            replay_message_receiver: replay_to_block_verification.1,
            replay_message_sender: block_verification_to_replay.0,
            shutdown_signal: shutdown_signal.clone(),
            blocks_in_progress: Vec::new(),
            buffered_replay_response_messages: VecDeque::new(),
        };

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            block_verification_to_replay.1,
            scheduler_thread_join_handle,
        )
    }

    /// Runs until shutdown is requested or every replay message sender is dropped.
    ///
    /// Blocks still in progress are dropped on exit without an outcome being sent for them.
    /// Dropping the scheduler drops its outcome sender, disconnecting the stage's receiver.
    fn run_scheduler_event_loop(mut self) -> Result<(), SchedulerExitReason> {
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            self.try_flush_buffered_replay_responses()?;

            select! {
                recv(self.replay_message_receiver) -> replay_message => {
                    let replay_message = replay_message.map_err(|_: RecvError| SchedulerExitReason::DisconnectedResponseChannel)?;
                    let () = self.handle_replay_message(replay_message)?;
                }
                default(SHUTDOWN_POLL_INTERVAL) => {},
            }
        }

        Ok(())
    }

    /// Sends buffered replay responses without blocking, stopping at the first
    /// failed send. Any remaining responses stay buffered until the next flush.
    fn try_flush_buffered_replay_responses(&mut self) -> Result<(), DisconnectedResponseChannel> {
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

    fn handle_replay_message(
        &mut self,
        replay_message: ReplayToBlockVerificationMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        match replay_message {
            ReplayToBlockVerificationMessage::Begin(begin_message) => {
                self.handle_begin_message(begin_message);
                Ok(())
            }
            ReplayToBlockVerificationMessage::Entry(entry_message) => {
                self.handle_entry_message(entry_message);
                Ok(())
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

    fn handle_entry_message(&mut self, _entry_message: EntryMessage) {}

    fn handle_abort_message(
        &mut self,
        AbortMessage { bank_id }: AbortMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(block_verification_state) = self.take_block_verification_state(&bank_id) else {
            // The block completed or failed verification before the abort message arrived
            return Ok(());
        };

        self.buffered_replay_response_messages
            .push_back(BlockVerificationToReplayMessage {
                slot: block_verification_state.slot,
                bank_id: block_verification_state.bank_id,
                verification_status: BlockVerificationOutcome::Aborted,
            });
        self.try_flush_buffered_replay_responses()
    }

    fn handle_all_entries_submitted_message(
        &mut self,
        AllEntriesSubmittedMessage { bank_id }: AllEntriesSubmittedMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(mut block_verification_state) = self.take_block_verification_state(&bank_id)
        else {
            // The block already failed verification
            return Ok(());
        };

        block_verification_state
            .progress_tracker
            .all_entries_are_submitted = true;

        self.try_complete_block(block_verification_state)
    }

    /// Takes the state of the block being verified in `bank_id` out of the scheduler state.
    fn take_block_verification_state(
        &mut self,
        bank_id: &BankId,
    ) -> Option<BlockVerificationState> {
        let block_verification_state_index = self
            .blocks_in_progress
            .iter()
            .position(|state| state.bank_id == *bank_id)?;

        Some(
            self.blocks_in_progress
                .swap_remove(block_verification_state_index),
        )
    }

    /// Sends a [`BlockVerificationOutcome::Verified`] message back to replay if every verification
    /// operation of `block_verification_state` has finished, and returns the state to the
    /// scheduler otherwise.
    fn try_complete_block(
        &mut self,
        block_verification_state: BlockVerificationState,
    ) -> Result<(), DisconnectedResponseChannel> {
        if !block_verification_state.progress_tracker.is_completed() {
            self.blocks_in_progress.push(block_verification_state);
            return Ok(());
        }

        self.buffered_replay_response_messages
            .push_back(BlockVerificationToReplayMessage {
                slot: block_verification_state.slot,
                bank_id: block_verification_state.bank_id,
                verification_status: BlockVerificationOutcome::Verified,
            });
        self.try_flush_buffered_replay_responses()
    }
}

struct BlockVerificationState {
    bank_id: BankId,
    #[expect(dead_code)]
    parent_bank_last_entry_hash: Hash,
    slot: Slot,
    progress_tracker: BlockVerificationStatus,
}

impl BlockVerificationState {
    fn new(bank_id: BankId, parent_bank_last_entry_hash: Hash, slot: Slot) -> Self {
        Self {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
            progress_tracker: BlockVerificationStatus::default(),
        }
    }
}

struct DisconnectedResponseChannel;

#[derive(Default)]
struct BlockVerificationStatus {
    all_entries_are_submitted: bool,
}

impl BlockVerificationStatus {
    fn is_completed(&self) -> bool {
        self.all_entries_are_submitted
    }
}
