use {
    crate::messages::{BlockVerificationToReplayMessage, ReplayToBlockVerificationMessage},
    agave_transaction_view::transaction_view::{SanitizedTransactionView, TransactionView},
    bytes::Bytes,
    crossbeam_channel::{Receiver, Sender, bounded},
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
};

pub mod messages;

const REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE: usize = 100;
const BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE: usize = 100;

pub struct BlockVerificationScheduler {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    replay_message_sender: Sender<BlockVerificationToReplayMessage>,

    bank_ids_in_flight: HashMap<BankId, BankStatus>,
}

impl BlockVerificationScheduler {
    fn run_scheduler() -> (
        Sender<ReplayToBlockVerificationMessage>,
        Receiver<BlockVerificationToReplayMessage>,
    ) {
        let replay_to_block_verification = bounded(REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE);
        let block_verification_to_replay = bounded(BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE);

        let scheduler = Self {
            replay_message_receiver: replay_to_block_verification.1,
            replay_message_sender: block_verification_to_replay.0,
            bank_ids_in_flight: HashMap::new(),
        };

        std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        (
            replay_to_block_verification.0,
            block_verification_to_replay.1,
        )
    }

    fn run_scheduler_event_loop(mut self) {
        while let Ok(replay_message) = self.replay_message_receiver.recv() {}
    }
}

struct BankSchedulingState {
    sig_verify_completed: bool,
    entry_hash_verification_completed: bool,
    replay_marked_as_completed: bool,
    cancellation_token: Arc<AtomicBool>,
}

impl BankSchedulingState {
    pub(super) fn new() -> Self {
        Self {
            sig_verify_completed: false,
            entry_hash_verification_completed: false,
            replay_marked_as_completed: false,
            cancellation_token: Arc::new(AtomicBool::new(true)),
        }
    }

    fn reached_terminal_state(&self) -> bool {
        self.sig_verify_completed && self.sig_verify_completed
    }

    fn mark_sig_verify_completed(&mut self) {
        self.sig_verify_completed = true;
    }

    fn mark_entry_hash_verification_completed(&mut self) {
        self.entry_hash_verification_completed = true;
    }

    fn replay_marked_as_completed(&mut self) {
        self.replay_marked_as_completed = true;
    }
}

impl Drop for BankSchedulingState {
    fn drop(&mut self) {
        self.cancellation_token.store(false, Ordering::Relaxed);
    }
}
