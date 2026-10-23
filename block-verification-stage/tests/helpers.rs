use {
    block_verification_stage::{
        BlockVerificationStage, SchedulerConfig,
        messages::BlockVerificationToReplayMessage,
        stage::{BlockVerificationSession, Started},
    },
    bytes::Bytes,
    solana_clock::{BankId, Slot},
    solana_entry::entry::{EntryView, entry_views_for_tests, next_entry_mut},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{num::NonZeroUsize, time::Duration},
};

pub const TEST_SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig {
    // Room for every message a test sends, so sending never waits on the scheduler.
    replay_to_block_verification_channel_size: 1024,
    block_verification_to_replay_channel_size: 10,
    num_entry_hash_workers: NonZeroUsize::new(2).unwrap(),
    max_entry_hash_verifications_in_flight: NonZeroUsize::new(64).unwrap(),
};
pub const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
pub const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
pub const BANK_ID: BankId = 123;
pub const SLOT: Slot = 120;
pub const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns a chain of `num_entries` entries following `start_hash`, alternating between ticks and
/// entries with a transaction, each `num_hashes` after the previous one.
pub fn entry_views_for_chain(
    start_hash: Hash,
    num_entries: usize,
    num_hashes: u64,
) -> Vec<EntryView<Bytes>> {
    let keypair = Keypair::new();
    let mut hash = start_hash;
    let entries = (0..num_entries)
        .map(|index| {
            let transactions = if index.is_multiple_of(2) {
                vec![]
            } else {
                vec![system_transaction::transfer(
                    &keypair,
                    &keypair.pubkey(),
                    1,
                    start_hash,
                )]
            };
            next_entry_mut(&mut hash, num_hashes, transactions)
        })
        .collect();
    entry_views_for_tests(entries)
}

pub fn submit_entries(
    session: &BlockVerificationSession<Started>,
    entry_views: Vec<EntryView<Bytes>>,
) {
    for entry_view in entry_views {
        session.submit_entry(entry_view).unwrap();
    }
}

pub fn recv_outcome(stage: &BlockVerificationStage) -> BlockVerificationToReplayMessage {
    stage
        .scheduler_message_receiver()
        .recv_timeout(RECV_TIMEOUT)
        .unwrap()
}
