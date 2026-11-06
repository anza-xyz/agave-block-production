use {
    block_verification_stage::{
        BlockVerificationStage, SchedulerConfig,
        messages::BlockVerificationToReplayMessage,
        stage::{BlockVerificationSession, Started},
    },
    bytes::Bytes,
    solana_account::AccountSharedData,
    solana_accounts_db::bank_id::BankId,
    solana_clock::Slot,
    solana_entry::entry::{EntryView, entry_views_for_tests, next_entry_mut},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_leader_schedule::SlotLeader,
    solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
    solana_signer::Signer,
    solana_system_interface::program as system_program,
    solana_system_transaction as system_transaction,
    solana_transaction::Transaction,
    std::{
        num::NonZeroUsize,
        sync::{Arc, RwLock},
        time::Duration,
    },
};

pub const TEST_SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig {
    // Room for every message a test sends, so sending never waits on the scheduler.
    replay_to_block_verification_channel_size: 1024,
    block_verification_to_replay_channel_size: 10,
    num_entry_hash_workers: NonZeroUsize::new(2).unwrap(),
    max_entry_hash_verifications_in_flight: NonZeroUsize::new(64).unwrap(),
    num_signature_workers: NonZeroUsize::new(2).unwrap(),
    max_signature_verifications_in_flight: NonZeroUsize::new(16).unwrap(),
    num_transaction_check_workers: NonZeroUsize::new(2).unwrap(),
    max_transaction_checks_in_flight: NonZeroUsize::new(16).unwrap(),
    num_transaction_execution_workers: NonZeroUsize::new(2).unwrap(),
    max_transaction_executions_in_flight: NonZeroUsize::new(16).unwrap(),
};
pub const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
pub const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
/// The bank id of the first bank created from a root bank.
pub const BANK_ID: BankId = BankId::new(1);
pub const SLOT: Slot = 120;
pub const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// The lamports [`TestBank::payer`] has in genesis.
pub const PAYER_LAMPORTS: u64 = 1_000_000_000;

/// Bank forks with the bank at `SLOT`, and a funded payer and a blockhash for its transactions.
pub struct TestBank {
    bank_forks: Arc<RwLock<BankForks>>,
    payer: Keypair,
    recent_blockhash: Hash,
}

impl TestBank {
    pub fn new() -> Self {
        let payer = Keypair::new();
        let mut genesis_config = create_genesis_config(1).genesis_config;
        genesis_config.add_account(
            payer.pubkey(),
            AccountSharedData::new(PAYER_LAMPORTS, 0, &system_program::id()),
        );
        let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        // The root bank lacks the slot hashes sysvar that address lookup tables are resolved with.
        let bank = Bank::new_from_parent_with_bank_forks(
            &bank_forks,
            root_bank,
            SlotLeader::default(),
            SLOT,
        );
        assert_eq!(bank.bank_id(), BANK_ID);
        Self {
            bank_forks,
            payer,
            recent_blockhash: bank.last_blockhash(),
        }
    }

    pub fn bank_forks(&self) -> Arc<RwLock<BankForks>> {
        self.bank_forks.clone()
    }

    pub fn payer(&self) -> &Keypair {
        &self.payer
    }

    pub fn recent_blockhash(&self) -> Hash {
        self.recent_blockhash
    }

    pub fn bank(&self) -> Arc<Bank> {
        self.bank_forks
            .read()
            .unwrap()
            .get(SLOT)
            .expect("the bank at `SLOT` is never removed from bank forks")
    }

    /// Returns a [`transfer`] from the payer that the bank can execute.
    pub fn transfer(&self, recipient: &Keypair, num_signatures: u8) -> Transaction {
        transfer(
            &self.payer,
            recipient,
            num_signatures,
            self.recent_blockhash,
        )
    }

    /// Returns a chain of `num_entries` entries following `start_hash`, alternating between ticks
    /// and entries with a transfer from the payer with `num_signatures` signatures, each
    /// `num_hashes` after the previous one. Each transfer goes to a new recipient, so no two are
    /// the same.
    pub fn entry_views_for_chain(
        &self,
        start_hash: Hash,
        num_entries: usize,
        num_hashes: u64,
        num_signatures: u8,
    ) -> Vec<EntryView<Bytes>> {
        let mut hash = start_hash;
        let entries = (0..num_entries)
            .map(|index| {
                let transactions = if index.is_multiple_of(2) {
                    vec![]
                } else {
                    vec![self.transfer(&Keypair::new(), num_signatures)]
                };
                next_entry_mut(&mut hash, num_hashes, transactions)
            })
            .collect();
        entry_views_for_tests(entries)
    }
}

/// Replaces the signatures of `transaction` with those of `signers` over its message.
pub fn sign(transaction: &mut Transaction, signers: &[&Keypair]) {
    let message = transaction.message_data();
    transaction.signatures = signers
        .iter()
        .map(|signer| signer.sign_message(&message))
        .collect();
}

/// Returns a transfer from `payer` to `recipient`, signed by the payer, and by the recipient too
/// with two signatures. Its static account keys are the payer, the recipient and the system
/// program.
pub fn transfer(
    payer: &Keypair,
    recipient: &Keypair,
    num_signatures: u8,
    recent_blockhash: Hash,
) -> Transaction {
    let mut transaction =
        system_transaction::transfer(payer, &recipient.pubkey(), 1, recent_blockhash);
    transaction.message.header.num_required_signatures = num_signatures;
    sign(
        &mut transaction,
        &[payer, recipient][..usize::from(num_signatures)],
    );
    transaction
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
