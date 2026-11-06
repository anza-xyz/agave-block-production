use {
    crate::helpers::{
        BOGUS_HASH, PARENT_BANK_LAST_ENTRY_HASH, PAYER_LAMPORTS, RECV_TIMEOUT, SLOT,
        TEST_SCHEDULER_CONFIG, TestBank, recv_outcome, sign, submit_entries, transfer,
    },
    block_verification_stage::{
        SchedulerConfig,
        messages::{
            BlockVerificationOutcome, BlockVerificationToReplayMessage, VerificationFailureReason,
        },
        run_scheduler,
        stage::{
            Aborted, AllEntriesSubmitted, BlockVerificationSession, SchedulerShutdownError, Started,
        },
    },
    crossbeam_channel::RecvTimeoutError,
    helpers::BANK_ID,
    solana_account::AccountSharedData,
    solana_accounts_db::{account_locks::DEFAULT_TX_ACCOUNT_LOCKS, bank_id::BankId},
    solana_address_lookup_table_interface::{
        program as address_lookup_table_program,
        state::{AddressLookupTable, LookupTableMeta},
    },
    solana_clock::Slot,
    solana_cost_model::cost_tracker::CostTrackerLimits,
    solana_entry::entry::{
        entry_views_for_tests, next_entry, next_entry_mut, next_versioned_entry,
    },
    solana_keypair::Keypair,
    solana_message::{AccountMeta, AddressLookupTableAccount, VersionedMessage, v0, v1},
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_limits::MAX_V1_ACCOUNTS,
    solana_signer::Signer,
    solana_system_interface::instruction as system_instruction,
    solana_system_transaction as system_transaction,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_error::TransactionError,
    std::{
        assert_matches,
        borrow::Cow,
        iter,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
    test_case::test_case,
};

pub mod helpers;

#[test]
fn block_with_no_entries_notifying_that_all_entries_are_submitted_passes_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn aborting_block_before_all_entries_submitted_sends_aborted_and_releases_bank_id_and_slot() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    // The bank id and slot are released once the block is aborted, so they can be reused.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// The block with no entries is verified as soon as all its entries are submitted, so the
// scheduler no longer tracks it when the abort arrives. The abort is then ignored, and only the verified
// outcome is sent.
#[test]
fn aborting_block_after_it_completed_is_ignored() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<Aborted> = session
        .notify_all_entries_submitted()
        .unwrap()
        .abort_block_verification()
        .unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    // The bank id and slot are released once the block is verified, so they can be reused. The
    // next message being for this block also shows no message was sent for the ignored abort.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn outcomes_of_concurrent_blocks_are_sent_in_completion_order() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    const FIRST_BANK_ID: BankId = BankId::new(1);
    const FIRST_SLOT: Slot = 120;
    const SECOND_BANK_ID: BankId = BankId::new(2);
    const SECOND_SLOT: Slot = 121;
    const THIRD_BANK_ID: BankId = BankId::new(3);
    const THIRD_SLOT: Slot = 122;

    let first_session: BlockVerificationSession<Started> = stage
        .begin_block(FIRST_BANK_ID, FIRST_SLOT, BOGUS_HASH)
        .unwrap();
    let second_session: BlockVerificationSession<Started> = stage
        .begin_block(SECOND_BANK_ID, SECOND_SLOT, BOGUS_HASH)
        .unwrap();

    let _second_session: BlockVerificationSession<AllEntriesSubmitted> =
        second_session.notify_all_entries_submitted().unwrap();
    let _first_session: BlockVerificationSession<Aborted> =
        first_session.abort_block_verification().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SECOND_SLOT,
            bank_id: SECOND_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );
    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: FIRST_SLOT,
            bank_id: FIRST_BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    // Outcomes are sent in order, so a following block's outcome being next shows no other
    // message was sent for the two blocks.
    let third_session: BlockVerificationSession<Started> = stage
        .begin_block(THIRD_BANK_ID, THIRD_SLOT, BOGUS_HASH)
        .unwrap();
    let _third_session: BlockVerificationSession<AllEntriesSubmitted> =
        third_session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: THIRD_SLOT,
            bank_id: THIRD_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// The outcome of a block tells replay that no worker uses its bank any more, so a block aborted
// while one of its transactions executes gets its outcome only once that execution is done.
#[test]
fn aborting_block_while_its_transaction_executes_sends_aborted_once_the_execution_is_done() {
    // Long enough for the scheduler to handle the abort, were it to send the outcome right away.
    const NO_OUTCOME_TIMEOUT: Duration = Duration::from_millis(100);

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let bank = test_bank.bank();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let transaction = test_bank.transfer(&Keypair::new(), 1);
    let signature = transaction.signatures[0];

    // The execution of a transaction adds its cost to the block once the transaction is committed,
    // so holding the cost tracker of the bank keeps the committed transaction executing.
    let cost_tracker = bank.read_cost_tracker().unwrap();

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );

    let waiting_since = Instant::now();
    while bank.get_signature_status(&signature).is_none() {
        assert!(
            waiting_since.elapsed() < RECV_TIMEOUT,
            "the transaction must be committed"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let _session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();
    assert_matches!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(NO_OUTCOME_TIMEOUT),
        Err(RecvTimeoutError::Timeout)
    );

    drop(cost_tracker);
    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// Beginning a block whose bank id or slot is still in progress violates a scheduler invariant,
// which panics the scheduler thread.
#[test_case(BankId::new(1), 120, BankId::new(1), 121; "reused_bank_id")]
#[test_case(BankId::new(1), 120, BankId::new(2), 120; "reused_slot")]
#[test_case(BankId::new(1), 120, BankId::new(1), 120; "reused_slot_and_bank_id")]
#[should_panic = "internal invariant violated. A duplicate"]
fn reusing_bank_id_or_slot_in_progress_panics_scheduler(
    first_bank_id: BankId,
    first_slot: Slot,
    second_bank_id: BankId,
    second_slot: Slot,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let _first_session: BlockVerificationSession<Started> = stage
        .begin_block(first_bank_id, first_slot, BOGUS_HASH)
        .unwrap();

    let _second_session: BlockVerificationSession<Started> = stage
        .begin_block(second_bank_id, second_slot, BOGUS_HASH)
        .unwrap();

    // The panic drops the scheduler's sender for outcomes, disconnecting the receiver.
    assert_matches!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT),
        Err(RecvTimeoutError::Disconnected)
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    stage
        .join()
        .expect_err("the scheduler thread panicked due to a reuse of slot/bank");
}

#[test]
fn shutdown_stops_scheduler_with_block_in_progress() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    // The session still holds a sender, so only the shutdown signal stops the scheduler.
    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));

    assert_matches!(
        session.notify_all_entries_submitted(),
        Err(SchedulerShutdownError)
    );
}

#[test_case(1; "one_signature")]
#[test_case(2; "two_signatures")]
fn block_with_valid_entries_passes_verification(num_signatures: u8) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    // More entries than jobs can be in flight, so entries are received in several rounds. Every
    // other entry has a transaction, whose signatures are verified.
    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        test_bank.entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 100, 3, num_signatures),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// Each transfer is paid by the recipient of the transfer before it, whose account only exists once
// that transfer is committed. So the block only passes verification when its transactions execute
// in block order.
#[test]
fn transactions_execute_in_block_order_and_are_committed_to_the_bank() {
    const NUM_TRANSFERS: usize = 20;
    const TRANSFERRED_LAMPORTS: u64 = 1_000;

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let payers: Vec<_> = iter::once(test_bank.payer().insecure_clone())
        .chain(iter::repeat_with(Keypair::new))
        .take(NUM_TRANSFERS.checked_add(1).unwrap())
        .collect();
    let transactions: Vec<_> = payers
        .iter()
        .zip(payers.iter().skip(1))
        .map(|(payer, recipient)| {
            system_transaction::transfer(
                payer,
                &recipient.pubkey(),
                TRANSFERRED_LAMPORTS,
                test_bank.recent_blockhash(),
            )
        })
        .collect();

    // The transfers are spread over several entries, as checks of different entries can finish
    // out of order too.
    let mut hash = PARENT_BANK_LAST_ENTRY_HASH;
    let entries = transactions
        .chunks(3)
        .map(|transactions| next_entry_mut(&mut hash, 3, transactions.to_vec()))
        .collect();

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(&session, entry_views_for_tests(entries));
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    // Transaction fees are zero in the test banks.
    let bank = test_bank.bank();
    let balances: Vec<_> = payers
        .iter()
        .map(|payer| bank.get_balance(&payer.pubkey()))
        .collect();
    let mut expected_balances = vec![0; payers.len()];
    expected_balances[0] = PAYER_LAMPORTS - TRANSFERRED_LAMPORTS;
    expected_balances[NUM_TRANSFERS] = TRANSFERRED_LAMPORTS;
    assert_eq!(balances, expected_balances);

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[derive(Debug)]
enum InvalidEntry {
    /// The first entry does not follow the parent bank's last entry hash.
    NotFollowingParentBankLastEntryHash,
    /// The last entry submitted before the failure has a corrupted hash.
    CorruptedHash,
}

#[test_case(InvalidEntry::NotFollowingParentBankLastEntryHash; "first_entry_not_following_parent_bank_last_entry_hash")]
#[test_case(InvalidEntry::CorruptedHash; "entry_with_corrupted_hash")]
fn block_with_invalid_entry_fails_verification_before_all_entries_are_submitted(
    invalid_entry: InvalidEntry,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    const NUM_ENTRIES: usize = 10;
    const NUM_ENTRIES_SUBMITTED_BEFORE_FAILURE: usize = 5;
    const NEXT_BANK_ID: BankId = BankId::new(2);
    const NEXT_SLOT: Slot = SLOT + 1;

    let mut entries_submitted_with_failure =
        test_bank.entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, NUM_ENTRIES, 3, 1);
    let entries_submitted_after_failure =
        entries_submitted_with_failure.split_off(NUM_ENTRIES_SUBMITTED_BEFORE_FAILURE);

    let parent_bank_last_entry_hash = match invalid_entry {
        InvalidEntry::NotFollowingParentBankLastEntryHash => BOGUS_HASH,
        InvalidEntry::CorruptedHash => {
            entries_submitted_with_failure.last_mut().unwrap().hash = BOGUS_HASH;
            PARENT_BANK_LAST_ENTRY_HASH
        }
    };

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, parent_bank_last_entry_hash)
        .unwrap();

    submit_entries(&session, entries_submitted_with_failure);

    // The block fails before all its entries are submitted.
    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::InvalidEntryHash
            ),
        }
    );

    // The failed block is no longer tracked, so the rest of its entries and the notification
    // that all its entries are submitted are ignored.
    submit_entries(&session, entries_submitted_after_failure);
    let _failed_session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    // Outcomes are sent in order, so the next block's outcome being the next message shows no
    // other outcome was sent for the failed block.
    let next_session: BlockVerificationSession<Started> = stage
        .begin_block(NEXT_BANK_ID, NEXT_SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    let _next_session: BlockVerificationSession<AllEntriesSubmitted> =
        next_session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: NEXT_SLOT,
            bank_id: NEXT_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[derive(Debug)]
enum Tampering {
    SignedByWrongKeypair,
    SignedOverOtherMessage,
}

// A transaction's signatures are valid only when each of its required signers signed its message.
#[test_case(Tampering::SignedByWrongKeypair; "signed_by_wrong_keypair")]
#[test_case(Tampering::SignedOverOtherMessage; "signed_over_other_message")]
fn block_with_invalid_signature_fails_verification(tampering: Tampering) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let recipient = Keypair::new();
    let other = Keypair::new();
    let mut transaction = test_bank.transfer(&recipient, 2);

    // The tamperings keep the number of signatures and keys valid, so the transaction passes its
    // check and executes, and only its signature verification fails.
    match tampering {
        Tampering::SignedByWrongKeypair => sign(&mut transaction, &[test_bank.payer(), &other]),
        Tampering::SignedOverOtherMessage => {
            transaction.signatures[1] = recipient.sign_message(b"other message");
        }
    }

    // The entry hashes are valid, as they are computed from the tampered signatures.
    let mut hash = PARENT_BANK_LAST_ENTRY_HASH;
    let entries = vec![
        next_entry_mut(&mut hash, 3, vec![]),
        next_entry_mut(&mut hash, 3, vec![transaction]),
        next_entry_mut(&mut hash, 3, vec![]),
    ];

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(&session, entry_views_for_tests(entries));
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::InvalidSignature
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[derive(Debug)]
enum CountTampering {
    MissingSignature,
    ExtraSignature,
    MissingStaticAccountKey,
}

// A transaction has exactly one signature for each of its required signers, and a static account
// key for each of them.
#[test_case(CountTampering::MissingSignature; "missing_signature")]
#[test_case(CountTampering::ExtraSignature; "extra_signature")]
#[test_case(CountTampering::MissingStaticAccountKey; "missing_static_account_key")]
fn block_with_wrong_number_of_signatures_or_keys_fails_verification(tampering: CountTampering) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let payer = Keypair::new();
    let recipient = Keypair::new();
    let other = Keypair::new();
    let mut transaction = transfer(&payer, &recipient, 2, BOGUS_HASH);

    // The tamperings keep every signature that can be paired with a static account key valid for
    // that key. So the transaction fails verification only because the numbers are checked.
    match tampering {
        CountTampering::MissingSignature => sign(&mut transaction, &[&payer]),
        CountTampering::ExtraSignature => {
            // The recipient signs, but only the payer is a required signer.
            transaction.message.header.num_required_signatures = 1;
            sign(&mut transaction, &[&payer, &recipient]);
        }
        CountTampering::MissingStaticAccountKey => {
            // Only the payer and the recipient remain as static account keys, but three signers
            // are required.
            transaction.message.instructions.clear();
            transaction.message.account_keys.truncate(2);
            transaction.message.header.num_readonly_unsigned_accounts = 0;
            transaction.message.header.num_required_signatures = 3;
            sign(&mut transaction, &[&payer, &recipient, &other]);
        }
    }

    // The entry hashes are valid, as they are computed from the tampered signatures.
    let mut hash = PARENT_BANK_LAST_ENTRY_HASH;
    let entries = vec![
        next_entry_mut(&mut hash, 3, vec![]),
        next_entry_mut(&mut hash, 3, vec![transaction]),
        next_entry_mut(&mut hash, 3, vec![]),
    ];

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(&session, entry_views_for_tests(entries));
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    // The signature verification and the sanitization in the transaction check both reject the
    // numbers, and whichever fails first decides the outcome.
    assert_matches!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::InvalidSignature
                    | VerificationFailureReason::InvalidTransaction(
                        TransactionError::SanitizeFailure
                    )
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[derive(Debug)]
enum MessageTampering {
    ProgramIdOutOfBounds,
    DuplicateAccountKey,
}

#[test_case(MessageTampering::ProgramIdOutOfBounds, TransactionError::SanitizeFailure; "program_id_out_of_bounds")]
#[test_case(MessageTampering::DuplicateAccountKey, TransactionError::AccountLoadedTwice; "duplicate_account_key")]
fn block_with_invalid_transaction_message_fails_verification(
    tampering: MessageTampering,
    expected_error: TransactionError,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    let payer = Keypair::new();
    let mut transaction = transfer(&payer, &Keypair::new(), 1, BOGUS_HASH);
    match tampering {
        MessageTampering::ProgramIdOutOfBounds => {
            transaction.message.instructions[0].program_id_index =
                transaction.message.account_keys.len() as u8;
        }
        MessageTampering::DuplicateAccountKey => {
            transaction.message.account_keys[1] = transaction.message.account_keys[0];
        }
    }
    sign(&mut transaction, &[&payer]);

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::InvalidTransaction(expected_error)
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test_case(DEFAULT_TX_ACCOUNT_LOCKS, BlockVerificationOutcome::Verified; "at_limit")]
#[test_case(
    DEFAULT_TX_ACCOUNT_LOCKS + 1,
    BlockVerificationOutcome::VerificationFailed(VerificationFailureReason::InvalidTransaction(
        TransactionError::TooManyAccountLocks
    ));
    "over_limit"
)]
fn block_with_v0_transaction_passes_verification_only_up_to_account_lock_limit(
    num_accounts: usize,
    expected_verification_status: BlockVerificationOutcome,
) {
    let test_bank = TestBank::new();
    let payer = test_bank.payer();
    let mut instruction =
        system_instruction::transfer(&payer.pubkey(), &Keypair::new().pubkey(), 1);
    let address_lookup_table = AddressLookupTableAccount {
        key: Keypair::new().pubkey(),
        addresses: (instruction.accounts.len().saturating_add(1)..num_accounts)
            .map(|_| Keypair::new().pubkey())
            .collect(),
    };
    instruction.accounts.extend(
        address_lookup_table
            .addresses
            .iter()
            .map(|address| AccountMeta::new_readonly(*address, false)),
    );

    let table_data = AddressLookupTable {
        meta: LookupTableMeta::default(),
        addresses: Cow::Borrowed(&address_lookup_table.addresses),
    }
    .serialize_for_tests()
    .unwrap();
    test_bank.bank().store_account(
        &address_lookup_table.key,
        &AccountSharedData::new_with_data(1, table_data, &address_lookup_table_program::id()),
    );

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let message = v0::Message::try_compile(
        &payer.pubkey(),
        &[instruction],
        &[address_lookup_table],
        test_bank.recent_blockhash(),
    )
    .unwrap();
    let num_loaded_accounts = message.address_table_lookups[0].readonly_indexes.len();
    assert_eq!(
        message
            .account_keys
            .len()
            .saturating_add(num_loaded_accounts),
        num_accounts
    );
    let transaction =
        VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer]).unwrap();

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_versioned_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: expected_verification_status,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn block_with_transaction_using_missing_address_lookup_table_fails_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        TestBank::new().bank_forks(),
    );

    // The recipient is loaded from an address lookup table that does not exist in the bank.
    let payer = Keypair::new();
    let recipient = Keypair::new();
    let address_lookup_table = AddressLookupTableAccount {
        key: Keypair::new().pubkey(),
        addresses: vec![recipient.pubkey()],
    };
    let message = v0::Message::try_compile(
        &payer.pubkey(),
        &[system_instruction::transfer(
            &payer.pubkey(),
            &recipient.pubkey(),
            1,
        )],
        &[address_lookup_table],
        BOGUS_HASH,
    )
    .unwrap();
    let transaction =
        VersionedTransaction::try_new(VersionedMessage::V0(message), &[&payer]).unwrap();

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_versioned_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::InvalidTransaction(
                    TransactionError::AddressLookupTableNotFound
                )
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test_case(MAX_V1_ACCOUNTS, BlockVerificationOutcome::Verified; "max_accounts")]
#[test_case(
    MAX_V1_ACCOUNTS + 1,
    BlockVerificationOutcome::VerificationFailed(VerificationFailureReason::InvalidTransaction(
        TransactionError::SanitizeFailure
    ));
    "too_many_accounts"
)]
fn block_with_v1_transaction_passes_verification_only_up_to_max_accounts(
    num_accounts: usize,
    expected_verification_status: BlockVerificationOutcome,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let payer = test_bank.payer();
    let mut instruction = system_instruction::transfer(&payer.pubkey(), &Pubkey::new_unique(), 1);
    instruction.accounts.extend(
        (instruction.accounts.len().saturating_add(1)..num_accounts)
            .map(|_| AccountMeta::new_readonly(Pubkey::new_unique(), false)),
    );
    let message = v1::Message::try_compile_with_config(
        &payer.pubkey(),
        &[instruction],
        test_bank.recent_blockhash(),
        v1::TransactionConfig::empty(),
    )
    .unwrap();
    assert_eq!(message.account_keys.len(), num_accounts);
    let transaction =
        VersionedTransaction::try_new(VersionedMessage::V1(message), &[payer]).unwrap();

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_versioned_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: expected_verification_status,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn block_with_transaction_failing_execution_fails_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    // The blockhash is not in the bank, so the transaction cannot be committed. Its signature and
    // its check are valid, so only its execution fails.
    let transaction = transfer(test_bank.payer(), &Keypair::new(), 1, BOGUS_HASH);

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::TransactionExecutionFailed(
                    TransactionError::BlockhashNotFound
                )
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// Only transactions that cannot be committed fail a block, as in replay. A transaction whose
// instruction fails is still committed.
#[test]
fn block_with_transaction_failing_but_committed_passes_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    // The payer cannot afford the transfer, so its instruction fails.
    let transaction = system_transaction::transfer(
        test_bank.payer(),
        &Keypair::new().pubkey(),
        PAYER_LAMPORTS + 1,
        test_bank.recent_blockhash(),
    );
    let signature = transaction.signatures[0];

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );
    let bank = test_bank.bank();
    assert_matches!(
        bank.get_signature_status(&signature),
        Some(Err(TransactionError::InstructionError(0, _)))
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn block_exceeding_the_block_cost_limit_fails_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    // No transaction fits in the block.
    test_bank
        .bank()
        .write_cost_tracker()
        .unwrap()
        .set_limits(CostTrackerLimits {
            account_cost: u64::MAX,
            block_cost: 0,
            allocated_data_size: u64::MAX,
        });
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let transaction = test_bank.transfer(&Keypair::new(), 1);

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_tests(vec![next_entry(
            &PARENT_BANK_LAST_ENTRY_HASH,
            3,
            vec![transaction],
        )]),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed(
                VerificationFailureReason::TransactionExecutionFailed(
                    TransactionError::WouldExceedMaxBlockCostLimit
                )
            ),
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// A bank missing from bank forks says nothing about whether the block is valid, so the block does
// not fail verification. Bank forks only has `BANK_ID` at `SLOT`.
#[test_case(BANK_ID, SLOT + 1; "missing_slot")]
#[test_case(BankId::new(2), SLOT; "replaced_bank")]
fn block_whose_bank_is_not_in_bank_forks_has_bank_not_found_outcome(bank_id: BankId, slot: Slot) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let test_bank = TestBank::new();
    let stage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        shutdown_flag.clone(),
        test_bank.bank_forks(),
    );

    let session: BlockVerificationSession<Started> = stage
        .begin_block(bank_id, slot, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        test_bank.entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 2, 3, 1),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot,
            bank_id,
            verification_status: BlockVerificationOutcome::BankNotFound,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

/// tests that buffered signature verifications and transaction checks get done
#[test]
fn block_with_an_entry_larger_than_the_request_queues_passes_verification() {
    const NUM_TRANSACTIONS: usize = 100;
    const MAX_SIGNATURE_VERIFICATIONS_IN_FLIGHT: NonZeroUsize = NonZeroUsize::new(1).unwrap();
    const MAX_TRANSACTION_CHECKS_IN_FLIGHT: NonZeroUsize = NonZeroUsize::new(1).unwrap();

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let config = SchedulerConfig {
        max_signature_verifications_in_flight: MAX_SIGNATURE_VERIFICATIONS_IN_FLIGHT,
        max_transaction_checks_in_flight: MAX_TRANSACTION_CHECKS_IN_FLIGHT,
        ..TEST_SCHEDULER_CONFIG
    };
    let test_bank = TestBank::new();
    let stage = run_scheduler(config, shutdown_flag.clone(), test_bank.bank_forks());

    let transactions = (0..NUM_TRANSACTIONS)
        .map(|_| test_bank.transfer(&Keypair::new(), 1))
        .collect();

    let mut hash = PARENT_BANK_LAST_ENTRY_HASH;
    let entries = vec![next_entry_mut(&mut hash, 3, transactions)];

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(&session, entry_views_for_tests(entries));
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}
