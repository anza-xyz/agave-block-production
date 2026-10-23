use {
    crate::helpers::{
        BOGUS_HASH, PARENT_BANK_LAST_ENTRY_HASH, RECV_TIMEOUT, SLOT, TEST_SCHEDULER_CONFIG,
        entry_views_for_chain, recv_outcome, sign, submit_entries, transfer,
    },
    block_verification_stage::{
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
    solana_clock::{BankId, Slot},
    solana_entry::entry::{entry_views_for_tests, next_entry_mut},
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::{
        assert_matches,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
    test_case::test_case,
};

pub mod helpers;

#[test]
fn block_with_no_entries_notifying_that_all_entries_are_submitted_passes_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    const FIRST_BANK_ID: BankId = 1;
    const FIRST_SLOT: Slot = 120;
    const SECOND_BANK_ID: BankId = 2;
    const SECOND_SLOT: Slot = 121;
    const THIRD_BANK_ID: BankId = 3;
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

// Beginning a block whose bank id or slot is still in progress violates a scheduler invariant,
// which panics the scheduler thread.
#[test_case(1, 120, 1, 121; "reused_bank_id")]
#[test_case(1, 120, 2, 120; "reused_slot")]
#[test_case(1, 120, 1, 120; "reused_slot_and_bank_id")]
#[should_panic = "internal invariant violated. A duplicate"]
fn reusing_bank_id_or_slot_in_progress_panics_scheduler(
    first_bank_id: BankId,
    first_slot: Slot,
    second_bank_id: BankId,
    second_slot: Slot,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    // More entries than jobs can be in flight, so entries are received in several rounds. Every
    // other entry has a transaction, whose signatures are verified.
    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 100, 3, num_signatures),
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
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    const NUM_ENTRIES: usize = 10;
    const NUM_ENTRIES_SUBMITTED_BEFORE_FAILURE: usize = 5;
    const NEXT_BANK_ID: BankId = BANK_ID + 1;
    const NEXT_SLOT: Slot = SLOT + 1;

    let mut entries_submitted_with_failure =
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, NUM_ENTRIES, 3, 1);
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
    MissingSignature,
    ExtraSignature,
    MissingStaticAccountKey,
}

// A transaction's signatures are valid only when each of its required signers signed its message,
// and it has exactly one signature for each of them.
#[test_case(Tampering::SignedByWrongKeypair; "signed_by_wrong_keypair")]
#[test_case(Tampering::SignedOverOtherMessage; "signed_over_other_message")]
#[test_case(Tampering::MissingSignature; "missing_signature")]
#[test_case(Tampering::ExtraSignature; "extra_signature")]
#[test_case(Tampering::MissingStaticAccountKey; "missing_static_account_key")]
fn block_with_invalid_signature_fails_verification(tampering: Tampering) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let payer = Keypair::new();
    let recipient = Keypair::new();
    let other = Keypair::new();
    let mut transaction = transfer(&payer, &recipient, 2);

    // The tamperings of the number of signatures or keys keep every signature that can be paired
    // with a static account key valid for that key. So the transaction fails verification only
    // because the numbers are checked.
    match tampering {
        Tampering::SignedByWrongKeypair => sign(&mut transaction, &[&payer, &other]),
        Tampering::SignedOverOtherMessage => {
            transaction.signatures[1] = recipient.sign_message(b"other message");
        }
        Tampering::MissingSignature => sign(&mut transaction, &[&payer]),
        Tampering::ExtraSignature => {
            // The recipient signs, but only the payer is a required signer.
            transaction.message.header.num_required_signatures = 1;
            sign(&mut transaction, &[&payer, &recipient]);
        }
        Tampering::MissingStaticAccountKey => {
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
