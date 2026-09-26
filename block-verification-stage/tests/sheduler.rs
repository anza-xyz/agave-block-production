use {
    block_verification_stage::{
        BlockVerificationStage, SchedulerConfig,
        messages::BlockVerificationOutcome,
        run_scheduler,
        stage::{
            Aborted, AllEntriesSubmitted, BeginBlockError, BlockVerificationSession,
            SchedulerShutdownError, Started,
        },
        verification_components::{
            entry_hash_verification::MockVerifyEntryHash,
            signature_verification::MockVerifySignature,
        },
    },
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::assert_matches,
};

const TEST_SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig {
    replay_to_block_verification_channel_size: 10,
    block_verification_to_replay_channel_size: 10,
};

const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);

#[test]
fn block_with_no_entries_marked_as_completed_passes_verification() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    let session: BlockVerificationSession<AllEntriesSubmitted> = session.mark_completed().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Completed
    )
}

#[test]
fn block_with_no_entries_aborted_aborts_verification() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    let session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Aborted
    )
}

#[test]
fn reusing_slot_id_causes_failure() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const FIRST_BANK_ID: BankId = 1;
    const SECOND_BANK_ID: BankId = 2;
    const SLOT: Slot = 120;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(FIRST_BANK_ID, SLOT, BOGUS_HASH).unwrap();

    let begin_block_result = stage.begin_block(SECOND_BANK_ID, SLOT, BOGUS_HASH);
    assert_matches!(begin_block_result, Err(BeginBlockError::SlotIdOccupied));

    let session: BlockVerificationSession<AllEntriesSubmitted> = session.mark_completed().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Completed
    );

    let session: BlockVerificationSession<Started> =
        stage.begin_block(SECOND_BANK_ID, SLOT, BOGUS_HASH).unwrap();

    let session: BlockVerificationSession<AllEntriesSubmitted> = session.mark_completed().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Completed
    );
}

#[test]
fn reusing_bank_id_causes_failure() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const BANK_ID: BankId = 123;
    const FIRST_SLOT: Slot = 120;
    const SECOND_SLOT: Slot = 121;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, FIRST_SLOT, BOGUS_HASH).unwrap();

    let begin_block_result = stage.begin_block(BANK_ID, SECOND_SLOT, BOGUS_HASH);
    assert_matches!(begin_block_result, Err(BeginBlockError::BankIdOccupied));

    let session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Aborted
    );

    // The bank id is released once the first block is aborted, so it can be registered again.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SECOND_SLOT, BOGUS_HASH).unwrap();

    let session: BlockVerificationSession<AllEntriesSubmitted> = session.mark_completed().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Completed
    );
}

#[test]
fn dropping_started_session_releases_bank_id_and_slot() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    // Dropping the session sends an abort, which the scheduler handles before the next begin.
    drop(session);

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    let session: BlockVerificationSession<AllEntriesSubmitted> = session.mark_completed().unwrap();

    let block_verification_outcome = session.wait_for_outcome().unwrap();

    assert_eq!(
        block_verification_outcome,
        BlockVerificationOutcome::Completed
    );
}

#[test]
fn shutdown_stops_scheduler_with_block_in_progress() {
    let entry_hash_verifier = MockVerifyEntryHash::default();
    let signature_verifier = MockVerifySignature::default();

    let stage: BlockVerificationStage = run_scheduler(
        TEST_SCHEDULER_CONFIG,
        signature_verifier,
        entry_hash_verifier,
    );

    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    // The session still holds a sender, so only the shutdown token stops the scheduler.
    stage.shutdown().unwrap();

    assert_matches!(session.mark_completed(), Err(SchedulerShutdownError));
}
