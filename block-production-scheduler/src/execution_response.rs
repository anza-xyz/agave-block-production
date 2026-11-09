use {
    crate::{
        Scheduler, check_response::TransactionState, schedule::ExecutionBatch,
        transaction_container::TransactionContainer,
    },
    agave_scheduler_bindings::{
        ExecutionWorkerToPackMessage, processed_codes,
        worker_message_types::{ExecutionResponse, not_included_reasons},
    },
    agave_scheduling_utils::{
        responses_region::ExecutionResponsesPtr,
        thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadId},
    },
    rts_alloc::Allocator,
};

const MAX_EXECUTION_RESPONSE_PACKETS_PER_ITERATION: usize = 1024;

impl Scheduler {
    pub(super) fn handle_execution_worker_responses(&mut self) {
        let mut received = 0usize;
        for worker in 0..self.workers.len() {
            while let Some(message) = self.workers[worker].worker_to_pack.try_read() {
                received = received.saturating_add(usize::from(message.batch.num_transactions));
                self.handle_execution_worker_response(worker, message);
                if received >= MAX_EXECUTION_RESPONSE_PACKETS_PER_ITERATION {
                    return;
                }
            }
        }
    }

    fn handle_execution_worker_response(
        &mut self,
        worker: ThreadId,
        message: ExecutionWorkerToPackMessage,
    ) {
        // SAFETY: the worker returns our batch with its metadata intact and no remaining accesses.
        let batch = unsafe {
            ExecutionBatch::from_sharable_transaction_batch_region(&message.batch, &self.allocator)
        };
        let num_transactions = batch.len();
        let (_, first) = batch
            .iter()
            .next()
            .expect("published batches are never empty");
        if self
            .transactions
            .get(first)
            .expect("in-flight batch members stay in the container until completed")
            .is_bundle()
        {
            // SAFETY: the bundle stays retained by its id; the returned container is exclusively ours.
            unsafe { batch.free() };
            let completed = self.complete_bundle(worker, &message, first, num_transactions);
            self.finish_batch(worker, num_transactions, completed);
            return;
        }
        let mut completed = CompletedBatch::default();
        if message.processed_code == processed_codes::PROCESSED {
            assert_eq!(
                usize::from(message.responses.num_transaction_responses),
                batch.len(),
                "execution worker invariant violated: response count must match batch length"
            );
            // SAFETY: processed messages transfer an initialized response allocation to us.
            let responses = unsafe {
                ExecutionResponsesPtr::from_transaction_response_region(
                    &message.responses,
                    &self.allocator,
                )
            };
            for ((_, id), response) in batch.iter().zip(responses.iter()) {
                let action = ExecutionResponseAction::from_response(response);
                if response.not_included_reason == not_included_reasons::NONE {
                    completed.actual_cost =
                        completed.actual_cost.saturating_add(response.cost_units);
                    self.leader_slot_metrics.completed_transactions = self
                        .leader_slot_metrics
                        .completed_transactions
                        .saturating_add(1);
                } else if matches!(action, ExecutionResponseAction::Release) {
                    completed.dropped = completed.dropped.saturating_add(1);
                }
                completed.estimated_cost =
                    completed
                        .estimated_cost
                        .saturating_add(complete_transaction(
                            id,
                            worker,
                            action,
                            &mut self.transactions,
                            &mut self.account_locks,
                            &mut self.deferred_execution,
                            &self.allocator,
                        ));
            }
            // SAFETY: all responses have been consumed and the allocation is exclusively ours.
            unsafe { responses.free(&self.allocator) };
        } else {
            // Unprocessed messages have an undefined response region.
            let action = ExecutionResponseAction::from_processed_code(message.processed_code);
            if matches!(action, ExecutionResponseAction::Release) {
                completed.dropped = completed.dropped.saturating_add(batch.len());
            }
            for (_, id) in batch.iter() {
                completed.estimated_cost =
                    completed
                        .estimated_cost
                        .saturating_add(complete_transaction(
                            id,
                            worker,
                            action,
                            &mut self.transactions,
                            &mut self.account_locks,
                            &mut self.deferred_execution,
                            &self.allocator,
                        ));
            }
        }
        // SAFETY: transactions have been retained or freed; the returned container is exclusively ours.
        unsafe { batch.free() };
        self.finish_batch(worker, num_transactions, completed);
    }

    /// Completes a bundle once from its folded responses.
    fn complete_bundle(
        &mut self,
        worker: ThreadId,
        message: &ExecutionWorkerToPackMessage,
        id: usize,
        num_transactions: usize,
    ) -> CompletedBatch {
        let mut actual_cost = 0u64;
        let mut committed = false;
        let action = if message.processed_code == processed_codes::PROCESSED {
            assert_eq!(
                usize::from(message.responses.num_transaction_responses),
                num_transactions,
                "execution worker invariant violated: response count must match batch length"
            );
            // SAFETY: processed messages transfer an initialized response allocation to us.
            let responses = unsafe {
                ExecutionResponsesPtr::from_transaction_response_region(
                    &message.responses,
                    &self.allocator,
                )
            };
            // All-or-nothing: members are either all committed or none are.
            committed = responses
                .iter()
                .next()
                .is_some_and(|response| response.not_included_reason == not_included_reasons::NONE);
            let action = if committed {
                for response in responses.iter() {
                    actual_cost = actual_cost.saturating_add(response.cost_units);
                }
                self.leader_slot_metrics.completed_transactions = self
                    .leader_slot_metrics
                    .completed_transactions
                    .saturating_add(num_transactions);
                ExecutionResponseAction::Release
            } else {
                // Only the failing member reports why the bundle was not committed.
                responses
                    .iter()
                    .find(|response| {
                        response.not_included_reason
                            != not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE
                    })
                    .map_or(
                        ExecutionResponseAction::Release,
                        ExecutionResponseAction::from_response,
                    )
            };
            // SAFETY: all responses have been consumed and the allocation is exclusively ours.
            unsafe { responses.free(&self.allocator) };
            action
        } else {
            // Unprocessed messages have an undefined response region.
            ExecutionResponseAction::from_processed_code(message.processed_code)
        };
        let dropped = if !committed && matches!(action, ExecutionResponseAction::Release) {
            num_transactions
        } else {
            0
        };
        let estimated_cost = complete_transaction(
            id,
            worker,
            action,
            &mut self.transactions,
            &mut self.account_locks,
            &mut self.deferred_execution,
            &self.allocator,
        );
        CompletedBatch {
            estimated_cost,
            actual_cost,
            dropped,
        }
    }

    fn finish_batch(
        &mut self,
        worker: ThreadId,
        num_transactions: usize,
        completed: CompletedBatch,
    ) {
        self.in_flight
            .complete_batch(worker, num_transactions, completed.estimated_cost);
        self.leader_slot_metrics.dropped_transactions = self
            .leader_slot_metrics
            .dropped_transactions
            .saturating_add(completed.dropped);
        self.scheduled_cost = self
            .scheduled_cost
            .saturating_sub(completed.estimated_cost)
            .saturating_add(completed.actual_cost);
    }
}

#[derive(Default)]
struct CompletedBatch {
    estimated_cost: u64,
    actual_cost: u64,
    dropped: usize,
}

fn complete_transaction(
    id: usize,
    worker: ThreadId,
    action: ExecutionResponseAction,
    transactions: &mut TransactionContainer<TransactionState>,
    account_locks: &mut ThreadAwareAccountLocks,
    deferred_execution: &mut Vec<usize>,
    allocator: &Allocator,
) -> u64 {
    let transaction = transactions.get_mut(id).unwrap();
    assert_eq!(transaction.execution_worker.take(), Some(worker));
    let cost = transaction.cost;
    account_locks.unlock_accounts(transaction.account_keys_with_writable(), worker);
    match action {
        ExecutionResponseAction::RequeueInCurrentSlot => {
            transactions.requeue(id);
        }
        ExecutionResponseAction::DeferToNextSlot => deferred_execution.push(id),
        ExecutionResponseAction::Release => {
            let transaction = transactions.remove(id).unwrap();
            // SAFETY: the worker and scheduler have released all references to this transaction.
            unsafe { transaction.free(allocator) };
        }
    }
    cost
}

/// What happens to a transaction once its execution batch returns.
#[derive(Clone, Copy)]
enum ExecutionResponseAction {
    RequeueInCurrentSlot,
    DeferToNextSlot,
    Release,
}

impl ExecutionResponseAction {
    fn from_response(response: &ExecutionResponse) -> Self {
        match response.not_included_reason {
            not_included_reasons::BANK_NOT_AVAILABLE | not_included_reasons::ACCOUNT_IN_USE => {
                Self::RequeueInCurrentSlot
            }
            not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
            | not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
            | not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
            | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT => Self::DeferToNextSlot,
            _ => Self::Release,
        }
    }

    fn from_processed_code(code: u8) -> Self {
        match code {
            processed_codes::MAX_WORKING_SLOT_EXCEEDED => Self::RequeueInCurrentSlot,
            _ => Self::Release,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            check_response::BUNDLE_EXECUTION_FLAGS,
            progress_tracker::SchedulerState,
            tests::{insert, insert_bundle, setup_with_workers},
        },
        agave_scheduler_bindings::{
            ExecutionResponseRegion, PackToExecutionWorkerMessage, SharableTransactionBatchRegion,
            worker_message_types::{
                execution_statuses,
                not_included_reasons::{
                    ACCOUNT_IN_USE, ALL_OR_NOTHING_BATCH_FAILURE, BLOCKHASH_NOT_FOUND,
                    WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT,
                },
            },
        },
        agave_scheduler_handshake::AgaveSession,
        agave_scheduling_utils::{
            responses_region::execution_responses_from_iter, thread_aware_account_locks::ThreadSet,
        },
        core::time::Duration,
        solana_pubkey::Pubkey,
        std::time::Instant,
        test_case::test_case,
    };

    fn batch(
        scheduler: &mut Scheduler,
        worker: usize,
        ids: &[usize],
    ) -> SharableTransactionBatchRegion {
        let mut batch = ExecutionBatch::allocate(&scheduler.allocator).unwrap();
        let mut cost = 0u64;
        let mut num_transactions = 0usize;
        for &id in ids {
            let priority_id = *scheduler
                .transactions
                .iter_by_priority()
                .find(|entry| entry.id == id)
                .unwrap();
            scheduler.transactions.dequeue(&priority_id);
            let state = scheduler.transactions.get_mut(id).unwrap();
            state.execution_worker = Some(worker);
            scheduler
                .account_locks
                .try_lock_accounts(
                    state.account_keys_with_writable(),
                    ThreadSet::any(scheduler.workers.len()),
                    |_| worker,
                )
                .unwrap();
            // SAFETY: the retained transactions belong to this allocator.
            for region in unsafe { state.to_regions(&scheduler.allocator) } {
                // SAFETY: the initialized transaction remains retained until its response is handled.
                unsafe { batch.try_push(region, id) }.unwrap();
                num_transactions = num_transactions.saturating_add(1);
            }
            cost = cost.saturating_add(state.cost);
        }
        scheduler
            .in_flight
            .track_batch(worker, num_transactions, cost);
        scheduler.scheduled_cost = scheduler.scheduled_cost.saturating_add(cost);
        batch.to_sharable_transaction_batch_region()
    }

    fn response(reason: u8, cost_units: u64) -> ExecutionResponse {
        ExecutionResponse {
            execution_slot: 100,
            not_included_reason: reason,
            execution_status: if reason == not_included_reasons::NONE {
                execution_statuses::SUCCESS
            } else {
                execution_statuses::NOT_INCLUDED
            },
            cost_units,
            fee_payer_balance: 0,
        }
    }

    fn respond(
        scheduler: &Scheduler,
        agave: &mut AgaveSession,
        worker: usize,
        batch: SharableTransactionBatchRegion,
        responses: &[ExecutionResponse],
    ) {
        let responses =
            execution_responses_from_iter(&scheduler.allocator, responses.iter().copied()).unwrap();
        agave.workers[worker]
            .worker_to_pack
            .try_write(ExecutionWorkerToPackMessage {
                batch,
                responses,
                processed_code: processed_codes::PROCESSED,
            })
            .unwrap();
    }

    /// Asserts no worker holds a lock on `keys` by write-locking them on worker 1.
    fn assert_unlocked(scheduler: &mut Scheduler, keys: &[u8]) {
        let keys: Vec<_> = keys.iter().map(|&key| Pubkey::from([key; 32])).collect();
        let locks = || keys.iter().map(|key| (key, true));
        assert_eq!(
            scheduler
                .account_locks
                .try_lock_accounts(locks(), ThreadSet::only(1), |_| 1),
            Ok(1)
        );
        scheduler.account_locks.unlock_accounts(locks(), 1);
    }

    #[test]
    fn completes_and_retries_returned_transactions() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        scheduler.handle_slot_change();
        let included = insert(&mut scheduler, 4, 100, &[1]);
        let immediate = insert(&mut scheduler, 3, 100, &[2]);
        let deferred = insert(&mut scheduler, 2, 100, &[3]);
        let rejected = insert(&mut scheduler, 1, 100, &[4]);
        let batch = batch(
            &mut scheduler,
            0,
            &[included, immediate, deferred, rejected],
        );
        let responses = execution_responses_from_iter(
            &scheduler.allocator,
            [
                response(not_included_reasons::NONE, 60),
                response(not_included_reasons::ACCOUNT_IN_USE, 0),
                response(not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT, 0),
                response(not_included_reasons::BLOCKHASH_NOT_FOUND, 0),
            ]
            .into_iter(),
        )
        .unwrap();
        agave.workers[0]
            .worker_to_pack
            .try_write(ExecutionWorkerToPackMessage {
                batch,
                responses,
                processed_code: processed_codes::PROCESSED,
            })
            .unwrap();
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.in_flight.worker_load(0).cost_units, 0);
        assert_eq!(scheduler.scheduled_cost, 60);
        assert!(scheduler.transactions.get(included).is_none());
        assert!(scheduler.transactions.get(rejected).is_none());
        assert_eq!(
            scheduler
                .transactions
                .get(immediate)
                .unwrap()
                .execution_worker,
            None
        );
        assert_eq!(
            scheduler
                .transactions
                .get(deferred)
                .unwrap()
                .execution_worker,
            None
        );
        assert_eq!(
            scheduler
                .transactions
                .iter_by_priority()
                .map(|entry| entry.id)
                .collect::<Vec<_>>(),
            [immediate]
        );
        assert_eq!(scheduler.deferred_execution, [deferred]);
        let keys = [1, 2, 3, 4].map(|key| Pubkey::from([key; 32]));
        scheduler
            .account_locks
            .try_lock_accounts(
                keys.iter().map(|key| (key, true)),
                ThreadSet::any(2),
                |eligible| {
                    assert!(eligible.contains(1));
                    1
                },
            )
            .unwrap();
        scheduler
            .account_locks
            .unlock_accounts(keys.iter().map(|key| (key, true)), 1);

        scheduler.handle_slot_change();
        assert_eq!(scheduler.deferred_execution, [deferred]);
        scheduler.state = SchedulerState::LeaderReady {
            slot: 101,
            remaining_cost_units: 1_000,
            slot_start: Instant::now(),
            slot_duration: Duration::from_millis(400),
        };
        scheduler.handle_slot_change();
        assert!(scheduler.deferred_execution.is_empty());
        assert_eq!(scheduler.scheduled_cost, 0);
        assert_eq!(scheduler.transactions.pop_highest(), Some(immediate));
        assert_eq!(scheduler.transactions.pop_highest(), Some(deferred));
    }

    #[test]
    fn unprocessed_batches_release_workers_without_reading_response_regions() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        let retry = insert(&mut scheduler, 2, 100, &[1]);
        let invalid = insert(&mut scheduler, 1, 100, &[2]);
        for (worker, id, processed_code) in [
            (0, retry, processed_codes::MAX_WORKING_SLOT_EXCEEDED),
            (1, invalid, processed_codes::INVALID),
        ] {
            let batch = batch(&mut scheduler, worker, &[id]);
            agave.workers[worker]
                .worker_to_pack
                .try_write(ExecutionWorkerToPackMessage {
                    batch,
                    processed_code,
                    responses: ExecutionResponseRegion {
                        num_transaction_responses: 0,
                        transaction_responses_offset: usize::MAX,
                    },
                })
                .unwrap();
        }
        scheduler.handle_execution_worker_responses();
        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.scheduled_cost, 0);
        assert_eq!(scheduler.transactions.pop_highest(), Some(retry));
        assert!(scheduler.transactions.get(invalid).is_none());
        assert!(scheduler.deferred_execution.is_empty());
    }

    #[test]
    fn completes_committed_bundle_and_releases_duplicate_keys() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        let bundle = insert_bundle(
            &mut scheduler,
            1,
            100,
            &[(&[1, 5, 7], &[6, 8]), (&[2, 6, 7], &[5, 8])],
        );
        let batch = batch(&mut scheduler, 0, &[bundle]);
        respond(
            &scheduler,
            &mut agave,
            0,
            batch,
            &[
                response(not_included_reasons::NONE, 30),
                response(not_included_reasons::NONE, 40),
            ],
        );
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.transactions.get(bundle).is_none());
        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.scheduled_cost, 70);
        assert_eq!(scheduler.leader_slot_metrics.completed_transactions, 2);
        assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 0);
        assert_unlocked(&mut scheduler, &[1, 2, 5, 6, 7, 8]);
    }

    enum Outcome {
        Requeued,
        Deferred,
        Dropped,
    }

    #[test_case([ALL_OR_NOTHING_BATCH_FAILURE, ACCOUNT_IN_USE], Outcome::Requeued; "requeues_on_account_in_use")]
    #[test_case([ALL_OR_NOTHING_BATCH_FAILURE, WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT], Outcome::Deferred; "defers_on_account_cost_limit")]
    #[test_case([BLOCKHASH_NOT_FOUND, ALL_OR_NOTHING_BATCH_FAILURE], Outcome::Dropped; "drops_on_blockhash_not_found")]
    #[test_case([ALL_OR_NOTHING_BATCH_FAILURE, ALL_OR_NOTHING_BATCH_FAILURE], Outcome::Dropped; "drops_without_failing_reason")]
    fn handles_uncommitted_bundle_by_failing_member_reason(reasons: [u8; 2], outcome: Outcome) {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        let bundle = insert_bundle(&mut scheduler, 1, 100, &[(&[1], &[]), (&[2], &[])]);
        let batch = batch(&mut scheduler, 0, &[bundle]);
        respond(
            &scheduler,
            &mut agave,
            0,
            batch,
            &reasons.map(|reason| response(reason, 0)),
        );
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.scheduled_cost, 0);
        assert_eq!(scheduler.leader_slot_metrics.completed_transactions, 0);
        assert_unlocked(&mut scheduler, &[1, 2]);
        match outcome {
            Outcome::Requeued => {
                assert_eq!(
                    scheduler.transactions.get(bundle).unwrap().execution_worker,
                    None
                );
                assert_eq!(scheduler.transactions.pop_highest(), Some(bundle));
                assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 0);
            }
            Outcome::Deferred => {
                assert_eq!(scheduler.deferred_execution, [bundle]);
                assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 0);
            }
            Outcome::Dropped => {
                assert!(scheduler.transactions.get(bundle).is_none());
                assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 2);
            }
        }
    }

    #[test]
    fn unprocessed_bundles_follow_processed_code() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        let retry = insert_bundle(&mut scheduler, 2, 100, &[(&[1], &[]), (&[2], &[])]);
        let invalid = insert_bundle(&mut scheduler, 1, 100, &[(&[3], &[]), (&[4], &[])]);
        for (worker, id, processed_code) in [
            (0, retry, processed_codes::MAX_WORKING_SLOT_EXCEEDED),
            (1, invalid, processed_codes::INVALID),
        ] {
            let batch = batch(&mut scheduler, worker, &[id]);
            agave.workers[worker]
                .worker_to_pack
                .try_write(ExecutionWorkerToPackMessage {
                    batch,
                    processed_code,
                    responses: ExecutionResponseRegion {
                        num_transaction_responses: 0,
                        transaction_responses_offset: usize::MAX,
                    },
                })
                .unwrap();
        }
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.scheduled_cost, 0);
        assert_eq!(scheduler.transactions.pop_highest(), Some(retry));
        assert!(scheduler.transactions.get(invalid).is_none());
        assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 2);
        assert_unlocked(&mut scheduler, &[1, 2, 3, 4]);
    }

    #[test]
    fn handles_bundle_and_transaction_batches_in_one_drain() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        let bundle = insert_bundle(&mut scheduler, 3, 100, &[(&[1], &[]), (&[2], &[])]);
        let included = insert(&mut scheduler, 2, 100, &[3]);
        let rejected = insert(&mut scheduler, 1, 100, &[4]);
        let bundle_batch = batch(&mut scheduler, 0, &[bundle]);
        let transaction_batch = batch(&mut scheduler, 0, &[included, rejected]);
        respond(
            &scheduler,
            &mut agave,
            0,
            bundle_batch,
            &[
                response(not_included_reasons::NONE, 10),
                response(not_included_reasons::NONE, 20),
            ],
        );
        respond(
            &scheduler,
            &mut agave,
            0,
            transaction_batch,
            &[
                response(not_included_reasons::NONE, 5),
                response(not_included_reasons::BLOCKHASH_NOT_FOUND, 0),
            ],
        );
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.scheduled_cost, 35);
        assert!(scheduler.transactions.get(bundle).is_none());
        assert!(scheduler.transactions.get(included).is_none());
        assert!(scheduler.transactions.get(rejected).is_none());
        assert_eq!(scheduler.leader_slot_metrics.completed_transactions, 3);
        assert_eq!(scheduler.leader_slot_metrics.dropped_transactions, 1);
        assert_unlocked(&mut scheduler, &[1, 2, 3, 4]);
    }

    fn start_leader_slot(scheduler: &mut Scheduler, slot: u64) {
        scheduler.state = SchedulerState::LeaderReady {
            slot,
            remaining_cost_units: 1_000,
            slot_start: Instant::now(),
            slot_duration: Duration::from_millis(400),
        };
        scheduler.handle_slot_change();
    }

    /// Takes every batch the scheduler has published, with its worker.
    fn take_published(agave: &mut AgaveSession) -> Vec<(usize, PackToExecutionWorkerMessage)> {
        let mut published = Vec::new();
        for (worker, session) in agave.workers.iter_mut().enumerate() {
            while let Some(message) = session.pack_to_worker.try_read() {
                published.push((worker, message));
            }
        }
        published
    }

    /// Asserts the scheduler published exactly one batch, holding `bundle`, and returns it.
    fn take_bundle_batch(
        agave: &mut AgaveSession,
        slot: u64,
        bundle_len: u32,
    ) -> (usize, SharableTransactionBatchRegion) {
        let [(worker, message)] = take_published(agave).try_into().unwrap();
        assert_eq!(message.flags, BUNDLE_EXECUTION_FLAGS);
        assert_eq!(message.max_working_slot, slot);
        assert_eq!(u32::from(message.batch.num_transactions), bundle_len);
        (worker, message.batch)
    }

    #[test]
    fn requeued_bundle_is_rescheduled_as_a_bundle() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        start_leader_slot(&mut scheduler, 100);
        let bundle = insert_bundle(&mut scheduler, 2, 10, &[(&[1], &[]), (&[2], &[])]);
        let transaction = insert(&mut scheduler, 1, 10, &[3]);
        scheduler.schedule(1_000);

        let published = take_published(&mut agave);
        assert_eq!(published.len(), 2);
        for (worker, message) in published {
            let responses = if message.flags == BUNDLE_EXECUTION_FLAGS {
                vec![
                    response(not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE, 0),
                    response(not_included_reasons::ACCOUNT_IN_USE, 0),
                ]
            } else {
                vec![response(not_included_reasons::NONE, 5)]
            };
            respond(&scheduler, &mut agave, worker, message.batch, &responses);
        }
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.transactions.get(transaction).is_none());
        assert_eq!(
            scheduler.transactions.get(bundle).unwrap().execution_worker,
            None
        );
        assert_unlocked(&mut scheduler, &[1, 2, 3]);

        scheduler.schedule(1_000);
        let (worker, batch) = take_bundle_batch(&mut agave, 100, 2);
        respond(
            &scheduler,
            &mut agave,
            worker,
            batch,
            &[
                response(not_included_reasons::NONE, 4),
                response(not_included_reasons::NONE, 6),
            ],
        );
        scheduler.handle_execution_worker_responses();

        assert!(scheduler.transactions.get(bundle).is_none());
        assert!(scheduler.in_flight.is_empty());
        assert_eq!(scheduler.leader_slot_metrics.completed_transactions, 3);
        assert_eq!(scheduler.scheduled_cost, 15);
        assert_unlocked(&mut scheduler, &[1, 2]);
    }

    #[test]
    fn deferred_bundle_is_rescheduled_in_next_slot() {
        let (mut scheduler, mut agave) = setup_with_workers(2, 2);
        start_leader_slot(&mut scheduler, 100);
        let bundle = insert_bundle(&mut scheduler, 1, 10, &[(&[1], &[]), (&[2], &[])]);
        scheduler.schedule(1_000);
        let (worker, batch) = take_bundle_batch(&mut agave, 100, 2);
        respond(
            &scheduler,
            &mut agave,
            worker,
            batch,
            &[
                response(not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE, 0),
                response(not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT, 0),
            ],
        );
        scheduler.handle_execution_worker_responses();
        assert_eq!(scheduler.deferred_execution, [bundle]);

        scheduler.schedule(1_000);
        assert!(take_published(&mut agave).is_empty());

        start_leader_slot(&mut scheduler, 101);
        assert!(scheduler.deferred_execution.is_empty());
        scheduler.schedule(1_000);
        let (_, batch) = take_bundle_batch(&mut agave, 101, 2);
        // SAFETY: the scheduler published this batch; only the container is freed.
        unsafe {
            ExecutionBatch::from_sharable_transaction_batch_region(&batch, &scheduler.allocator)
                .free()
        };
    }
}
