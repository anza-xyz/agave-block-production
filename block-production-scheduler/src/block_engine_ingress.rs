use {
    crate::{
        Scheduler,
        block_engine_verifier::VerifiedMessage,
        check_batch::{CHECK_FLAGS, dispatch_to_check},
        tpu_ingress::{TpuTransactionMeta, should_accept_packets},
    },
    agave_scheduling_utils::transaction_ptr::{OwnedTransactionPtr, TransactionPtr},
    core::num::NonZeroUsize,
    rts_alloc::Allocator,
    solana_perf::packet::BytesPacket,
};

const MAX_BLOCK_ENGINE_PACKETS_PER_ITERATION: NonZeroUsize = NonZeroUsize::new(256).unwrap();
const BLOCK_ENGINE_PACKET_META: TpuTransactionMeta = TpuTransactionMeta {
    flags: 0,
    src_addr: [0; 16],
};

impl Scheduler {
    pub(super) fn handle_block_engine_ingress(&mut self) {
        let Some(receiver) = self.block_engine_receiver.as_mut() else {
            return;
        };
        let accept_packets = should_accept_packets(&self.state);
        let metrics = &mut self.leader_slot_metrics;
        let mut received = 0usize;
        while received < MAX_BLOCK_ENGINE_PACKETS_PER_ITERATION.get() {
            // Unlike TPU, keep messages queued while checks are saturated.
            if accept_packets && self.outstanding_check_batches >= self.check_response_capacity {
                break;
            }
            let Some(message) = receiver.try_read() else {
                break;
            };
            match message {
                VerifiedMessage::Packets(batch) => {
                    received = received.saturating_add(batch.len());
                    let max_batches = if accept_packets {
                        self.check_response_capacity
                            .saturating_sub(self.outstanding_check_batches)
                    } else {
                        0
                    };
                    let mut transactions = batch.iter().filter_map(|packet| {
                        Some((
                            copy_to_allocator(&self.allocator, packet)?,
                            BLOCK_ENGINE_PACKET_META,
                        ))
                    });
                    let dispatched = dispatch_to_check(
                        &self.allocator,
                        &self.check_sender,
                        CHECK_FLAGS,
                        max_batches,
                        &mut transactions,
                    );
                    self.outstanding_check_batches = self
                        .outstanding_check_batches
                        .wrapping_add(dispatched.batches);
                    self.outstanding_check_packets = self
                        .outstanding_check_packets
                        .saturating_add(dispatched.packets);
                    metrics.block_engine_packets =
                        metrics.block_engine_packets.saturating_add(batch.len());
                    metrics.block_engine_dropped_packets = metrics
                        .block_engine_dropped_packets
                        .saturating_add(batch.len().saturating_sub(dispatched.packets));
                }
                VerifiedMessage::Bundle(bundle) => {
                    received = received.saturating_add(bundle.len());
                    metrics.block_engine_bundles = metrics.block_engine_bundles.saturating_add(1);
                    metrics.block_engine_dropped_bundles =
                        metrics.block_engine_dropped_bundles.saturating_add(1);
                }
            }
        }
    }
}

fn copy_to_allocator<'a>(
    allocator: &'a Allocator,
    packet: &BytesPacket,
) -> Option<OwnedTransactionPtr<'a>> {
    let data = packet.data(..)?;
    let ptr = allocator.allocate(u32::try_from(data.len()).ok()?)?;
    // SAFETY: the fresh allocation holds `data.len()` bytes, is exclusively ours and does not
    // overlap `data`.
    unsafe {
        core::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        Some(OwnedTransactionPtr::new(
            TransactionPtr::from_raw_parts(ptr, data.len()),
            allocator,
        ))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            check_batch::{MAX_PACKETS_PER_CHECK_BATCH, RawCheckBatch},
            progress_tracker::SchedulerState,
            tests::{setup, setup_with_capacities},
        },
        agave_scheduler_handshake::AgaveSession,
        core::ptr::NonNull,
        shaq::spsc::Producer,
        solana_perf::packet::BytesPacketBatch,
        std::assert_matches,
    };

    fn connect(scheduler: &mut Scheduler) -> Producer<VerifiedMessage> {
        let (producer, consumer) = shaq::spsc::pair(64).unwrap();
        scheduler.block_engine_receiver = Some(consumer);
        producer
    }

    fn payloads(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|index| vec![index as u8; 8]).collect()
    }

    fn batch_of(payloads: &[Vec<u8>]) -> BytesPacketBatch {
        payloads
            .iter()
            .map(|payload| BytesPacket::from_bytes(None, payload.clone()))
            .collect()
    }

    fn next_queued(scheduler: &mut Scheduler) -> Option<VerifiedMessage> {
        scheduler.block_engine_receiver.as_mut().unwrap().try_read()
    }

    fn drain_checks(scheduler: &Scheduler, agave: &AgaveSession) -> Vec<Vec<u8>> {
        let mut transactions = Vec::new();
        while let Some(message) = agave.check_workers[0].pack_to_check_worker.try_read() {
            assert_eq!(message.flags, CHECK_FLAGS);
            // SAFETY: no workers run here; the batch is exclusively owned by this test.
            let batch = unsafe {
                RawCheckBatch::from_sharable_transaction_batch_region(
                    &message.batch,
                    &scheduler.allocator,
                )
            };
            for (transaction, meta) in batch.iter() {
                assert_eq!(meta, BLOCK_ENGINE_PACKET_META);
                transactions.push(transaction.as_ref().to_vec());
            }
            // SAFETY: this test exclusively owns the batch and its transactions.
            unsafe { batch.free_with_transactions() };
        }
        transactions
    }

    fn exhaust_allocator(scheduler: &Scheduler) -> Vec<NonNull<u8>> {
        core::iter::from_fn(|| scheduler.allocator.allocate(8)).collect()
    }

    fn release(scheduler: &Scheduler, allocations: Vec<NonNull<u8>>) {
        for allocation in allocations {
            // SAFETY: each allocation is uniquely owned by this test and has not been freed.
            unsafe { scheduler.allocator.free(allocation) };
        }
    }

    #[test]
    fn ignores_missing_block_engine() {
        let (mut scheduler, agave) = setup(4);
        scheduler.handle_block_engine_ingress();
        assert!(drain_checks(&scheduler, &agave).is_empty());
    }

    #[test]
    fn forwards_packets_to_checks() {
        let (mut scheduler, agave) = setup(4);
        let mut block_engine = connect(&mut scheduler);
        let payloads = payloads(MAX_PACKETS_PER_CHECK_BATCH + 1);
        block_engine
            .try_write(VerifiedMessage::Packets(batch_of(&payloads)))
            .unwrap();

        scheduler.handle_block_engine_ingress();

        assert_eq!(scheduler.outstanding_check_batches, 2);
        assert_eq!(scheduler.outstanding_check_packets, payloads.len());
        let metrics = &scheduler.leader_slot_metrics;
        assert_eq!(metrics.block_engine_packets, payloads.len());
        assert_eq!(metrics.block_engine_dropped_packets, 0);
        assert_eq!(drain_checks(&scheduler, &agave), payloads);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn drains_and_drops_when_not_accepting() {
        let (mut scheduler, agave) = setup(4);
        scheduler.state = SchedulerState::new();
        let mut block_engine = connect(&mut scheduler);
        block_engine
            .try_write(VerifiedMessage::Packets(batch_of(&payloads(3))))
            .unwrap();
        block_engine
            .try_write(VerifiedMessage::Bundle(batch_of(&payloads(2))))
            .unwrap();

        scheduler.handle_block_engine_ingress();

        assert_matches!(next_queued(&mut scheduler), None);
        assert_eq!(scheduler.outstanding_check_packets, 0);
        let metrics = &scheduler.leader_slot_metrics;
        assert_eq!(metrics.block_engine_packets, 3);
        assert_eq!(metrics.block_engine_dropped_packets, 3);
        assert_eq!(metrics.block_engine_dropped_bundles, 1);
        assert!(drain_checks(&scheduler, &agave).is_empty());
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn leaves_messages_queued_when_checks_are_saturated() {
        let (mut scheduler, agave) = setup_with_capacities(4, 1, 1, 2, 2);
        let mut block_engine = connect(&mut scheduler);
        let payloads = payloads(MAX_PACKETS_PER_CHECK_BATCH + 1);
        block_engine
            .try_write(VerifiedMessage::Packets(batch_of(&payloads)))
            .unwrap();
        block_engine
            .try_write(VerifiedMessage::Packets(batch_of(&payloads[..1])))
            .unwrap();

        scheduler.handle_block_engine_ingress();

        // The first message fills the only response slot; its remainder is dropped and the
        // second message waits for capacity.
        assert_matches!(
            next_queued(&mut scheduler),
            Some(VerifiedMessage::Packets(_))
        );
        assert_matches!(next_queued(&mut scheduler), None);
        assert_eq!(scheduler.outstanding_check_batches, 1);
        let metrics = &scheduler.leader_slot_metrics;
        assert_eq!(metrics.block_engine_packets, payloads.len());
        assert_eq!(metrics.block_engine_dropped_packets, 1);
        assert_eq!(
            drain_checks(&scheduler, &agave),
            payloads[..MAX_PACKETS_PER_CHECK_BATCH]
        );
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn drops_bundles_until_bundle_checks_exist() {
        let (mut scheduler, agave) = setup(4);
        let mut block_engine = connect(&mut scheduler);
        block_engine
            .try_write(VerifiedMessage::Bundle(batch_of(&payloads(2))))
            .unwrap();

        scheduler.handle_block_engine_ingress();

        assert_matches!(next_queued(&mut scheduler), None);
        let metrics = &scheduler.leader_slot_metrics;
        assert_eq!(metrics.block_engine_bundles, 1);
        assert_eq!(metrics.block_engine_dropped_bundles, 1);
        assert!(drain_checks(&scheduler, &agave).is_empty());
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn limits_packets_per_iteration() {
        let (mut scheduler, agave) = setup(64);
        let mut block_engine = connect(&mut scheduler);
        let batch = batch_of(&payloads(MAX_PACKETS_PER_CHECK_BATCH));
        let messages =
            MAX_BLOCK_ENGINE_PACKETS_PER_ITERATION.get() / MAX_PACKETS_PER_CHECK_BATCH + 1;
        for _ in 0..messages {
            block_engine
                .try_write(VerifiedMessage::Packets(batch.clone()))
                .unwrap();
        }

        scheduler.handle_block_engine_ingress();
        assert_eq!(
            scheduler.leader_slot_metrics.block_engine_packets,
            MAX_BLOCK_ENGINE_PACKETS_PER_ITERATION.get()
        );

        scheduler.handle_block_engine_ingress();
        assert_eq!(
            scheduler.leader_slot_metrics.block_engine_packets,
            messages * MAX_PACKETS_PER_CHECK_BATCH
        );
        assert_matches!(next_queued(&mut scheduler), None);
        assert_eq!(
            drain_checks(&scheduler, &agave).len(),
            messages * MAX_PACKETS_PER_CHECK_BATCH
        );
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn frees_packets_when_allocation_fails() {
        let (mut scheduler, agave) = setup(4);
        let mut block_engine = connect(&mut scheduler);
        let allocations = exhaust_allocator(&scheduler);
        block_engine
            .try_write(VerifiedMessage::Packets(batch_of(&payloads(2))))
            .unwrap();

        scheduler.handle_block_engine_ingress();

        assert_eq!(scheduler.outstanding_check_packets, 0);
        assert_eq!(
            scheduler.leader_slot_metrics.block_engine_dropped_packets,
            2
        );
        assert!(drain_checks(&scheduler, &agave).is_empty());
        release(&scheduler, allocations);
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }
}
