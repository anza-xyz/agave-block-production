use {
    crate::tpu_ingress::TpuTransactionMeta,
    agave_scheduler_bindings::{PackToCheckWorkerMessage, check_message_flags},
    agave_scheduling_utils::transaction_ptr::{OwnedTransactionPtr, TransactionPtrBatch},
    core::mem::ManuallyDrop,
    rts_alloc::Allocator,
};

pub(super) const MAX_PACKETS_PER_CHECK_BATCH: usize = 16;
pub(super) const CHECK_FLAGS: u16 = check_message_flags::AGE_NONCE_CHECKS
    | check_message_flags::LOAD_FEE_PAYER_BALANCE
    | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES
    | check_message_flags::CALCULATE_SCHEDULING_DETAILS;

pub(super) type RawCheckBatch<'a> =
    TransactionPtrBatch<'a, TpuTransactionMeta, MAX_PACKETS_PER_CHECK_BATCH>;

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Dispatched {
    pub(super) batches: usize,
    pub(super) packets: usize,
}

/// Transactions not sent remain in the iterator.
pub(super) fn dispatch_to_check<'a>(
    allocator: &'a Allocator,
    queue: &shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    flags: u16,
    max_batches: usize,
    transactions: &mut impl Iterator<Item = (OwnedTransactionPtr<'a>, TpuTransactionMeta)>,
) -> Dispatched {
    let mut dispatched = Dispatched::default();
    'batches: while dispatched.batches < max_batches {
        let Some(first) = transactions.next() else {
            break;
        };
        let Some(mut batch) = CheckBatch::allocate(allocator) else {
            break;
        };
        let members = core::iter::once(first)
            .chain(transactions.by_ref().take(MAX_PACKETS_PER_CHECK_BATCH - 1));
        for (transaction, metadata) in members {
            if !batch.try_push(transaction, metadata) {
                break 'batches;
            }
        }
        let Some(count) = batch.try_send(queue, flags) else {
            break;
        };
        dispatched.batches = dispatched.batches.wrapping_add(1);
        dispatched.packets = dispatched.packets.saturating_add(count);
    }
    dispatched
}

/// Owns a batch container and its transactions, freeing both on drop unless sent to a queue.
struct CheckBatch<'a> {
    batch: ManuallyDrop<RawCheckBatch<'a>>,
}

impl<'a> CheckBatch<'a> {
    fn allocate(allocator: &'a Allocator) -> Option<Self> {
        RawCheckBatch::allocate(allocator).map(|batch| Self {
            batch: ManuallyDrop::new(batch),
        })
    }

    /// Transfers ownership to the batch, or drops the transaction if full.
    fn try_push(&mut self, transaction: OwnedTransactionPtr<'a>, meta: TpuTransactionMeta) -> bool {
        // SAFETY: the transaction owns initialized bytes in this allocator. On success the
        // batch takes ownership and keeps those bytes alive.
        if unsafe { self.batch.try_push(transaction.to_region(), meta) }.is_err() {
            return false;
        }
        let _ = transaction.into_inner();
        true
    }

    /// Returns the number of packets sent, or drops the batch on failure.
    ///
    /// On success, the scheduler must free the batch and transactions only after receiving
    /// the worker response.
    fn try_send(
        self,
        queue: &shaq::mpmc::Producer<PackToCheckWorkerMessage>,
        flags: u16,
    ) -> Option<usize> {
        let count = self.batch.len();
        let message = PackToCheckWorkerMessage {
            batch: self.batch.to_sharable_transaction_batch_region(),
            flags,
        };
        queue.try_write(message).ok()?;
        core::mem::forget(self);
        Some(count)
    }
}

impl Drop for CheckBatch<'_> {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the container and its transactions. The inner batch is taken
        // only during drop and is never accessed again.
        let batch = unsafe { ManuallyDrop::take(&mut self.batch) };
        // SAFETY: this wrapper exclusively owns the batch and its transaction allocations.
        unsafe { batch.free_with_transactions() };
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::tests::setup, agave_scheduler_bindings::SharableTransactionRegion};

    fn transaction(allocator: &Allocator) -> OwnedTransactionPtr<'_> {
        let ptr = allocator.allocate(1).unwrap();
        // SAFETY: the freshly allocated byte is writable, belongs to this allocator, and is
        // exclusively owned by the returned pointer.
        unsafe {
            ptr.write(0);
            OwnedTransactionPtr::from_region(
                SharableTransactionRegion {
                    offset: allocator.offset(ptr),
                    length: 1,
                },
                allocator,
            )
        }
    }

    #[test]
    fn dispatch_sends_flags_and_leaves_unsent_transactions() {
        const FLAGS: u16 = check_message_flags::STATUS_CACHE_CHECKS;
        let (scheduler, agave) = setup(2);
        let meta = TpuTransactionMeta {
            flags: 0,
            src_addr: [0; 16],
        };
        let mut transactions = (0..2 * MAX_PACKETS_PER_CHECK_BATCH + 1)
            .map(|_| (transaction(&scheduler.allocator), meta));

        let dispatched = dispatch_to_check(
            &scheduler.allocator,
            &scheduler.check_sender,
            FLAGS,
            1,
            &mut transactions,
        );
        assert_eq!(
            dispatched,
            Dispatched {
                batches: 1,
                packets: MAX_PACKETS_PER_CHECK_BATCH,
            }
        );
        assert_eq!(transactions.count(), MAX_PACKETS_PER_CHECK_BATCH + 1);

        let receiver = &agave.check_workers[0].pack_to_check_worker;
        let message = receiver.try_read().unwrap();
        assert_eq!(message.flags, FLAGS);
        assert!(receiver.try_read().is_none());
        // SAFETY: no workers run here; the received batch is exclusively owned by this test.
        let batch = unsafe {
            RawCheckBatch::from_sharable_transaction_batch_region(
                &message.batch,
                &scheduler.allocator,
            )
        };
        assert_eq!(batch.len(), MAX_PACKETS_PER_CHECK_BATCH);
        // SAFETY: this test exclusively owns the batch and its transactions.
        unsafe { batch.free_with_transactions() };
        assert_eq!(scheduler.allocator.outstanding_allocation_bytes(), 0);
    }
}
