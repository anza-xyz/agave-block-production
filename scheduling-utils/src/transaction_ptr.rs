use {
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionBatchRegion, SharableTransactionRegion,
    },
    agave_transaction_view::transaction_data::TransactionData,
    core::ptr::NonNull,
    rts_alloc::Allocator,
    std::marker::PhantomData,
};

#[derive(Debug)]
pub struct TransactionPtr {
    ptr: NonNull<u8>,
    count: usize,
}

impl TransactionData for TransactionPtr {
    fn data(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }
}

impl TransactionData for &TransactionPtr {
    fn data(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }
}

impl TransactionPtr {
    /// Constructions a [`TransactionPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_sharable_transaction_region`].
    pub unsafe fn from_raw_parts(ptr: NonNull<u8>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// # Safety
    /// - `sharable_transaction_region` must reference a valid offset and length
    ///   within the `allocator`.
    pub unsafe fn from_sharable_transaction_region(
        sharable_transaction_region: &SharableTransactionRegion,
        allocator: &Allocator,
    ) -> Self {
        // SAFETY: `sharable_transaction_region.offset` was allocated by `allocator`.
        let ptr = unsafe { allocator.ptr_from_offset(sharable_transaction_region.offset) };
        Self {
            ptr,
            count: sharable_transaction_region.length as usize,
        }
    }

    /// Translate the ptr type into a sharable region.
    ///
    /// # Safety
    /// - `allocator` must be the allocator owning the memory region pointed
    ///   to by `self`.
    pub unsafe fn to_sharable_transaction_region(
        &self,
        allocator: &Allocator,
    ) -> SharableTransactionRegion {
        // SAFETY: The `TransactionPtr` creation `Self::from_sharable_transaction_region`
        // is already conditioned on the offset being valid, if that safety constraint
        // was satisfied translation back to offset is safe.
        let offset = unsafe { allocator.offset(self.ptr) };
        SharableTransactionRegion {
            offset,
            length: self.count as u32,
        }
    }

    /// Frees the memory region pointed to in the `allocator`.
    /// This should only be called by the owner of the memory
    /// i.e. the external scheduler.
    ///
    /// # Safety
    /// - Data region pointed to by `TransactionPtr` belongs to the `allocator`.
    /// - Inner `ptr` must not have been previously freed.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr) }
    }
}

/// A batch of transaction pointers that can be iterated over.
///
/// `CAPACITY` determines the fixed position of the metadata array within the backing allocation.
/// Metadata readers and writers must use the same `CAPACITY`.
pub struct TransactionPtrBatch<'a, M = (), const CAPACITY: usize = MAX_TRANSACTIONS_PER_MESSAGE> {
    tx_ptr: NonNull<SharableTransactionRegion>,
    meta_ptr: NonNull<M>,
    num_transactions: usize,
    allocator: &'a Allocator,

    _meta: PhantomData<M>,
}

/// The requested transactions exceed the batch's remaining capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchCapacityError;

impl core::fmt::Display for BatchCapacityError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("transaction batch capacity exceeded")
    }
}

impl core::error::Error for BatchCapacityError {}

impl<'a, M, const CAPACITY: usize> TransactionPtrBatch<'a, M, CAPACITY> {
    pub const TRANSACTION_CORE_SIZE: usize = size_of::<SharableTransactionRegion>();
    pub const TRANSACTION_CORE_END: usize = Self::TRANSACTION_CORE_SIZE * CAPACITY;

    pub const TRANSACTION_META_START: usize =
        Self::TRANSACTION_CORE_END.next_multiple_of(align_of::<M>());
    pub const TRANSACTION_META_SIZE: usize = size_of::<M>() * CAPACITY;
    pub const TRANSACTION_META_END: usize =
        Self::TRANSACTION_META_START + Self::TRANSACTION_META_SIZE;

    const TRANSACTION_BATCH_SIZE_ASSERT: () = {
        assert!(CAPACITY <= MAX_TRANSACTIONS_PER_MESSAGE);
        assert!(Self::TRANSACTION_META_END <= 4096);
    };

    /// Allocates a batch container for up to `CAPACITY` transaction regions and metadata values.
    pub fn allocate(allocator: &'a Allocator) -> Option<Self> {
        let () = Self::TRANSACTION_BATCH_SIZE_ASSERT;
        let allocation = allocator.allocate(Self::TRANSACTION_META_END as u32)?;
        let base = allocation;
        let tx_ptr = base.cast();
        // SAFETY: `Self::TRANSACTION_META_START` is within the allocation made above.
        let meta_ptr = unsafe { base.byte_add(Self::TRANSACTION_META_START).cast() };

        Some(Self {
            tx_ptr,
            meta_ptr,
            num_transactions: 0,
            allocator,

            _meta: PhantomData,
        })
    }

    /// # Safety
    /// - [`SharableTransactionBatchRegion`] must reference a valid offset and length
    ///   within the `allocator`.
    /// - ALL [`SharableTransactionRegion`]  within the batch must be valid.
    ///   See [`TransactionPtr::from_sharable_transaction_region`] for details.
    /// - `M` must match the actual `M` used within this allocation.
    /// - `CAPACITY` must match the capacity used when writing the metadata array.
    pub unsafe fn from_sharable_transaction_batch_region(
        sharable_transaction_batch_region: &SharableTransactionBatchRegion,
        allocator: &'a Allocator,
    ) -> Self {
        let () = Self::TRANSACTION_BATCH_SIZE_ASSERT;
        let num_transactions = usize::from(sharable_transaction_batch_region.num_transactions);
        assert!(
            num_transactions <= CAPACITY,
            "batch exceeds TransactionPtrBatch capacity"
        );
        // SAFETY: `sharable_transaction_batch_region.transactions_offset` was allocated by `allocator`.
        let base = unsafe {
            allocator.ptr_from_offset(sharable_transaction_batch_region.transactions_offset)
        };
        let tx_ptr = base.cast();
        // SAFETY:
        // - Assuming the batch was originally allocated to support `M`, this call will also be
        //   safe.
        let meta_ptr = unsafe { base.byte_add(Self::TRANSACTION_META_START).cast() };

        Self {
            tx_ptr,
            meta_ptr,
            num_transactions,
            allocator,

            _meta: PhantomData,
        }
    }

    /// The number of transactions in this batch.
    pub const fn len(&self) -> usize {
        self.num_transactions
    }

    /// Whether the batch is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends one transaction region and its associated metadata to the batch.
    ///
    /// Returns `Err` with the inputs unchanged when the batch is full.
    ///
    /// # Safety
    /// - `transaction` must reference valid, initialized bytes within this batch's allocator.
    /// - Those bytes must remain valid while the batch or its transaction pointers are used.
    pub unsafe fn push(
        &mut self,
        transaction: SharableTransactionRegion,
        meta: M,
    ) -> Result<(), (SharableTransactionRegion, M)>
    where
        M: Copy,
    {
        if self.num_transactions == CAPACITY {
            return Err((transaction, meta));
        }
        // SAFETY: `num_transactions` is strictly below this batch's capacity.
        unsafe {
            self.tx_ptr.add(self.num_transactions).write(transaction);
            self.meta_ptr.add(self.num_transactions).write(meta);
        }
        self.num_transactions = self.num_transactions.wrapping_add(1);
        Ok(())
    }

    /// Appends the transaction region and metadata produced by `map` for each item.
    ///
    /// If the entire slice does not fit, returns an error without calling `map` or modifying
    /// the batch. If `map` panics, previously appended entries remain initialized in the batch.
    ///
    /// # Safety
    /// - Every returned region must reference valid, initialized bytes within this batch's allocator.
    /// - Those bytes must remain valid while the batch or its transaction pointers are used.
    pub unsafe fn try_extend_from_slice<T>(
        &mut self,
        items: &[T],
        mut map: impl FnMut(&T) -> (SharableTransactionRegion, M),
    ) -> Result<(), BatchCapacityError>
    where
        M: Copy,
    {
        if items.len() > CAPACITY.wrapping_sub(self.num_transactions) {
            return Err(BatchCapacityError);
        }
        for item in items {
            let (transaction, meta) = map(item);
            // SAFETY: the entire slice fits in the remaining capacity, checked above.
            unsafe {
                self.tx_ptr.add(self.num_transactions).write(transaction);
                self.meta_ptr.add(self.num_transactions).write(meta);
            }
            self.num_transactions = self.num_transactions.wrapping_add(1);
        }
        Ok(())
    }

    /// Returns a transaction region that was previously written to `index`.
    pub fn transaction_region(&self, index: usize) -> SharableTransactionRegion {
        assert!(
            index < self.num_transactions,
            "batch index was not initialized"
        );
        // SAFETY: `index` was checked against the initialized transaction count above.
        unsafe { self.tx_ptr.add(index).read() }
    }

    /// Returns the sharable message region for this initialized batch.
    pub fn to_sharable_transaction_batch_region(&self) -> SharableTransactionBatchRegion {
        // SAFETY: `tx_ptr` was derived from this allocator when the batch was allocated.
        let transactions_offset = unsafe { self.allocator.offset(self.tx_ptr.cast()) };
        SharableTransactionBatchRegion {
            num_transactions: self
                .num_transactions
                .try_into()
                .expect("batch capacity is at most 64"),
            transactions_offset,
        }
    }

    /// Frees every transaction allocation referenced by this batch.
    ///
    /// This does not free the batch container; call [`Self::free`] afterwards when it is no
    /// longer needed.
    ///
    /// # Safety
    ///
    /// - This batch must be exclusively owned.
    /// - Every transaction region must reference a unique allocation owned by this allocator.
    /// - The batch must not be iterated over or sent after this call.
    pub unsafe fn free_transactions(&self) {
        for index in 0..self.num_transactions {
            let transaction = self.transaction_region(index);
            // SAFETY: the caller guarantees that this transaction allocation is owned by this
            // allocator and has not already been freed.
            unsafe { self.allocator.free_offset(transaction.offset) };
        }
    }

    /// Iterator returning [`TransactionPtr`] for each transaction in the batch.
    pub fn iter(&'a self) -> impl Iterator<Item = (TransactionPtr, M)> + 'a {
        (0..self.num_transactions).map(|idx| unsafe {
            let tx = self.tx_ptr.add(idx);
            let tx = TransactionPtr::from_sharable_transaction_region(tx.as_ref(), self.allocator);
            let meta = self.meta_ptr.add(idx).read();

            (tx, meta)
        })
    }

    /// Free the transaction batch container.
    ///
    /// # Safety
    ///
    /// - [`SharableTransactionBatchRegion`] must be exclusively owned by this pointer.
    ///
    /// # Note
    ///
    /// This will not free the underlying transactions as their lifetimes may be differ from that of
    /// the batch.
    pub unsafe fn free(self) {
        unsafe { self.allocator.free(self.tx_ptr.cast()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocator() -> Allocator {
        let file = tempfile::tempfile().unwrap();
        // SAFETY: this fresh file is initialized exactly once.
        unsafe { Allocator::create(&file, 4 * 1024 * 1024, 1, 65536) }.unwrap()
    }

    fn transaction(allocator: &Allocator) -> SharableTransactionRegion {
        let ptr = allocator.allocate(1).unwrap();
        // SAFETY: the freshly allocated byte is writable and belongs to this allocator.
        unsafe {
            ptr.write(0);
            SharableTransactionRegion {
                offset: allocator.offset(ptr),
                length: 1,
            }
        }
    }

    #[test]
    fn extend_checks_capacity_before_mapping_and_preserves_order() {
        let allocator = allocator();
        let regions = [
            transaction(&allocator),
            transaction(&allocator),
            transaction(&allocator),
        ];
        let mut batch = TransactionPtrBatch::<u64, 3>::allocate(&allocator).unwrap();
        let mut mapped = 0;
        let mut map = |region: &SharableTransactionRegion| {
            mapped += 1;
            (*region, mapped)
        };

        // SAFETY: all regions reference initialized allocations kept alive until cleanup below.
        unsafe {
            batch
                .try_extend_from_slice(&regions[..1], &mut map)
                .unwrap();
            assert_eq!(
                batch.try_extend_from_slice(&regions, &mut map),
                Err(BatchCapacityError)
            );
            assert_eq!(batch.len(), 1);
            batch
                .try_extend_from_slice(&regions[1..], &mut map)
                .unwrap();
            batch.try_extend_from_slice(&[], &mut map).unwrap();
        }
        assert_eq!(mapped, 3);
        assert_eq!(batch.len(), 3);
        for (index, (_, meta)) in batch.iter().enumerate() {
            assert_eq!(batch.transaction_region(index), regions[index]);
            assert_eq!(meta, index as u64 + 1);
        }
        // SAFETY: the batch and its unique transaction allocations are exclusively owned here.
        unsafe {
            batch.free_transactions();
            batch.free();
        }
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn extend_retains_initialized_entries_when_mapper_panics() {
        let allocator = allocator();
        let regions = [transaction(&allocator), transaction(&allocator)];
        let mut batch = TransactionPtrBatch::<u64, 2>::allocate(&allocator).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: returned regions reference initialized allocations kept alive below.
            unsafe {
                batch
                    .try_extend_from_slice(&regions, |region| {
                        assert_ne!(*region, regions[1], "mapper panic");
                        (*region, 42)
                    })
                    .unwrap();
            }
        }));
        assert!(result.is_err());
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.transaction_region(0), regions[0]);
        assert_eq!(batch.iter().next().unwrap().1, 42);
        // SAFETY: the first allocation is owned by the batch; the second was never appended.
        unsafe {
            batch.free_transactions();
            batch.free();
            allocator.free_offset(regions[1].offset);
        }
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }
}
