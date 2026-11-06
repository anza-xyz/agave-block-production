use {
    agave_transaction_view::transaction_view::UnsanitizedTransactionView, bytes::Bytes,
    solana_accounts_db::bank_id::BankId, solana_clock::Slot,
    solana_entry::entry::EntryVerificationData, solana_hash::Hash,
    solana_runtime_transaction::runtime_transaction::ReplayTransaction,
    solana_transaction_error::TransactionError,
};

#[derive(Debug)]
pub(crate) struct EntryHashVerificationRequest {
    pub(crate) bank_id: BankId,
    /// The hash of the previous entry, or the parent bank's last entry hash for the first entry.
    pub(crate) start_hash: Hash,
    pub(crate) entry_verification_data: EntryVerificationData,
}

#[derive(Debug)]
pub(crate) struct EntryHashVerificationResponse {
    pub(crate) bank_id: BankId,
    pub(crate) verified: bool,
}

#[derive(Debug)]
pub(crate) struct SignatureVerificationRequest {
    pub(crate) bank_id: BankId,
    pub(crate) transaction: UnsanitizedTransactionView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct SignatureVerificationResponse {
    pub(crate) bank_id: BankId,
    pub(crate) verified: bool,
}

#[derive(Debug)]
pub(crate) struct TransactionCheckRequest {
    pub(crate) bank_id: BankId,
    pub(crate) slot: Slot,
    pub(crate) transaction_index: usize,
    pub(crate) transaction: UnsanitizedTransactionView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct TransactionCheckResponse {
    pub(crate) bank_id: BankId,
    pub(crate) transaction_index: usize,
    pub(crate) result: Result<Box<ReplayTransaction>, TransactionCheckError>,
}

#[derive(Debug)]
pub(crate) enum TransactionCheckError {
    BankNotFound,
    InvalidTransaction(TransactionError),
}

#[derive(Debug)]
pub(crate) struct TransactionExecutionRequest {
    pub(crate) bank_id: BankId,
    pub(crate) slot: Slot,
    pub(crate) transaction: Box<ReplayTransaction>,
}

#[derive(Debug)]
pub(crate) struct TransactionExecutionResponse {
    pub(crate) bank_id: BankId,
    pub(crate) result: Result<(), TransactionExecutionError>,
}

#[derive(Debug)]
pub(crate) enum TransactionExecutionError {
    BankNotFound,
    ExecutionFailed(TransactionError),
}
