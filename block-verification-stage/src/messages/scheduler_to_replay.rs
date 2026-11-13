use {
    solana_accounts_db::bank_id::BankId, solana_clock::Slot,
    solana_transaction_error::TransactionError,
};

#[derive(Debug, PartialEq, Eq)]
pub struct BlockVerificationToReplayMessage {
    pub slot: Slot,
    pub bank_id: BankId,
    pub verification_status: BlockVerificationOutcome,
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockVerificationOutcome {
    Verified,
    VerificationFailed(VerificationFailureReason),
    Aborted,
    /// The bank of the block was not in bank forks when its transactions were being processed.
    BankNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationFailureReason {
    InvalidEntryHash,
    InvalidSignature,
    InvalidTransaction(TransactionError),
    TransactionExecutionFailed(TransactionError),
}
