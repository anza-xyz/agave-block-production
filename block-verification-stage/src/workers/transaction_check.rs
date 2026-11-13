use {
    crate::messages::worker::{
        TransactionCheckError, TransactionCheckRequest, TransactionCheckResponse,
    },
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    bytes::Bytes,
    solana_accounts_db::bank_id::BankId,
    solana_clock::Slot,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::runtime_transaction::ReplayTransaction,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::TransactionVerificationMode,
    solana_transaction_error::TransactionError,
    std::sync::RwLock,
};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerChk";

/// Sanitizes the transaction, resolves its address lookup tables against the bank of the block,
/// and checks its number of account locks.
pub(crate) fn check_transaction(
    bank_forks: &RwLock<BankForks>,
    TransactionCheckRequest {
        bank_id,
        slot,
        transaction_index,
        transaction,
    }: TransactionCheckRequest,
) -> TransactionCheckResponse {
    TransactionCheckResponse {
        bank_id,
        transaction_index,
        result: check(bank_forks, bank_id, slot, transaction).map(Box::new),
    }
}

fn check(
    bank_forks: &RwLock<BankForks>,
    bank_id: BankId,
    slot: Slot,
    transaction: UnsanitizedTransactionView<Bytes>,
) -> Result<ReplayTransaction, TransactionCheckError> {
    let bank = bank_forks
        .read()
        .unwrap()
        .get(slot)
        .filter(|bank| bank.bank_id() == bank_id)
        .ok_or(TransactionCheckError::BankNotFound)?;

    let transaction = bank
        .verify_transaction(transaction, TransactionVerificationMode::HashOnly)
        .map_err(TransactionCheckError::InvalidTransaction)?;

    let transaction_account_lock_limit = bank
        .get_transaction_account_lock_limits()
        .for_version(transaction.version().into())
        .map_err(TransactionCheckError::InvalidTransaction)?;
    let exceeds_account_lock_limit =
        transaction.account_keys().len() > transaction_account_lock_limit;
    if exceeds_account_lock_limit {
        return Err(TransactionCheckError::InvalidTransaction(
            TransactionError::TooManyAccountLocks,
        ));
    }

    Ok(transaction)
}
