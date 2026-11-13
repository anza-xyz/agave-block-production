use {
    crate::messages::worker::{
        TransactionExecutionError, TransactionExecutionRequest, TransactionExecutionResponse,
    },
    solana_accounts_db::bank_id::BankId,
    solana_clock::Slot,
    solana_runtime::{
        bank_forks::BankForks,
        transaction_execution::{
            TransactionBatchWithIndexes, check_block_cost_limits, execute_batch,
        },
        vote_sender_types::ReplayVoteSendType,
    },
    solana_runtime_transaction::runtime_transaction::ReplayTransaction,
    solana_svm_timings::ExecuteTimings,
    std::sync::RwLock,
};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerExec";

/// Executes and commits the transaction in the bank of the block, and adds its cost to the cost of
/// the block.
pub(crate) fn execute_transaction(
    bank_forks: &RwLock<BankForks>,
    TransactionExecutionRequest {
        bank_id,
        slot,
        transaction,
    }: TransactionExecutionRequest,
) -> TransactionExecutionResponse {
    TransactionExecutionResponse {
        bank_id,
        result: execute(bank_forks, bank_id, slot, &transaction),
    }
}

fn execute(
    bank_forks: &RwLock<BankForks>,
    bank_id: BankId,
    slot: Slot,
    transaction: &ReplayTransaction,
) -> Result<(), TransactionExecutionError> {
    let bank = bank_forks
        .read()
        .unwrap()
        .get(slot)
        .filter(|bank| bank.bank_id() == bank_id)
        .ok_or(TransactionExecutionError::BankNotFound)?;

    let batch = TransactionBatchWithIndexes {
        batch: bank.prepare_unlocked_batch_from_single_tx(transaction),
        // The indexes are only reported to the transaction status sender, which is not passed.
        transaction_indexes: vec![],
    };
    // Execution metrics are not collected yet.
    let mut timings = ExecuteTimings::default();

    let costs = execute_batch(
        &batch,
        &bank,
        None,
        None,
        ReplayVoteSendType::Executed {
            replay_bank_id: bank_id,
            replay_slot: slot,
        },
        &mut timings,
        None,
        None,
    )
    .map_err(TransactionExecutionError::ExecutionFailed)?;
    let [cost] = costs.as_slice() else {
        unreachable!("a batch of one transaction has one cost");
    };

    check_block_cost_limits(
        &mut bank.write_cost_tracker().unwrap(),
        transaction,
        cost.tracked_cost(),
    )
    .map_err(TransactionExecutionError::ExecutionFailed)
}
