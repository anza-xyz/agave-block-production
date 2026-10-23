use {
    crate::messages::{SignatureVerificationRequest, SignatureVerificationResponse},
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    bytes::Bytes,
};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerSig";

pub(crate) fn verify_signatures(
    SignatureVerificationRequest {
        bank_id,
        transactions,
    }: SignatureVerificationRequest,
) -> SignatureVerificationResponse {
    SignatureVerificationResponse {
        bank_id,
        verified: transactions.iter().all(verify_transaction_signatures),
    }
}

/// Verifies each signature of `transaction` against the static account key of its signer.
fn verify_transaction_signatures(transaction: &UnsanitizedTransactionView<Bytes>) -> bool {
    let signatures = transaction.signatures();
    let static_account_keys = transaction.static_account_keys();
    let num_required_signatures = usize::from(transaction.num_required_signatures());

    let has_a_signature_for_every_signer = signatures.len() == num_required_signatures;
    let every_signer_has_a_static_account_key =
        static_account_keys.len() >= num_required_signatures;

    if !(has_a_signature_for_every_signer && every_signer_has_a_static_account_key) {
        return false;
    }

    let message = transaction.message_data();
    signatures
        .iter()
        .zip(static_account_keys)
        .all(|(signature, signer)| signature.verify(signer.as_ref(), message))
}
