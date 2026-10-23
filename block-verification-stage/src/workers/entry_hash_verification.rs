use crate::messages::{EntryHashVerificationRequest, EntryHashVerificationResponse};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerHash";

pub(crate) fn verify_entry_hash(
    EntryHashVerificationRequest {
        bank_id,
        start_hash,
        entry_verification_data,
    }: EntryHashVerificationRequest,
) -> EntryHashVerificationResponse {
    EntryHashVerificationResponse {
        bank_id,
        verified: entry_verification_data.verify(&start_hash),
    }
}
