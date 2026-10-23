use {
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    bytes::Bytes,
    solana_clock::{BankId, Slot},
    solana_entry::entry::{EntryVerificationData, EntryView},
    solana_hash::Hash,
};

#[derive(Debug, PartialEq, Eq)]
pub enum BlockVerificationOutcome {
    Verified,
    VerificationFailed(VerificationFailureReason),
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationFailureReason {
    InvalidEntryHash,
    InvalidSignature,
}

#[derive(Debug)]
pub(crate) enum ReplayToBlockVerificationMessage {
    Begin(BeginMessage),
    Entry(EntryMessage),
    AllEntriesSubmitted(AllEntriesSubmittedMessage),
    Abort(AbortMessage),
}

#[derive(Debug)]
pub(crate) struct BeginMessage {
    pub(crate) bank_id: BankId,
    pub(crate) parent_bank_last_entry_hash: Hash,
    pub(crate) slot: Slot,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlockVerificationToReplayMessage {
    pub slot: Slot,
    pub bank_id: BankId,
    pub verification_status: BlockVerificationOutcome,
}

#[derive(Debug)]
pub(crate) struct EntryMessage {
    pub(crate) bank_id: BankId,
    pub(crate) entry_view: EntryView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct AllEntriesSubmittedMessage {
    pub(crate) bank_id: BankId,
}

#[derive(Debug)]
pub(crate) struct AbortMessage {
    pub(crate) bank_id: BankId,
}

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
    pub(crate) transactions: Vec<UnsanitizedTransactionView<Bytes>>,
}

#[derive(Debug)]
pub(crate) struct SignatureVerificationResponse {
    pub(crate) bank_id: BankId,
    pub(crate) verified: bool,
}
