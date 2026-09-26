use {
    crate::utils::oneshot,
    bytes::Bytes,
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryView,
    solana_hash::Hash,
};

#[derive(Debug, PartialEq, Eq)]
pub enum BlockVerificationOutcome {
    Completed,
    Aborted,
    VerificationFailed(BlockVerificationFailure),
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockVerificationFailure {
    SignatureVerificationFailure,
    EntryHashVerificationFailure,
}

#[derive(Debug)]
pub(crate) enum ReplayToBlockVerificationMessage {
    Begin(BeginMessage),
    Entry(EntryMessage),
    Complete(CompleteMessage),
    Abort(AbortMessage),
}

#[derive(Debug)]
pub(crate) struct BeginMessage {
    pub(crate) bank_id: BankId,
    pub(crate) parent_bank_last_entry_hash: Hash,
    pub(crate) slot: Slot,
    pub(crate) begin_message_response_sender: oneshot::Sender<BeginMessageResponse>,
}

#[derive(Debug)]
pub(crate) enum BeginMessageResponse {
    Success {
        verification_receiver: oneshot::Receiver<BlockVerificationOutcome>,
    },
    BankIdOccupied,
    SlotIdOccupied,
}

#[derive(Debug)]
pub(crate) struct EntryMessage {
    #[expect(dead_code, reason = "handling entries will be added in follow up")]
    pub(crate) bank_id: BankId,
    #[expect(dead_code, reason = "handling entries will be added in follow up")]
    pub(crate) entry_view: EntryView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct CompleteMessage {
    pub(crate) bank_id: BankId,
}

#[derive(Debug)]
pub(crate) struct AbortMessage {
    pub(crate) bank_id: BankId,
}
