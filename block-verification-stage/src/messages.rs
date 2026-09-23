pub enum ReplayToBlockVerificationMessage {
    Begin {
        bank_id: BankId,
        parent_bank_last_entry_hash: Hash,
        slot_id: Slot,
    },
    Entries {
        bank_id: BankId,
        entries: Vec<EntryView<Bytes>>,
    },
    Complete(BankId),
    Abort(BankId),
}

pub enum BlockVerificationToReplayMessage {}
