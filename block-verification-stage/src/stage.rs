//! Replay-facing API of the block verification stage.
//!
//! A [`BlockVerificationSession`] tracks one block through the following type states:
//!
//! ```text
//!   BlockVerificationStage::begin_block(bank_id, slot, parent_hash)
//!          │
//!          │ Ok(session)
//!          ▼
//!   ┌─────────────┐  submit_entry(entry)
//!   │   Started   │◄─────────┐
//!   │             │──────────┘
//!   └─────────────┘
//!     │    │
//!     │    │ notify_all_entries_submitted()
//!     │    ▼
//!     │  ┌─────────────────────┐
//!     │  │ AllEntriesSubmitted │── Verified | VerificationFailed ──►┐
//!     │  └─────────────────────┘                                    │
//!     │    │                                                        │
//!     ├────┘ abort_block_verification()                             │
//!     │                                                             │
//!     ▼                                                             │
//!   ┌─────────────┐                                                 │
//!   │   Aborted   │── Aborted ─────────────────────────────────────►┤
//!   └─────────────┘                                                 │
//!                                      scheduler_message_receiver() ▼
//!                                                                replay
//!
//!   Outcome, sent once per block on BlockVerificationStage::scheduler_message_receiver()
//!   as BlockVerificationToReplayMessage { slot, bank_id, verification_status }:
//!     Verified | VerificationFailed  ──► all entries submitted and verification finished
//!     Aborted                        ──► aborted before verification finished
//!   An abort arriving after the outcome was sent is ignored.
//!
//! ```

use {
    crate::{
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage,
            BlockVerificationToReplayMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        scheduler::SchedulerExitReason,
    },
    bytes::Bytes,
    crossbeam_channel::{Receiver, Sender, TrySendError},
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryView,
    solana_hash::Hash,
    std::{marker::PhantomData, panic::resume_unwind, thread::JoinHandle},
};

/// A type-state used over [`BlockVerificationSession<Started>`] representing the state
/// where verification of the block has started.
///
/// In this state entries can be added with [`BlockVerificationSession::submit_entry`] until
/// the session is either aborted with [`BlockVerificationSession::abort_block_verification`]
/// or all its entries are submitted with
/// [`BlockVerificationSession::notify_all_entries_submitted`], which transitions it to the
/// [`Aborted`] or [`AllEntriesSubmitted`] type state respectively.
#[derive(Debug)]
pub struct Started;

/// A type-state used over [`BlockVerificationSession<AllEntriesSubmitted>`] representing the state
/// where replay has submitted every entry of the block. Verification may still be running.
///
/// In this state the [`BlockVerificationSession`] can be aborted with
/// [`BlockVerificationSession::abort_block_verification`]. Once verification finishes, the
/// outcome is sent on [`BlockVerificationStage::scheduler_message_receiver`].
#[derive(Debug)]
pub struct AllEntriesSubmitted;

/// A type-state used over [`BlockVerificationSession<Aborted>`] representing the state
/// where the verification request has been aborted.
///
/// This state is terminal. Once the scheduler has released the block,
/// [`BlockVerificationOutcome::Aborted`] is sent on
/// [`BlockVerificationStage::scheduler_message_receiver`], unless the outcome of the
/// verification was already sent.
///
/// [`BlockVerificationOutcome::Aborted`]: crate::messages::BlockVerificationOutcome::Aborted
#[derive(Debug)]
pub struct Aborted;

/// A state that can request abortion to the scheduler with [`BlockVerificationSession::abort_block_verification`].
pub trait AbortBlockVerification {}
impl AbortBlockVerification for Started {}
impl AbortBlockVerification for AllEntriesSubmitted {}

/// The block verification stage, which owns the scheduler thread.
///
/// Replay calls [`BlockVerificationStage::begin_block`] to start verifying a block, which
/// returns a [`BlockVerificationSession`] for that block. Dropping the stage stops the
/// scheduler.
#[derive(Debug)]
pub struct BlockVerificationStage {
    replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
    replay_message_receiver: Receiver<BlockVerificationToReplayMessage>,
    scheduler_thread_join_handle: JoinHandle<Result<(), SchedulerExitReason>>,
}

impl BlockVerificationStage {
    pub(crate) fn new(
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        replay_message_receiver: Receiver<BlockVerificationToReplayMessage>,
        scheduler_thread_join_handle: JoinHandle<Result<(), SchedulerExitReason>>,
    ) -> Self {
        Self {
            replay_message_sender,
            replay_message_receiver,
            scheduler_thread_join_handle,
        }
    }

    /// A channel receiver where the [`BlockVerificationToReplayMessage`] outcome for all
    /// scheduled blocks through this block verification stage handle via
    /// [`BlockVerificationStage::begin_block`] / [`BlockVerificationSession`] are sent back.
    pub fn scheduler_message_receiver(&self) -> &Receiver<BlockVerificationToReplayMessage> {
        &self.replay_message_receiver
    }

    /// Waits for the scheduler thread to exit
    pub fn join(self) -> Result<(), SchedulerExitReason> {
        match self.scheduler_thread_join_handle.join() {
            Ok(exit_reason) => exit_reason,
            // propagate panic
            Err(error) => resume_unwind(error),
        }
    }

    /// Starts verifying the block for `bank_id` at `slot`, returning a session to submit its
    /// entries to.
    ///
    /// The outcome of the block is sent on [`BlockVerificationStage::scheduler_message_receiver`].
    ///
    /// ### NB!
    /// `bank_id` and `slot` must not be used by another block before its outcome is received, even
    /// once that block is aborted. Violating this panics the scheduler thread and shuts it down.
    pub fn begin_block(
        &self,
        bank_id: BankId,
        slot: Slot,
        parent_bank_last_entry_hash: Hash,
    ) -> Result<BlockVerificationSession<Started>, SchedulerSendError<()>> {
        BlockVerificationSession::try_new(
            bank_id,
            slot,
            parent_bank_last_entry_hash,
            self.replay_message_sender.clone(),
        )
    }
}

/// The verification of a single block, from [`BlockVerificationStage::begin_block`] until
/// its outcome is received.
///
/// `SchedulingState` is one of [`Started`], [`AllEntriesSubmitted`] or [`Aborted`]. Dropping
/// the [`BlockVerificationSession`] does not abort verification of the block.
#[derive(Debug)]
pub struct BlockVerificationSession<SchedulingState> {
    bank_id: BankId,
    replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
    scheduling_state: PhantomData<fn() -> SchedulingState>,
}

impl<SchedulingState> BlockVerificationSession<SchedulingState>
where
    SchedulingState: AbortBlockVerification,
{
    /// Aborts verification of the block.
    pub fn abort_block_verification(
        self,
    ) -> Result<BlockVerificationSession<Aborted>, SchedulerSendError<Self>> {
        if let Err(error) = self.send(ReplayToBlockVerificationMessage::Abort(AbortMessage {
            bank_id: self.bank_id,
        })) {
            return Err(map_send_error(error, |_message| self));
        }

        Ok(BlockVerificationSession::<Aborted> {
            bank_id: self.bank_id,
            replay_message_sender: self.replay_message_sender,
            scheduling_state: PhantomData,
        })
    }
}

impl<SchedulingState> BlockVerificationSession<SchedulingState> {
    fn send(
        &self,
        message: ReplayToBlockVerificationMessage,
    ) -> Result<(), TrySendError<ReplayToBlockVerificationMessage>> {
        self.replay_message_sender.try_send(message)
    }
}

impl BlockVerificationSession<Started> {
    pub(crate) fn try_new(
        bank_id: BankId,
        slot: Slot,
        parent_bank_last_entry_hash: Hash,
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
    ) -> Result<Self, SchedulerSendError<()>> {
        let begin_message = ReplayToBlockVerificationMessage::Begin(BeginMessage {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        });

        let session = Self {
            bank_id,
            replay_message_sender,
            scheduling_state: PhantomData,
        };

        session
            .send(begin_message)
            .map_err(|error| map_send_error(error, drop))?;

        Ok(session)
    }

    pub fn submit_entry(
        &self,
        entry_view: EntryView<Bytes>,
    ) -> Result<(), SchedulerSendError<EntryView<Bytes>>> {
        self.send(ReplayToBlockVerificationMessage::Entry(EntryMessage {
            bank_id: self.bank_id,
            entry_view,
        }))
        .map_err(|error| {
            map_send_error(error, |message| {
                let ReplayToBlockVerificationMessage::Entry(EntryMessage { entry_view, .. }) =
                    message
                else {
                    unreachable!("the rejected message is the EntryMessage constructed above");
                };
                entry_view
            })
        })
    }

    /// Tells the scheduler that every entry of the block has been submitted.
    pub fn notify_all_entries_submitted(
        self,
    ) -> Result<BlockVerificationSession<AllEntriesSubmitted>, SchedulerSendError<Self>> {
        if let Err(error) = self.send(ReplayToBlockVerificationMessage::AllEntriesSubmitted(
            AllEntriesSubmittedMessage {
                bank_id: self.bank_id,
            },
        )) {
            return Err(map_send_error(error, |_message| self));
        }

        Ok(BlockVerificationSession::<AllEntriesSubmitted> {
            bank_id: self.bank_id,
            replay_message_sender: self.replay_message_sender,
            scheduling_state: PhantomData,
        })
    }
}

/// A failed send to the scheduler
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum SchedulerSendError<T> {
    #[error("the scheduler's channel is full")]
    Full(T),
    #[error("the scheduler has shut down")]
    SchedulerShutdown(T),
}

fn map_send_error<T>(
    error: TrySendError<ReplayToBlockVerificationMessage>,
    inner_value: impl FnOnce(ReplayToBlockVerificationMessage) -> T,
) -> SchedulerSendError<T> {
    match error {
        TrySendError::Full(message) => SchedulerSendError::Full(inner_value(message)),
        TrySendError::Disconnected(message) => {
            SchedulerSendError::SchedulerShutdown(inner_value(message))
        }
    }
}
