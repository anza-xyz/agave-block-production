use {
    crate::{
        messages::{
            AbortMessage, BeginMessage, BeginMessageResponse, BlockVerificationOutcome,
            CompleteMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        utils::{cancellation_token::CancellationToken, oneshot},
    },
    bytes::Bytes,
    crossbeam_channel::{RecvError, SendError, Sender},
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryView,
    solana_hash::Hash,
    std::{marker::PhantomData, thread::JoinHandle},
};

/// A type-state used over [`BlockVerificationSession<Started>`] representing the state
/// where verification of the block has started.
///
/// In this state entries can be added with [`BlockVerificationSession::submit_entry`] until
/// the session is either aborted with [`BlockVerificationSession::abort_block_verification`]
/// or marked as completed with [`BlockVerificationSession::mark_completed`], which transitions it to the [`Aborted`]
/// or [`AllEntriesSubmitted`] type state respectively.
#[derive(Debug)]
pub struct Started;

/// A type-state used over [`BlockVerificationSession<AllEntriesSubmitted>`] representing the state
/// where replay has submitted every entry of the block. Verification may still be running.
///
/// In this state the [`BlockVerificationSession`] can either wait for the verification
/// result with [`BlockVerificationSession::wait_for_outcome`] or abort with
/// [`BlockVerificationSession::abort_block_verification`].
#[derive(Debug)]
pub struct AllEntriesSubmitted;

/// A type-state used over [`BlockVerificationSession<Aborted>`] representing the state
/// where the verification request has been aborted.
///
/// This state is terminal and only [`BlockVerificationSession::wait_for_outcome`] can be
/// called to get the final result.
#[derive(Debug)]
pub struct Aborted;

/// A state that can wait for its outcome from the scheduler to call [`BlockVerificationSession::wait_for_outcome`]
pub trait WaitForOutcome {}
impl WaitForOutcome for AllEntriesSubmitted {}
impl WaitForOutcome for Aborted {}

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
    /// Signals the scheduler thread to exit when cancelled or dropped.
    shutdown_token: CancellationToken,
    scheduler_thread_join_handle: JoinHandle<()>,
}

impl BlockVerificationStage {
    pub(crate) fn new(
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        shutdown_token: CancellationToken,
        scheduler_thread_join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            replay_message_sender,
            shutdown_token,
            scheduler_thread_join_handle,
        }
    }

    /// Stops the scheduler and waits for its thread to exit.
    ///
    /// Blocks still in progress are dropped, so waiting for their outcome returns
    /// `SchedulerShutdownError`. Dropping the stage also stops the scheduler, but without
    /// waiting for the thread.
    pub fn shutdown(self) -> std::thread::Result<()> {
        self.shutdown_token.cancel();
        self.scheduler_thread_join_handle.join()
    }

    /// Starts verifying the block for `bank_id` at `slot`, returning a session to submit its
    /// entries to.
    ///
    /// Fails if another in-progress block already uses `bank_id` or `slot`.
    pub fn begin_block(
        &self,
        bank_id: BankId,
        slot: Slot,
        parent_bank_last_entry_hash: Hash,
    ) -> Result<BlockVerificationSession<Started>, BeginBlockError> {
        let (begin_message_response_sender, begin_message_response_receiver) = oneshot::channel();

        let begin_message = ReplayToBlockVerificationMessage::Begin(BeginMessage {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
            begin_message_response_sender,
        });

        self.replay_message_sender
            .send(begin_message)
            .map_err(|_err: SendError<_>| BeginBlockError::SchedulerShutDown)?;

        let oneshot_response_receiver = match begin_message_response_receiver.recv() {
            Ok(BeginMessageResponse::Success {
                verification_receiver,
            }) => Ok(verification_receiver),
            Ok(BeginMessageResponse::BankIdOccupied) => Err(BeginBlockError::BankIdOccupied),
            Ok(BeginMessageResponse::SlotIdOccupied) => Err(BeginBlockError::SlotIdOccupied),
            Err(RecvError) => Err(BeginBlockError::SchedulerShutDown),
        }?;

        Ok(BlockVerificationSession::new(
            bank_id,
            self.replay_message_sender.clone(),
            oneshot_response_receiver,
        ))
    }
}

/// The verification of a single block, from [`BlockVerificationStage::begin_block`] until
/// its outcome is received.
///
/// `SchedulingState` is one of [`Started`], [`AllEntriesSubmitted`] or [`Aborted`]. Dropping
/// the session before calling [`BlockVerificationSession::wait_for_outcome`] aborts
/// verification of the block.
#[derive(Debug)]
pub struct BlockVerificationSession<SchedulingState> {
    sender: abort_on_drop::AbortOnDropSender,
    block_verification_outcome_receiver: oneshot::Receiver<BlockVerificationOutcome>,

    scheduling_state: PhantomData<fn() -> SchedulingState>,
}

impl<SchedulingState> BlockVerificationSession<SchedulingState>
where
    SchedulingState: AbortBlockVerification,
{
    pub fn abort_block_verification(
        self,
    ) -> Result<BlockVerificationSession<Aborted>, SchedulerShutdownError> {
        let mut sender = self.sender;
        sender.send_abort()?;

        Ok(BlockVerificationSession::<Aborted> {
            sender,
            block_verification_outcome_receiver: self.block_verification_outcome_receiver,
            scheduling_state: PhantomData,
        })
    }
}

impl BlockVerificationSession<Started> {
    pub(crate) fn new(
        bank_id: BankId,
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        block_verification_outcome_receiver: oneshot::Receiver<BlockVerificationOutcome>,
    ) -> Self {
        Self {
            sender: abort_on_drop::AbortOnDropSender::new(bank_id, replay_message_sender),
            block_verification_outcome_receiver,
            scheduling_state: PhantomData,
        }
    }

    pub fn submit_entry(&self, entry_view: EntryView<Bytes>) -> Result<(), SchedulerShutdownError> {
        self.sender.send_entry(entry_view)
    }

    /// Tells the scheduler that every entry of the block has been submitted.
    pub fn mark_completed(
        self,
    ) -> Result<BlockVerificationSession<AllEntriesSubmitted>, SchedulerShutdownError> {
        self.sender.send_complete()?;

        Ok(BlockVerificationSession::<AllEntriesSubmitted> {
            sender: self.sender,
            block_verification_outcome_receiver: self.block_verification_outcome_receiver,
            scheduling_state: PhantomData,
        })
    }
}

impl<SchedulingState: WaitForOutcome> BlockVerificationSession<SchedulingState> {
    pub fn wait_for_outcome(mut self) -> Result<BlockVerificationOutcome, SchedulerShutdownError> {
        self.sender.disarm();

        self.block_verification_outcome_receiver
            .recv()
            .map_err(|_err: RecvError| SchedulerShutdownError)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BeginBlockError {
    SchedulerShutDown,
    BankIdOccupied,
    SlotIdOccupied,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchedulerShutdownError;

mod abort_on_drop {
    use super::*;

    /// Sends messages for one block to the scheduler.
    ///
    /// Sends `Abort` for `bank_id` when dropped unless disarmed.
    #[derive(Debug)]
    pub(super) struct AbortOnDropSender {
        bank_id: BankId,
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        armed: bool,
    }

    impl AbortOnDropSender {
        pub(super) fn new(
            bank_id: BankId,
            replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        ) -> Self {
            Self {
                bank_id,
                replay_message_sender,
                armed: true,
            }
        }

        pub(super) fn send_entry(
            &self,
            entry_view: EntryView<Bytes>,
        ) -> Result<(), SchedulerShutdownError> {
            self.send(ReplayToBlockVerificationMessage::Entry(EntryMessage {
                bank_id: self.bank_id,
                entry_view,
            }))
        }

        pub(super) fn send_complete(&self) -> Result<(), SchedulerShutdownError> {
            self.send(ReplayToBlockVerificationMessage::Complete(
                CompleteMessage {
                    bank_id: self.bank_id,
                },
            ))
        }

        /// Sends `Abort` and disarms
        pub(super) fn send_abort(&mut self) -> Result<(), SchedulerShutdownError> {
            self.armed = false;
            self.send(ReplayToBlockVerificationMessage::Abort(AbortMessage {
                bank_id: self.bank_id,
            }))
        }

        /// Stops `Abort` from being sent on drop.
        pub(super) fn disarm(&mut self) {
            self.armed = false;
        }

        fn send(
            &self,
            message: ReplayToBlockVerificationMessage,
        ) -> Result<(), SchedulerShutdownError> {
            self.replay_message_sender
                .send(message)
                .map_err(|_err: SendError<_>| SchedulerShutdownError)
        }
    }

    impl Drop for AbortOnDropSender {
        fn drop(&mut self) {
            if self.armed {
                // An abort for a block the scheduler no longer tracks is ignored.
                let _ = self.send_abort();
            }
        }
    }
}
