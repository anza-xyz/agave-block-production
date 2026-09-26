//! A single-use channel backed by Crossbeam.

pub use crossbeam_channel::{RecvError, SendError};

/// The sending half of a oneshot channel.
#[derive(Debug)]
pub struct Sender<T>(crossbeam_channel::Sender<T>);

/// The receiving half of a oneshot channel.
#[derive(Debug)]
pub struct Receiver<T>(crossbeam_channel::Receiver<T>);

/// Creates a channel that can carry a single value.
///
/// The value is buffered, so sending does not wait for the receiver.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = crossbeam_channel::bounded(1);
    (Sender(sender), Receiver(receiver))
}

impl<T> Sender<T> {
    /// Sends the value, consuming this endpoint.
    ///
    /// Returns the value in an error if the receiver has been dropped.
    pub fn send(self, value: T) -> Result<(), SendError<T>> {
        self.0.send(value)
    }
}

impl<T> Receiver<T> {
    /// Waits for the value, consuming this endpoint.
    ///
    /// Returns an error if the sender is dropped without sending a value.
    pub fn recv(self) -> Result<T, RecvError> {
        self.0.recv()
    }
}
