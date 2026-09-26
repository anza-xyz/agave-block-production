use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
pub(crate) struct CancellationToken {
    is_cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub(crate) fn new() -> Self {
        Self {
            is_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn token_ref(&self) -> CancellationTokenRef {
        CancellationTokenRef {
            is_cancelled: self.is_cancelled.clone(),
        }
    }

    pub(crate) fn cancel(self) {
        // drop impl runs cancellation
        drop(self)
    }
}

impl Drop for CancellationToken {
    fn drop(&mut self) {
        self.is_cancelled.store(true, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct CancellationTokenRef {
    is_cancelled: Arc<AtomicBool>,
}

impl CancellationTokenRef {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(Ordering::Relaxed)
    }
}
