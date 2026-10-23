use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) struct ShutdownOnDrop(Arc<AtomicBool>);

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl From<Arc<AtomicBool>> for ShutdownOnDrop {
    fn from(value: Arc<AtomicBool>) -> Self {
        Self(value)
    }
}

impl ShutdownOnDrop {
    pub(crate) fn signal(&self) -> &Arc<AtomicBool> {
        &self.0
    }

    pub(crate) fn shutdown(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
