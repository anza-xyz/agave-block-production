use std::{
    panic::resume_unwind,
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};
pub use {config::SchedulerConfig, stage::BlockVerificationStage};

mod config;
mod scheduler;
pub(crate) mod utils;
mod workers;

pub mod messages;
pub mod stage;

pub fn run_scheduler(
    scheduler_config: SchedulerConfig,
    shutdown_signal: Arc<AtomicBool>,
) -> BlockVerificationStage {
    scheduler::BlockVerificationScheduler::run_scheduler(scheduler_config, shutdown_signal)
}

pub(crate) fn join_thread<T>(join_handle: JoinHandle<T>) -> T {
    match join_handle.join() {
        Ok(exit_reason) => exit_reason,
        // propagate panic
        Err(error) => resume_unwind(error),
    }
}
