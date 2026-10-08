use std::sync::{Arc, atomic::AtomicBool};
pub use {config::SchedulerConfig, stage::BlockVerificationStage};

mod config;
mod scheduler;

pub mod messages;
pub mod stage;

pub fn run_scheduler(
    scheduler_config: SchedulerConfig,
    shutdown_signal: Arc<AtomicBool>,
) -> BlockVerificationStage {
    scheduler::BlockVerificationScheduler::run_scheduler(scheduler_config, shutdown_signal)
}
