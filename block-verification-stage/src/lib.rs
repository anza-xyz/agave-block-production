use verification_components::{
    entry_hash_verification::VerifyEntryHash, signature_verification::VerifySignature,
};
pub use {config::SchedulerConfig, stage::BlockVerificationStage};

mod config;
mod scheduler;
mod utils;

pub mod messages;
pub mod stage;
pub mod verification_components;

pub fn run_scheduler<S, E>(
    scheduler_config: SchedulerConfig,
    signature_verifier: S,
    entry_hash_verifier: E,
) -> BlockVerificationStage
where
    S: VerifySignature + Send + 'static,
    E: VerifyEntryHash + Send + 'static,
{
    scheduler::BlockVerificationScheduler::run_scheduler(
        scheduler_config,
        signature_verifier,
        entry_hash_verifier,
    )
}
