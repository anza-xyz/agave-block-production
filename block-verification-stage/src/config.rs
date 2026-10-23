use std::num::NonZeroUsize;

const DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE: usize = 2048;
const DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE: usize = 2048;
const DEFAULT_NUM_ENTRY_HASH_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const DEFAULT_MAX_ENTRY_HASH_VERIFICATIONS_IN_FLIGHT: NonZeroUsize =
    NonZeroUsize::new(4096).unwrap();
const DEFAULT_NUM_SIGNATURE_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const DEFAULT_MAX_SIGNATURE_VERIFICATIONS_IN_FLIGHT: NonZeroUsize =
    NonZeroUsize::new(4096).unwrap();

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub replay_to_block_verification_channel_size: usize,
    pub block_verification_to_replay_channel_size: usize,
    pub num_entry_hash_workers: NonZeroUsize,
    pub max_entry_hash_verifications_in_flight: NonZeroUsize,
    pub num_signature_workers: NonZeroUsize,
    pub max_signature_verifications_in_flight: NonZeroUsize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            replay_to_block_verification_channel_size:
                DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE,
            block_verification_to_replay_channel_size:
                DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE,
            num_entry_hash_workers: DEFAULT_NUM_ENTRY_HASH_WORKERS,
            max_entry_hash_verifications_in_flight: DEFAULT_MAX_ENTRY_HASH_VERIFICATIONS_IN_FLIGHT,
            num_signature_workers: DEFAULT_NUM_SIGNATURE_WORKERS,
            max_signature_verifications_in_flight: DEFAULT_MAX_SIGNATURE_VERIFICATIONS_IN_FLIGHT,
        }
    }
}
