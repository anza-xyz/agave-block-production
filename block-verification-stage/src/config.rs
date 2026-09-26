const DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE: usize = 100;
const DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub replay_to_block_verification_channel_size: usize,
    pub block_verification_to_replay_channel_size: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            replay_to_block_verification_channel_size:
                DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE,
            block_verification_to_replay_channel_size:
                DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE,
        }
    }
}
