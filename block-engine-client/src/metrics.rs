use {
    crate::SessionError,
    solana_metrics::datapoint_info,
    std::time::{Duration, Instant},
};

const REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct ClientMetrics {
    last_report: Instant,
    pub(crate) streams_closed: usize,
    pub(crate) transport_errors: usize,
    pub(crate) status_errors: usize,
    pub(crate) missing_access_token_errors: usize,
    pub(crate) invalid_access_token_errors: usize,
    pub(crate) empty_packet_responses: usize,
    pub(crate) received_packets: usize,
    pub(crate) discarded_packets: usize,
    pub(crate) malformed_packets: usize,
    pub(crate) forwarded_packets: usize,
    pub(crate) queue_full_dropped_packets: usize,
    pub(crate) received_bundles: usize,
    pub(crate) missing_bundles: usize,
    pub(crate) empty_bundles: usize,
    pub(crate) invalid_member_bundles: usize,
    pub(crate) forwarded_bundles: usize,
    pub(crate) queue_full_dropped_bundles: usize,
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self {
            last_report: Instant::now(),
            streams_closed: 0,
            transport_errors: 0,
            status_errors: 0,
            missing_access_token_errors: 0,
            invalid_access_token_errors: 0,
            empty_packet_responses: 0,
            received_packets: 0,
            discarded_packets: 0,
            malformed_packets: 0,
            forwarded_packets: 0,
            queue_full_dropped_packets: 0,
            received_bundles: 0,
            missing_bundles: 0,
            empty_bundles: 0,
            invalid_member_bundles: 0,
            forwarded_bundles: 0,
            queue_full_dropped_bundles: 0,
        }
    }
}

impl ClientMetrics {
    pub(crate) fn record_session_error(&mut self, err: &SessionError) {
        let counter = match err {
            SessionError::Transport(_) => &mut self.transport_errors,
            SessionError::Status(_) => &mut self.status_errors,
            SessionError::MissingAccessToken => &mut self.missing_access_token_errors,
            SessionError::InvalidAccessToken(_) => &mut self.invalid_access_token_errors,
        };
        *counter = counter.saturating_add(1);
    }

    pub(crate) fn maybe_report(&mut self) {
        if self.last_report.elapsed() >= REPORT_INTERVAL {
            self.report();
        }
    }

    fn report(&mut self) {
        datapoint_info!(
            "block-engine-client",
            ("streams_closed", self.streams_closed, i64),
            ("transport_errors", self.transport_errors, i64),
            ("status_errors", self.status_errors, i64),
            (
                "missing_access_token_errors",
                self.missing_access_token_errors,
                i64
            ),
            (
                "invalid_access_token_errors",
                self.invalid_access_token_errors,
                i64
            ),
            ("empty_packet_responses", self.empty_packet_responses, i64),
            ("received_packets", self.received_packets, i64),
            ("discarded_packets", self.discarded_packets, i64),
            ("malformed_packets", self.malformed_packets, i64),
            ("forwarded_packets", self.forwarded_packets, i64),
            (
                "queue_full_dropped_packets",
                self.queue_full_dropped_packets,
                i64
            ),
            ("received_bundles", self.received_bundles, i64),
            ("missing_bundles", self.missing_bundles, i64),
            ("empty_bundles", self.empty_bundles, i64),
            ("invalid_member_bundles", self.invalid_member_bundles, i64),
            ("forwarded_bundles", self.forwarded_bundles, i64),
            (
                "queue_full_dropped_bundles",
                self.queue_full_dropped_bundles,
                i64
            ),
        );
        *self = Self::default();
    }
}
