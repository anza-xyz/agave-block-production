use {
    agave_block_engine_client::BlockEngineMessage,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    shaq::spsc::{Consumer, Producer},
    solana_metrics::datapoint_info,
    solana_perf::{
        packet::{BytesPacket, BytesPacketBatch},
        sigverify::verify_transaction_view_signatures,
    },
    std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
};

const THREAD_NAME: &str = "solBlkEngVerify";
const REPORT_INTERVAL: Duration = Duration::from_secs(1);
/// Bounds how long an exit request waits on an idle input queue.
const READ_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_BUNDLE_MEMBERS: usize = 5;

#[derive(Debug)]
pub(crate) enum VerifiedMessage {
    Packets(BytesPacketBatch),
    Bundle(BytesPacketBatch),
}

pub(crate) fn spawn_verifier(
    input: Consumer<BlockEngineMessage>,
    output: Producer<VerifiedMessage>,
    exit: Arc<AtomicBool>,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || run(input, output, &exit))
}

fn run(
    mut input: Consumer<BlockEngineMessage>,
    mut output: Producer<VerifiedMessage>,
    exit: &AtomicBool,
) {
    let mut metrics = VerifierMetrics::default();
    while !exit.load(Ordering::Relaxed) {
        if let Ok(message) = input.read_timeout(READ_TIMEOUT) {
            handle_message(message, &mut output, &mut metrics);
        }
        metrics.maybe_report();
    }
    metrics.report();
}

fn handle_message(
    message: BlockEngineMessage,
    output: &mut Producer<VerifiedMessage>,
    metrics: &mut VerifierMetrics,
) {
    match message {
        BlockEngineMessage::Packets(mut batch) => {
            verify_packets(&mut batch, metrics);
            let count = batch.len();
            if count != 0 && output.try_write(VerifiedMessage::Packets(batch)).is_err() {
                metrics.queue_full_dropped_packets =
                    metrics.queue_full_dropped_packets.saturating_add(count);
            }
        }
        BlockEngineMessage::Bundles(bundles) => {
            metrics.received_bundles = metrics.received_bundles.saturating_add(bundles.len());
            for bundle in bundles {
                match verify_bundle(&bundle.packets) {
                    BundleVerificationVerdict::Oversized => {
                        metrics.oversized_bundles = metrics.oversized_bundles.saturating_add(1);
                        continue;
                    }
                    BundleVerificationVerdict::DuplicateMember => {
                        metrics.duplicate_member_bundles =
                            metrics.duplicate_member_bundles.saturating_add(1);
                        continue;
                    }
                    BundleVerificationVerdict::InvalidMember => {
                        metrics.invalid_member_bundles =
                            metrics.invalid_member_bundles.saturating_add(1);
                        continue;
                    }
                    BundleVerificationVerdict::Verified => {}
                }
                if output
                    .try_write(VerifiedMessage::Bundle(bundle.packets))
                    .is_err()
                {
                    metrics.queue_full_dropped_bundles =
                        metrics.queue_full_dropped_bundles.saturating_add(1);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Verified,
    ParseFailed,
    SigverifyFailed,
}

fn verify_packets(batch: &mut BytesPacketBatch, metrics: &mut VerifierMetrics) {
    metrics.received_packets = metrics.received_packets.saturating_add(batch.len());
    batch.retain(|packet: &BytesPacket| {
        let verdict = packet.data(..).map_or(Verdict::ParseFailed, verify_packet);
        match verdict {
            Verdict::Verified => true,
            Verdict::ParseFailed => {
                metrics.parse_failed_packets = metrics.parse_failed_packets.saturating_add(1);
                false
            }
            Verdict::SigverifyFailed => {
                metrics.sigverify_failed_packets =
                    metrics.sigverify_failed_packets.saturating_add(1);
                false
            }
        }
    });
}

fn verify_packet(data: &[u8]) -> Verdict {
    let Ok(view) = SanitizedTransactionView::try_new_sanitized(data) else {
        return Verdict::ParseFailed;
    };
    if verify_transaction_view_signatures(&view) {
        Verdict::Verified
    } else {
        Verdict::SigverifyFailed
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BundleVerificationVerdict {
    Verified,
    Oversized,
    DuplicateMember,
    InvalidMember,
}

fn verify_bundle(packets: &BytesPacketBatch) -> BundleVerificationVerdict {
    if packets.len() > MAX_BUNDLE_MEMBERS {
        return BundleVerificationVerdict::Oversized;
    }
    let mut signatures = [None; MAX_BUNDLE_MEMBERS];
    for (index, packet) in packets.iter().enumerate() {
        let Some(view) = packet
            .data(..)
            .and_then(|data| SanitizedTransactionView::try_new_sanitized(data).ok())
        else {
            return BundleVerificationVerdict::InvalidMember;
        };
        let signature = view.signatures().first().copied();
        if signatures[..index].contains(&signature) {
            return BundleVerificationVerdict::DuplicateMember;
        }
        if !verify_transaction_view_signatures(&view) {
            return BundleVerificationVerdict::InvalidMember;
        }
        signatures[index] = signature;
    }
    BundleVerificationVerdict::Verified
}

struct VerifierMetrics {
    last_report: Instant,
    /// Packets received outside bundles.
    received_packets: usize,
    /// Packets that do not parse or sanitize as transactions.
    parse_failed_packets: usize,
    sigverify_failed_packets: usize,
    queue_full_dropped_packets: usize,
    received_bundles: usize,
    /// Bundles with more than `MAX_BUNDLE_MEMBERS` members.
    oversized_bundles: usize,
    duplicate_member_bundles: usize,
    /// Bundles with a member that fails parsing or signature verification.
    invalid_member_bundles: usize,
    queue_full_dropped_bundles: usize,
}

impl Default for VerifierMetrics {
    fn default() -> Self {
        Self {
            last_report: Instant::now(),
            received_packets: 0,
            parse_failed_packets: 0,
            sigverify_failed_packets: 0,
            queue_full_dropped_packets: 0,
            received_bundles: 0,
            oversized_bundles: 0,
            duplicate_member_bundles: 0,
            invalid_member_bundles: 0,
            queue_full_dropped_bundles: 0,
        }
    }
}

impl VerifierMetrics {
    fn maybe_report(&mut self) {
        if self.last_report.elapsed() >= REPORT_INTERVAL {
            self.report();
        }
    }

    fn report(&mut self) {
        datapoint_info!(
            "block-production-scheduler-block-engine-verifier",
            ("received_packets", self.received_packets, i64),
            ("parse_failed_packets", self.parse_failed_packets, i64),
            (
                "sigverify_failed_packets",
                self.sigverify_failed_packets,
                i64
            ),
            (
                "queue_full_dropped_packets",
                self.queue_full_dropped_packets,
                i64
            ),
            ("received_bundles", self.received_bundles, i64),
            ("oversized_bundles", self.oversized_bundles, i64),
            (
                "duplicate_member_bundles",
                self.duplicate_member_bundles,
                i64
            ),
            ("invalid_member_bundles", self.invalid_member_bundles, i64),
            (
                "queue_full_dropped_bundles",
                self.queue_full_dropped_bundles,
                i64
            ),
        );
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, agave_block_engine_client::BlockEngineBundle, solana_perf::test_tx::test_tx,
        solana_transaction::versioned::VersionedTransaction, std::assert_matches,
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn valid() -> Vec<u8> {
        wincode::serialize(&VersionedTransaction::from(test_tx())).unwrap()
    }

    fn bad_signature() -> Vec<u8> {
        let mut bytes = valid();
        bytes[1] ^= 0xff;
        bytes
    }

    fn garbage() -> Vec<u8> {
        vec![0xff; 16]
    }

    fn batch(packets: Vec<Vec<u8>>) -> BytesPacketBatch {
        packets
            .into_iter()
            .map(|bytes| BytesPacket::from_bytes(None, bytes))
            .collect()
    }

    fn bundle(packets: Vec<Vec<u8>>) -> BlockEngineBundle {
        BlockEngineBundle {
            uuid: String::new(),
            packets: batch(packets),
        }
    }

    fn data(batch: &BytesPacketBatch) -> Vec<&[u8]> {
        batch
            .iter()
            .map(|packet| packet.data(..).unwrap())
            .collect()
    }

    fn spawn(
        output_capacity: usize,
    ) -> (
        Producer<BlockEngineMessage>,
        Consumer<VerifiedMessage>,
        Arc<AtomicBool>,
        JoinHandle<()>,
    ) {
        let (upstream, input) = shaq::spsc::pair(64).unwrap();
        let (output, downstream) = shaq::spsc::pair(output_capacity).unwrap();
        let exit = Arc::new(AtomicBool::new(false));
        let verifier = spawn_verifier(input, output, exit.clone()).unwrap();
        (upstream, downstream, exit, verifier)
    }

    #[test]
    fn verify_packet_verdicts() {
        assert_eq!(verify_packet(&valid()), Verdict::Verified);
        assert_eq!(verify_packet(&bad_signature()), Verdict::SigverifyFailed);
        assert_eq!(verify_packet(&garbage()), Verdict::ParseFailed);
    }

    #[test]
    fn verify_packets_removes_failed_packets() {
        let first = valid();
        let second = valid();
        let mut packets = batch(vec![
            first.clone(),
            bad_signature(),
            garbage(),
            second.clone(),
        ]);
        let mut metrics = VerifierMetrics::default();

        verify_packets(&mut packets, &mut metrics);

        assert_eq!(data(&packets), [first.as_slice(), second.as_slice()]);
        assert_eq!(metrics.received_packets, 4);
        assert_eq!(metrics.parse_failed_packets, 1);
        assert_eq!(metrics.sigverify_failed_packets, 1);
    }

    #[test]
    fn verify_bundle_requires_every_member() {
        assert_eq!(
            verify_bundle(&batch(vec![valid(), valid()])),
            BundleVerificationVerdict::Verified
        );
        assert_eq!(
            verify_bundle(&batch(vec![valid(), bad_signature()])),
            BundleVerificationVerdict::InvalidMember
        );
        assert_eq!(
            verify_bundle(&batch(vec![garbage(), valid()])),
            BundleVerificationVerdict::InvalidMember
        );
    }

    #[test]
    fn verify_bundle_limits_members() {
        let members = |count| batch((0..count).map(|_| valid()).collect());
        assert_eq!(
            verify_bundle(&members(MAX_BUNDLE_MEMBERS)),
            BundleVerificationVerdict::Verified
        );
        assert_eq!(
            verify_bundle(&members(MAX_BUNDLE_MEMBERS + 1)),
            BundleVerificationVerdict::Oversized
        );
    }

    #[test]
    fn verify_bundle_rejects_duplicate_members() {
        let duplicate = valid();
        assert_eq!(
            verify_bundle(&batch(vec![duplicate.clone(), valid(), duplicate])),
            BundleVerificationVerdict::DuplicateMember
        );
    }

    #[test]
    fn handle_message_counts_bundles_dropped_on_full_output() {
        let (mut output, mut downstream) = shaq::spsc::pair(1).unwrap();
        let capacity = downstream.capacity();
        let mut metrics = VerifierMetrics::default();
        let bundles = (0..capacity + 2).map(|_| bundle(vec![valid()])).collect();

        handle_message(
            BlockEngineMessage::Bundles(bundles),
            &mut output,
            &mut metrics,
        );

        assert_eq!(metrics.received_bundles, capacity + 2);
        assert_eq!(metrics.queue_full_dropped_bundles, 2);
        handle_message(
            BlockEngineMessage::Packets(batch(vec![valid(), valid()])),
            &mut output,
            &mut metrics,
        );
        assert_eq!(metrics.queue_full_dropped_packets, 2);
        for _ in 0..capacity {
            assert_matches!(downstream.try_read(), Some(VerifiedMessage::Bundle(_)));
        }
        assert_matches!(downstream.try_read(), None);
    }

    #[test]
    fn forwards_verified_packets() {
        let (mut upstream, mut downstream, exit, verifier) = spawn(16);
        let bytes = valid();

        upstream
            .try_write(BlockEngineMessage::Packets(batch(vec![
                bytes.clone(),
                bad_signature(),
            ])))
            .unwrap();
        upstream
            .try_write(BlockEngineMessage::Packets(batch(vec![garbage()])))
            .unwrap();
        upstream
            .try_write(BlockEngineMessage::Bundles(vec![bundle(vec![valid()])]))
            .unwrap();

        let first = downstream.read_timeout(TEST_TIMEOUT).unwrap();
        assert_matches!(&first, VerifiedMessage::Packets(packets) if data(packets) == [bytes.as_slice()]);
        // The bundle sent last proves the all-garbage batch was dropped, not still in flight.
        assert_matches!(
            downstream.read_timeout(TEST_TIMEOUT),
            Ok(VerifiedMessage::Bundle(_))
        );
        exit.store(true, Ordering::Relaxed);
        verifier.join().unwrap();
    }

    #[test]
    fn forwards_each_verified_bundle_separately() {
        let (mut upstream, mut downstream, exit, verifier) = spawn(16);
        let first = valid();
        let second = valid();

        upstream
            .try_write(BlockEngineMessage::Bundles(vec![
                bundle(vec![first.clone()]),
                bundle(vec![valid(), bad_signature()]),
                bundle(vec![second.clone()]),
            ]))
            .unwrap();

        assert_matches!(
            downstream.read_timeout(TEST_TIMEOUT),
            Ok(VerifiedMessage::Bundle(packets)) if data(&packets) == [first.as_slice()]
        );
        assert_matches!(
            downstream.read_timeout(TEST_TIMEOUT),
            Ok(VerifiedMessage::Bundle(packets)) if data(&packets) == [second.as_slice()]
        );
        exit.store(true, Ordering::Relaxed);
        verifier.join().unwrap();
        assert!(downstream.try_read().is_none());
    }

    #[test]
    fn stops_on_exit() {
        let (_upstream, _downstream, exit, verifier) = spawn(16);
        exit.store(true, Ordering::Relaxed);
        verifier.join().unwrap();
    }
}
