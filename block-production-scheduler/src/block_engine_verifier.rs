use {
    agave_block_engine_client::BlockEngineMessage,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    crossbeam_channel::{Receiver, Sender},
    solana_metrics::datapoint_info,
    solana_perf::{
        packet::{BytesPacket, BytesPacketBatch, PacketBatch},
        sigverify::verify_transaction_view_signatures,
    },
    std::{
        io,
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
};

const REPORT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BUNDLE_MEMBERS: usize = 5;

#[derive(Debug)]
pub(crate) enum VerifiedMessage {
    Packets(PacketBatch),
    Bundle(PacketBatch),
}

pub(crate) fn spawn_verifier(
    receiver: Receiver<BlockEngineMessage>,
    sender: Sender<VerifiedMessage>,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("solBlkEngVerify".into())
        .spawn(move || run(receiver, sender))
}

fn run(receiver: Receiver<BlockEngineMessage>, sender: Sender<VerifiedMessage>) {
    let mut metrics = VerifierMetrics::default();
    'messages: for message in receiver {
        match message {
            BlockEngineMessage::Packets(mut batch) => {
                verify_packets(&mut batch, &mut metrics);
                if !batch.is_empty() && sender.send(VerifiedMessage::Packets(batch)).is_err() {
                    break;
                }
            }
            BlockEngineMessage::Bundles(bundles) => {
                metrics.received_bundles = metrics.received_bundles.saturating_add(bundles.len());
                for bundle in bundles {
                    let dropped = match verify_bundle(&bundle.packets) {
                        BundleVerdict::Verified => None,
                        BundleVerdict::Oversized => Some(&mut metrics.oversized_bundles),
                        BundleVerdict::DuplicateMember => {
                            Some(&mut metrics.duplicate_member_bundles)
                        }
                        BundleVerdict::InvalidMember => Some(&mut metrics.invalid_member_bundles),
                    };
                    if let Some(dropped) = dropped {
                        *dropped = dropped.saturating_add(1);
                        continue;
                    }
                    if sender
                        .send(VerifiedMessage::Bundle(bundle.packets))
                        .is_err()
                    {
                        break 'messages;
                    }
                }
            }
        }
        metrics.maybe_report();
    }
    metrics.report();
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Verified,
    ParseFailed,
    SigverifyFailed,
}

fn verify_packets(batch: &mut PacketBatch, metrics: &mut VerifierMetrics) {
    metrics.received_packets = metrics.received_packets.saturating_add(batch.len());
    let mut keep = |packet: &BytesPacket| {
        let verdict = packet.data(..).map_or(Verdict::ParseFailed, verify_packet);
        let failed = match verdict {
            Verdict::Verified => return true,
            Verdict::ParseFailed => &mut metrics.parse_failed_packets,
            Verdict::SigverifyFailed => &mut metrics.sigverify_failed_packets,
        };
        *failed = failed.saturating_add(1);
        false
    };
    match batch {
        PacketBatch::Bytes(packets) => packets.retain(|packet| keep(packet)),
        PacketBatch::Single(packet) => {
            if !keep(packet) {
                *batch = PacketBatch::Bytes(BytesPacketBatch::new());
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BundleVerdict {
    Verified,
    Oversized,
    DuplicateMember,
    InvalidMember,
}

fn verify_bundle(packets: &PacketBatch) -> BundleVerdict {
    if packets.len() > MAX_BUNDLE_MEMBERS {
        return BundleVerdict::Oversized;
    }
    let mut signatures = [None; MAX_BUNDLE_MEMBERS];
    for (index, packet) in packets.iter().enumerate() {
        let Some(view) = packet
            .data(..)
            .and_then(|data| SanitizedTransactionView::try_new_sanitized(data).ok())
        else {
            return BundleVerdict::InvalidMember;
        };
        let signature = view.signatures().first().copied();
        if signatures[..index].contains(&signature) {
            return BundleVerdict::DuplicateMember;
        }
        if !verify_transaction_view_signatures(&view) {
            return BundleVerdict::InvalidMember;
        }
        signatures[index] = signature;
    }
    BundleVerdict::Verified
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

struct VerifierMetrics {
    last_report: Instant,
    /// Packets received outside bundles.
    received_packets: usize,
    /// Packets that do not parse or sanitize as transactions.
    parse_failed_packets: usize,
    /// Packets that parse but fail signature verification.
    sigverify_failed_packets: usize,
    /// Bundles received.
    received_bundles: usize,
    /// Bundles with more than `MAX_BUNDLE_MEMBERS` members.
    oversized_bundles: usize,
    /// Bundles containing the same transaction more than once.
    duplicate_member_bundles: usize,
    /// Bundles with a member that fails parsing or signature verification.
    invalid_member_bundles: usize,
}

impl Default for VerifierMetrics {
    fn default() -> Self {
        Self {
            last_report: Instant::now(),
            received_packets: 0,
            parse_failed_packets: 0,
            sigverify_failed_packets: 0,
            received_bundles: 0,
            oversized_bundles: 0,
            duplicate_member_bundles: 0,
            invalid_member_bundles: 0,
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
            ("received_bundles", self.received_bundles, i64),
            ("oversized_bundles", self.oversized_bundles, i64),
            (
                "duplicate_member_bundles",
                self.duplicate_member_bundles,
                i64
            ),
            ("invalid_member_bundles", self.invalid_member_bundles, i64),
        );
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, agave_block_engine_client::BlockEngineBundle, crossbeam_channel::unbounded,
        solana_perf::test_tx::test_tx, solana_transaction::versioned::VersionedTransaction,
        std::assert_matches,
    };

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

    fn packet(bytes: Vec<u8>) -> BytesPacket {
        BytesPacket::from_bytes(None, bytes)
    }

    fn batch(packets: Vec<Vec<u8>>) -> PacketBatch {
        PacketBatch::from(packets.into_iter().map(packet).collect::<Vec<_>>())
    }

    fn bundle(packets: Vec<Vec<u8>>) -> BlockEngineBundle {
        BlockEngineBundle {
            uuid: String::new(),
            packets: batch(packets),
        }
    }

    fn data(batch: &PacketBatch) -> Vec<&[u8]> {
        batch
            .iter()
            .map(|packet| packet.data(..).unwrap())
            .collect()
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
    fn verify_packets_keeps_valid_single_packet() {
        let bytes = valid();
        let mut packets = PacketBatch::Single(packet(bytes.clone()));
        let mut metrics = VerifierMetrics::default();

        verify_packets(&mut packets, &mut metrics);

        assert_eq!(data(&packets), [bytes.as_slice()]);
    }

    #[test]
    fn verify_packets_empties_failed_single_packet() {
        let mut packets = PacketBatch::Single(packet(bad_signature()));
        let mut metrics = VerifierMetrics::default();

        verify_packets(&mut packets, &mut metrics);

        assert!(packets.is_empty());
        assert_eq!(metrics.received_packets, 1);
        assert_eq!(metrics.sigverify_failed_packets, 1);
    }

    #[test]
    fn verify_bundle_requires_every_member() {
        assert_eq!(
            verify_bundle(&batch(vec![valid(), valid()])),
            BundleVerdict::Verified
        );
        assert_eq!(
            verify_bundle(&batch(vec![valid(), bad_signature()])),
            BundleVerdict::InvalidMember
        );
        assert_eq!(
            verify_bundle(&batch(vec![garbage(), valid()])),
            BundleVerdict::InvalidMember
        );
    }

    #[test]
    fn verify_bundle_limits_members() {
        let members = |count| batch((0..count).map(|_| valid()).collect());
        assert_eq!(
            verify_bundle(&members(MAX_BUNDLE_MEMBERS)),
            BundleVerdict::Verified
        );
        assert_eq!(
            verify_bundle(&members(MAX_BUNDLE_MEMBERS + 1)),
            BundleVerdict::Oversized
        );
    }

    #[test]
    fn verify_bundle_rejects_duplicate_members() {
        let duplicate = valid();
        assert_eq!(
            verify_bundle(&batch(vec![duplicate.clone(), valid(), duplicate])),
            BundleVerdict::DuplicateMember
        );
    }

    #[test]
    fn forwards_verified_packets() {
        let (upstream, receiver) = unbounded();
        let (sender, downstream) = unbounded();
        let verifier = spawn_verifier(receiver, sender).unwrap();
        let bytes = valid();

        upstream
            .send(BlockEngineMessage::Packets(batch(vec![
                bytes.clone(),
                bad_signature(),
            ])))
            .unwrap();
        upstream
            .send(BlockEngineMessage::Packets(batch(vec![garbage()])))
            .unwrap();
        drop(upstream);
        verifier.join().unwrap();

        let messages: Vec<_> = downstream.iter().collect();
        assert_matches!(
            messages.as_slice(),
            [VerifiedMessage::Packets(packets)] if data(packets) == [bytes.as_slice()]
        );
    }

    #[test]
    fn forwards_each_verified_bundle_separately() {
        let (upstream, receiver) = unbounded();
        let (sender, downstream) = unbounded();
        let verifier = spawn_verifier(receiver, sender).unwrap();
        let first = valid();
        let second = valid();

        upstream
            .send(BlockEngineMessage::Bundles(vec![
                bundle(vec![first.clone()]),
                bundle(vec![valid(), bad_signature()]),
                bundle(vec![second.clone()]),
            ]))
            .unwrap();
        drop(upstream);
        verifier.join().unwrap();

        let messages: Vec<_> = downstream.iter().collect();
        assert_matches!(
            messages.as_slice(),
            [VerifiedMessage::Bundle(first_bundle), VerifiedMessage::Bundle(second_bundle)]
                if data(first_bundle) == [first.as_slice()]
                    && data(second_bundle) == [second.as_slice()]
        );
    }

    #[test]
    fn exits_when_downstream_disconnects() {
        let (upstream, receiver) = unbounded();
        let (sender, downstream) = unbounded();
        let verifier = spawn_verifier(receiver, sender).unwrap();
        drop(downstream);

        upstream
            .send(BlockEngineMessage::Packets(batch(vec![valid()])))
            .unwrap();
        verifier.join().unwrap();

        assert!(
            upstream
                .send(BlockEngineMessage::Packets(batch(vec![valid()])))
                .is_err()
        );
    }
}
