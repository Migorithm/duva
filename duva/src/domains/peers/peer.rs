use super::connections::connection_types::WriteConnected;

use crate::domains::IoError;
use crate::domains::TSerdeDynamicRead;
use crate::domains::peers::command::PeerMessage;
use crate::domains::replications::ReplicationId;

use crate::domains::replications::ReplicationRole;
use crate::domains::replications::state::ReplicationState;
use crate::prelude::PeerIdentifier;
use crate::types::Callback;
use std::collections::VecDeque;
use std::time::Instant;
use tokio::task::JoinHandle;

#[derive(Debug, PartialEq)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) phi: PhiAccrualDetector,
    state: ReplicationState,
}

impl Peer {
    pub(crate) fn new(
        w: impl Into<WriteConnected>,
        state: ReplicationState,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
        Self {
            w_conn: w.into(),
            listener_kill_trigger,
            phi: PhiAccrualDetector::new(Instant::now()),
            state,
        }
    }
    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.state.node_id
    }
    pub(crate) fn state(&self) -> &ReplicationState {
        &self.state
    }
    pub(crate) fn replid(&self) -> &ReplicationId {
        &self.state.replid
    }
    pub(crate) fn curr_match_index(&self) -> u64 {
        self.state.last_log_index
    }

    // ! leader operation
    pub(crate) fn set_match_idx(&mut self, log_index: u64) {
        self.state.last_log_index = log_index;
    }
    pub(crate) fn record_heartbeat(&mut self) {
        self.phi.record_heartbeat(Instant::now());
    }

    pub(crate) async fn send(&mut self, io: impl Into<PeerMessage>) -> Result<(), IoError> {
        self.w_conn.send(io.into()).await
    }

    pub(crate) async fn kill(self) -> Box<dyn TSerdeDynamicRead> {
        self.listener_kill_trigger.kill().await
    }

    pub(crate) fn is_replica(&self, replid: &ReplicationId) -> bool {
        self.state.replid == *replid
    }

    pub(crate) fn is_follower(&self, replid: &ReplicationId) -> bool {
        self.is_replica(replid) && self.state.role == ReplicationRole::Follower
    }

    pub(crate) fn set_role(&mut self, role: ReplicationRole) {
        self.state.role = role;
    }

    pub(crate) fn role(&self) -> ReplicationRole {
        self.state.role.clone()
    }
}

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(Callback<()>, JoinHandle<Box<dyn TSerdeDynamicRead>>);
impl ListeningActorKillTrigger {
    pub(crate) fn new(
        kill_trigger: Callback<()>,
        listning_task: JoinHandle<Box<dyn TSerdeDynamicRead>>,
    ) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> Box<dyn TSerdeDynamicRead> {
        self.0.send(());
        self.1.await.unwrap()
    }
}

impl PartialEq for ListeningActorKillTrigger {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1.is_finished() == other.1.is_finished()
    }
}
impl Eq for ListeningActorKillTrigger {}

#[derive(Debug, PartialEq)]
pub(crate) struct PhiAccrualDetector {
    last_seen: Instant,
    hb_hist: VecDeque<f64>,
    mean: f64,
    sum: f64,
}

const HISTORY_SIZE: usize = 256;

impl PhiAccrualDetector {
    pub(crate) fn new(now: Instant) -> Self {
        PhiAccrualDetector {
            last_seen: now,
            hb_hist: VecDeque::with_capacity(HISTORY_SIZE),
            mean: 0.0,
            sum: 0.0,
        }
    }

    pub(crate) fn record_heartbeat(&mut self, now: Instant) {
        let interval = now.duration_since(self.last_seen).as_secs_f64() * 1000.0; // ms
        self.last_seen = now;

        if self.hb_hist.len() == HISTORY_SIZE
            && let Some(old_interval) = self.hb_hist.pop_front()
        {
            self.sum -= old_interval;
        }
        self.hb_hist.push_back(interval);
        self.sum += interval;

        let n = self.hb_hist.len() as f64;
        if n > 0.0 {
            self.mean = self.sum / n;
        }
    }

    fn calculate_phi_at(&self, now: Instant) -> SuspicionLevel {
        // Don’t suspect peers until we have a baseline
        if self.hb_hist.len() < 10 {
            return SuspicionLevel::new(0.0);
        }

        let elapsed = now.duration_since(self.last_seen).as_secs_f64() * 1000.0; // ms

        // Avoid division by zero (if mean is crazy small)
        let mean = self.mean.max(1e-6);

        // φ(t) = (t / λ) * log10(e)
        let phi = (elapsed / mean) * std::f64::consts::E.log10();

        SuspicionLevel::new(phi)
    }

    pub(crate) fn is_dead(&self, now: Instant) -> bool {
        self.calculate_phi_at(now) == SuspicionLevel::Dead
            || self.last_seen.elapsed().as_secs() > 60
    }

    #[cfg(test)]
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum SuspicionLevel {
    Healthy, // Normal operation.
    Suspect, // Log a warning, increment a metric. Maybe deprioritize the node for new connections.
    Faulty, // Actively stop sending new requests to the node. Begin gracefully shedding connections
    Dead,   // Convict the node. Remove it from the cluster and trigger re-replication or failover.
}
impl SuspicionLevel {
    pub(crate) fn new(phi_score: f64) -> Self {
        if phi_score > 12.0 {
            SuspicionLevel::Dead
        } else if phi_score > 8.0 {
            SuspicionLevel::Faulty
        } else if phi_score > 5.0 {
            SuspicionLevel::Suspect
        } else {
            SuspicionLevel::Healthy
        }
    }
}

#[test]
fn test_prioritize_nodes_with_same_replid() {
    use std::io::Write;

    let file_content = r#"
    127.0.0.1:6000 0196477d-f227-72f2-81eb-6a3703076de8 0 11 follower
    127.0.0.1:6001 0196477d-f227-72f2-81eb-6a3703076de8 0 13 follower
    127.0.0.1:6002 myself,0196477d-f227-72f2-81eb-6a3703076de8 0 15 leader
    127.0.0.1:6003 99999999-aaaa-bbbb-cccc-111111111111 0 5 leader
    127.0.0.1:6004 deadbeef-dead-beef-dead-beefdeadbeef 0 5 leader
    "#;

    // Create temp file and write content
    let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    write!(temp_file, "{file_content}").expect("Failed to write to temp file");

    // Read and prioritize nodes
    let nodes = ReplicationState::from_file(temp_file.path().to_str().unwrap());

    // There should be 4 nodes, all with priority 0 (same ID as myself)
    assert_eq!(nodes.len(), 5);

    assert_eq!(nodes.iter().filter(|n| n.role == ReplicationRole::Follower).count(), 2);
    assert_eq!(nodes.iter().filter(|n| n.role == ReplicationRole::Leader).count(), 3);

    // Optionally print for debugging
    for node in nodes {
        println!("{node:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn test_history_size_limit() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Record more heartbeats than HISTORY_SIZE
        for _ in 1..=(HISTORY_SIZE + 10) {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Should not exceed HISTORY_SIZE
        assert_eq!(detector.hb_hist.len(), HISTORY_SIZE);

        // Should have removed the oldest entries
        // The first entry should now be from heartbeat #11 (100ms * 11 = 1100ms)
        assert_eq!(detector.hb_hist[0], 100.0);

        // Sum should only include the last HISTORY_SIZE entries
        let expected_sum = 100.0 * HISTORY_SIZE as f64;
        assert_eq!(detector.sum, expected_sum);
        assert_eq!(detector.mean, 100.0);
    }

    #[test]
    fn test_is_dead_insufficient_history() {
        let start = Instant::now();
        let detector = PhiAccrualDetector::new(start);

        // With no heartbeats recorded, should not be considered dead
        assert!(!detector.is_dead(start + Duration::from_secs(10)));

        // Even with some heartbeats but less than 10, should not be dead
        let mut detector_with_few_hb = PhiAccrualDetector::new(start);
        let mut current_time = start;
        for _ in 0..5 {
            current_time += Duration::from_millis(100);
            detector_with_few_hb.record_heartbeat(current_time);
        }

        assert!(!detector_with_few_hb.is_dead(current_time + Duration::from_secs(10)));
    }

    #[test]
    fn test_phi_calculation_normal_operation() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Record 15 heartbeats with 100ms intervals to establish baseline
        for _ in 0..15 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Check phi shortly after expected heartbeat time (should be healthy)
        let check_time = current_time + Duration::from_millis(110);
        assert!(!detector.is_dead(check_time));

        // Check phi at exactly expected mean interval (should be healthy)
        let check_time = current_time + Duration::from_millis(100);
        assert!(!detector.is_dead(check_time));
    }

    #[test]
    fn test_is_dead_with_long_delay() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Establish baseline with 20 heartbeats at 100ms intervals
        for _ in 0..20 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Check if considered dead after a very long delay
        // For phi > 12, we need: (elapsed / mean) * log10(e) > 12
        // So: elapsed > 12 * mean / log10(e) = 12 * 100 / 0.434 ≈ 2765ms
        let dead_time = current_time + Duration::from_millis(3000);
        assert!(detector.is_dead(dead_time));
    }

    #[test]
    fn test_varying_heartbeat_intervals() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Record heartbeats with varying intervals
        let intervals = vec![50, 100, 75, 125, 90, 110, 80, 120, 95, 105, 85, 115, 70, 130];
        let mut sum = 0.0;

        for interval_ms in &intervals {
            current_time += Duration::from_millis(*interval_ms);
            detector.record_heartbeat(current_time);
            sum += *interval_ms as f64;
        }

        let expected_mean = sum / intervals.len() as f64;
        assert!((detector.mean - expected_mean).abs() < 1e-10);
        assert_eq!(detector.hb_hist.len(), intervals.len());
    }

    #[test]
    fn test_mean_calculation_accuracy() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Test with known intervals
        let intervals = vec![100.0, 200.0, 300.0];
        for &interval in &intervals {
            current_time += Duration::from_millis(interval as u64);
            detector.record_heartbeat(current_time);
        }

        let expected_mean = 200.0; // (100 + 200 + 300) / 3
        assert_eq!(detector.mean, expected_mean);
        assert_eq!(detector.sum, 600.0);
    }

    #[test]
    fn test_rolling_window_behavior() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Fill the history with 100ms intervals
        for _ in 0..HISTORY_SIZE {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        assert_eq!(detector.mean, 100.0);
        assert_eq!(detector.sum, 100.0 * HISTORY_SIZE as f64);

        // Add one more heartbeat with a different interval
        current_time += Duration::from_millis(200);
        detector.record_heartbeat(current_time);

        // Should still have HISTORY_SIZE entries
        assert_eq!(detector.hb_hist.len(), HISTORY_SIZE);

        // The oldest 100ms entry should be gone, replaced by 200ms
        // New sum = old_sum - 100 + 200 = (256 * 100) - 100 + 200 = 25600 + 100 = 25700
        let expected_sum = (HISTORY_SIZE - 1) as f64 * 100.0 + 200.0;
        assert_eq!(detector.sum, expected_sum);

        let expected_mean = expected_sum / HISTORY_SIZE as f64;
        assert_eq!(detector.mean, expected_mean);
    }

    #[test]
    fn test_suspicion_level_progression() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Establish baseline with 20 heartbeats at 100ms intervals
        for _ in 0..20 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Test different delay levels
        // For φ = (elapsed / mean) * log10(e), with mean = 100ms, log10(e) ≈ 0.434

        // Healthy: φ < 5, so elapsed < 5 * 100 / 0.434 ≈ 1152ms
        let healthy_time = current_time + Duration::from_millis(1000);
        assert_eq!(detector.calculate_phi_at(healthy_time), SuspicionLevel::Healthy);

        // Suspect: 5 ≤ φ < 8, so 1152ms ≤ elapsed < 1843ms
        let suspect_time = current_time + Duration::from_millis(1300);
        assert_eq!(detector.calculate_phi_at(suspect_time), SuspicionLevel::Suspect);

        // Faulty: 8 ≤ φ < 12, so 1843ms ≤ elapsed < 2765ms
        let faulty_time = current_time + Duration::from_millis(2000);
        assert_eq!(detector.calculate_phi_at(faulty_time), SuspicionLevel::Faulty);

        // Dead: φ ≥ 12, so elapsed ≥ 2765ms
        let dead_time = current_time + Duration::from_millis(3000);
        assert_eq!(detector.calculate_phi_at(dead_time), SuspicionLevel::Dead);
        assert!(detector.is_dead(dead_time));
    }

    #[test]
    fn test_adaptive_behavior_with_changing_patterns() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Start with fast heartbeats (50ms intervals)
        for _ in 0..15 {
            current_time += Duration::from_millis(50);
            detector.record_heartbeat(current_time);
        }

        let fast_mean = detector.mean;
        assert_eq!(fast_mean, 50.0);

        // Switch to slower heartbeats (200ms intervals)
        for _ in 0..15 {
            current_time += Duration::from_millis(200);
            detector.record_heartbeat(current_time);
        }

        // Mean should have adjusted
        let new_mean = detector.mean;
        assert!(new_mean > fast_mean);
        assert!(new_mean < 200.0); // Still influenced by the faster intervals

        // A delay that would be "dead" for fast heartbeats might be acceptable for slow ones
        let moderate_delay = current_time + Duration::from_millis(400);
        let phi_level = detector.calculate_phi_at(moderate_delay);

        // This depends on the exact mean, but should be less suspicious than with fast-only baseline
        assert!(phi_level != SuspicionLevel::Dead || detector.mean < 50.0);
    }

    #[test]
    fn test_edge_case_very_large_intervals() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Record heartbeats with very large intervals (10 seconds)
        for _ in 0..15 {
            current_time += Duration::from_secs(10);
            detector.record_heartbeat(current_time);
        }

        assert_eq!(detector.mean, 10000.0); // 10 seconds in milliseconds

        // A 30-second delay: φ = (30000 / 10000) * log10(e) = 3 * 0.434 ≈ 1.3 (Healthy)
        let delayed_time = current_time + Duration::from_secs(30);
        let phi_level = detector.calculate_phi_at(delayed_time);
        assert_eq!(phi_level, SuspicionLevel::Healthy);

        // For Dead level (φ ≥ 12), need: elapsed ≥ 12 * mean / log10(e)
        // elapsed ≥ 12 * 10000 / 0.434 ≈ 276,498ms ≈ 276 seconds
        let very_delayed_time = current_time + Duration::from_secs(280);
        assert!(detector.is_dead(very_delayed_time));

        // Test intermediate levels
        // For Suspect (φ ≥ 5): elapsed ≥ 5 * 10000 / 0.434 ≈ 115,207ms ≈ 115 seconds
        let suspect_time = current_time + Duration::from_secs(120);
        let suspect_phi = detector.calculate_phi_at(suspect_time);
        assert!(matches!(suspect_phi, SuspicionLevel::Suspect | SuspicionLevel::Faulty));
    }

    #[test]
    fn test_consistent_timing_low_phi() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Record very consistent heartbeats
        for _ in 0..50 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Check phi at exactly the expected interval
        let expected_time = current_time + Duration::from_millis(100);
        let phi_level = detector.calculate_phi_at(expected_time);

        // φ = (100 / 100) * log10(e) = 1 * 0.434 ≈ 0.434 (Healthy)
        assert_eq!(phi_level, SuspicionLevel::Healthy);
    }

    #[test]
    fn test_irregular_timing_adaptation() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Create irregular but bounded intervals
        let intervals = vec![50, 150, 75, 125, 100, 200, 60, 140, 80, 120, 90, 110, 70, 130, 85];

        for interval_ms in intervals {
            current_time += Duration::from_millis(interval_ms);
            detector.record_heartbeat(current_time);
        }

        // The detector should adapt to the variability
        // Mean should be around 100ms, but phi calculation should account for variance
        let moderate_delay = current_time + Duration::from_millis(300);
        let phi_level = detector.calculate_phi_at(moderate_delay);

        // With irregular timing, a 3x delay might still be healthy or just suspect
        assert!(matches!(phi_level, SuspicionLevel::Healthy | SuspicionLevel::Suspect));
    }

    #[test]
    fn test_recovery_after_false_positive() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Establish baseline
        for _ in 0..15 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);
        }

        // Simulate a long delay (would be considered dead)
        let long_delay = current_time + Duration::from_millis(3000);
        assert!(detector.is_dead(long_delay));

        // Now record a heartbeat after the long delay
        detector.record_heartbeat(long_delay);

        // The detector should have updated its last_seen time
        assert_eq!(detector.last_seen(), long_delay);

        // Check that we're back to normal operation shortly after
        let normal_next = long_delay + Duration::from_millis(100);
        assert!(!detector.is_dead(normal_next));
    }

    #[test]
    fn test_minimum_mean_threshold() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);

        // Create scenario where calculated mean would be extremely small
        // Record heartbeat at same instant (0 interval)
        detector.record_heartbeat(start);

        // Add tiny intervals to get above minimum history
        let mut current_time = start;
        for _ in 0..15 {
            current_time += Duration::from_nanos(100); // Very tiny intervals
            detector.record_heartbeat(current_time);
        }

        // Mean should be extremely small, but phi calculation should use 1e-6 minimum
        let delayed_time = current_time + Duration::from_millis(1);

        // Should not panic and should handle the minimum mean threshold
        let phi_level = detector.calculate_phi_at(delayed_time);
        assert!(matches!(phi_level, SuspicionLevel::Dead)); // Very small mean makes delays look huge
    }

    #[test]
    fn test_empty_history_pop_safety() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);

        // This tests the safety of the pop_front operation when history is empty
        // The implementation uses if let Some(old_interval) = self.hb_hist.pop_front()
        // which should handle empty deque gracefully

        detector.record_heartbeat(start + Duration::from_millis(100));

        // Should not panic
        assert_eq!(detector.hb_hist.len(), 1);
        assert_eq!(detector.sum, 100.0);
    }

    #[test]
    fn test_realistic_distributed_system_scenario() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Simulate realistic heartbeat pattern: 1 second intervals with small variations
        let intervals_ms = vec![
            1000, 1020, 980, 1010, 990, 1030, 970, 1015, 995, 1025, 985, 1005, 1000, 1012, 988,
            1018, 992, 1008, 1002, 998, 1015, 985, 1025, 975, 1035, 965, 1022, 978, 1007, 993,
        ];

        for interval_ms in intervals_ms {
            current_time += Duration::from_millis(interval_ms);
            detector.record_heartbeat(current_time);
        }

        // Mean should be close to 1000ms (with small variations it should be very close)
        assert!((detector.mean - 1000.0).abs() < 30.0);

        // Normal operation: next heartbeat within expected window (φ ≈ 1.05)
        let normal_next = current_time + Duration::from_millis(1050);
        assert!(!detector.is_dead(normal_next));

        // For a 1000ms mean, Dead threshold (φ ≥ 12) needs:
        // elapsed ≥ 12 * 1000 / log10(e) ≈ 12 * 1000 / 0.434 ≈ 27,649ms ≈ 27.6 seconds
        let dead_delay = current_time + Duration::from_secs(30);
        assert!(detector.is_dead(dead_delay));

        // Test intermediate suspicion level at ~10 seconds (φ ≈ 4.3, should be Healthy)
        let moderate_delay = current_time + Duration::from_secs(10);
        assert!(!detector.is_dead(moderate_delay));
    }

    #[test]
    fn test_performance_with_maximum_history() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Fill to maximum capacity and verify performance doesn't degrade
        for _ in 0..HISTORY_SIZE * 2 {
            current_time += Duration::from_millis(100);
            detector.record_heartbeat(current_time);

            // Verify invariants throughout
            assert!(detector.hb_hist.len() <= HISTORY_SIZE);
            assert!(detector.mean >= 0.0);
            assert!(detector.sum >= 0.0);

            // Verify sum consistency
            let expected_sum: f64 = detector.hb_hist.iter().sum();
            assert!((detector.sum - expected_sum).abs() < 1e-10);
        }

        // Final state should be stable
        assert_eq!(detector.hb_hist.len(), HISTORY_SIZE);
        assert_eq!(detector.mean, 100.0);
    }

    #[test]
    fn test_phi_calculation_mathematical_accuracy() {
        let start = Instant::now();
        let mut detector = PhiAccrualDetector::new(start);
        let mut current_time = start;

        // Set up known baseline: 20 heartbeats at exactly 1000ms intervals
        for _ in 0..20 {
            current_time += Duration::from_millis(1000);
            detector.record_heartbeat(current_time);
        }

        assert_eq!(detector.mean, 1000.0);

        // Test specific phi calculation
        let test_delay = current_time + Duration::from_millis(2000);
        let phi_level = detector.calculate_phi_at(test_delay);

        // Manual calculation: φ = (2000 / 1000) * log10(e) = 2 * 0.4342944... ≈ 0.869
        // This should be Healthy (φ < 5)
        assert_eq!(phi_level, SuspicionLevel::Healthy);

        // Test with delay that should give φ ≈ 5
        // Need: elapsed = 5 * 1000 / log10(e) ≈ 5 * 1000 / 0.434 ≈ 11520ms
        let boundary_delay = current_time + Duration::from_millis(11520);
        let boundary_phi = detector.calculate_phi_at(boundary_delay);
        assert!(matches!(boundary_phi, SuspicionLevel::Suspect | SuspicionLevel::Healthy));
    }
}
