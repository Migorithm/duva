use super::connections::connection_types::WriteConnected;
use super::identifier::TPeerAddress;
use crate::domains::QueryIO;
use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use crate::domains::{IoError, TRead};
use crate::prelude::PeerIdentifier;
use crate::types::Callback;
use std::collections::VecDeque;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug, PartialEq)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) phi: PhiAccrualDetector,
    state: PeerState,
}

impl Peer {
    pub(crate) fn new(
        w: impl Into<WriteConnected>,
        state: PeerState,
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
        &self.state.id
    }
    pub(crate) fn state(&self) -> &PeerState {
        &self.state
    }
    pub(crate) fn replid(&self) -> &ReplicationId {
        &self.state.replid
    }
    pub(crate) fn match_index(&self) -> u64 {
        self.state.match_index
    }
    pub(crate) fn set_match_index(&mut self, match_index: u64) {
        self.state.match_index = match_index;
        self.phi.record_heartbeat(Instant::now());
    }

    pub(crate) async fn send(&mut self, io: impl Into<QueryIO> + Send) -> Result<(), IoError> {
        self.w_conn.write(io.into()).await
    }

    pub(crate) async fn kill(self) -> Box<dyn TRead> {
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

#[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct PeerState {
    pub(crate) id: PeerIdentifier,
    pub(crate) match_index: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) role: ReplicationRole,
}

impl PeerState {
    pub(crate) fn id(&self) -> &PeerIdentifier {
        &self.id
    }

    pub(crate) fn parse_node_info(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }

        let [addr, id_part, _, match_index, role] = parts[..] else {
            return None;
        };

        let repl_id = Self::extract_replid(id_part)?;

        let match_index = match_index.parse().unwrap_or_default();

        Some(Self {
            id: PeerIdentifier(addr.bind_addr().unwrap()),
            replid: repl_id.into(),
            match_index,
            role: role.to_string().into(),
        })
    }

    fn extract_replid(id_part: &str) -> Option<String> {
        if id_part.contains("myself,") {
            Some(id_part[7..].to_string())
        } else {
            Some(id_part.to_string())
        }
    }

    pub(crate) fn from_file(path: &str) -> Vec<Self> {
        let Some(contents) = Self::read_file_if_valid(path) else {
            return vec![];
        };

        let Some(my_repl_id) = Self::extract_my_repl_id(&contents) else {
            return vec![];
        };

        let mut nodes: Vec<Self> = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(Self::parse_node_info)
            .collect();

        nodes.sort_by_key(|n| n.replid.to_string() == my_repl_id);
        nodes
    }

    fn read_file_if_valid(path: &str) -> Option<String> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()?;

        if modified.elapsed().unwrap_or_default().as_secs() > 300 {
            return None;
        }

        std::fs::read_to_string(path).ok()
    }

    fn extract_my_repl_id(contents: &str) -> Option<String> {
        contents.lines().filter(|line| !line.trim().is_empty()).find_map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for part in parts {
                if part.contains("myself,") {
                    return Some(part[7..].to_string());
                }
            }
            None
        })
    }

    pub(crate) fn format(&self, peer_id: &PeerIdentifier) -> String {
        if self.id == *peer_id {
            return format!(
                "{} myself,{} 0 {} {}",
                self.id, self.replid, self.match_index, self.role
            );
        }
        format!("{} {} 0 {} {}", self.id, self.replid, self.match_index, self.role)
    }

    pub(crate) fn is_self(&self, bind_addr: &str) -> bool {
        self.id.bind_addr().unwrap() == bind_addr
    }
}

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(Callback<()>, JoinHandle<Box<dyn TRead>>);
impl ListeningActorKillTrigger {
    pub(crate) fn new(
        kill_trigger: Callback<()>,
        listning_task: JoinHandle<Box<dyn TRead>>,
    ) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> Box<dyn TRead> {
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
    std_dev: f64,
    sum: f64,
    sum_of_squares: f64,
}

const HISTORY_SIZE: usize = 256;

impl PhiAccrualDetector {
    pub(crate) fn new(now: Instant) -> Self {
        PhiAccrualDetector {
            last_seen: now,
            hb_hist: VecDeque::with_capacity(HISTORY_SIZE),
            mean: 0.0,
            std_dev: 0.0,
            sum: 0.0,
            sum_of_squares: 0.0,
        }
    }
    pub(crate) fn record_heartbeat(&mut self, now: Instant) {
        let interval = now.duration_since(self.last_seen).as_millis() as f64;
        self.last_seen = now;

        if self.hb_hist.len() == HISTORY_SIZE {
            if let Some(old_interval) = self.hb_hist.pop_front() {
                self.sum -= old_interval;
                self.sum_of_squares -= old_interval * old_interval;
            }
        }
        self.hb_hist.push_back(interval);
        self.sum += interval;
        self.sum_of_squares += interval * interval;
        let n = self.hb_hist.len() as f64;

        if n > 0.0 {
            self.mean = self.sum / n;
            let variance = (self.sum_of_squares / n) - (self.mean * self.mean);
            // Due to floating point inaccuracies, variance can sometimes be a tiny negative number.
            // Clamp at 0 before taking the square root.
            self.std_dev = if variance > 0.0 { variance.sqrt() } else { 0.0 };
        }
    }

    fn calculate_phi_at(&self, now: Instant) -> SuspicionLevel {
        // ! Rule of thumb: don't suspect a peer until you have a baseline of ~10 samples.
        if self.hb_hist.len() < 10 {
            return SuspicionLevel::new(0.0);
        }

        // * 1. Calculate P_later, the probability of a heartbeat arriving *after* this time.
        //    P_later = 1 - CDF(time_since_last_seen)

        // A minimum standard deviation prevents instability when network jitter is very low.
        let min_std_dev = 5.0; // e.g., 5ms
        let std_dev = self.std_dev.max(min_std_dev);

        let cdf_value = self.normal_cumulative_distribution(now, self.mean, std_dev);
        let p_later = 1.0 - cdf_value;

        // 2. The phi value is derived from this probability.
        //    phi = -log10(P_later)
        //    We clamp p_later to a tiny positive number to avoid log(0) which is -infinity.
        SuspicionLevel::new(-p_later.max(1e-16).log10())
    }
    // This tells you the probability that a random sample is less than or equal to `x`.
    // "What is the probability of getting a result that is 'x' or less?"
    fn normal_cumulative_distribution(&self, now: Instant, mean: f64, std_dev: f64) -> f64 {
        let time_since_last_seen = now.duration_since(self.last_seen).as_millis() as f64;
        if std_dev <= 0.0 {
            return if time_since_last_seen < mean { 0.0 } else { 1.0 };
        }
        // Standardize the variable
        let z = (time_since_last_seen - mean) / (std_dev * 2.0_f64.sqrt());
        0.5 * (1.0 + erf_approx(z))
    }

    pub(crate) fn is_dead(&self, now: Instant) -> bool {
        self.calculate_phi_at(now) == SuspicionLevel::Dead
    }

    #[cfg(test)]
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }
}

/// Approximates the Gaussian Error Function (erf), needed for the normal CDF.
fn erf_approx(x: f64) -> f64 {
    use std::f64::consts::E;
    // Using Abramowitz and Stegun formula 7.1.26 for high accuracy
    let p = 0.3275911;
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x_abs = x.abs();

    let t = 1.0 / (1.0 + p * x_abs);
    let term = t * (a1 + t * (a2 + t * (a3 + t * (a4 + t * a5))));
    let result = 1.0 - term * E.powf(-x_abs * x_abs);

    sign * result
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

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
    use std::time::Duration;

    #[test]
    fn test_initial_state_phi_is_zero() {
        let now = Instant::now();
        let checker = PhiAccrualDetector::new(now);

        // Phi should be 0 until enough samples are collected.
        assert_eq!(checker.calculate_phi_at(now), SuspicionLevel::Healthy);
    }

    #[test]
    fn test_stable_heartbeats_and_low_phi() {
        let mut now = Instant::now();
        let mut checker = PhiAccrualDetector::new(now);
        let interval = Duration::from_millis(100);

        // Record 20 heartbeats at a stable 100ms interval.
        for _ in 0..20 {
            now += interval;
            checker.record_heartbeat(now);
        }

        // The mean should be exactly 100.
        assert!((checker.mean - 100.0).abs() < 1e-9);
        // The standard deviation should be effectively zero.
        assert!((checker.std_dev).abs() < 1e-9);

        // Immediately after a heartbeat, phi should be very low.
        let phi_immediately = checker.calculate_phi_at(now);
        assert_eq!(phi_immediately, SuspicionLevel::Healthy);

        // A short time later, phi should still be low.
        let phi_shortly_after = checker.calculate_phi_at(now + Duration::from_millis(50));
        assert_eq!(phi_shortly_after, SuspicionLevel::Healthy);
    }

    #[test]
    fn test_failure_detection_phi_accrues_with_significant_jitter() {
        let mut now = Instant::now();
        let mut checker = PhiAccrualDetector::new(now);

        // Establish a history with SIGNIFICANT jitter.
        // The mean is still 100ms, but the deviation is now large.
        let intervals = [60, 140, 110, 90, 130, 70, 50, 150, 80, 120, 100, 100, 145, 55];
        for &ms in intervals.iter() {
            now += Duration::from_millis(ms);
            checker.record_heartbeat(now);
        }

        // The mean will be ~100 and std_dev will now be substantial.
        println!("Test Setup -> Mean: {:.2}, StdDev: {:.2}", checker.mean, checker.std_dev);
        assert!((checker.mean - 100.0).abs() < 1.0);
        assert!(checker.std_dev > 30.0);

        // Now, test the delays against this more tolerant history.
        let failure_time_1 = now + Duration::from_millis(300);
        let phi_1 = checker.calculate_phi_at(failure_time_1);
        println!("Phi at 300ms: {:?}", phi_1);

        let failure_time_2 = now + Duration::from_millis(800);
        let phi_2 = checker.calculate_phi_at(failure_time_2);
        println!("Phi at 800ms: {:?}", phi_2);

        // With a large std_dev, a 300ms delay is suspicious but not "infinite".
        // Its phi value will be high, but well below the cap.

        assert_eq!(phi_1, SuspicionLevel::Faulty);

        // An 800ms delay is still extreme enough to likely hit the cap.
        assert_eq!(
            phi_2,
            SuspicionLevel::Dead,
            "Phi for 800ms delay should be at or near the cap."
        );
    }

    #[test]
    fn test_network_jitter_tolerance() {
        let mut now = Instant::now();

        // --- Scenario 1: Stable Heartbeats ---
        let mut stable_checker = PhiAccrualDetector::new(now);
        for _ in 0..20 {
            now += Duration::from_millis(100);
            stable_checker.record_heartbeat(now);
        }

        // --- Scenario 2: Jittered Heartbeats ---
        let mut jitter_now = now; // Use a separate time tracker for the second checker
        let mut jitter_checker = PhiAccrualDetector::new(jitter_now);
        let intervals = [50, 150, 50, 150, 50, 150, 50, 150, 50, 150, 50, 150];
        for &ms in intervals.iter() {
            jitter_now += Duration::from_millis(ms);
            jitter_checker.record_heartbeat(jitter_now);
        }

        // --- Verification ---
        // Both checkers should have a mean around 100
        assert!((stable_checker.mean - 100.0).abs() < 1e-9);
        assert!((jitter_checker.mean - 100.0).abs() < 1e-9);

        // The stable checker has near-zero std_dev, the jittered one has high std_dev
        assert!(stable_checker.std_dev < 1e-9);
        assert!(jitter_checker.std_dev > 49.0, "StdDev should be high due to jitter");

        // Now, let's simulate a 300ms delay for both from their last seen time
        let stable_fail_time = stable_checker.last_seen + Duration::from_millis(300);
        let jitter_fail_time = jitter_checker.last_seen + Duration::from_millis(300);

        let phi_stable = stable_checker.calculate_phi_at(stable_fail_time);
        let phi_jitter = jitter_checker.calculate_phi_at(jitter_fail_time);

        println!("Phi (Stable) at 300ms delay: {:?}", phi_stable);
        println!("Phi (Jitter) at 300ms delay: {:?}", phi_jitter);

        // --- The key assertions ---
        // 1. Both correctly identify the 300ms delay as suspicious.
        // 2. The jittered checker is far MORE TOLERANT (lower phi) than the stable one.
        assert_eq!(phi_stable, SuspicionLevel::Dead);
        assert_eq!(phi_jitter, SuspicionLevel::Healthy);
    }
}
