use std::{thread::sleep, time::Duration};

use crate::common::Client;

mod test_leader_election;
mod test_leader_election_twice;
mod test_raft_happy_case;
mod test_set_twice_after_election;
mod test_sync;

fn panic_if_election_not_done(order: &str, port1: u16, port2: u16, num_possible_nodes: u32) {
    let mut first_election_cnt = 0;
    let mut flag = false;
    let mut h1 = Client::new(port1);
    let mut h2 = Client::new(port2);

    let start = std::time::Instant::now();
    while first_election_cnt < 50 {
        let mut res = h1.send_and_get_vec("role", num_possible_nodes);
        if res.is_empty() {
            res = h2.send_and_get_vec("role", num_possible_nodes);
        }
        println!(
            "[{}ms] Poll {}: port1={} port2={} res={:?}",
            start.elapsed().as_millis(),
            first_election_cnt,
            port1,
            port2,
            res,
        );
        if res.contains(&format!("127.0.0.1:{}:{}", port1, "leader"))
            || res.contains(&format!("127.0.0.1:{}:{}", port2, "leader"))
        {
            flag = true;
            break;
        }
        first_election_cnt += 1;
        sleep(Duration::from_millis(500));
    }
    if !flag {
        println!(
            "[{}ms] Leader election failed after {} attempts (ports: {}, {})",
            start.elapsed().as_millis(),
            first_election_cnt,
            port1,
            port2
        );
    }
    assert!(flag, "{order} election fail");
}
