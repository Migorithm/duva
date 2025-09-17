use duva::prelude::ELECTION_TIMEOUT_MAX;

use crate::{
    common::{Client, ServerEnv, form_cluster},
    replication_ops::panic_if_election_not_done,
};
use std::{thread::sleep, time::Duration};

// ! EDGE case : when last_log_term is not updated, after the election, first write operation succeeds but second one doesn't
// ! This is because the leader doesn't have the last_log_term of the first write operation
// ! This test is to see if the leader can set the value twice after the election
fn run_set_twice_after_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // WHEN
    drop(leader_p);

    sleep(Duration::from_millis(ELECTION_TIMEOUT_MAX + 300));

    // THEN

    let mut h1 = Client::new(follower_p1.port);
    let mut h2 = Client::new(follower_p2.port);

    panic_if_election_not_done(follower_p1.port, follower_p2.port);

    let res = h1.send_and_get_vec("role", 3);

    if res.contains(&format!("127.0.0.1:{}:{}", follower_p1.port, "leader")) {
        // THEN - one of the replicas should become the leader
        assert_eq!(h1.send_and_get("set 1 2"), "OK");
        assert_eq!(h1.send_and_get("set 2 3"), "OK");
    }

    if res.contains(&format!("127.0.0.1:{}:{}", follower_p2.port, "leader")) {
        // THEN - one of the replicas should become the leader
        assert_eq!(h2.send_and_get("set 1 2"), "OK");
        assert_eq!(h2.send_and_get("set 2 3"), "OK");
    }
    Ok(())
}

#[test]
fn test_set_twice_after_election() -> anyhow::Result<()> {
    run_set_twice_after_election(true)?;
    Ok(())
}
