use crate::common::Client;
use crate::common::ServerEnv;
use crate::common::form_cluster;
use duva::prelude::LEADER_HEARTBEAT_INTERVAL_MAX;
use std::thread::sleep;
use std::time::Duration;

fn run_discover_leader(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower1_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower2_env = ServerEnv::default().with_append_only(with_append_only);

    let [mut leader_p, _follower1, _follower2] =
        form_cluster([&mut leader_env, &mut follower1_env, &mut follower2_env]);

    let mut h = Client::new(leader_p.port);

    // generate keys
    let num_of_keys = 100;
    for i in 0..num_of_keys {
        h.send_and_get(format!("SET {i} {i}"));
    }
    // wait until all keys are replicated
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));

    // WHEN
    leader_p.kill()?;
    // wait for the election to complete
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX * 2));

    // THEN
    let res = h.send_and_get_vec("KEYS *", num_of_keys);

    assert_eq!(res.len(), num_of_keys as usize);

    Ok(())
}

#[test]
fn test_discover_leader() -> anyhow::Result<()> {
    run_discover_leader(false)?;
    run_discover_leader(true)?;
    Ok(())
}
