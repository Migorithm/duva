use std::{thread::sleep, time::Duration};

use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

use crate::common::{Client, ServerEnv, form_cluster};

fn run_set_operation_reaches_to_all_replicas(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default()
        .with_append_only(with_append_only)
        .with_file_name("follower_dbfilename");

    let [leader_p, repl_p] = form_cluster([&mut env, &mut follower_env1], true);

    // WHEN -- set operation is made
    let mut client_handler = Client::new(leader_p.port);
    client_handler.send_and_get("SET foo bar", 1);

    //THEN
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX + 15));

    let mut client = Client::new(repl_p.port);
    let res = client.send_and_get("GET foo", 1);
    assert_eq!(res, vec!["bar"]);

    Ok(())
}

#[test]
fn test_set_operation_reaches_to_all_replicas() -> anyhow::Result<()> {
    run_set_operation_reaches_to_all_replicas(false)?;
    run_set_operation_reaches_to_all_replicas(true)?;

    Ok(())
}
