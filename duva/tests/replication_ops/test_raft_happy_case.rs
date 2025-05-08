use std::{thread::sleep, time::Duration};

use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_set_operation_reaches_to_all_replicas(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);

    // loads the leader/follower processes
    let mut leader_p = spawn_server_process(&env, true)?;
    let mut client_handler = Client::new(leader_p.port);

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_file_name("follower_dbfilename")
        .with_append_only(with_append_only);

    let mut repl_p = spawn_server_process(&repl_env, true)?;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0))?;
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0))?;

    // WHEN -- set operation is made
    client_handler.send_and_get("SET foo bar", 1);

    //THEN
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX * 2));

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
