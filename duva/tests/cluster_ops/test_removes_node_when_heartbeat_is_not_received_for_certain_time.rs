use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_removes_node_when_heartbeat_is_not_received_for_certain_time(
    with_append_only: bool,
) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&env, true)?;

    let repl_env = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut repl_p = spawn_server_process(&repl_env, true)?;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0))?;
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0))?;

    let mut h = Client::new(leader_p.port);
    let cmd = "cluster info";
    let cluster_info = h.send_and_get(cmd, 1);
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:1");

    // WHEN
    repl_p.kill()?;
    sleep(Duration::from_secs(2));
    let cluster_info = h.send_and_get(cmd, 1);

    //THEN
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:0");

    Ok(())
}

#[test]
fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() -> anyhow::Result<()> {
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(false)?;
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(true)?;

    Ok(())
}
