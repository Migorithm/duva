use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, form_cluster};

fn run_removes_node_when_heartbeat_is_not_received_for_certain_time(
    with_append_only: bool,
) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, mut repl_p] = form_cluster(&mut [&mut env, &mut repl_env], true);

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
