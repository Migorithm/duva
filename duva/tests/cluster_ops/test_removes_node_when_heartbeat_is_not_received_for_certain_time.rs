use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, form_cluster};

fn run_removes_node_when_heartbeat_is_not_received_for_certain_time(
    with_append_only: bool,
) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only).with_hf(5);
    let mut repl_env = ServerEnv::default().with_append_only(with_append_only).with_hf(5);

    let [leader_p, repl_p] = form_cluster([&mut env, &mut repl_env]);
    sleep(Duration::from_millis(200));

    let mut h = Client::new(leader_p.port);
    assert_eq!(h.send_and_get_vec("cluster info", 1), vec!["cluster_known_nodes:1"]);

    // WHEN
    drop(repl_p);

    sleep(Duration::from_millis(1000));

    //THEN
    assert_eq!(h.send_and_get_vec("cluster info", 1), vec!["cluster_known_nodes:0"]);

    Ok(())
}

#[test]
fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() -> anyhow::Result<()> {
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(false)?;
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(true)?;

    Ok(())
}
