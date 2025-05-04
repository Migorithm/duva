use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_removes_node_when_heartbeat_is_not_received_for_certain_time(
    with_append_only: bool,
) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&env).await?;

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut repl_p = spawn_server_process(&repl_env).await?;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0)).await?;
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0)).await?;

    let mut h = Client::new(leader_p.port);
    let cmd = "cluster info";
    let cluster_info = h.send_and_get(cmd, 1);
    assert_eq!(cluster_info.await.first().unwrap(), "cluster_known_nodes:1");

    // WHEN
    repl_p.kill().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let cluster_info = h.send_and_get(cmd, 1);

    //THEN
    assert_eq!(cluster_info.await.first().unwrap(), "cluster_known_nodes:0");

    Ok(())
}

#[tokio::test]
async fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() -> anyhow::Result<()> {
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(false).await?;
    run_removes_node_when_heartbeat_is_not_received_for_certain_time(true).await?;

    Ok(())
}
