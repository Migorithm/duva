/// issue: 297
use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_reconnection_on_reboot(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&env).await?;

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut repl_p = spawn_server_process(&repl_env).await?;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0)).await?;
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0)).await?;

    repl_p.kill().await?;

    // WHEN running repl without leader bind address
    let repl_env = ServerEnv::default()
        .with_topology_path(repl_env.topology_path)
        .with_append_only(with_append_only);
    let mut repl_p = spawn_server_process(&repl_env).await?;

    //THEN
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0)).await?;
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0)).await?;

    let mut cli_to_follower = Client::new(repl_env.port);
    let role = cli_to_follower.send_and_get("ROLE".as_bytes(), 1).await;
    assert_eq!(role, vec!["follower".to_string()]);

    let mut cli_to_leader = Client::new(leader_p.port);
    let role = cli_to_leader.send_and_get("ROLE".as_bytes(), 1).await;
    assert_eq!(role, vec!["leader".to_string()]);

    Ok(())
}

#[tokio::test]
async fn test_reconnection_on_reboot() -> anyhow::Result<()> {
    run_reconnection_on_reboot(false).await?;
    run_reconnection_on_reboot(true).await?;

    Ok(())
}
