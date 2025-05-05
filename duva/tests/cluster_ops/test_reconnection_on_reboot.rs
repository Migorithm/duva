/// issue: 297
use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_reconnection_on_reboot(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut p1 = spawn_server_process(&env1).await?;

    let env2 = ServerEnv::default()
        .with_leader_bind_addr(p1.bind_addr())
        .with_append_only(with_append_only);
    let mut p2 = spawn_server_process(&env2).await?;

    p2.wait_for_message(&p1.heartbeat_msg(0)).await?;
    p1.wait_for_message(&p2.heartbeat_msg(0)).await?;

    // Set some values
    let mut cli_to_p1 = Client::new(p1.port);
    cli_to_p1.send_and_get("SET x value1".as_bytes(), 1).await;
    cli_to_p1.send_and_get("SET y value2".as_bytes(), 1).await;
    cli_to_p1.send_and_get("SET z value3".as_bytes(), 1).await;
    drop(cli_to_p1);

    p2.kill().await?;
    // WHEN running repl without p1 bind address
    let env2 = ServerEnv::default()
        .with_topology_path(env2.topology_path)
        .with_append_only(with_append_only);

    p2 = spawn_server_process(&env2).await?;

    //THEN

    p2.wait_for_message(&p1.heartbeat_msg(0)).await?;
    p1.wait_for_message(&p2.heartbeat_msg(0)).await?;
    let mut cli_to_p2 = Client::new(p2.port);
    let p2_role = cli_to_p2.send_and_get("ROLE".as_bytes(), 1).await;

    let mut cli_to_p1 = Client::new(p1.port);
    let p1_role = cli_to_p1.send_and_get("ROLE".as_bytes(), 1).await;

    assert_eq!(p2_role, vec!["follower".to_string()]);
    assert_eq!(p1_role, vec!["leader".to_string()]);

    // Check that the values are still there
    let val = cli_to_p2.send_and_get("GET x", 1).await;
    assert_eq!(val, vec!["value1"]);
    let val = cli_to_p2.send_and_get("GET y", 1).await;
    assert_eq!(val, vec!["value2"]);

    let val = cli_to_p2.send_and_get("GET z", 1).await;
    assert_eq!(val, vec!["value3"]);

    Ok(())
}

#[tokio::test]
async fn test_reconnection_on_reboot() -> anyhow::Result<()> {
    run_reconnection_on_reboot(false).await?;
    run_reconnection_on_reboot(true).await?;

    Ok(())
}
