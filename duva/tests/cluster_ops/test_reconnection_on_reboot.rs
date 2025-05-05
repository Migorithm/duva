/// issue: 297
use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_reconnection_on_reboot(with_append_only: bool, kill_p1: bool) -> anyhow::Result<()> {
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
    cli_to_p1.send("SET x value1".as_bytes()).await?;
    cli_to_p1.send("SET y value2".as_bytes()).await?;
    cli_to_p1.send("SET z value3".as_bytes()).await?;
    drop(cli_to_p1);

    if kill_p1 {
        p1.kill().await?;
        // WHEN running repl without p2 bind address
        let env1 = ServerEnv::default()
            .with_topology_path(env1.topology_path)
            .with_append_only(with_append_only);
        p1 = spawn_server_process(&env1).await?;
    } else {
        p2.kill().await?;
        // WHEN running repl without p1 bind address
        let env2 = ServerEnv::default()
            .with_topology_path(env2.topology_path)
            .with_append_only(with_append_only);

        p2 = spawn_server_process(&env2).await?;
    }

    //THEN

    p2.wait_for_message(&p1.heartbeat_msg(0)).await?;
    p1.wait_for_message(&p2.heartbeat_msg(0)).await?;
    let mut cli_to_p2 = Client::new(p2.port);
    let p2_role = cli_to_p2.send_and_get("ROLE".as_bytes(), 1).await;

    let mut cli_to_p1 = Client::new(p1.port);
    let p1_role = cli_to_p1.send_and_get("ROLE".as_bytes(), 1).await;

    if kill_p1 {
        assert_eq!(p2_role, vec!["leader".to_string()]);
        assert_eq!(p1_role, vec!["follower".to_string()]);
    } else {
        assert_eq!(p2_role, vec!["follower".to_string()]);
        assert_eq!(p1_role, vec!["leader".to_string()]);
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
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
    run_reconnection_on_reboot(false, false).await?;
    run_reconnection_on_reboot(true, false).await?;

    Ok(())
}
