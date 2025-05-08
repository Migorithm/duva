use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_lazy_discovery_of_leader(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut p1 = spawn_server_process(&env1).await?;

    let mut p1_h = Client::new(p1.port);

    p1_h.send_and_get("set key 1", 1).await;
    p1_h.send_and_get("set key2 2", 1).await;
    assert_eq!(p1_h.send_and_get("KEYS *", 2).await, vec!["0) \"key\"", "1) \"key2\""]);

    let env2 = ServerEnv::default().with_append_only(with_append_only);
    let p2 = spawn_server_process(&env2).await?;

    let mut p2_h = Client::new(p2.port);
    p2_h.send_and_get("set other value", 1).await;
    p2_h.send_and_get("set other2 value2", 1).await;
    assert_eq!(p2_h.send_and_get("KEYS *", 2).await, vec!["0) \"other2\"", "1) \"other\""]);

    // WHEN
    assert_eq!(
        p1_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &env2.port), 1).await,
        ["OK".to_string(),]
    );

    // TODO(dpark): Would there be a way to perform a closed-loop wait?

    // THEN
    assert_eq!(p1_h.send_and_get("role", 1).await, vec!["follower"]);
    assert_eq!(p2_h.send_and_get("role", 1).await, vec!["leader"]);

    // * Following is required to test replicaof successuflly update topology changes
    p1.terminate().await?;
    let new_env_with_same_topology = ServerEnv::default()
        .with_topology_path(env1.topology_path)
        .with_append_only(with_append_only);
    let p1 = spawn_server_process(&new_env_with_same_topology).await?;

    let mut p1_h = Client::new(p1.port);
    assert_eq!(p1_h.send_and_get("get key", 1).await, vec!["(nil)"]);
    assert_eq!(p1_h.send_and_get("get key2", 1).await, vec!["(nil)"]);
    assert_eq!(p1_h.send_and_get("get other", 1).await, vec!["value"]);
    assert_eq!(p1_h.send_and_get("get other2", 1).await, vec!["value2"]);

    Ok(())
}

#[tokio::test]
async fn test_lazy_discovery_of_leader() -> anyhow::Result<()> {
    run_lazy_discovery_of_leader(false).await?;
    run_lazy_discovery_of_leader(true).await?;

    Ok(())
}
