use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_lazy_discovery_of_leader(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);
    let leader_p = spawn_server_process(&env).await?;

    let mut target_h = Client::new(leader_p.port);

    target_h.send_and_get("SET key value", 1).await;
    target_h.send_and_get("SET key2 value2", 1).await;
    assert_eq!(target_h.send_and_get("KEYS *", 2).await, vec!["0) \"key\"", "1) \"key2\""]);

    let replica_env = ServerEnv::default().with_append_only(with_append_only);
    let replica_p = spawn_server_process(&replica_env).await?;
    let mut other_h = Client::new(replica_p.port);

    other_h.send_and_get("SET other value", 1).await;
    other_h.send_and_get("SET other2 value2", 1).await;

    let cluster_info = other_h.send_and_get("CLUSTER INFO", 1).await;
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:0");

    assert_eq!(other_h.send_and_get("KEYS *", 2).await, vec!["0) \"other2\"", "1) \"other\""]);

    // WHEN
    assert_eq!(
        target_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &replica_env.port), 1).await,
        ["OK".to_string(),]
    );

    // TODO(dpark): Would there be a way to perform a closed-loop wait?
    tokio::time::sleep(tokio::time::Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX * 2)).await;

    // THEN
    assert_eq!(target_h.send_and_get("GET other".as_bytes(), 1).await, vec!["value"]);
    assert_eq!(target_h.send_and_get("GET other2".as_bytes(), 1).await, vec!["value2"]);

    Ok(())
}

#[tokio::test]
async fn test_lazy_discovery_of_leader() -> anyhow::Result<()> {
    run_lazy_discovery_of_leader(false).await?;
    run_lazy_discovery_of_leader(true).await?;

    Ok(())
}
