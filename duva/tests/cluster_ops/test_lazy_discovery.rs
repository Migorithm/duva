use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_lazy_discovery_of_leader() {
    // GIVEN
    let target_env = ServerEnv::default();
    let leader_p = spawn_server_process(&target_env).await;

    let mut target_h = Client::new(leader_p.port);

    target_h.send_and_get("SET key value".as_bytes(), 1).await;
    target_h.send_and_get("SET key2 value2".as_bytes(), 1).await;
    assert_eq!(
        target_h.send_and_get("KEYS *".as_bytes(), 2).await,
        vec!["0) \"key\"", "1) \"key2\""]
    );

    let replica_env = ServerEnv::default();
    let replica_p = spawn_server_process(&replica_env).await;
    let mut other_h = Client::new(replica_p.port);

    other_h.send_and_get("SET other value".as_bytes(), 1).await;
    other_h.send_and_get("SET other2 value2".as_bytes(), 1).await;

    let cluster_info = other_h.send_and_get("CLUSTER INFO".as_bytes(), 1).await;
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:0");

    // TODO can be flaky?
    assert_eq!(
        other_h.send_and_get("KEYS *".as_bytes(), 2).await,
        vec!["0) \"other2\"", "1) \"other\""]
    );

    // WHEN
    assert_eq!(
        target_h
            .send_and_get(format!("REPLICAOF 127.0.0.1 {}", &replica_env.port).as_bytes(), 1)
            .await,
        ["OK".to_string(),]
    );

    // THEN
    assert_eq!(
        other_h.send_and_get("CLUSTER INFO".as_bytes(), 1).await,
        vec!["cluster_known_nodes:1".to_string()]
    );

    assert_eq!(
        target_h.send_and_get("KEYS *".as_bytes(), 2).await,
        vec!["0) \"other2\"", "1) \"other\""]
    );
}
