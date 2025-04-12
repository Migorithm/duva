use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_lazy_discovery_of_leader() {
    // GIVEN
    let target_env = ServerEnv::default();
    let target_p = spawn_server_process(&target_env);

    let mut target_h = Client::new(target_p.port);

    target_h.send_and_get("SET key value".as_bytes(), 1);
    target_h.send_and_get("SET key2 value2".as_bytes(), 1);
    assert_eq!(target_h.send_and_get("KEYS *".as_bytes(), 2), vec!["0) \"key\"", "1) \"key2\""]);

    let other_env = ServerEnv::default();
    let other_p = spawn_server_process(&other_env);
    let mut other_h = Client::new(other_p.port);

    other_h.send_and_get("SET other value".as_bytes(), 1);
    other_h.send_and_get("SET other2 value2".as_bytes(), 1);

    let cluster_info = other_h.send_and_get("CLUSTER INFO".as_bytes(), 1);
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:0");

    // TODO can be flaky?
    assert_eq!(other_h.send_and_get("KEYS *".as_bytes(), 2), vec!["0) \"other2\"", "1) \"other\""]);

    // WHEN
    assert_eq!(
        target_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &other_env.port).as_bytes(), 2),
        [
            format!(
                "Topology change: [PeerIdentifier(\"127.0.0.1:{}\"), PeerIdentifier(\"127.0.0.1:{}\")]",
                other_env.port, target_env.port,
            ),
            "OK".to_string(),
        ]
    );

    // THEN
    assert_eq!(
        other_h.send_and_get("CLUSTER INFO".as_bytes(), 2),
        vec![
            format!(
                "Topology change: [PeerIdentifier(\"127.0.0.1:{}\"), PeerIdentifier(\"127.0.0.1:{}\")]",
                target_env.port, other_env.port,
            ),
            "cluster_known_nodes:1".to_string()
        ]
    );

    assert_eq!(
        target_h.send_and_get("KEYS *".as_bytes(), 2),
        vec!["0) \"other2\"", "1) \"other\""]
    );
}
