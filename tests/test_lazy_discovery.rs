use crate::common::{array, contains_all};
use common::{ServerEnv, spawn_server_process};
use duva::clients::ClientStreamHandler;

mod common;

#[tokio::test]
async fn test_lazy_discovery_of_leader() {
    // GIVEN
    let target_env = ServerEnv::default();
    let target_p = spawn_server_process(&target_env);
    let mut target_h = ClientStreamHandler::new(target_p.bind_addr()).await;

    target_h.send_and_get(&array(vec!["SET", "key", "value"])).await;
    target_h.send_and_get(&array(vec!["SET", "key2", "value2"])).await;

    assert!(contains_all(
        target_h.send_and_get(&array(vec!["KEYS", "*"])).await,
        vec!["key", "key2"]
    ));

    let other_env = ServerEnv::default();
    let other_p = spawn_server_process(&other_env);
    let mut other_h = ClientStreamHandler::new(other_p.bind_addr()).await;

    other_h.send_and_get(&array(vec!["SET", "other", "value"])).await;
    other_h.send_and_get(&array(vec!["SET", "other2", "value2"])).await;

    assert!(contains_all(
        other_h.send_and_get(&array(vec!["KEYS", "*"])).await,
        vec!["other", "other2"]
    ));

    // WHEN
    assert_eq!(
        target_h
            .send_and_get(&array(vec!["REPLICAOF", "127.0.0.1", &other_env.port.to_string()]))
            .await,
        "+OK\r\n"
    );

    // THEN
    let cluster_info = other_h.send_and_get(&array(vec!["CLUSTER", "INFO"])).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    let response = target_h.send_and_get(&array(vec!["KEYS", "*"])).await;
    assert!(contains_all(response, vec!["other", "other2"]));
}
