use crate::common::array;
use common::{spawn_server_process, ServerEnv};
use duva::clients::ClientStreamHandler;

mod common;

#[tokio::test]
async fn test_lazy_discovery_of_leader(){
    // GIVEN
    let target_env = ServerEnv::default();
    let target_p = spawn_server_process(&target_env);
    let mut target_h = ClientStreamHandler::new(target_p.bind_addr()).await;

    let other_env = ServerEnv::default();
    let other_p = spawn_server_process(&other_env);
    let mut other_h = ClientStreamHandler::new(other_p.bind_addr()).await;
    // WHEN
    assert_eq!(target_h.send_and_get(&array(vec!["REPLICAOF", "127.0.0.1", &other_env.port.to_string()])).await, "+OK\r\n");

    // THEN
    let cluster_info = other_h.send_and_get(&array(vec!["CLUSTER","INFO"])).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));
}
