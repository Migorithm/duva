/// issue: 297
use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test() {
    // GIVEN
    let env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&env);

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("reconnection_tp.txt");
    let mut repl_p = spawn_server_process(&repl_env);

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    repl_p.kill().unwrap();

    // WHEN running repl without leader bind address
    let repl_env = ServerEnv::default().with_topology_path("reconnection_tp.txt");
    let mut repl_p = spawn_server_process(&repl_env);

    //THEN
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let mut other_h = Client::new(repl_env.port);
    let cluster_info = other_h.send_and_get("CLUSTER INFO".as_bytes(), 1);
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:1");
}
