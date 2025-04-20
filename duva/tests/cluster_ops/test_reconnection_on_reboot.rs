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

    let mut cli_to_follower = Client::new(repl_env.port);
    let role = cli_to_follower.send_and_get("ROLE".as_bytes(), 1);
    assert_eq!(role, vec!["follower".to_string()]);

    let mut cli_to_leader = Client::new(leader_p.port);
    let role = cli_to_leader.send_and_get("ROLE".as_bytes(), 1);
    assert_eq!(role, vec!["leader".to_string()]);
}
