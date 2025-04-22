use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};

#[tokio::test]
async fn test_full_sync_on_newly_added_replica() {
    // GIVEN
    let env =
        ServerEnv::default().with_topology_path("test_full_sync_on_newly_added_replica-leader.tp");
    // Start the leader server as a child process
    let mut leader_p = spawn_server_process(&env);
    let mut h = Client::new(leader_p.port);

    h.send_and_get("SET foo bar", 1);

    // WHEN run replica
    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("test_full_sync_on_newly_added_replica-follower.tp");

    let mut replica_process = spawn_server_process(&repl_env);
    check_internodes_communication(&mut [&mut leader_p, &mut replica_process], 0, 1000).unwrap();

    // THEN
    let mut client_to_repl = Client::new(replica_process.port);
    assert_eq!(client_to_repl.send_and_get("KEYS *", 1), vec!["0) \"foo\""]);
}
