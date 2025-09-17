use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_full_sync_on_newly_added_replica(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);

    // Start the leader server as a child process
    let leader_p = spawn_server_process(&env)?;
    let mut h = Client::new(leader_p.port);

    h.send_and_get("SET foo bar");

    // WHEN run replica
    let repl_env = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);

    let replica_process = spawn_server_process(&repl_env)?;

    // THEN
    let mut client_to_repl = Client::new(replica_process.port);
    assert_eq!(client_to_repl.send_and_get_vec("KEYS *", 1), vec!["1) \"foo\""]);

    Ok(())
}

#[test]
fn test_full_sync_on_newly_added_replica() -> anyhow::Result<()> {
    run_full_sync_on_newly_added_replica(true)?;

    Ok(())
}
