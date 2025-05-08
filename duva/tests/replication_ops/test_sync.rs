use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};

fn run_full_sync_on_newly_added_replica(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);

    // Start the leader server as a child process
    let mut leader_p = spawn_server_process(&env, true)?;
    let mut h = Client::new(leader_p.port);

    h.send_and_get("SET foo bar", 1);

    // WHEN run replica
    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);

    let mut replica_process = spawn_server_process(&repl_env, true)?;
    check_internodes_communication(&mut [&mut leader_p, &mut replica_process], 0, 1000)?;

    // THEN
    let mut client_to_repl = Client::new(replica_process.port);
    assert_eq!(client_to_repl.send_and_get("KEYS *", 1), vec!["0) \"foo\""]);

    Ok(())
}

#[test]
fn test_full_sync_on_newly_added_replica() -> anyhow::Result<()> {
    run_full_sync_on_newly_added_replica(false)?;
    run_full_sync_on_newly_added_replica(true)?;

    Ok(())
}
