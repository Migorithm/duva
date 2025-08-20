use std::{thread::sleep, time::Duration};

use duva::prelude::ELECTION_TIMEOUT_MAX;

use crate::common::{Client, ServerEnv, form_cluster};

fn run_set_operation_reaches_to_all_replicas(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default()
        .with_append_only(with_append_only)
        .with_file_name("follower_dbfilename");

    let [leader_p, repl_p] = form_cluster([&mut env, &mut follower_env1]);

    // WHEN -- set operation is made
    let mut client_handler = Client::new(leader_p.port);
    client_handler.send_and_get("SET foo bar");

    //THEN
    sleep(Duration::from_millis(ELECTION_TIMEOUT_MAX + 15));

    let mut client = Client::new(repl_p.port);
    assert_eq!(client.send_and_get("GET foo"), "bar");

    Ok(())
}

#[test]
fn test_set_operation_reaches_to_all_replicas() -> anyhow::Result<()> {
    run_set_operation_reaches_to_all_replicas(false)?;
    run_set_operation_reaches_to_all_replicas(true)?;

    Ok(())
}
