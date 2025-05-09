use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};

fn run_reconnection_on_reboot(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut env2 = ServerEnv::default().with_append_only(with_append_only);

    // Form cluster with leader and replica
    let [mut p1, mut p2] = form_cluster(&mut [&mut env1, &mut env2], true);

    p2.wait_for_message(&p1.heartbeat_msg(0))?;
    p1.wait_for_message(&p2.heartbeat_msg(0))?;

    // Set some values
    let mut cli_to_p1 = Client::new(p1.port);
    cli_to_p1.send_and_get("SET x value1".as_bytes(), 1);
    cli_to_p1.send_and_get("SET y value2".as_bytes(), 1);
    cli_to_p1.send_and_get("SET z value3".as_bytes(), 1);
    drop(cli_to_p1);

    p2.kill()?;

    // WHEN running repl without p1 bind address
    let env2 = env2.clone();
    p2 = spawn_server_process(&env2, true)?;

    //THEN

    p2.wait_for_message(&p1.heartbeat_msg(0))?;
    p1.wait_for_message(&p2.heartbeat_msg(0))?;
    let mut cli_to_p2 = Client::new(p2.port);
    let p2_role = cli_to_p2.send_and_get("ROLE".as_bytes(), 1);

    let mut cli_to_p1 = Client::new(p1.port);
    let p1_role = cli_to_p1.send_and_get("ROLE".as_bytes(), 1);

    assert_eq!(p2_role, vec!["follower".to_string()]);
    assert_eq!(p1_role, vec!["leader".to_string()]);

    // Check that the values are still there
    let val = cli_to_p2.send_and_get("GET x", 1);
    assert_eq!(val, vec!["value1"]);
    let val = cli_to_p2.send_and_get("GET y", 1);
    assert_eq!(val, vec!["value2"]);

    let val = cli_to_p2.send_and_get("GET z", 1);
    assert_eq!(val, vec!["value3"]);

    Ok(())
}

#[test]
fn test_reconnection_on_reboot() -> anyhow::Result<()> {
    run_reconnection_on_reboot(false)?;
    run_reconnection_on_reboot(true)?;

    Ok(())
}
