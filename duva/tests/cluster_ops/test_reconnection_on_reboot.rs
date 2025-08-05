use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};

fn run_reconnection_on_reboot(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut env2 = ServerEnv::default().with_append_only(with_append_only);

    // Form cluster with leader and replica
    let [p1, mut p2] = form_cluster([&mut env1, &mut env2]);

    // Set some values
    let mut cli_to_p1 = Client::new(p1.port);
    cli_to_p1.send_and_get("SET x value1");
    cli_to_p1.send_and_get("SET y value2");
    cli_to_p1.send_and_get("SET z value3");
    drop(cli_to_p1);

    p2.kill()?;

    // WHEN running repl without p1 bind address
    let env2 = env2.clone();
    p2 = spawn_server_process(&env2)?;

    //THEN

    let mut cli_to_p2 = Client::new(p2.port);
    let p2_role = cli_to_p2.send_and_get_vec("ROLE", 2);

    let mut cli_to_p1 = Client::new(p1.port);
    let p1_role = cli_to_p1.send_and_get_vec("ROLE", 2);

    assert!(p2_role.contains(&format!("127.0.0.1:{}:{}", p2.port, "follower")));
    assert!(p1_role.contains(&format!("127.0.0.1:{}:{}", p1.port, "leader")));

    // Check that the values are still there
    assert_eq!(cli_to_p2.send_and_get("GET x"), "value1");
    assert_eq!(cli_to_p2.send_and_get("GET y"), "value2");
    assert_eq!(cli_to_p2.send_and_get("GET z"), "value3");

    Ok(())
}

#[test]
fn test_reconnection_on_reboot() -> anyhow::Result<()> {
    run_reconnection_on_reboot(false)?;
    run_reconnection_on_reboot(true)?;

    Ok(())
}
