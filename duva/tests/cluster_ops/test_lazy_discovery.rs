use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lazy_discovery_of_leader(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN

    let env1 = ServerEnv::default().with_append_only(with_append_only);
    let p1 = spawn_server_process(&env1)?;

    let mut p1_h = Client::new(p1.port);

    p1_h.send_and_get("set key 1");
    p1_h.send_and_get("set key2 2");
    assert_eq!(p1_h.send_and_get_vec("KEYS *", 2), vec!["1) \"key\"", "2) \"key2\""]);

    let env2 = ServerEnv::default().with_append_only(with_append_only);
    let p2 = spawn_server_process(&env2)?;

    let mut p2_h = Client::new(p2.port);
    p2_h.send_and_get("set other value");
    p2_h.send_and_get("set other2 value2");
    assert_eq!(p2_h.send_and_get_vec("KEYS *", 2), vec!["1) \"other2\"", "2) \"other\""]);

    // WHEN
    assert_eq!(p1_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &env2.port)), "OK");

    // THEN
    let role_response = p1_h.send_and_get_vec("role", 2);
    assert!(role_response.contains(&format!("127.0.0.1:{}:{}", p1.port, "follower")));
    assert!(role_response.contains(&format!("127.0.0.1:{}:{}", env2.port, "leader")));

    // * Following is required to test replicaof successuflly update topology changes
    drop(p1);

    let new_env_with_same_topology = ServerEnv::default()
        .with_port(env1.port)
        .with_topology_path(env1.topology_path)
        .with_append_only(with_append_only);

    let p1 = spawn_server_process(&new_env_with_same_topology)?;

    let mut p1_h = Client::new(p1.port);

    assert_eq!(p1_h.send_and_get("get key"), "(nil)");
    assert_eq!(p1_h.send_and_get("get key2"), "(nil)");
    assert_eq!(p1_h.send_and_get("get other"), "value");
    assert_eq!(p1_h.send_and_get("get other2"), "value2");

    Ok(())
}

#[test]
fn test_lazy_discovery_of_leader() -> anyhow::Result<()> {
    run_lazy_discovery_of_leader(false)?;
    run_lazy_discovery_of_leader(true)?;
    Ok(())
}

fn run_invalid_replicaof(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env1 = ServerEnv::default().with_append_only(with_append_only);
    let p1 = spawn_server_process(&env1)?;

    let mut p1_h = Client::new(p1.port);

    p1_h.send_and_get("set key 1");

    // WHEN
    assert_eq!(
        p1_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &env1.port)),
        "(error) invalid operation: cannot replicate to self"
    );

    assert_eq!(p1_h.send_and_get("get key"), "1");

    Ok(())
}

#[test]
fn test_run_invalid_replicaof() -> anyhow::Result<()> {
    run_invalid_replicaof(true)?;

    Ok(())
}
