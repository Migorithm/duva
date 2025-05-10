use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lazy_discovery_of_leader(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut p1 = spawn_server_process(&env1, false)?;

    let mut p1_h = Client::new(p1.port);

    p1_h.send_and_get("set key 1", 1);
    p1_h.send_and_get("set key2 2", 1);
    assert_eq!(p1_h.send_and_get("KEYS *", 2), vec!["0) \"key\"", "1) \"key2\""]);

    let env2 = ServerEnv::default().with_append_only(with_append_only);
    let p2 = spawn_server_process(&env2, false)?;

    let mut p2_h = Client::new(p2.port);
    p2_h.send_and_get("set other value", 1);
    p2_h.send_and_get("set other2 value2", 1);
    assert_eq!(p2_h.send_and_get("KEYS *", 2), vec!["0) \"other2\"", "1) \"other\""]);

    // WHEN
    assert_eq!(
        p1_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &env2.port), 1),
        ["OK".to_string(),]
    );

    // TODO(dpark): Would there be a way to perform a closed-loop wait?

    // THEN
    assert_eq!(p1_h.send_and_get("role", 1), vec!["follower"]);
    assert_eq!(p2_h.send_and_get("role", 1), vec!["leader"]);

    // * Following is required to test replicaof successuflly update topology changes
    p1.terminate()?;
    let new_env_with_same_topology = ServerEnv::default()
        .with_topology_path(env1.topology_path)
        .with_append_only(with_append_only);
    let p1 = spawn_server_process(&new_env_with_same_topology, true)?;

    let mut p1_h = Client::new(p1.port);
    assert_eq!(p1_h.send_and_get("get key", 1), vec!["(nil)"]);
    assert_eq!(p1_h.send_and_get("get key2", 1), vec!["(nil)"]);
    assert_eq!(p1_h.send_and_get("get other", 1), vec!["value"]);
    assert_eq!(p1_h.send_and_get("get other2", 1), vec!["value2"]);

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
    let p1 = spawn_server_process(&env1, false)?;

    let mut p1_h = Client::new(p1.port);

    p1_h.send_and_get("set key 1", 1);

    // WHEN
    assert_eq!(
        p1_h.send_and_get(format!("REPLICAOF 127.0.0.1 {}", &env1.port), 1),
        ["(error) invalid operation: cannot replicate to self".to_string(),]
    );

    assert_eq!(p1_h.send_and_get("get key", 1), vec!["1"]);

    Ok(())
}

#[test]
fn test_run_invalid_replicaof() -> anyhow::Result<()> {
    run_invalid_replicaof(false)?;
    run_invalid_replicaof(true)?;

    Ok(())
}
