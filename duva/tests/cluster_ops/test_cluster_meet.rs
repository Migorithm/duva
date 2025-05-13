use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};

fn run_cluster_meet(append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let (mut env, mut env2) = (
        ServerEnv::default().with_append_only(append_only),
        ServerEnv::default().with_append_only(append_only),
    );
    let [_leader_p, _repl_p] = form_cluster([&mut env, &mut env2], false);

    // WHEN - load up standalone replica set
    let env3 = ServerEnv::default();
    let _test_p = spawn_server_process(&env3, false).unwrap();

    let mut client_handler = Client::new(env3.port);
    let cmd = format!("cluster meet localhost:{}", env.port);

    // WHEN & THEN
    assert_eq!(client_handler.send_and_get(&cmd), "OK");

    // WHEN & THEN
    assert_eq!(client_handler.send_and_get_vec("cluster info", 1), vec!["cluster_known_nodes:2"]);
    assert_eq!(client_handler.send_and_get("role"), "leader");

    Ok(())
}

/// Test if `cluster meet` lets a replica set joins the existing cluster without losing their ownership over data sets
// ! migration should be done either separately or togerther with cluster meet operation.
// ! this should be configurable depending on business usecases
#[test]
fn test_cluster_meet() -> anyhow::Result<()> {
    run_cluster_meet(false)?;
    run_cluster_meet(true)?;

    Ok(())
}
