use crate::common::{Client, ServerEnv, form_cluster};

fn run_cluster_meet(append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let (mut env, mut env2) = (
        ServerEnv::default().with_append_only(append_only),
        ServerEnv::default().with_append_only(append_only),
    );
    let [_leader_p, _repl_p] = form_cluster([&mut env, &mut env2]);

    // WHEN - load up replica set
    let (mut env3, mut env4) = (
        ServerEnv::default().with_append_only(append_only),
        ServerEnv::default().with_append_only(append_only),
    );
    let [_leader_p2, _repl_p2] = form_cluster([&mut env3, &mut env4]);

    let mut client_handler = Client::new(env3.port);
    let cmd = format!("cluster meet localhost:{}", env.port);

    // WHEN & THEN
    assert_eq!(client_handler.send_and_get(&cmd), "OK");

    // WHEN & THEN
    let mut success_cnt = 0;
    // backoff time try for 3 seconds
    let until = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while until > std::time::Instant::now() {
        let res = client_handler.send_and_get_vec("cluster info", 1);
        if res.contains(&"cluster_known_nodes:3".to_string()) {
            assert_eq!(client_handler.send_and_get("role"), "leader");
            success_cnt += 1;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut replica_handler = Client::new(env4.port);
    // WHEN query is given to joining replica
    while until > std::time::Instant::now() {
        let res = replica_handler.send_and_get_vec("cluster info", 1);
        if res.contains(&"cluster_known_nodes:3".to_string()) {
            assert_eq!(replica_handler.send_and_get("role"), "follower");
            success_cnt += 1;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    assert_eq!(success_cnt, 2, "Cluster meet failed to add new nodes");
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
