use std::{thread::sleep, time::Duration};

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
    let cmd = format!("cluster meet 127.0.0.1:{}", env.port);

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

fn run_cluster_meet_with_migration(append_only: bool) -> anyhow::Result<()> {
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

    // Set keys in first cluster
    let mut client_handler1 = Client::new(env.port);
    for i in 0..500 {
        let cmd = format!("set {} {}", i, i);
        assert_eq!(client_handler1.send_and_get(&cmd), "OK");
    }

    // Set keys in second cluster
    let mut client_handler2 = Client::new(env3.port);
    for i in 500..1000 {
        let cmd = format!("set {} {}", i, i);
        assert_eq!(client_handler2.send_and_get(&cmd), "OK");
    }

    // Perform cluster meet with migration
    let cmd = format!("cluster meet 127.0.0.1:{} eager", env.port);
    assert_eq!(client_handler2.send_and_get(&cmd), "OK");

    // Wait for cluster meet to complete and rebalancing to start
    sleep(Duration::from_secs(2)); // Increased wait time to ensure cluster meet completes

    // Wait for rebalancing to complete (this might take some time)
    let mut keys_accessible_from_node1 = 0;
    let mut keys_accessible_from_node2 = 0;
    let mut node1_keys = Vec::new();
    let mut node2_keys = Vec::new();

    // Check keys from first node
    for i in 0..1000 {
        let cmd = format!("get {}", i);
        let res1 = client_handler1.send_and_get(&cmd);
        let res2 = client_handler2.send_and_get(&cmd);
        if res1 == format!("{}", i) {
            keys_accessible_from_node1 += 1;
            node1_keys.push(i);
        }
        if res2 == format!("{}", i) {
            keys_accessible_from_node2 += 1;
            node2_keys.push(i);
        }
    }

    // Sort the keys for easier gap detection
    node1_keys.sort();
    node2_keys.sort();

    // Verify that keys were redistributed
    assert!(node1_keys != (0..500).collect::<Vec<_>>());
    assert!(node2_keys != (500..1000).collect::<Vec<_>>());
    // verify that all keys are accessible from both nodes
    assert!(dbg!(keys_accessible_from_node1 + keys_accessible_from_node2) == 1000);

    Ok(())
}

/// Test if `cluster meet {host:port} eager` triggers migration of keys from one cluster to another
#[test]
fn test_cluster_meet_with_migration() -> anyhow::Result<()> {
    run_cluster_meet_with_migration(false)?;
    run_cluster_meet_with_migration(true)?;

    Ok(())
}
