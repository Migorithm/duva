use duva::prelude::LEADER_HEARTBEAT_INTERVAL_MAX;

use crate::common::{Client, ServerEnv, form_cluster};

fn run_cluster_forget_makes_all_nodes_forget_target_node(
    with_append_only: bool,
) -> anyhow::Result<()> {
    // GIVEN

    let mut env = ServerEnv::default().with_hf(1).with_append_only(with_append_only);
    let mut repl_env = ServerEnv::default().with_hf(1).with_append_only(with_append_only);
    let mut repl_env2 = ServerEnv::default().with_hf(1).with_append_only(with_append_only);

    let [leader_p, repl_p, repl_p2] = form_cluster([&mut env, &mut repl_env, &mut repl_env2]);

    // WHEN
    let mut client_handler = Client::new(leader_p.port);
    assert_eq!(
        client_handler.send_and_get(format!("cluster forget {}", &repl_p.bind_addr())),
        "OK"
    );

    // THEN
    assert_eq!(client_handler.send_and_get_vec("cluster info", 1), vec!["cluster_known_nodes:1"]);

    let mut repl_cli = Client::new(repl_p2.port);

    std::thread::sleep(std::time::Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX + 1));

    assert_eq!(repl_cli.send_and_get_vec("cluster info", 1), vec!["cluster_known_nodes:1"]);

    Ok(())
}

#[test]
fn test_cluster_forget_makes_all_nodes_forget_target_node() -> anyhow::Result<()> {
    run_cluster_forget_makes_all_nodes_forget_target_node(false)?;
    run_cluster_forget_makes_all_nodes_forget_target_node(true)?;

    Ok(())
}
