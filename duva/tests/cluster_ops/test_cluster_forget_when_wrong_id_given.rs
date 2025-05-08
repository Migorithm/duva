/// Cluster forget {node id} is used in order to remove a node, from the set of known nodes for the node receiving the command.
/// In other words the specified node is removed from the nodes table of the node receiving the command.
use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_cluster_forget_node_return_error_when_wrong_id_given(
    env: ServerEnv,
) -> anyhow::Result<()> {
    // GIVEN

    let leader_p = spawn_server_process(&env, true).await?;

    // WHEN
    let mut client_handler = Client::new(leader_p.port);
    let replica_id = "localhost:19933";
    let cmd = format!("cluster forget {}", &replica_id);
    let cluster_info = client_handler.send_and_get(&cmd, 1).await;

    // THEN
    assert_eq!(cluster_info.first().unwrap(), "(error) No such peer");

    // WHEN
    let cluster_info = client_handler.send_and_get("cluster info", 1).await;

    // THEN
    assert_eq!(cluster_info.first().unwrap(), "cluster_known_nodes:0");

    Ok(())
}

#[tokio::test]
async fn test_cluster_forget_node_return_error_when_wrong_id_given() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_cluster_forget_node_return_error_when_wrong_id_given(env).await?;
    }

    Ok(())
}
