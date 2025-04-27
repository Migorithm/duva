use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_keys() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env).await?;

    let mut h = Client::new(process.port);

    let num_keys_to_store = 500;

    // WHEN set 500 keys with the value `bar`.
    for key in 0..num_keys_to_store {
        assert_eq!(h.send_and_get(format!("SET {} bar", key), 1).await, vec!["OK"]);
    }

    // Fire keys command
    let res = h.send_and_get("KEYS *", 500).await;

    assert!(res.len() >= num_keys_to_store as usize);

    Ok(())
}
