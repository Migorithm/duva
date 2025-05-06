use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_keys(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env).await?;

    let mut h = Client::new(process.port);

    let num_keys_to_store = 100;

    // WHEN set 500 keys with the value `bar`.
    for key in 0..num_keys_to_store {
        h.send(format!("SET {} bar", key).as_bytes()).await.unwrap();
    }

    // Fire keys command
    let res = h.send_and_get("KEYS *", 100).await;

    assert!(res.len() >= num_keys_to_store as usize);

    Ok(())
}

#[tokio::test]
async fn test_keys() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_keys(env).await?;
    }

    Ok(())
}
