/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_exists(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env).await?;

    let mut h = Client::new(process.port);
    assert_eq!(h.send_and_get("SET a b", 1).await, vec!["OK"]);

    assert_eq!(h.send_and_get("SET c d", 1).await, vec!["OK"]);

    // WHEN & THEN
    assert_eq!(h.send_and_get("exists a c d", 1).await, vec!["(integer) 2"]);

    assert_eq!(h.send_and_get("exists x", 1).await, vec!["(integer) 0"]);

    Ok(())
}

#[tokio::test]
async fn test_exists() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_exists(env).await?;
    }

    Ok(())
}
