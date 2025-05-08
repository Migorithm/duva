use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_ttl(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false).await?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET somanyrand bar PX 5000", 1).await, vec!["OK"]);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // slight delay so seconds gets floored
    let res = h.send_and_get("TTL somanyrand", 1).await;

    // THEN
    assert_eq!(res, vec!["(integer) 4"]);
    assert_eq!(h.send_and_get("TTL non_existing_key", 1).await, vec!["(integer) -1"]);

    Ok(())
}

#[tokio::test]
async fn test_ttl() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_ttl(env).await?;
    }

    Ok(())
}
