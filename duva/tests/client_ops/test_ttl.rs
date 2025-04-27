use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_ttl() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env).await?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET somanyrand bar PX 5000", 1).await, vec!["OK"]);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // slight delay so seconds gets floored
    let res = h.send_and_get("TTL somanyrand", 1).await;

    // THEN
    assert_eq!(res, vec!["(integer) 4"]);
    assert_eq!(h.send_and_get("TTL non_existing_key", 1).await, vec!["(integer) -1"]);
}
