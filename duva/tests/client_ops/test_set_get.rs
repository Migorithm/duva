/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_set_get() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env).await?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET somanyrand bar PX 300", 1).await, vec!["OK"]);
    let res = h.send_and_get("GET somanyrand", 1).await;
    assert_eq!(res, vec!["bar"]);

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    // THEN

    let res = h.send_and_get("GET somanyrand", 1).await;
    assert_eq!(res, vec!["(nil)"]);
}
