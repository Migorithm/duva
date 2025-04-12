/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
use crate::common::{Client, ServerEnv, spawn_server_process};
#[tokio::test]
async fn test_exists() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);

    let mut h = Client::new(process.port);
    assert_eq!(h.send_and_get("SET a b", 1), vec!["OK"]);

    assert_eq!(h.send_and_get("SET c d", 1), vec!["OK"]);

    // WHEN & THEN
    assert_eq!(h.send_and_get("exists a c d", 1), vec!["(integer) 2"]);

    assert_eq!(h.send_and_get("exists x", 1), vec!["(integer) 0"]);
}
