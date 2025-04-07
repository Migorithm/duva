/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{ServerEnv, array, spawn_server_process};

use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_exists() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "a", "b"])).await,
        QueryIO::SimpleString("OK RINDEX 1".into()).serialize()
    );
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "c", "d"])).await,
        QueryIO::SimpleString("OK RINDEX 2".into()).serialize()
    );

    // WHEN & THEN
    assert_eq!(
        h.send_and_get(&array(vec!["exists", "a", "c", "d"])).await,
        QueryIO::SimpleString("2".into()).serialize() // 2 means 2 keys deleted
    );

    assert_eq!(
        h.send_and_get(&array(vec!["exists", "x"])).await,
        QueryIO::SimpleString("0".into()).serialize() // 2 means 2 keys deleted
    );
}
