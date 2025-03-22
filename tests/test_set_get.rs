/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{ServerEnv, array, spawn_server_process};

use duva::{client_utils::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_set_get() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN - set key with expiry
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "somanyrand", "bar", "PX", "300"])).await,
        QueryIO::SimpleString("OK RINDEX 1".into()).serialize()
    );
    let res = h.send_and_get(&array(vec!["GET", "somanyrand"])).await;
    assert_eq!(res, QueryIO::BulkString("bar".into()).serialize());

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // THEN
    let res = h.send_and_get(&array(vec!["GET", "somanyrand"])).await;
    assert_eq!(res, QueryIO::Null.serialize());
}
