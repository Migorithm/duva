/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
mod common;
use common::{array, spawn_server_process};

use redis_starter_rust::{client_utils::ClientStreamHandler, services::query_io::QueryIO};

#[tokio::test]
async fn test_set_get() {
    // GIVEN
    let process = spawn_server_process();

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    h.send({ array(vec!["SET", "somanyrand", "bar", "PX", "300"]).into_bytes() }.as_slice()).await;
    // THEN
    assert_eq!(h.get_response().await, QueryIO::SimpleString("OK".to_string()).serialize());

    // WHEN
    h.send({ array(vec!["GET", "somanyrand"]).into_bytes() }.as_slice()).await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, QueryIO::BulkString("bar".to_string()).serialize());

    // WHEN - wait for 300ms
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    h.send({ array(vec!["GET", "somanyrand"]).into_bytes() }.as_slice()).await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, QueryIO::Null.serialize());
}
