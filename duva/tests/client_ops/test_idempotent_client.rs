use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

use crate::common::{ServerEnv, array, session_request, spawn_server_process};

#[tokio::test]
async fn test_send_session_request() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    // WHEN - set key with expiry
    assert_eq!(
        h.send_and_get(&array(vec!["SET", "somanyrand", "bar", "PX", "300"])).await,
        QueryIO::SimpleString("OK RINDEX 1".into()).serialize()
    );
    let res = h.send_and_get(&session_request(1, vec!["GET", "somanyrand"])).await;
    assert_eq!(res, QueryIO::BulkString("bar".into()).serialize());

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    // THEN
    let res = h.send_and_get(&session_request(1, vec!["GET", "somanyrand"])).await;
    assert_eq!(res, QueryIO::Null.serialize());
}
