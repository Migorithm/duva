mod common;
use common::{ServerEnv, array, spawn_server_process};
use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_keys() {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env);

    let mut h = ClientStreamHandler::new(process.bind_addr()).await;

    let num_keys_to_store = 500;

    // WHEN set 500 keys with the value `bar`.
    for key in 0..num_keys_to_store {
        h.send(&array(vec!["SET", &key.to_string(), "bar"])).await;
        assert_eq!(
            h.get_response().await,
            QueryIO::SimpleString(format!("OK RINDEX {}", key + 1)).serialize()
        );
    }

    // Fire keys command
    h.send(&array(vec!["KEYS", "*"])).await;
    let res = h.get_response().await;

    assert!(res.starts_with(format!("*{}\r\n", num_keys_to_store).as_str()));
}
