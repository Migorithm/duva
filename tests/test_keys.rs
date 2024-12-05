mod common;
use crate::common::{keys_command, ok_response, set_command};
use common::{init_config_with_free_port, start_test_server, TestStreamHandler};

use redis_starter_rust::adapters::cancellation_token::CancellationToken;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_keys() {
    // GIVEN
    let config = init_config_with_free_port().await;

    let _ = start_test_server::<CancellationToken>(config.clone()).await;

    let mut stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = stream.split().into();
    let num_of_keys = 500;

    // WHEN set 100000 keys
    for i in 0..500 {
        h.send(set_command(&i.to_string(), "bar").as_slice()).await;
        assert_eq!(h.get_response().await, ok_response());
    }

    // Fire keys command
    h.send(keys_command("*").as_slice()).await;
    let res = h.get_response().await;

    assert!(res.starts_with(format!("*{}\r\n", num_of_keys).as_str()));
}
