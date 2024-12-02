mod common;
use common::{integration_test_config, start_test_server, TestStreamHandler};

use redis_starter_rust::adapters::cancellation_token::CancellationToken;
use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    let config = integration_test_config().await;

    let _ = start_test_server::<CancellationToken>(config).await;

    let mut stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = stream.split().into();
    let num_of_keys = 500;

    // WHEN set 100000 keys
    for i in 0..500 {
        h.send(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$3\r\nbar\r\n",
                i.to_string().len(),
                i
            )
            .as_bytes(),
        )
        .await;
        assert_eq!(h.get_response().await, "+OK\r\n");
    }

    // Fire keys command
    h.send(b"*2\r\n$4\r\nKEYS\r\n$3\r\n\"*\"\r\n").await;
    let res = h.get_response().await;

    assert!(res.starts_with(format!("*{}\r\n", num_of_keys).as_str()));
}
