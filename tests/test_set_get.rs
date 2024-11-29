mod common;
use common::TestStreamHandler;
use redis_starter_rust::adapters::persistence::endecoder::EnDecoder;
use redis_starter_rust::{config::Config, start_up};
use std::sync::OnceLock;
use tokio::net::TcpStream;

static CONFIG: OnceLock<Config> = OnceLock::new();
pub fn integration_test_config(port: u16) -> &'static Config {
    CONFIG.get_or_init(|| Config::default().set_port(port))
}

#[tokio::test]
async fn test_get_set() {
    // GIVEN
    let config = integration_test_config(11111);

    tokio::spawn(start_up(config, 3, EnDecoder));
    //warm up time
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let mut stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = stream.split().into();

    // WHEN
    h.send(b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$5\r\n30000\r\n")
        .await;

    // THEN
    assert_eq!(h.get_response().await, "+OK\r\n");

    // WHEN
    h.send(b"*2\r\n$3\r\nGET\r\n$10\r\nsomanyrand\r\n").await;

    // THEN
    let res = h.get_response().await;
    assert_eq!(res, "$3\r\nbar\r\n");
}
