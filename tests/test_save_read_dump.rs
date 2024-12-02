/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::start_test_server;
use common::{integration_test_config, TestStreamHandler};
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

#[tokio::test]
async fn test() {
    // GIVEN
    let test_file_name = create_unique_file_name("test_save_dump");
    env::set_var("dbfilename", &test_file_name);

    let config = integration_test_config().await;
    let _ = start_test_server(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    // set without expiry time
    h.send(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").await;
    assert_eq!(h.get_response().await, "+OK\r\n");
    // set with expiry time
    h.send(b"*5\r\n$3\r\nSET\r\n$4\r\nfoo2\r\n$4\r\nbar2\r\n$2\r\npx\r\n$10\r\n9999999999\r\n'").await;
    assert_eq!(h.get_response().await, "+OK\r\n");
    // check keys
    h.send(b"*2\r\n$4\r\nKEYS\r\n$3\r\n\"*\"\r\n").await;
    assert_eq!(h.get_response().await, "*2\r\n$3\r\nfoo\r\n$4\r\nfoo2\r\n");
    // save
    h.send(b"*1\r\n$4\r\nSAVE\r\n").await;

    // THEN
    assert_eq!(h.get_response().await, "$-1\r\n");

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // start another instance
    let config = integration_test_config().await;
    let _ = start_test_server(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // keys
    h.send(b"*2\r\n$4\r\nKEYS\r\n$3\r\n\"*\"\r\n").await;
    assert_eq!(h.get_response().await, "*2\r\n$3\r\nfoo\r\n$4\r\nfoo2\r\n");

    // remove file
    std::fs::remove_file(&test_file_name).unwrap();
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
