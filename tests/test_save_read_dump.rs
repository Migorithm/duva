/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::{array, keys_command, null_response, ok_response, save_command, set_command, set_command_with_expiry, start_test_server};
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
    h.send(set_command("foo", "bar").as_slice()).await;
    assert_eq!(h.get_response().await, ok_response());
    // set with expiry time
    // h.send(b"*5\r\n$3\r\nSET\r\n$4\r\nfoo2\r\n$4\r\nbar2\r\n$2\r\npx\r\n$10\r\n9999999999\r\n'").await;
    h.send(set_command_with_expiry("foo2", "bar2", 9999999999).as_slice()).await;
    assert_eq!(h.get_response().await, ok_response());
    // check keys
    h.send(keys_command().as_slice()).await;
    assert_eq!(h.get_response().await, array(vec!["foo", "foo2"]));
    // save
    h.send(save_command().as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, null_response());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // start another instance
    let config = integration_test_config().await;
    let _ = start_test_server(config).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // keys
    h.send(keys_command().as_slice()).await;
    assert_eq!(h.get_response().await, array(vec!["foo", "foo2"]));

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
