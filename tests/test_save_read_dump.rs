/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::{array, start_test_server};
use common::{init_config_manager_with_free_port, TestStreamHandler};
use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        config::{ConfigCommand, ConfigMessage},
        query_io::QueryIO,
    },
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

struct FileName(String);
impl Drop for FileName {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

// TODO response cannot be deterministic!
#[tokio::test]
async fn test_save_read_dump() {
    // GIVEN
    let test_file_name = FileName(create_unique_file_name("test_save_dump"));
    let config = init_config_manager_with_free_port().await;
    config
        .send(ConfigMessage::Command(ConfigCommand::SetDbFileName(
            test_file_name.0.clone(),
        )))
        .await
        .unwrap();

    let _ = start_test_server(CancellationTokenFactory, config.clone()).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    // set without expiry time
    h.send({ array(vec!["SET", "foo", "bar"]).into_bytes() }.as_slice())
        .await;
    assert_eq!(
        h.get_response().await,
        QueryIO::SimpleString("OK".to_string()).serialize()
    );
    // set with expiry time
    h.send({ array(vec!["SET", "foo2", "bar2", "PX", "9999999999"]).into_bytes() }.as_slice())
        .await;
    assert_eq!(
        h.get_response().await,
        QueryIO::SimpleString("OK".to_string()).serialize()
    );
    // check keys
    h.send({ array(vec!["KEYS", "*"]).into_bytes() }.as_slice())
        .await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
    // save
    h.send(array(vec!["SAVE"]).into_bytes().as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, QueryIO::Null.serialize());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // start another instance
    let _ = start_test_server(CancellationTokenFactory, config.clone()).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // keys
    h.send({ array(vec!["KEYS", "*"]).into_bytes() }.as_slice())
        .await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
