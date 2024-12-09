/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::{
    array, keys_command, null_response, ok_response, save_command, set_command,
    set_command_with_expiry, start_test_server,
};
use common::{init_config_with_free_port, TestStreamHandler};
use redis_starter_rust::adapters::cancellation_token::CancellationToken;
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
    let mut config = init_config_with_free_port().await;
    let old_ref = config.dbfilename;

    config.dbfilename = Box::leak(Box::new(test_file_name.0.clone()));
    unsafe {
        let ptr: *const str = old_ref;
        let _ = Box::from_raw(ptr as *mut str);
    }

    let _ = start_test_server::<CancellationToken>(config.clone()).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // WHEN
    // set without expiry time
    h.send(set_command("foo", "bar").as_slice()).await;
    assert_eq!(h.get_response().await, ok_response());
    // set with expiry time
    h.send(set_command_with_expiry("foo2", "bar2", 9999999999).as_slice())
        .await;
    assert_eq!(h.get_response().await, ok_response());
    // check keys
    h.send(keys_command("*").as_slice()).await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
    // save
    h.send(save_command().as_slice()).await;

    // THEN
    assert_eq!(h.get_response().await, null_response());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // start another instance
    let _ = start_test_server::<CancellationToken>(config.clone()).await;

    let mut client_stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = client_stream.split().into();

    // keys
    h.send(keys_command("*").as_slice()).await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
