/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::array;
use common::get_available_port;
use common::wait_for_message;
use common::TestProcessChild;
use redis_starter_rust::client_utils::ClientStreamHandler;
use redis_starter_rust::services::query_io::QueryIO;
use std::process::Command;
use std::process::Stdio;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

struct FileName(String);
impl Drop for FileName {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

fn run_server_with_dbfilename(dbfilename: &str) -> TestProcessChild {
    let port = get_available_port();
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string(), "--dbfilename", dbfilename]);

    let mut process = TestProcessChild(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        port,
    );
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on localhost:{}...", port + 10000).as_str(),
        1,
    );

    process
}

// TODO response cannot be deterministic!
#[tokio::test]
async fn test_save_read_dump() {
    // GIVEN
    let test_file_name = FileName(create_unique_file_name("test_save_dump"));

    let master_process = run_server_with_dbfilename(test_file_name.0.as_str());

    let mut h: ClientStreamHandler = ClientStreamHandler::new(master_process.bind_addr()).await;

    // WHEN
    // set without expiry time
    h.send(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(h.get_response().await, QueryIO::SimpleString(b"OK".into()).serialize());
    // set with expiry time
    h.send(&array(vec!["SET", "foo2", "bar2", "PX", "9999999999"])).await;
    assert_eq!(h.get_response().await, QueryIO::SimpleString(b"OK".into()).serialize());
    // check keys
    h.send(&array(vec!["KEYS", "*"])).await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
    // save
    h.send(&array(vec!["SAVE"])).await;

    // THEN
    assert_eq!(h.get_response().await, QueryIO::Null.serialize());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // keys
    h.send(&array(vec!["KEYS", "*"])).await;
    assert_eq!(h.get_response().await, array(vec!["foo2", "foo"]));
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
