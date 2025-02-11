/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.

/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
mod common;
use crate::common::array;
use common::get_available_port;

use common::TestProcessChild;
use duva::client_utils::ClientStreamHandler;
use duva::services::query_io::QueryIO;
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

    let mut process = TestProcessChild::new(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        port,
    );
    process
        .wait_for_message(
            format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str(),
            1,
        )
        .unwrap();

    process
}

// TODO response cannot be deterministic!
#[tokio::test]
async fn test_save_read_dump() {
    // GIVEN
    let test_file_name = FileName(create_unique_file_name("test_save_dump"));

    let master_process = run_server_with_dbfilename(test_file_name.0.as_str());

    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;

    // WHEN
    // set without expiry time
    let res = h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(res, QueryIO::SimpleString("OK".into()).serialize());

    // set with expiry time

    assert_eq!(
        h.send_and_get(&array(vec!["SET", "foo2", "bar2", "PX", "9999999999"])).await,
        QueryIO::SimpleString("OK".into()).serialize()
    );

    // check keys
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // check replication info
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info: Vec<&str> = res.split("\r\n").collect();
    let prev_master_repl_id = info[3].split(":").collect::<Vec<&str>>()[1];
    let prev_master_repl_offset = info[4].split(":").collect::<Vec<&str>>()[1];

    assert_eq!(h.send_and_get(&array(vec!["SAVE"])).await, QueryIO::Null.serialize());

    // wait for the file to be created
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // THEN
    // kill master process
    drop(master_process);

    // run server with the same file name
    let master_process = run_server_with_dbfilename(test_file_name.0.as_str());
    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;

    // keys
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo2", "foo"]));

    // replication info
    let res = h.send_and_get(&array(vec!["INFO", "replication"])).await;
    let info: Vec<&str> = res.split("\r\n").collect();
    let master_repl_id = info[3].split(":").collect::<Vec<&str>>()[1];
    let master_repl_offset = info[4].split(":").collect::<Vec<&str>>()[1];

    // THEN
    assert_eq!(master_repl_id, prev_master_repl_id);
    assert_eq!(master_repl_offset, prev_master_repl_offset);
}

fn create_unique_file_name(function_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

    format!("test_{}_{}.rdb", function_name, timestamp)
}
