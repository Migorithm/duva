use bytes::Bytes;
use duva::make_smart_pointer;
use duva::services::query_io::QueryIO;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};

static PORT_DISTRIBUTOR: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(49152);

pub fn get_available_port() -> u16 {
    PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub fn spawn_server_process() -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, None);
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on localhost:{}...", port + 10000).as_str(),
        1,
    );

    process
}

pub fn spawn_server_as_slave(master: &TestProcessChild) -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, Some(master.bind_addr()));
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on localhost:{}...", port + 10000).as_str(),
        1,
    );

    process
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        self.0.kill().expect("Failed to kill process");
    }
}

impl TestProcessChild {
    pub fn bind_addr(&self) -> String {
        format!("localhost:{}", self.1)
    }
    pub fn port(&self) -> u16 {
        self.1
    }
}
// scan for available port

pub struct TestProcessChild(pub Child, pub u16);

make_smart_pointer!(TestProcessChild, Child);

pub fn run_server_process(port: u16, replicaof: Option<String>) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string(), "--hf", "100"]);

    if let Some(replicaof) = replicaof {
        command.args(["--replicaof", &replicaof]);
    }

    TestProcessChild(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        port,
    )
}

pub fn wait_for_message<T: Read>(read: &mut T, target: &str, target_count: usize) {
    let mut buf = BufReader::new(read).lines();
    let mut cnt = 1;

    while let Some(Ok(line)) = buf.next() {
        if line.starts_with(target) {
            if cnt == target_count {
                break;
            } else {
                cnt += 1;
            }
        }
    }
}

pub fn wait_for_and_get_message<T: Read>(
    read: &mut T,
    target: &str,
    target_count: usize,
) -> String {
    let mut buf = BufReader::new(read).lines();
    let mut cnt = 1;
    let mut message = String::new();

    while let Some(Ok(line)) = buf.next() {
        if line.starts_with(target) {
            if cnt == target_count {
                message = line;
                break;
            } else {
                cnt += 1;
            }
        }
    }
    message
}

pub fn array(arr: Vec<&str>) -> Bytes {
    QueryIO::Array(arr.iter().map(|s| QueryIO::BulkString(s.to_string().into())).collect())
        .serialize()
}
