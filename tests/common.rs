use bytes::Bytes;
use duva::make_smart_pointer;
use duva::services::query_io::QueryIO;
use std::alloc::System;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

static PORT_DISTRIBUTOR: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(49152);

pub fn get_available_port() -> u16 {
    PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub fn spawn_server_process() -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, None);
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str(),
        1,
    );

    process
}

pub fn spawn_server_as_slave(master: &TestProcessChild) -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, Some(master.bind_addr()));
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str(),
        1,
    );

    process
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

impl TestProcessChild {
    pub fn bind_addr(&self) -> String {
        format!("127.0.0.1:{}", self.1)
    }
    pub fn port(&self) -> u16 {
        self.1
    }

    pub fn heartbeat_msg(&self, expected_count: usize) -> String {
        format!("[INFO] from {}, hc:{}", self.bind_addr(), expected_count)
    }
}
// scan for available port

pub struct TestProcessChild(pub Child, pub u16);
impl TestProcessChild {
    pub fn wait_for_message(&mut self, target: &str, target_count: usize) {
        let read = self.0.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count);
    }
}
make_smart_pointer!(TestProcessChild, Child);

pub fn run_server_process(port: u16, replicaof: Option<String>) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string(), "--hf", "100", "--ttl", "1500"]);

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

fn wait_for_message<T: Read>(read: &mut T, target: &str, target_count: usize) {
    // let internal_count = Instant::now();
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

        // if internal_count.elapsed().as_secs() > 5 {
        //     panic!("Timeout while waiting for message: {}", target);
        // }
    }
}

pub fn array(arr: Vec<&str>) -> Bytes {
    QueryIO::Array(arr.iter().map(|s| QueryIO::BulkString(s.to_string().into())).collect())
        .serialize()
}

pub fn check_internodes_communication(processes: &mut Vec<TestProcessChild>, hop_count: usize) {
    for i in 0..processes.len() {
        // First get the message from all other processes
        let messages: Vec<_> = processes
            .iter()
            .enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, target)| {
                (0..hop_count + 1)
                    .into_iter()
                    .map(|_| target.heartbeat_msg(hop_count))
                    .collect::<Vec<String>>()
            })
            .flatten()
            .collect();

        // Then wait for all messages
        for msg in messages {
            processes[i].wait_for_message(&msg, 1);
        }
    }
}
