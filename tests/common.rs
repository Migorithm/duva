use bytes::Bytes;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::make_smart_pointer;

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

static PORT_DISTRIBUTOR: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(49152);

pub fn get_available_port() -> u16 {
    PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub fn spawn_server_process() -> TestProcessChild {
    let port: u16 = get_available_port();
    println!("Starting server on port {}", port);
    let mut process = run_server_process(port, None);

    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str(),
        1,
        Some(2),
    )
    .unwrap();

    process
}

pub fn spawn_server_as_follower(leader_process: &TestProcessChild) -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, Some(leader_process.bind_addr()));
    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str(),
        1,
        Some(2),
    )
    .unwrap();

    process
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

impl TestProcessChild {
    pub fn bind_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn heartbeat_msg(&self, expected_count: usize) -> String {
        format!("[INFO] from {}, hc:{}", self.bind_addr(), expected_count)
    }
}
// scan for available port

#[derive(Debug)]
pub struct TestProcessChild {
    process: Child,
    port: u16,
}

impl TestProcessChild {
    pub fn new(process: Child, port: u16) -> Self {
        TestProcessChild { process, port }
    }

    /// Attempts to gracefully terminate the process, falling back to force kill if necessary
    pub fn terminate(&mut self) -> std::io::Result<()> {
        // First try graceful shutdown
        // Give the process some time to shutdown gracefully
        let timeout = Duration::from_secs(2);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match self.process.try_wait()? {
                Some(_) => return Ok(()),
                None => thread::sleep(Duration::from_millis(100)),
            }
        }

        // Force kill if still running
        self.process.kill()?;
        self.process.wait()?;

        Ok(())
    }

    /// Checks if the process is still running
    pub fn is_running(&mut self) -> bool {
        self.process.try_wait().map(|status| status.is_none()).unwrap_or(false)
    }

    pub fn wait_for_message(&mut self, target: &str, target_count: usize) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count, None)
    }

    pub fn timed_wait_for_message(
        &mut self,
        target: &str,
        target_count: usize,
        wait_for: u64,
    ) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count, Some(wait_for))
    }
}

make_smart_pointer!(TestProcessChild, Child => process);

pub fn run_server_process(port: u16, replicaof: Option<String>) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string(), "--hf", "100", "--ttl", "1500"]);

    if let Some(replicaof) = replicaof {
        command.args(["--replicaof", &replicaof]);
    }

    TestProcessChild::new(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        port,
    )
}

fn wait_for_message<T: Read>(
    read: &mut T,
    target: &str,
    target_count: usize,
    timeout: Option<u64>,
) -> anyhow::Result<()> {
    let internal_count = Instant::now();
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

        if let Some(timeout) = timeout {
            if internal_count.elapsed().as_secs() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for message"));
            }
        }
    }

    Ok(())
}

pub fn array(arr: Vec<&str>) -> Bytes {
    QueryIO::Array(arr.iter().map(|s| QueryIO::BulkString(s.to_string().into())).collect())
        .serialize()
}

pub fn check_internodes_communication(
    processes: &mut Vec<TestProcessChild>,
    hop_count: usize,
    time_out: u64,
) -> anyhow::Result<()> {
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
            processes[i].timed_wait_for_message(&msg, 1, time_out)?;
        }
    }
    Ok(())
}
