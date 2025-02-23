use bytes::Bytes;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::make_smart_pointer;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread::{self};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Let the OS assign a free port dynamically to reduce port conflicts:
pub fn get_available_port() -> u16 {
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind to a random port")
            .local_addr()
            .unwrap()
            .port();

        if (49152..55000).contains(&port) {
            return port;
        }
    }
}

pub fn spawn_server_process(file_name: Option<String>) -> TestProcessChild {
    let port: u16 = get_available_port();
    println!("Starting server on port {}", port);
    let mut process = run_server_process(port, None, file_name);

    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        vec![format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str()],
        1,
        Some(2),
    )
    .unwrap();

    process
}

pub fn spawn_server_as_follower(leader_bind_addr: String) -> TestProcessChild {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, Some(leader_bind_addr), None);
    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        vec![format!("listening peer connection on 127.0.0.1:{}...", port + 10000).as_str()],
        1,
        Some(2),
    )
    .unwrap();

    process
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        let _ = self.terminate();
        if let Some(file_name) = self.file_name.as_ref() {
        } else {
            // remove if exists
            let _ = std::fs::remove_file("dump.rdb.aof");
        }
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
    file_name: Option<String>,
}

impl TestProcessChild {
    pub fn new(process: Child, port: u16, file_name: Option<String>) -> Self {
        TestProcessChild { process, port, file_name }
    }

    /// Attempts to gracefully terminate the process, falling back to force kill if necessary
    pub fn terminate(&mut self) -> std::io::Result<()> {
        // First try graceful shutdown
        // Give the process some time to shutdown gracefully
        let timeout = Duration::from_secs(1);
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

    pub fn wait_for_message(&mut self, target: &str, target_count: usize) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, vec![target], target_count, None)
    }

    pub fn timed_wait_for_message(
        &mut self,
        target: Vec<&str>,
        target_count: usize,
        wait_for: u64,
    ) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count, Some(wait_for))
    }
}

make_smart_pointer!(TestProcessChild, Child => process);

pub fn run_server_process(
    port: u16,
    replicaof: Option<String>,
    file_name: Option<String>,
) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string(), "--hf", "100", "--ttl", "1500"]);

    if let Some(replicaof) = replicaof {
        command.args(["--replicaof", &replicaof]);
    }
    if let Some(file_name) = file_name.as_ref() {
        command.args(["--dbfilename", &file_name]);
    }

    TestProcessChild::new(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        port,
        file_name,
    )
}

fn wait_for_message<T: Read>(
    read: &mut T,
    mut target: Vec<&str>,
    target_count: usize,
    timeout: Option<u64>,
) -> anyhow::Result<()> {
    let internal_count = Instant::now();
    let mut buf = BufReader::new(read).lines();
    let mut cnt = 1;

    let mut current_target = target.remove(0);
    while let Some(Ok(line)) = buf.next() {
        if line.starts_with(current_target) {
            if cnt == target_count {
                if target.is_empty() {
                    break;
                }
                current_target = target.remove(0);
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
    processes: &mut [&mut TestProcessChild],
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
            processes[i].timed_wait_for_message(vec![&msg], 1, time_out)?;
        }
    }
    Ok(())
}
