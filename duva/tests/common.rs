#![allow(dead_code, unused_variables)]

use bytes::{Bytes, BytesMut};
use duva::domains::query_parsers::query_io::{QueryIO, deserialize};
use duva::make_smart_pointer;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

pub struct ServerEnv {
    pub port: u16,
    pub file_name: FileName,
    pub leader_bind_addr: Option<String>,
    pub hf: u128,
    pub ttl: u128,
    pub use_wal: bool,
    pub topology_path: TopologyPath,
}

impl Default for ServerEnv {
    fn default() -> Self {
        ServerEnv {
            port: get_available_port(),
            file_name: FileName(None),
            leader_bind_addr: None,
            hf: 100,
            ttl: 1500,
            use_wal: false,
            topology_path: TopologyPath(None),
        }
    }
}

impl ServerEnv {
    pub fn with_file_name(mut self, file_name: impl Into<String>) -> Self {
        self.file_name = FileName(Some(file_name.into()));
        self
    }

    pub fn with_leader_bind_addr(mut self, leader_bind_addr: String) -> Self {
        self.leader_bind_addr = Some(leader_bind_addr);
        self
    }
    pub fn with_hf(mut self, hf: u128) -> Self {
        self.hf = hf;
        self
    }
    pub fn with_ttl(mut self, ttl: u128) -> Self {
        self.ttl = ttl;
        self
    }
    pub fn with_topology_path(mut self, topology_path: impl Into<String>) -> Self {
        self.topology_path = TopologyPath(Some(topology_path.into()));
        self
    }
}

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

#[derive(Clone, Default)]
pub struct FileName(pub Option<String>);
impl Drop for FileName {
    fn drop(&mut self) {
        if let Some(file_name) = self.0.as_ref() {
            // remove if exists
            let _ = std::fs::remove_file(file_name);
            let _ = std::fs::remove_file(format!("{}.wal", file_name));
        } else {
            // remove if exists
            let _ = std::fs::remove_file("dump.rdb.wal");
        }
    }
}

pub struct TopologyPath(pub Option<String>);
impl Drop for TopologyPath {
    fn drop(&mut self) {
        if let Some(topology_path) = self.0.as_ref() {
            let _ = std::fs::remove_file(topology_path);
        } else {
            let _ = std::fs::remove_file("duva.tp");
        }
    }
}

pub fn spawn_server_process(env: &ServerEnv) -> TestProcessChild {
    println!("Starting server on port {}", env.port);
    let mut process = run_server_process(&env);

    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        vec![format!("listening peer connection on 127.0.0.1:{}...", env.port + 10000).as_str()],
        1,
        Some(10000),
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

    pub fn heartbeat_msg(&self, expected_count: usize) -> String {
        format!("[INFO] from {}, hc:{}", self.bind_addr(), expected_count)
    }
}
// scan for available port

#[derive(Debug)]
pub struct TestProcessChild {
    process: Child,
    pub port: u16,
}

impl TestProcessChild {
    pub fn new(process: Child, port: u16) -> Self {
        TestProcessChild { process, port }
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
                None => sleep(Duration::from_millis(100)),
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
        wait_for: u128,
    ) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count, Some(wait_for))
    }
}

make_smart_pointer!(TestProcessChild, Child => process);

pub fn run_server_process(env: &ServerEnv) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args([
        "run",
        "-p",
        "duva",
        "--",
        "--port",
        &env.port.to_string(),
        "--hf",
        &env.hf.to_string(),
        "--ttl",
        &env.ttl.to_string(),
        "--use_wal",
        &env.use_wal.to_string(),
        "--tpp",
        &env.topology_path.0.as_ref().unwrap_or(&"duva.tp".to_string()),
    ]);

    if let Some(replicaof) = env.leader_bind_addr.as_ref() {
        command.args(["--replicaof", &replicaof]);
    }
    if let Some(file_name) = env.file_name.0.as_ref() {
        command.args(["--dbfilename", &file_name]);
    }

    TestProcessChild::new(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
        env.port,
    )
}

fn wait_for_message<T: Read>(
    read: &mut T,
    mut target: Vec<&str>,
    target_count: usize,
    timeout_in_millis: Option<u128>,
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

        if let Some(timeout) = timeout_in_millis {
            if internal_count.elapsed().as_millis() > timeout {
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

pub fn session_request(request_id: u64, arr: Vec<&str>) -> Bytes {
    QueryIO::SessionRequest {
        request_id,
        value: arr.iter().map(|s| QueryIO::BulkString(s.to_string().into())).collect(),
    }
    .serialize()
}

/// Check if all processes can communicate with each other
pub fn check_internodes_communication(
    processes: &mut [&mut TestProcessChild],
    hop_count: usize,
    time_out: u128,
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

pub struct Client {
    pub child: Child,
    reader: Option<BufReader<std::process::ChildStdout>>,
}

impl Client {
    pub fn new(port: u16) -> Client {
        let mut command = Command::new("cargo");
        command.args([
            "run",
            "-p",
            "duva-client",
            "--bin",
            "cli",
            "--",
            "--port",
            &port.to_string(),
        ]);

        Client {
            child: command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Failed to start CLI"),
            reader: None,
        }
    }

    pub fn send(&mut self, command: &[u8]) -> anyhow::Result<()> {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(command)?;
        stdin.write_all(b"\r\n")?;
        stdin.flush()?;
        Ok(())
    }

    pub fn read(&mut self) -> Result<String, ()> {
        // Initialize reader if it doesn't exist
        if self.reader.is_none() {
            self.reader = Some(BufReader::new(self.child.stdout.take().unwrap()));
        }

        let reader = self.reader.as_mut().unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).map_err(|_| ())?;
        Ok(line.trim().to_string())
    }

    pub fn send_and_get(&mut self, command: impl AsRef<[u8]>, mut cnt: u16) -> Vec<String> {
        self.send(command.as_ref()).unwrap();

        let mut res = vec![];
        while cnt > 0 {
            cnt -= 1;
            if let Ok(line) = self.read() {
                res.push(line);
            }
        }
        res
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
