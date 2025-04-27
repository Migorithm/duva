#![allow(dead_code, unused_variables)]

use bytes::Bytes;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::make_smart_pointer;
use std::net::TcpListener;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::sleep;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

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
            topology_path: TopologyPath(Uuid::now_v7().to_string()),
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
        self.topology_path = TopologyPath(topology_path.into());
        self
    }
}

// Let the OS assign a free port dynamically to reduce port conflicts:
pub fn get_available_port() -> u16 {
    let ok_range = 0..55000;
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind to a random port")
            .local_addr()
            .unwrap()
            .port();

        if ok_range.contains(&port) {
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

pub struct TopologyPath(pub String);
impl Drop for TopologyPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub async fn spawn_server_process(env: &ServerEnv) -> anyhow::Result<TestProcessChild> {
    println!("Starting server on port {}", env.port);
    let mut process = run_server_process(&env);

    wait_for_message(
        process.process.stdout.as_mut().unwrap(),
        vec![format!("listening peer connection on 127.0.0.1:{}...", env.port + 10000).as_str()],
        1,
        Some(10000),
    )
    .await?;

    Ok(process)
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
    pub async fn terminate(&mut self) -> std::io::Result<()> {
        if self.process.id().is_none() {
            // Do nothing if already terminated
            return Ok(());
        }

        // First try graceful shutdown
        // Give the process some time to shutdown gracefully
        let timeout = Duration::from_secs(1);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match self.process.try_wait()? {
                Some(_) => return Ok(()),
                None => sleep(Duration::from_millis(100)).await,
            }
        }

        // Force kill if still running
        self.process.kill().await?;
        self.process.wait().await?;

        Ok(())
    }

    pub async fn wait_for_message(
        &mut self,
        target: &str,
        target_count: usize,
    ) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, vec![target], target_count, None).await
    }

    pub async fn timed_wait_for_message(
        &mut self,
        target: Vec<&str>,
        target_count: usize,
        wait_for: u128,
    ) -> anyhow::Result<()> {
        let read = self.process.stdout.as_mut().unwrap();

        wait_for_message(read, target, target_count, Some(wait_for)).await
    }
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        let _ = self.terminate();
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
        &env.topology_path.0,
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

async fn wait_for_message<T: AsyncRead + Unpin>(
    read: &mut T,
    mut target: Vec<&str>,
    target_count: usize,
    timeout_in_millis: Option<u128>,
) -> anyhow::Result<()> {
    let internal_count = Instant::now();
    let mut buf = BufReader::new(read).lines();
    let mut cnt = target_count;

    assert_eq!(target.len(), target_count);

    let mut current_target = target.remove(0);
    while let Some(line) = buf.next_line().await? {
        if line.starts_with(current_target) {
            cnt -= 1;

            if cnt == 0 {
                if target.is_empty() {
                    return Ok(());
                } else {
                    return Err(anyhow::anyhow!("Targets remain after target_count exhausted"));
                }
            }

            current_target = target.remove(0);
        }

        if let Some(timeout) = timeout_in_millis {
            if internal_count.elapsed().as_millis() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for message"));
            }
        }
    }

    return Err(anyhow::anyhow!("Error was found until reading nextline"));
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
pub async fn check_internodes_communication(
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
            processes[i].timed_wait_for_message(vec![&msg], 1, time_out).await?;
        }
    }
    Ok(())
}

pub struct Client {
    pub child: Child,
    reader: Option<BufReader<ChildStdout>>,
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
            "--features",
            "cli",
            "--",
            "--port",
            &port.to_string(),
        ]);

        command.env("DUVA_ENV", "test");

        Client {
            child: command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Failed to start CLI"),
            reader: None,
        }
    }

    pub async fn send(&mut self, command: &[u8]) -> anyhow::Result<()> {
        let stdin = self.child.stdin.as_mut().unwrap();
        stdin.write_all(command).await?;
        stdin.write_all(b"\r\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    pub async fn read(&mut self) -> Result<String, ()> {
        // Initialize reader if it doesn't exist
        if self.reader.is_none() {
            self.reader = Some(BufReader::new(self.child.stdout.take().unwrap()));
        }

        let reader = self.reader.as_mut().unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.map_err(|_| ())?;
        Ok(line.trim().to_string())
    }

    pub async fn send_and_get(&mut self, command: impl AsRef<[u8]>, mut cnt: u16) -> Vec<String> {
        self.send(command.as_ref()).await.unwrap();

        let mut res = vec![];
        while cnt > 0 {
            cnt -= 1;
            if let Ok(line) = self.read().await {
                res.push(line);
            }
        }
        res
    }

    pub async fn terminate(&mut self) -> std::io::Result<()> {
        if self.child.id().is_none() {
            // Do nothing if already terminated
            return Ok(());
        }
        let _ = self.child.kill().await?;
        let _ = self.child.wait().await?;

        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}
