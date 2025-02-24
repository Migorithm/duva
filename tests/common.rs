use bytes::Bytes;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::make_smart_pointer;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread::{self};
use std::time::{Duration, Instant};

pub struct ServerEnv {
    pub port: u16,
    pub file_name: FileName,
    pub leader_bind_addr: Option<String>,
    pub hf: u128,
    pub ttl: u128,
}

impl Default for ServerEnv {
    fn default() -> Self {
        ServerEnv {
            port: get_available_port(),
            file_name: FileName(None),
            leader_bind_addr: None,
            hf: 100,
            ttl: 1500,
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
            let _ = std::fs::remove_file(format!("{}.aof", file_name));
        } else {
            // remove if exists
            let _ = std::fs::remove_file("dump.rdb.aof");
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
        Some(2000),
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
        "--",
        "--port",
        &env.port.to_string(),
        "--hf",
        &env.hf.to_string(),
        "--ttl",
        &env.ttl.to_string(),
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
