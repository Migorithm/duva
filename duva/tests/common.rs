#![allow(dead_code, unused_variables)]
use bytes::Bytes;
use duva::domains::query_io::QueryIO;
use duva::make_smart_pointer;
use std::io::{BufRead, BufReader, Write};
use std::mem::MaybeUninit;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::process::{Child, ChildStdout, Command};
use std::thread::sleep;
use tempfile::TempDir;

use std::time::Duration;
use uuid::Uuid;

pub struct ServerEnv {
    pub port: u16,
    pub file_name: FileName,
    pub leader_bind_addr: Option<String>,
    pub hf: u128,
    pub append_only: bool,
    // Owns and cleans the directory.
    pub dir: TempDir,
    pub topology_path: PathBuf,
}

impl Default for ServerEnv {
    fn default() -> Self {
        let dir = TempDir::new().unwrap();
        let topology_path = dir.path().join(Uuid::now_v7().to_string());

        ServerEnv {
            port: get_available_port(),
            file_name: FileName(None),
            leader_bind_addr: None,
            hf: 100,

            append_only: false,
            dir,
            topology_path,
        }
    }
}

impl ServerEnv {
    // Create a new ServerEnv with a unique port
    pub fn clone(self) -> Self {
        ServerEnv { port: get_available_port(), ..self }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_file_name(mut self, file_name: impl Into<String>) -> Self {
        self.file_name = FileName(Some(file_name.into()));
        self
    }

    pub fn with_bind_addr(mut self, leader_bind_addr: String) -> Self {
        self.leader_bind_addr = Some(leader_bind_addr);
        self
    }
    pub fn with_hf(mut self, hf: u128) -> Self {
        self.hf = hf;
        self
    }

    pub fn with_topology_path(mut self, topology_path: impl AsRef<Path>) -> Self {
        self.topology_path = topology_path.as_ref().to_path_buf();
        self
    }
    pub fn with_append_only(mut self, append_only: bool) -> Self {
        self.append_only = append_only;
        self
    }
}

// Let the OS assign a free port dynamically to reduce port conflicts:
pub fn get_available_port() -> u16 {
    let ok_range = 0..55000;
    loop {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();

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
            let _ = std::fs::remove_file(format!("{file_name}.oplog"));
        } else {
            // remove if exists
            let _ = std::fs::remove_file("dump.rdb.oplog");
        }
    }
}

pub fn spawn_server_process(env: &ServerEnv) -> anyhow::Result<TestProcessChild> {
    let process = run_server_process(env);

    // Wait for server to be fully ready (increased timeout and better checks)
    let mut cnt = 20;
    while cnt > 0 {
        cnt -= 1;
        std::thread::sleep(std::time::Duration::from_millis(500));

        if let Ok(mut child) = std::panic::catch_unwind(|| Client::new(process.port)) {
            // First check: basic connectivity
            let ping_res = child.send_and_get("PING");
            if ping_res != "PONG" {
                continue;
            }

            // Second check: server is ready for cluster operations
            let role_res: String = child.send_and_get("role");
            if role_res.is_empty() {
                continue;
            }

            if role_res.contains("leader") || role_res.contains("follower") {
                // it's ready
                break;
            }
        }
    }

    if cnt == 0 {
        return Err(anyhow::anyhow!("Server failed to start within 10 seconds"));
    }

    Ok(process)
}

impl TestProcessChild {
    pub fn bind_addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn heartbeat_msg(&self, expected_count: usize) -> String {
        format!("from {}, hc:{}", self.bind_addr(), expected_count)
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
    fn terminate(&mut self) -> std::io::Result<()> {
        // First try graceful shutdown
        // Give the process some time to shutdown gracefully
        let timeout = Duration::from_secs(1);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match self.process.try_wait()? {
                | Some(_) => return Ok(()),
                | None => sleep(Duration::from_millis(100)),
            }
        }

        // Force kill if still running
        self.process.kill()?;
        self.process.wait()?;

        Ok(())
    }
}

impl Drop for TestProcessChild {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

make_smart_pointer!(TestProcessChild, Child => process);

pub fn run_server_process(env: &ServerEnv) -> TestProcessChild {
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let lock = LOCK.lock().unwrap();
        ONCE.call_once(|| {
            let mut command = Command::new("cargo");
            command.args(["build", "-p", "duva"]);
            let mut process = command.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
            process.wait().unwrap();
        });
    }
    let current = std::env::current_dir().unwrap();
    let path = current.parent().unwrap().join("target").join("debug").join("duva");
    let mut command = Command::new(path);
    command.args([
        "--port",
        &env.port.to_string(),
        "--hf",
        &env.hf.to_string(),
        "--append_only",
        &env.append_only.to_string(),
        "--dir",
        env.dir.path().to_str().unwrap(),
        "--tpp",
        env.topology_path.as_path().to_str().unwrap(),
        "--log_level",
        "debug",
    ]);

    if let Some(replicaof) = env.leader_bind_addr.as_ref() {
        command.args(["--replicaof", replicaof]);
    }
    if let Some(file_name) = env.file_name.0.as_ref() {
        command.args(["--dbfilename", file_name]);
    }

    command.env("DUVA_ENV", "test");

    TestProcessChild::new(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start server process"),
        env.port,
    )
}

pub fn array(arr: Vec<&str>) -> Bytes {
    QueryIO::Array(arr.iter().map(|s| QueryIO::BulkString(s.to_string().into())).collect())
        .serialize()
}

pub struct Client {
    pub child: Child,
    reader: Option<BufReader<ChildStdout>>,
}

impl Client {
    pub fn new(port: u16) -> Client {
        {
            static ONCE: std::sync::Once = std::sync::Once::new();
            static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let lock = LOCK.lock().unwrap();
            ONCE.call_once(|| {
                let mut command = Command::new("cargo");
                command.args(["build", "-p", "duva-client"]);
                let mut process =
                    command.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
                process.wait().unwrap();
            });
        }
        let current = std::env::current_dir().unwrap();
        let path = current.parent().unwrap().join("target").join("debug").join("cli");
        let mut command = Command::new(path);
        command.args(["--port", &port.to_string()]);

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

    pub fn send_and_get_vec(&mut self, command: impl AsRef<[u8]>, mut cnt: u32) -> Vec<String> {
        let mut res = vec![];

        // Try to send the command with retries
        let mut retries = 3;
        while retries > 0 {
            if let Ok(()) = self.send(command.as_ref()) {
                break;
            }
            retries -= 1;
            if retries > 0 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }

        // If send failed, return empty result
        if retries == 0 {
            return res;
        }

        while cnt > 0 {
            cnt -= 1;
            if let Ok(line) = self.read() {
                res.push(line);
            } else {
                // If read fails, break to avoid infinite loop
                break;
            }
        }
        res
    }
    pub fn send_and_get(&mut self, command: impl AsRef<[u8]>) -> String {
        let mut retries = 3;
        while retries > 0 {
            if let Ok(()) = self.send(command.as_ref()) {
                loop {
                    if let Ok(line) = self.read() {
                        return line;
                    }
                }
            }
            retries -= 1;
            if retries > 0 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        // If all retries failed, try one more time and return whatever we get
        self.send(command.as_ref()).unwrap_or_default();
        self.read().unwrap_or_default()
    }

    pub fn terminate(&mut self) -> std::io::Result<()> {
        self.child.kill()?;
        let _ = self.child.wait()?;

        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

pub fn form_cluster<const T: usize>(envs: [&mut ServerEnv; T]) -> [TestProcessChild; T] {
    // Using MaybeUninit to create an uninitialized array
    let mut processes: [MaybeUninit<TestProcessChild>; T] =
        unsafe { MaybeUninit::uninit().assume_init() };

    // Initialize the leader
    let leader_p = spawn_server_process(envs[0]).unwrap();
    let leader_bind_addr = leader_p.bind_addr();
    processes[0].write(leader_p);

    // Initialize replicas with delay between each
    for i in 1..T {
        envs[i].leader_bind_addr = Some(leader_bind_addr.clone());
        let repl_p = spawn_server_process(envs[i]).unwrap();
        processes[i].write(repl_p);

        // Small delay between replica startups to avoid connection conflicts
        if i < T - 1 {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    let process_refs =
        unsafe { processes.iter_mut().map(|p| &mut *(p.as_mut_ptr())).collect::<Vec<_>>() };

    // Convert the array of MaybeUninit to an initialized array safely
    unsafe {
        // Create a ManuallyDrop to prevent double-free when array is moved out
        let mut manual_drop = std::mem::ManuallyDrop::new(processes);

        // Get a pointer to the underlying array and reinterpret it
        let ptr = manual_drop.as_mut_ptr() as *mut [TestProcessChild; T];

        // Read from the pointer to get the initialized array
        ptr.read()
    }
}
