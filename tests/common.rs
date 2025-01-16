use redis_starter_rust::make_smart_pointer;
use redis_starter_rust::services::query_io::QueryIO;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};

static PORT_DISTRIBUTOR: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(49152);

pub fn get_available_port() -> u16 {
    PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

pub fn spawn_server_process() -> u16 {
    let port: u16 = get_available_port();
    let mut process = run_server_process(port, None);
    wait_for_message(
        process.0.stdout.as_mut().unwrap(),
        format!("listening peer connection on localhost:{}...", port + 10000).as_str(),
        1,
    );

    port
}

pub fn terminate_process(port: u16) {
    // Find the PID of the process using the port
    let output = Command::new("lsof")
        .args(&["-i", &format!(":{}", port), "-t"])
        .output()
        .expect("Failed to execute lsof command");

    if output.stdout.is_empty() {
        println!("No process is using port {}", port);
        return;
    }

    // Extract the PID from the command output

    for pid in String::from_utf8_lossy(&output.stdout).lines().collect::<Vec<&str>>() {
        // Kill the process by PID
        Command::new("kill").arg(pid.to_string()).spawn().expect("Failed to kill process");
        println!("Killed process {} on port {}", pid, port);
    }
}

// scan for available port

pub struct TestProcessChild(pub Child);

make_smart_pointer!(TestProcessChild, Child);

pub fn run_server_process(port: u16, replicaof: Option<String>) -> TestProcessChild {
    let mut command = Command::new("cargo");
    command.args(["run", "--", "--port", &port.to_string()]);

    if let Some(replicaof) = replicaof {
        command.args(["--replicaof", &replicaof]);
    }

    TestProcessChild(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server process"),
    )
}

pub fn wait_for_message<T: Read>(read: T, target: &str, target_count: usize) {
    let mut buf = BufReader::new(read).lines();
    let mut cnt = 1;

    while let Some(Ok(line)) = buf.next() {
        if line == target {
            if cnt == target_count {
                break;
            } else {
                cnt += 1;
            }
        }
    }
}

pub fn array(arr: Vec<&str>) -> String {
    QueryIO::Array(arr.iter().map(|s| QueryIO::BulkString(s.to_string())).collect()).serialize()
}
