use redis_starter_rust::services::config::actor::ConfigActor;
use redis_starter_rust::services::config::manager::ConfigManager;
use redis_starter_rust::services::interface::{TCancellationTokenFactory, TRead};
use redis_starter_rust::services::query_io::QueryIO;
use redis_starter_rust::{make_smart_pointer, StartUpFacade, TNotifyStartUp};
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};

use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::tcp::WriteHalf;

use tokio::net::TcpStream;

pub static PORT_DISTRIBUTOR: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(5000);

pub fn get_available_port() -> u16 {
    PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}
pub struct TestStreamHandler<'a> {
    pub read: ReadHalf<'a>,
    pub write: WriteHalf<'a>,
}

impl<'a> From<(ReadHalf<'a>, WriteHalf<'a>)> for TestStreamHandler<'a> {
    fn from((read, write): (ReadHalf<'a>, WriteHalf<'a>)) -> Self {
        Self { read, write }
    }
}

impl<'a> TestStreamHandler<'a> {
    pub async fn send(&mut self, operation: &[u8]) {
        self.write.write_all(operation).await.unwrap();
        self.write.flush().await.unwrap();
    }

    // read response from the server
    pub async fn get_response(&mut self) -> String {
        let mut buffer = Vec::new();
        let mut temp_buffer = [0; 1024];

        loop {
            let bytes_read = self.read.read(&mut temp_buffer).await.unwrap();
            if bytes_read == 0 {
                break;
            }
            buffer.extend_from_slice(&temp_buffer[..bytes_read]);
            if bytes_read < temp_buffer.len() {
                break;
            }
        }

        String::from_utf8_lossy(&buffer).into_owned()
    }
}

pub async fn init_config_manager_with_free_port() -> ConfigManager {
    let config = ConfigActor::default();
    let mut manager = ConfigManager::new(config);

    manager.port = PORT_DISTRIBUTOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    manager
}

// scan for available port

pub struct StartFlag(pub Arc<tokio::sync::Notify>);

impl TNotifyStartUp for StartFlag {
    fn notify_startup(&self) {
        self.0.notify_waiters();
    }
}

pub async fn start_test_server(
    cancellation_token_factory: impl TCancellationTokenFactory,
    config: ConfigManager,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let start_flag = StartFlag(notify.clone());

    let mut start_up_facade = StartUpFacade::new(cancellation_token_factory, config);

    let h = tokio::spawn(async move {
        start_up_facade.run(start_flag).await?;
        Ok(())
    });

    //warm up time
    notify.notified().await;
    h
}

pub struct TestProcessChild(pub Child);
impl Drop for TestProcessChild {
    fn drop(&mut self) {
        self.0.kill().unwrap();
    }
}
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

pub async fn threeway_handshake_helper(
    stream_handler: &mut TcpStream,
    replica_server_port: Option<u16>,
) -> Option<QueryIO> {
    let stream_port = replica_server_port.unwrap_or(stream_handler.local_addr().unwrap().port());
    let port_string = if stream_port > 10000 {
        format!("$5\r\n{}", stream_port)
    } else {
        format!("$4\r\n{}", stream_port)
    };

    // client sends PING command
    stream_handler.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();

    let val = stream_handler.read_values().await.unwrap()[0].clone();
    assert_eq!(val.serialize(), "+PONG\r\n");

    // client sends REPLCONF listening-port command
    stream_handler
        .write_all(
            format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n{}\r\n", port_string)
                .as_bytes(),
        )
        .await
        .unwrap();
    let val = stream_handler.read_values().await.unwrap()[0].clone();
    assert_eq!(val.serialize(), "+OK\r\n");

    // client sends REPLCONF capa command
    stream_handler
        .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await
        .unwrap();
    let val = stream_handler.read_values().await.unwrap()[0].clone();
    assert_eq!(val.serialize(), "+OK\r\n");

    // THEN - client receives OK

    // client sends PSYNC command
    stream_handler.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await.unwrap();
    let values = stream_handler.read_values().await.unwrap();

    assert_eq!(values[0].serialize(), "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");

    values.get(1).cloned()
}
