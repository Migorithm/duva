use redis_starter_rust::adapters::io::tokio_stream::TokioConnectStreamFactory;
use redis_starter_rust::services::config::config_manager::ConfigManager;
use redis_starter_rust::services::stream_manager::interface::TCancellationTokenFactory;
use redis_starter_rust::services::stream_manager::query_io::QueryIO;
use redis_starter_rust::{
    adapters::io::tokio_stream::TokioStreamListenerFactory, services::config::config_actor::Config,
};
use redis_starter_rust::{StartUpFacade, TNotifyStartUp};

use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpListener,
    },
};

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
    let config = Config::default();
    let mut manager = ConfigManager::new(config);

    manager.port = find_free_port_in_range(49152, 65535).await.unwrap();
    manager
}

pub async fn init_slave_config_manager_with_free_port(master_port: u16) -> ConfigManager {
    let mut config = Config::default();
    config.replication.master_host = Some("localhost".to_string());
    config.replication.master_port = Some(master_port);
    let mut manager = ConfigManager::new(config);
    manager.port = find_free_port_in_range(49152, 65535).await.unwrap();

    manager
}
// scan for available port
pub async fn find_free_port_in_range(start: u16, end: u16) -> Option<u16> {
    for port in start..=end {
        if TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            return Some(port);
        }
    }
    None
}

pub struct StartFlag(pub Arc<tokio::sync::Notify>);

impl TNotifyStartUp for StartFlag {
    fn notify_startup(&self) {
        self.0.notify_one();
    }
}

pub async fn start_test_server(
    cancellation_token_factory: impl TCancellationTokenFactory,
    config: ConfigManager,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let start_flag = StartFlag(notify.clone());

    let start_up_facade = StartUpFacade::new(
        TokioConnectStreamFactory,
        TokioStreamListenerFactory,
        cancellation_token_factory,
        config,
    );

    let h = tokio::spawn(async move {
        start_up_facade.run(start_flag).await?;
        Ok(())
    });

    //warm up time
    notify.notified().await;
    h
}

pub fn array(arr: Vec<&str>) -> String {
    QueryIO::Array(
        arr.iter()
            .map(|s| QueryIO::BulkString(s.to_string()))
            .collect(),
    )
    .serialize()
}

pub fn bulk_string(s: &str) -> String {
    QueryIO::BulkString(s.to_string()).serialize()
}

pub fn ok_response() -> String {
    QueryIO::SimpleString("OK".to_string()).serialize()
}

pub fn null_response() -> String {
    QueryIO::Null.serialize()
}

pub fn keys_command(pattern: &str) -> Vec<u8> {
    array(vec!["KEYS", pattern]).into_bytes()
}

pub fn config_command(command: &str, key: &str) -> Vec<u8> {
    array(vec!["CONFIG", command, key]).into_bytes()
}

pub fn info_command(key: &str) -> Vec<u8> {
    array(vec!["INFO", key]).into_bytes()
}

pub fn set_command_with_expiry(key: &str, value: &str, expiry: i64) -> Vec<u8> {
    array(vec!["SET", key, value, "PX", &expiry.to_string()]).into_bytes()
}

pub fn set_command(key: &str, value: &str) -> Vec<u8> {
    array(vec!["SET", key, value]).into_bytes()
}

pub fn get_command(key: &str) -> Vec<u8> {
    array(vec!["GET", key]).into_bytes()
}

pub fn save_command() -> Vec<u8> {
    array(vec!["SAVE"]).into_bytes()
}

pub async fn threeway_handshake_helper(
    stream_handler: &mut TestStreamHandler<'_>,
    client_port: u16,
) {
    // client sends PING command
    stream_handler.send(b"*1\r\n$4\r\nPING\r\n").await;
    stream_handler.get_response().await;

    // client sends REPLCONF listening-port command
    stream_handler
        .send(
            format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n",
                client_port
            )
            .as_bytes(),
        )
        .await;

    stream_handler.get_response().await;

    // client sends REPLCONF capa command
    stream_handler
        .send(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        .await;

    // THEN - client receives OK
    stream_handler.get_response().await;
    // client sends PSYNC command
    stream_handler
        .send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .await;

    stream_handler.get_response().await;
}
