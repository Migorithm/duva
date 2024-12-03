use redis_starter_rust::{
    config::Config, services::query_manager::interface::TCancellationTokenFactory, TNotifyStartUp,
};
use redis_starter_rust::services::query_manager::query_io::QueryIO;
use redis_starter_rust::{config::Config, TNotifyStartUp};
use std::sync::{Arc, OnceLock};
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
    }

    pub async fn get_response(&mut self) -> String {
        let mut buffer = [0; 1024];
        let n = self.read.read(&mut buffer).await.unwrap();
        String::from_utf8(buffer[0..n].to_vec()).unwrap()
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub async fn integration_test_config() -> &'static Config {
    let port = find_free_port_in_range(49152, 65535).await;

    CONFIG.get_or_init(|| Config::default().set_port(port.unwrap()))
}

// scan for available port
async fn find_free_port_in_range(start: u16, end: u16) -> Option<u16> {
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

pub async fn start_test_server<T: TCancellationTokenFactory>(
    config: &'static Config,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    // GIVEN
    let notify = Arc::new(tokio::sync::Notify::new());
    let start_flag = StartFlag(notify.clone());
    let h = tokio::spawn(redis_starter_rust::start_up::<T>(
        config,
        3,
        redis_starter_rust::adapters::persistence::EnDecoder,
        start_flag,
    ));

    //warm up time
    notify.notified().await;
    h
}

pub fn array(arr: Vec<&str>) -> String {
    QueryIO::Array(arr.iter()
        .map(|s| QueryIO::BulkString(s.to_string()))
        .collect())
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