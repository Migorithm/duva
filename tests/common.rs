use redis_starter_rust::services::config::config_actor::Config;
use redis_starter_rust::services::query_manager::interface::{TRead, TWrite};
use redis_starter_rust::services::query_manager::query_io::QueryIO;
use redis_starter_rust::{
    services::query_manager::interface::TCancellationTokenFactory, TNotifyStartUp,
};
use redis_starter_rust::{TSocketListener, TSocketListenerFactory};
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
    }

    pub async fn get_response(&mut self) -> String {
        let mut buffer = [0; 1024];
        let n = self.read.read(&mut buffer).await.unwrap();
        String::from_utf8(buffer[0..n].to_vec()).unwrap()
    }
}

pub async fn init_config_with_free_port() -> Config {
    let mut config = Config::default();
    config.port = find_free_port_in_range(49152, 65535).await.unwrap();
    config
}

pub async fn init_slave_config_with_free_port(master_port: u16) -> Config {
    let mut config: Config = Config::default();
    config.port = find_free_port_in_range(49152, 65535).await.unwrap();
    config.replication.master_host = Some("localhost".to_string());
    config.replication.master_port = Some(master_port);
    config
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

struct TestStreamListener(TcpListener);

impl TSocketListener for TestStreamListener {
    async fn accept(&self) -> anyhow::Result<(impl TWrite + TRead, std::net::SocketAddr)> {
        self.0.accept().await.map_err(Into::into)
    }
}
impl TSocketListenerFactory for TestStreamListener {
    async fn create_listner(bind_addr: String) -> impl TSocketListener {
        TestStreamListener(TcpListener::bind(bind_addr).await.unwrap())
    }
}

pub async fn start_test_server<T: TCancellationTokenFactory>(
    config: Config,
) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let start_flag = StartFlag(notify.clone());

    let h = tokio::spawn(redis_starter_rust::start_up::<T, TestStreamListener>(
        config,
        3,
        redis_starter_rust::adapters::endec::EnDecoder,
        start_flag,
    ));

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
