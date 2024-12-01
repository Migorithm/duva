use redis_starter_rust::config::Config;
use std::sync::OnceLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{ReadHalf, WriteHalf},
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
pub fn integration_test_config(port: u16) -> &'static Config {
    CONFIG.get_or_init(|| Config::default().set_port(port))
}
