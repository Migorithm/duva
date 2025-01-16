/// This module is to simulate the client stream handler
/// DO NOT USE THIS IN PRODUCTION
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{ReadHalf, WriteHalf},
};

pub struct ClientStreamHandler<'a> {
    pub read: ReadHalf<'a>,
    pub write: WriteHalf<'a>,
}

impl<'a> From<(ReadHalf<'a>, WriteHalf<'a>)> for ClientStreamHandler<'a> {
    fn from((read, write): (ReadHalf<'a>, WriteHalf<'a>)) -> Self {
        Self { read, write }
    }
}

impl<'a> ClientStreamHandler<'a> {
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
