use bytes::BytesMut;

use crate::{
    adapters::in_memory::InMemoryDb,
    handlers::Handler,
    interface::{Database, TRead, TWriteBuf},
    protocol::{self},
};

// Fake Stream to test the write_value function
struct FakeStream {
    pub written: Vec<u8>,
}

impl TRead for FakeStream {
    async fn read(&mut self, buf: &mut BytesMut) -> Result<usize, std::io::Error> {
        buf.extend_from_slice(&self.written);
        Ok(self.written.len())
    }
}

impl TWriteBuf for FakeStream {
    async fn write_buf(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        self.written.extend_from_slice(buf);
        Ok(())
    }
}

/// The following is to test out the set operation with no expiry
/// FakeStream should be used to create RespHandler.
/// `read_operation`` should be called on the handler to get the command.
/// The command should be parsed to get the command and arguments.
///
/// INPUT : "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
/// OUTPUT(when get method is invoked on the key) : "value"
#[tokio::test]
async fn test_set() {
    let stream = FakeStream {
        written: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut parser = protocol::MessageParser::new(stream);

    // WHEN
    Handler::handle(&mut parser).await.unwrap();

    let value = InMemoryDb.get("key").await.unwrap();

    // THEN
    assert_eq!(value, "value");
}

/// The following is to test out the set operation with expiry
/// `read_operation`` should be called on the handler to get the command.
/// The command should be parsed to get the command and arguments.
#[tokio::test]
async fn test_set_with_expiry() {
    let stream = FakeStream {
        written: "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut parser = protocol::MessageParser::new(stream);

    // WHEN
    Handler::handle(&mut parser).await.unwrap();

    let value = InMemoryDb.get("foo").await.unwrap();

    // THEN
    assert_eq!(value, "bar");

    // WHEN2 - wait for 5ms
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    let value = InMemoryDb.get("foo").await.unwrap();

    //THEN
    assert_eq!(value, "bar");
}

#[tokio::test]
async fn test_set_with_expire_should_expire_within_100ms() {
    let stream = FakeStream {
        written: "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut parser = protocol::MessageParser::new(stream);

    // WHEN
    Handler::handle(&mut parser).await.unwrap();

    let value = InMemoryDb.get("foo").await.unwrap();

    // THEN
    assert_eq!(value, "bar");

    // WHEN2 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = InMemoryDb.get("foo").await;

    //THEN
    assert_eq!(value, None);
}
