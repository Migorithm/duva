use std::sync::Arc;

use bytes::BytesMut;

use crate::{
    adapters::in_memory::InMemoryDb,
    config::Config,
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
    let mut handler = Handler {
        conf: Arc::new(Config::new()),
    };

    // WHEN
    handler.handle(&mut parser).await.unwrap();

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
    let mut handler = Handler {
        conf: Arc::new(Config::new()),
    };
    // WHEN

    handler.handle(&mut parser).await.unwrap();

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
    let mut handler = Handler {
        conf: Arc::new(Config::new()),
    };

    // WHEN
    handler.handle(&mut parser).await.unwrap();

    let value = InMemoryDb.get("foo").await.unwrap();

    // THEN
    assert_eq!(value, "bar");

    // WHEN2 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = InMemoryDb.get("foo").await;

    //THEN
    assert_eq!(value, None);
}

/// Cache config should be injected to the handler!
/// This is to enable client to configure things dynamically.
///
/// if the value of dir is /tmp, then the expected response to CONFIG GET dir is:
/// *2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n
#[tokio::test]
async fn test_config_get_dir() {
    //GIVEN
    let mut conf = Config::new();
    conf.dir = Some("/tmp".to_string());

    let stream = FakeStream {
        written: "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut parser = protocol::MessageParser::new(stream);

    let mut handler = Handler {
        conf: Arc::new(conf),
    };
    // WHEN
    handler.handle(&mut parser).await.unwrap();

    // THEN
    let res = "*2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n";
    let written = String::from_utf8(
        parser.stream.written[parser.stream.written.len() - res.len()..].to_vec(),
    )
    .unwrap();
    assert_eq!(written, res.to_string());
}



#[tokio::test]
async fn test_set_with_expire_when_data_expired_should_db_have_no_data() {
    let stream = FakeStream {
        written: "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut parser = protocol::MessageParser::new(stream);
    let mut conf = Config::new();
    conf.dir = Some("/tmp".to_string());

    let mut handler = Handler {
        conf: Arc::new(conf),
    };

    // WHEN
    handler.handle(&mut parser).await.unwrap();

    let value = InMemoryDb.get("foo").await.unwrap();

    // THEN
    assert_eq!(value, "bar");

    // WHEN2 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = InMemoryDb.get("foo").await;

    //THEN
    assert_eq!(value, None);

    // WHEN3 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = InMemoryDb.get("foo").await;

    //THEN
    assert_eq!(value, None);
    assert_eq!(InMemoryDb.size().await,0);
}
