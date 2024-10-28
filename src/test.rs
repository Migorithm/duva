use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::mpsc::Sender;

use crate::{
    adapters::in_memory::InMemoryDb,
    config::Config,
    services::{
        config_handler::ConfigHandler,
        interface::{Database, TRead, TWriteBuf},
        persistence::{persist_actor, PersistEnum},
        query_manager::{
            command::Args,
            value::{TtlCommand, Value},
            MessageParser,
        },
        ttl_handlers::{delete::delete_actor, set::set_ttl_actor},
        ServiceFacade,
    },
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
        self.written.clear();
        self.written.extend_from_slice(buf);
        Ok(())
    }
}

fn run_persistent_actors() -> Vec<Sender<PersistEnum>> {
    let mut senders_to_persistent_actors: Vec<Sender<PersistEnum>> = Vec::new();
    (0..3).for_each(|_| {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(persist_actor(rx));
        senders_to_persistent_actors.push(tx);
    });
    senders_to_persistent_actors
}

fn run_ttl_actors() -> Sender<TtlCommand> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let _ = tokio::spawn(set_ttl_actor(rx));
    let _ = tokio::spawn(delete_actor(InMemoryDb));
    tx
}

async fn get_key(key: &str, senders_to_handlers: &[Sender<PersistEnum>]) -> Value {
    let args = Args(vec![Value::BulkString(key.to_string())]);

    let shard_key = args.take_shard_key(senders_to_handlers.len()).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    senders_to_handlers[shard_key]
        .send(PersistEnum::Get(args.clone(), tx))
        .await
        .unwrap();

    rx.await.unwrap()
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
    let ttl_sender = run_ttl_actors();
    let mut parser = MessageParser::new(stream);
    let mut handler = ServiceFacade::new(ConfigHandler::new(Arc::new(Config::new())), ttl_sender);
    let persistence_handlers = run_persistent_actors();

    // WHEN
    handler
        .handle(&mut parser, &persistence_handlers)
        .await
        .unwrap();

    // parser.stream.written = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".as_bytes().to_vec();

    let value = get_key("key", &persistence_handlers).await;
    // THEN
    assert_eq!(value, Value::BulkString("value".to_string()),);
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
    let ttl_sender = run_ttl_actors();
    let mut parser = MessageParser::new(stream);
    let mut handler = ServiceFacade::new(ConfigHandler::new(Arc::new(Config::new())), ttl_sender);
    let senders_to_persistent_actors: Vec<Sender<PersistEnum>> = run_persistent_actors();
    // WHEN

    handler
        .handle(&mut parser, &senders_to_persistent_actors)
        .await
        .unwrap();

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
    let ttl_sender = run_ttl_actors();
    let mut parser = MessageParser::new(stream);
    let mut handler = ServiceFacade::new(ConfigHandler::new(Arc::new(Config::new())), ttl_sender);
    let senders_to_persistent_actors: Vec<Sender<PersistEnum>> = run_persistent_actors();

    // WHEN
    handler
        .handle(&mut parser, &senders_to_persistent_actors)
        .await
        .unwrap();

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
    let ttl_sender = run_ttl_actors();
    let mut parser = MessageParser::new(stream);
    let mut handler = ServiceFacade::new(ConfigHandler::new(Arc::new(conf)), ttl_sender);
    let senders_to_persistent_actors: Vec<Sender<PersistEnum>> = run_persistent_actors();

    // WHEN
    handler
        .handle(&mut parser, &senders_to_persistent_actors)
        .await
        .unwrap();

    // THEN
    let res = "*2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n";
    let written = String::from_utf8(parser.stream.written.to_vec()).unwrap();
    assert_eq!(written, res.to_string());
}
