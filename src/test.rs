use crate::{
    config::Config,
    controller::Controller,
    services::{
        config_handler::ConfigHandler,
        interface::{TRead, TWriteBuf},
        persistence::{
            router::{run_persistent_actors, PersistenceRouter},
            ttl_handlers::{
                delete::run_delete_expired_key_actor,
                set::{run_set_ttl_actor, TtlSetter},
            },
        },
        value::{Value, Values},
    },
};
use bytes::BytesMut;
use std::sync::Arc;

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

fn run_ttl_actors(persistence_router: &PersistenceRouter) -> TtlSetter {
    run_delete_expired_key_actor(persistence_router.clone());
    run_set_ttl_actor()
}

async fn get_key(key: &str, persistence_router: &PersistenceRouter) -> Value {
    let args = Values::new(vec![Value::BulkString(key.to_string())]);
    persistence_router.route_get(&args).await.unwrap()
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
    let persistence_handlers = run_persistent_actors(3);
    let ttl_sender = run_ttl_actors(&persistence_handlers);
    let mut controller = Controller::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    // WHEN
    controller
        .handle(&persistence_handlers, ttl_sender, config_handler)
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
    let persistence_router = run_persistent_actors(3);
    let ttl_sender = run_ttl_actors(&persistence_router);
    let mut controller = Controller::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    // WHEN
    controller
        .handle(&persistence_router, ttl_sender, config_handler)
        .await
        .unwrap();

    let value = get_key("foo", &persistence_router).await;

    // THEN
    assert_eq!(value, Value::BulkString("bar".to_string()));

    // WHEN2 - wait for 5ms
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    let value = get_key("foo", &persistence_router).await;

    //THEN
    assert_eq!(value, Value::BulkString("bar".to_string()));
}

#[tokio::test]
async fn test_set_with_expire_should_expire_within_100ms() {
    let stream = FakeStream {
        written: "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n"
            .as_bytes()
            .to_vec(),
    };
    let persistence_router = run_persistent_actors(3);
    let ttl_sender = run_ttl_actors(&persistence_router);

    let mut controller = Controller::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    // WHEN
    controller
        .handle(&persistence_router, ttl_sender, config_handler)
        .await
        .unwrap();

    let value = get_key("foo", &persistence_router).await;

    // THEN
    assert_eq!(value, Value::BulkString("bar".to_string()));

    // WHEN2 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = get_key("foo", &persistence_router).await;

    //THEN
    assert_eq!(value, Value::Null);
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
    let persistence_router = run_persistent_actors(3);
    let ttl_sender = run_ttl_actors(&persistence_router);

    let mut controller = Controller::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(conf));

    // WHEN
    controller
        .handle(&persistence_router, ttl_sender, config_handler)
        .await
        .unwrap();

    // THEN
    let res = "*2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n";
    let written = String::from_utf8(controller.stream.written.to_vec()).unwrap();
    assert_eq!(written, res.to_string());
}
