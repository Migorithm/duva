use crate::{
    adapters::controller::{
        interface::{TRead, TWriteBuf},
        QueryManager,
    },
    config::Config,
    services::{
        config_handler::ConfigHandler,
        query_io::QueryIO,
        statefuls::routers::{cache_dispatcher::CacheDispatcher, ttl_actor::TtlInbox},
        CacheEntry,
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

async fn get_key(key: &str, persistence_router: &CacheDispatcher) -> QueryIO {
    persistence_router.route_get(key.to_string()).await.unwrap()
}

async fn set_key_with_no_expiry(
    key: &str,
    value: &str,

    ttl_sender: TtlInbox,
    persistence_router: &CacheDispatcher,
) -> QueryIO {
    persistence_router
        .route_set(
            CacheEntry::KeyValue(key.to_string(), value.to_string()),
            ttl_sender,
        )
        .await
        .unwrap()
}

fn config_helper() -> Arc<Config> {
    let mut conf = Config::new();
    conf.dir = Some("/tmp".to_string());
    conf.into()
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
    let (persistence_handlers, ttl_inbox) = CacheDispatcher::run_cache_actors(3, config_helper());

    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    let stream = FakeStream {
        written: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
            .as_bytes()
            .to_vec(),
    };
    let mut controller = QueryManager::new(stream);

    // WHEN
    controller
        .handle(&persistence_handlers, ttl_inbox, config_handler)
        .await
        .unwrap();

    // parser.stream.written = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".as_bytes().to_vec();

    let value = get_key("key", &persistence_handlers).await;
    // THEN
    assert_eq!(value, QueryIO::BulkString("value".to_string()),);
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

    let (cache_dispatcher, ttl_inbox) = CacheDispatcher::run_cache_actors(3, config_helper());

    let mut controller = QueryManager::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    // WHEN
    controller
        .handle(&cache_dispatcher, ttl_inbox, config_handler)
        .await
        .unwrap();

    let value = get_key("foo", &cache_dispatcher).await;

    // THEN
    assert_eq!(value, QueryIO::BulkString("bar".to_string()));

    // WHEN2 - wait for 5ms
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    let value = get_key("foo", &cache_dispatcher).await;

    //THEN
    assert_eq!(value, QueryIO::BulkString("bar".to_string()));
}

#[tokio::test]
async fn test_set_with_expire_should_expire_within_100ms() {
    let stream = FakeStream {
        written: "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n"
            .as_bytes()
            .to_vec(),
    };
    let (cache_dispatcher, ttl_inbox) = CacheDispatcher::run_cache_actors(3, config_helper());

    let mut controller = QueryManager::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(Config::new()));

    // WHEN
    controller
        .handle(&cache_dispatcher, ttl_inbox, config_handler)
        .await
        .unwrap();

    let value = get_key("foo", &cache_dispatcher).await;

    // THEN
    assert_eq!(value, QueryIO::BulkString("bar".to_string()));

    // WHEN2 - wait for 100ms
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let value = get_key("foo", &cache_dispatcher).await;

    //THEN
    assert_eq!(value, QueryIO::Null);
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
    let (cache_dispatcher, ttl_inbox) = CacheDispatcher::run_cache_actors(3, config_helper());

    let mut controller = QueryManager::new(stream);
    let config_handler = ConfigHandler::new(Arc::new(conf));

    // WHEN
    controller
        .handle(&cache_dispatcher, ttl_inbox, config_handler)
        .await
        .unwrap();

    // THEN
    let res = "*2\r\n$3\r\ndir\r\n$4\r\n/tmp\r\n";
    let written = String::from_utf8(controller.stream.written.to_vec()).unwrap();
    assert_eq!(written, res.to_string());
}

#[tokio::test]
async fn test_keys() {
    //GIVEN
    let (cache_dispatcher, ttl_inbox) = CacheDispatcher::run_cache_actors(3, config_helper());

    set_key_with_no_expiry("key", "value", ttl_inbox.clone(), &cache_dispatcher).await;

    set_key_with_no_expiry("key2", "value", ttl_inbox.clone(), &cache_dispatcher).await;

    // Input will be given like : redis-cli KEYS "*"
    let stream = FakeStream {
        written: "*2\r\n$4\r\nKEYS\r\n$3\r\n\"*\"\r\n".as_bytes().to_vec(),
    };

    let mut controller = QueryManager::new(stream);

    // WHEN
    controller
        .handle(
            &cache_dispatcher,
            ttl_inbox,
            ConfigHandler::new(Arc::new(Config::new())),
        )
        .await
        .unwrap();

    //THEN string comparison

    assert!([
        "*2\r\n$3\r\nkey\r\n$4\r\nkey2\r\n",
        "*2\r\n$4\r\nkey2\r\n$3\r\nkey\r\n"
    ]
    .contains(
        &String::from_utf8(controller.stream.written.to_vec())
            .unwrap()
            .as_str()
    ));
}
