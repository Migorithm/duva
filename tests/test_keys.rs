mod common;
use common::{array, init_config_manager_with_free_port, start_test_server, TestStreamHandler};

use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory, services::query_io::QueryIO,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_keys() {
    // GIVEN
    let config = init_config_manager_with_free_port().await;

    let _ = start_test_server(CancellationTokenFactory, config.clone()).await;

    let mut stream = TcpStream::connect(config.bind_addr()).await.unwrap();
    let mut h: TestStreamHandler = stream.split().into();
    let num_of_keys = 500;

    // WHEN set 100000 keys
    for i in 0..500 {
        h.send({ array(vec!["SET", &i.to_string(), "bar"]).into_bytes() }.as_slice()).await;
        assert_eq!(h.get_response().await, QueryIO::SimpleString("OK".to_string()).serialize());
    }

    // Fire keys command
    h.send({ array(vec!["KEYS", "*"]).into_bytes() }.as_slice()).await;
    let res = h.get_response().await;

    assert!(res.starts_with(format!("*{}\r\n", num_of_keys).as_str()));
}
