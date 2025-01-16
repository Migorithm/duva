mod common;
use common::{array, spawn_server_process, terminate_process};

use redis_starter_rust::{client_utils::ClientStreamHandler, services::query_io::QueryIO};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_keys() {
    // GIVEN
    let master_port = spawn_server_process();

    let mut stream = TcpStream::connect(format!("localhost:{}", master_port)).await.unwrap();
    let mut h: ClientStreamHandler = stream.split().into();
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

    terminate_process(master_port);
}
