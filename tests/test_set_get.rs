mod common;

use common::{read_response, send_operation};
use redis_starter_rust::start_up;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[tokio::test]
async fn test_get_set() {
    // GIVEN
    let bind_addr = "localhost:11111";
    tokio::spawn(start_up(bind_addr));
    //warm up time
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let mut stream = TcpStream::connect(bind_addr).await.unwrap();
    let (mut read, mut write) = stream.split();

    // WHEN
    send_operation(
        &mut write,
        b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$5\r\n30000\r\n",
    )
    .await;

    // Read response
    let res = read_response(&mut read).await;

    // THEN
    assert_eq!(res, "+OK\r\n");

    // WHEN
    send_operation(&mut write, b"*2\r\n$3\r\nGET\r\n$10\r\nsomanyrand\r\n").await;

    // THEN
    let res = read_response(&mut read).await;
    assert_eq!(res, "$3\r\nbar\r\n");
}
