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

    // Send test data
    let set_operation =
        b"*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$5\r\n30000\r\n";

    // WHEN
    stream.write_all(set_operation).await.unwrap();

    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await.unwrap();

    // THEN
    assert_eq!(String::from_utf8(buffer[0..n].to_vec()).unwrap(), "+OK\r\n");

    // WHEN
    let get_operation = b"*2\r\n$3\r\nGET\r\n$10\r\nsomanyrand\r\n";
    stream.write_all(get_operation).await.unwrap();

    // Read response
    let n = stream.read(&mut buffer).await.unwrap();
    assert_eq!(
        String::from_utf8(buffer[0..n].to_vec()).unwrap(),
        "$3\r\nbar\r\n"
    );
}
