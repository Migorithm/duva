use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{ReadHalf, WriteHalf},
};

pub async fn send_operation(write: &mut WriteHalf<'_>, operation: &[u8]) {
    write.write_all(operation).await.unwrap();
}

pub async fn read_response(read: &mut ReadHalf<'_>) -> String {
    let mut buffer = [0; 1024];
    let n = read.read(&mut buffer).await.unwrap();
    String::from_utf8(buffer[0..n].to_vec()).unwrap()
}
