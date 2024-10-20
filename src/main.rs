use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

pub mod commands;
pub mod error;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(stream: TcpStream) {
    println!("accepted new connection");
    // buffered reader
    let (read_half, mut writer) = stream.into_split();

    let reader = BufReader::new(read_half);

    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        println!("Received {:?}", line);
        match commands::Command::try_from(line.as_str()) {
            Ok(commands::Command::Ping) => {
                writer.write(b"+PONG\r\n").await.unwrap();
            }
            Err(e) => {
                println!("error: {:?}", e);
                continue;
            }
        }
    }
}
