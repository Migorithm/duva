use clap::Parser;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    prelude::tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
    prelude::uuid::Uuid,
    services::interface::{TAuthRead, TRead, TSerWrite},
};
use rustyline::DefaultEditor;

#[derive(Parser)]
#[command(name = "redis-cli", version = "1.0", about = "A simple interactive Redis CLI in Rust")]
#[clap(disable_help_flag = true)]
struct Cli {
    #[arg(short, long, default_value = "6000")]
    port: u16,
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,
}

impl Cli {
    fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn build_resp_command(args: Vec<&str>) -> String {
    let mut command = format!("*{}\r\n", args.len());
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    command
}

async fn send_command(stream: &mut TcpStream, command: String) -> String {
    stream.write_all(command.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut response = vec![];
    stream.read_buf(&mut response).await.unwrap();
    String::from_utf8(response.to_vec()).unwrap()
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let mut rl = DefaultEditor::new().expect("Failed to initialize input editor");
    let mut stream = TcpStream::connect(&cli.address()).await.unwrap();

    stream.ser_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

    let AuthResponse::ClientId(client_id) = stream.auth_read().await.unwrap();
    let client_id = Uuid::parse_str(&client_id).unwrap();
    println!("Client ID: {}", client_id);
    println!("Connected to Redis at {}", cli.address());
    loop {
        let readline = rl.readline("duva-cli> ");
        match readline {
            Ok(line) => {
                let args: Vec<&str> = line.split_whitespace().collect();
                if args.is_empty() {
                    continue;
                }
                if args[0].eq_ignore_ascii_case("exit") {
                    println!("Exiting...");
                    break;
                }

                println!("{}", send_command(&mut stream, build_resp_command(args)).await);
            },
            Err(_) => {
                println!("Exiting...");
                break;
            },
        }
    }
}
