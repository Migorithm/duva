use clap::Parser;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    domains::query_parsers::query_io::{QueryIO, deserialize},
    prelude::{
        BytesMut,
        tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        },
        uuid::Uuid,
    },
    services::interface::TSerdeReadWrite,
};
use rustyline::{DefaultEditor, Editor, history::FileHistory};

use crate::{Cli, build_command};

const PROMPT: &str = "duva-cli> ";

pub(crate) struct ClientController {
    stream: TcpStream,
    client_id: Uuid,
    editor: Editor<(), FileHistory>,
}

impl ClientController {
    pub(crate) async fn new() -> Self {
        let cli: Cli = Cli::parse();
        let editor = DefaultEditor::new().expect("Failed to initialize input editor");
        let (stream, client_id) = ClientController::authenticate(&cli.address()).await;
        Self { stream, client_id, editor }
    }

    pub(crate) fn read_line(&mut self) -> Result<String, String> {
        self.editor.readline(PROMPT).map_err(|e| format!("Failed to read line: {}", e))
    }

    async fn authenticate(server_addr: &str) -> (TcpStream, Uuid) {
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        stream.ser_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

        let AuthResponse::ClientId(client_id) = stream.de_read().await.unwrap();
        let client_id = Uuid::parse_str(&client_id).unwrap();
        println!("Client ID: {}", client_id);
        println!("Connected to Redis at {}", server_addr);

        (stream, client_id)
    }

    pub(crate) async fn send_command(&mut self, args: Vec<&str>) -> Result<(), String> {
        let command = build_command(args);

        // TODO input validation required otherwise, it hangs
        self.stream.write_all(command.as_bytes()).await.unwrap();
        self.stream.flush().await.unwrap();

        let mut response = vec![];
        match self.stream.read_buf(&mut response).await {
            Ok(0) => return Err("Connection closed by server".to_string()),
            Ok(_) => {},
            Err(e) => return Err(format!("Failed to read response: {}", e)),
        };

        // Deserialize response and check if it follows RESP protocol
        match deserialize(BytesMut::from_iter(response)) {
            Ok((query_io, _)) => {
                match query_io {
                    QueryIO::BulkString(value) => {
                        println!("{}", value);
                        Ok(())
                    },
                    QueryIO::Err(err) => {
                        println!("{}", err);
                        Ok(()) // We still return Ok since we got a valid RESP error response
                    },
                    QueryIO::Null => {
                        println!("(nil)");
                        Ok(())
                    },
                    QueryIO::SimpleString(val) => {
                        println!("{}", val);
                        Ok(())
                    },
                    _ => {
                        let err_msg = "Unexpected response format";
                        println!("{}", err_msg);
                        Err(err_msg.to_string())
                    },
                }
            },
            Err(e) => {
                let err_msg = format!("Invalid RESP protocol: {}", e);
                println!("{}", err_msg);
                Err(err_msg)
            },
        }
    }
}
