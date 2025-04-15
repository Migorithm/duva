use duva::{domains::query_parsers::query_io::QueryIO, prelude::tokio::sync::oneshot};

pub fn separate_command_and_args(args: Vec<&str>) -> (&str, Vec<&str>) {
    // Split the input into command and arguments
    let (cmd, args) = args.split_at(1);
    let cmd = cmd[0];
    let args = args.to_vec();
    (cmd, args)
}

pub fn validate_input(action: &str, args: &[&str]) -> Result<ClientInputKind, String> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match action.to_uppercase().as_str() {
        "SET" => {
            if !(args.len() == 2 || args.len() == 4) {
                return Err("(error) ERR wrong number of arguments for 'set' command".to_string());
            }
            if args.len() == 4 && args[2].to_uppercase() == "EX" {
                return Err("(error) ERR syntax error".to_string());
            }
            Ok(ClientInputKind::Set)
        },
        "GET" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'get' command".to_string());
            }
            Ok(ClientInputKind::Get)
        },
        "KEYS" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'keys' command".to_string());
            }
            Ok(ClientInputKind::Keys)
        },
        "DEL" => {
            if args.len() < 1 {
                return Err("(error) ERR wrong number of arguments for 'del' command".to_string());
            }
            Ok(ClientInputKind::Del)
        },
        "EXISTS" => {
            if args.len() < 1 {
                return Err(
                    "(error) ERR wrong number of arguments for 'exists' command".to_string()
                );
            }
            Ok(ClientInputKind::Exists)
        },

        "PING" => Ok(ClientInputKind::Ping),
        "ECHO" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'echo' command".to_string());
            }
            Ok(ClientInputKind::Echo)
        },
        "INFO" => {
            if args.is_empty() {
                return Err("(error) ERR wrong number of arguments for 'info' command".to_string());
            }
            Ok(ClientInputKind::Info)
        },

        "CLUSTER" => {
            if args.is_empty() {
                return Err(
                    "(error) ERR wrong number of arguments for 'cluster' command".to_string()
                );
            }
            if args[0].to_uppercase() == "NODES" {
                return Ok(ClientInputKind::ClusterNodes);
            } else if args[0].to_uppercase() == "INFO" {
                return Ok(ClientInputKind::ClusterInfo);
            } else if args[0].to_uppercase() == "FORGET" {
                if args.len() != 2 {
                    return Err(
                        "(error) ERR wrong number of arguments for 'cluster forget' command"
                            .to_string(),
                    );
                }
                return Ok(ClientInputKind::ClusterForget);
            }
            Err("(error) ERR unknown subcommand".to_string())
        },
        "REPLICAOF" => {
            if args.len() != 2 {
                return Err(
                    "(error) ERR wrong number of arguments for 'replicaof' command".to_string()
                );
            };
            Ok(ClientInputKind::ReplicaOf)
        },
        "ROLE" => {
            if !args.is_empty() {
                return Err("(error) ERR wrong number of argument for 'role' command".to_string());
            }
            Ok(ClientInputKind::Role)
        },
        "CONFIG" => {
            if args.len() != 2 {
                return Err(
                    "(error) ERR wrong number of arguments for 'config' command".to_string()
                );
            }
            Ok(ClientInputKind::Config)
        },
        "SAVE" => {
            if !args.is_empty() {
                return Err("(error) ERR wrong number of arguments for 'save' command".to_string());
            }
            Ok(ClientInputKind::Save)
        },
        // Add other commands as needed
        unknown_cmd => {
            Err(format!("(error) ERR unknown command '{unknown_cmd}', with args beginning with",))
        },
    }
}

#[derive(Debug, Clone)]
pub enum ClientInputKind {
    Ping,
    Get,
    Set,
    Del,
    Echo,
    Config,
    Keys,
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget,
    Exists,
    ReplicaOf,
    Role,
    Incr,
}

#[derive(Debug)]
pub struct Input {
    pub kind: ClientInputKind,
    pub callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
}

impl Input {
    pub fn new(
        input: ClientInputKind,
        callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
    ) -> Self {
        Self { kind: input, callback }
    }
}
