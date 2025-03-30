pub(crate) fn build_command(cmd: String, args: Vec<String>) -> String {
    // Build the valid RESP command
    let mut command = format!("*{}\r\n${}\r\n{}\r\n", args.len() + 1, cmd.len(), cmd);
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    command
}

pub(crate) fn take_input(action: &str, args: &[String]) -> Result<ClientInputKind, String> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match action.to_uppercase().as_str() {
        "SET" => {
            if !(args.len() == 2 || args.len() == 4) {
                return Err("(error) ERR wrong number of arguments for 'set' command".to_string());
            }
            if args.len() == 4 {
                if args[2].to_uppercase() != "PX" {
                    return Err("(error) ERR syntax error".to_string());
                }
            }
            return Ok(ClientInputKind::Set);
        },
        "GET" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'get' command".to_string());
            }
            return Ok(ClientInputKind::Get);
        },
        "KEYS" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'keys' command".to_string());
            }
            return Ok(ClientInputKind::Keys);
        },
        "DEL" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'del' command".to_string());
            }
            return Ok(ClientInputKind::Delete);
        },

        "PING" => Ok(ClientInputKind::Ping),
        "ECHO" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'echo' command".to_string());
            }
            return Ok(ClientInputKind::Echo);
        },
        "INFO" => {
            if !args.is_empty() {
                return Err("(error) ERR wrong number of arguments for 'info' command".to_string());
            }
            return Ok(ClientInputKind::Info);
        },

        "CLUSTER" => {
            if args.len() < 1 {
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
            return Err("(error) ERR unknown subcommand".to_string());
        },

        // Add other commands as needed
        unknown_cmd => {
            return Err(format!(
                "(error) ERR unknown command '{unknown_cmd}', with args beginning with",
            ));
        },
    }
}

pub enum ClientInputKind {
    Ping,
    Get,
    IndexGet,
    Set,
    Delete,
    Echo,
    Config,
    Keys,
    Save,
    Info,
    ClusterInfo,
    ClusterNodes,
    ClusterForget,
}
