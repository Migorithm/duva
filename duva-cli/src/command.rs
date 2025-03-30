pub(crate) fn build_command(cmd: String, args: Vec<String>) -> String {
    // Build the valid RESP command
    let mut command = format!("*{}\r\n${}\r\n{}\r\n", args.len() + 1, cmd.len(), cmd);
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    command
}

pub(crate) fn validate_input(action: &str, args: &[String]) -> Result<(), String> {
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
        },
        "GET" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'get' command".to_string());
            }
        },
        "DEL" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'del' command".to_string());
            }
        },

        "PING" => {},
        "ECHO" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'echo' command".to_string());
            }
        },

        "CLUSTER" => {
            if args.len() < 1 {
                return Err(
                    "(error) ERR wrong number of arguments for 'cluster' command".to_string()
                );
            }
            if args[0].to_uppercase() != "NODES" && args[0].to_uppercase() != "INFO" {
                return Err("(error) ERR unknown subcommand".to_string());
            }
        },

        // Add other commands as needed
        unknown_cmd => {
            return Err(format!(
                "(error) ERR unknown command '{unknown_cmd}', with args beginning with",
            ));
        },
    }
    Ok(())
}
