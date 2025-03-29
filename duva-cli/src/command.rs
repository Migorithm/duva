pub(crate) fn build_command(args: Vec<&str>) -> String {
    // Build the valid RESP command
    let mut command = format!("*{}\r\n", args.len());
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    command
}

pub(crate) fn validate_input(args: &[&str]) -> Result<(), String> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match args[0].to_uppercase().as_str() {
        "SET" => {
            if !(args.len() == 3 || args.len() == 5) {
                return Err("(error) ERR wrong number of arguments for 'set' command".to_string());
            }
            if args.len() == 5 {
                if args[3].to_uppercase() != "PX" {
                    return Err("(error) ERR syntax error".to_string());
                }
            }
        },
        "GET" => {
            if args.len() != 2 {
                return Err("(error) ERR wrong number of arguments for 'get' command".to_string());
            }
        },
        "DEL" => {
            if args.len() < 2 {
                return Err("(error) ERR wrong number of arguments for 'del' command".to_string());
            }
        },
        "HSET" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return Err("(error) ERR wrong number of arguments for 'hset' command".to_string());
            }
        },
        "PING" => {
            if args.len() != 1 {
                return Err("(error) ERR wrong number of arguments for 'ping' command".to_string());
            }
        },
        "ECHO" => {
            if args.len() != 2 {
                return Err("(error) ERR wrong number of arguments for 'echo' command".to_string());
            }
        },
        //TODO fix this
        "CLUSTER" => {
            if args.len() < 2 {
                return Err(
                    "(error) ERR wrong number of arguments for 'cluster' command".to_string()
                );
            }
            if args[1].to_uppercase() != "NODES" && args[1].to_uppercase() != "INFO" {
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
