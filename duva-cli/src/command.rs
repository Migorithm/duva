pub(crate) fn build_resp_command(args: Vec<&str>) -> Result<String, String> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match args[0].to_uppercase().as_str() {
        "SET" => {
            if args.len() < 3 {
                return Err("(error) ERR wrong number of arguments for 'set' command".to_string());
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
        // Add other commands as needed
        _ => {
            // For unknown commands, we can either:
            // 1. Reject them
            // return Err(format!("Unknown command: {}", args[0]));
            // 2. Or let them pass but warn
            // eprintln!("Warning: Unknown command: {}", args[0]);
        },
    }

    // Build the valid RESP command
    let mut command = format!("*{}\r\n", args.len());
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    Ok(command)
}
