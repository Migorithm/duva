pub mod cli;
pub mod command;
pub mod controller;
pub mod error;

use cli::Cli;
use command::{build_command, validate_input};
use controller::{ClientController, PROMPT};
use duva::prelude::*;

#[tokio::main]
async fn main() {
    let mut controller = ClientController::new().await;

    loop {
        let readline = controller.editor.readline(PROMPT).unwrap_or_else(|_| std::process::exit(0));

        let args: Vec<String> = readline.split_whitespace().map(|s| s.to_string()).collect();
        // split command and arg where first element is command
        // and the rest are arguments
        let (cmd, args) = args.split_at(1);
        let cmd = cmd[0].to_string();
        let args = args.to_vec();

        if cmd.is_empty() {
            continue;
        }
        if cmd.eq_ignore_ascii_case("exit") {
            break;
        }

        if let Err(e) = validate_input(&cmd, &args) {
            println!("{}", e);
            continue;
        }
        if let Err(e) = controller.send_command(cmd, args).await {
            println!("{}", e);
        }
    }
}
