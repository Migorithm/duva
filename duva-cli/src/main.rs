pub mod cli;
pub mod command;
pub mod controller;
pub mod error;

use cli::Cli;
use command::{build_command, take_input};
use controller::{ClientController, PROMPT};
use duva::prelude::*;

#[tokio::main]
async fn main() {
    let mut controller = ClientController::new().await;

    loop {
        let readline = controller.editor.readline(PROMPT).unwrap_or_else(|_| std::process::exit(0));

        let args: Vec<String> = readline.split_whitespace().map(|s| s.to_string()).collect();
        if args.is_empty() {
            continue;
        }

        // split command and arg where first element is command
        // and the rest are arguments
        let (cmd, args) = args.split_at(1);
        let cmd = cmd[0].to_string();
        let args = args.to_vec();

        if cmd.eq_ignore_ascii_case("exit") {
            break;
        }

        match take_input(&cmd, &args) {
            Ok(input) => {
                if let Err(e) = controller.send_command(cmd, args, input).await {
                    println!("{}", e);
                }
            },
            Err(e) => {
                println!("{}", e);
            },
        }
    }
}
