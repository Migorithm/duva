pub mod cli;
pub mod command;
pub mod controller;
pub mod error;

use cli::Cli;
use command::{build_command, separate_command_and_args, take_input};
use controller::{ClientController, PROMPT};
use duva::prelude::*;

#[tokio::main]
async fn main() {
    let mut controller = ClientController::new().await;

    loop {
        let readline = controller.editor.readline(PROMPT).unwrap_or_else(|_| std::process::exit(0));

        let args: Vec<&str> = readline.split_whitespace().collect();
        if args.is_empty() {
            continue;
        }
        if args[0].eq_ignore_ascii_case("exit") {
            break;
        }

        // split command and arg where first element is command
        // and the rest are arguments
        let (cmd, args) = separate_command_and_args(args);

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
