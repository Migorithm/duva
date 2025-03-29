pub mod cli;
pub mod command;
pub mod controller;

use cli::Cli;
use command::{build_command, validate_input};
use controller::ClientController;
use duva::prelude::*;

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut controller = ClientController::new().await;

    loop {
        let readline = controller.read_line()?;

        let args: Vec<&str> = readline.split_whitespace().collect();
        if args.is_empty() {
            continue;
        }
        if args[0].eq_ignore_ascii_case("exit") {
            println!("Exiting...");
            break;
        }

        if let Err(e) = validate_input(&args) {
            println!("{}", e);
            continue;
        }
        if let Err(e) = controller.send_command(args).await {
            println!("{}", e);
        }
    }
    Ok(())
}
