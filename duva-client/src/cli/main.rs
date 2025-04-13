mod cli;
mod editor;

use clap::Parser;
use duva::prelude::tokio::{self, sync::oneshot};
use duva_client::{
    broker::BrokerMessage,
    command::{Input, separate_command_and_args, validate_input},
    controller::ClientController,
};

const PROMPT: &str = "duva-cli> ";

#[tokio::main]
async fn main() {
    let cli = cli::Cli::parse();
    let mut controller = ClientController::new(editor::create(), &cli.address()).await;

    loop {
        let readline = controller.target.readline(PROMPT).unwrap_or_else(|_| std::process::exit(0));

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

        match validate_input(cmd, &args) {
            Ok(input) => {
                let (tx, rx) = oneshot::channel();
                let input = Input::new(input, tx);
                let _ = controller
                    .broker_tx
                    .send(BrokerMessage::from_command(cmd.into(), args, input))
                    .await;
                let (kind, query_io) = rx.await.unwrap();
                controller.print_res(kind, query_io);
            },
            Err(e) => {
                println!("{}", e);
            },
        }
    }
}
