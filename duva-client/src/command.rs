use duva::{
    domains::query_io::QueryIO, prelude::tokio::sync::oneshot,
    presentation::clients::request::ClientAction,
};

pub fn separate_command_and_args(args: Vec<&str>) -> (&str, Vec<&str>) {
    // Split the input into command and arguments
    let (cmd, args) = args.split_at(1);
    let cmd = cmd[0];
    let args = args.to_vec();
    (cmd, args)
}

#[derive(Debug)]
pub struct Input {
    pub kind: ClientAction,
    pub callback: oneshot::Sender<(ClientAction, QueryIO)>,
}

impl Input {
    pub fn new(input: ClientAction, callback: oneshot::Sender<(ClientAction, QueryIO)>) -> Self {
        Self { kind: input, callback }
    }
}
