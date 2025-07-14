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
pub fn build_command_with_request_id(
    cmd: &str,
    request_id: u64,
    args: &Vec<String>,
) -> String {
    // Build the valid RESP command
    let mut command =
        format!("!{}\r\n*{}\r\n${}\r\n{}\r\n", request_id, args.len() + 1, cmd.len(), cmd);
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    command
}

#[derive(Debug)]
pub struct InputContext {
    pub kind: ClientAction,
    pub callback: oneshot::Sender<(ClientAction, QueryIO)>,
    pub results: Vec<QueryIO>,
    pub num_of_results: usize,
}

impl InputContext {
    pub(crate) fn new(
        kind: ClientAction,
        callback: oneshot::Sender<(ClientAction, QueryIO)>,
        num_of_results: usize,
    ) -> Self {
        match kind {
            | ClientAction::Keys { pattern: _ } => {
                Self { kind, callback, results: Vec::new(), num_of_results }
            }
            // TODO handle other cases like MSET, MGET, etc.
            | ref other_values => Self { kind, callback, results: Vec::new(), num_of_results: 1 },
        }
    }
    pub(crate) fn append_result(&mut self, result: QueryIO) {
        match self.kind {
            | ClientAction::Keys { pattern: _ } => {
                self.results.push(result);
            }
            // TODO handle other cases like MSET, MGET, etc.
            | _ => {}
        }
    }
    pub(crate) fn is_done(&self) -> bool {
        if self.results.len() == self.num_of_results {
            return true;
        }
        false
    }
    pub(crate) fn get_result(&self) -> QueryIO {
        *self.results.iter().reduce(|acc, x| &x.merge(acc))
    }
}
