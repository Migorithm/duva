use duva::prelude::anyhow;
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
pub fn build_command_with_request_id(cmd: &str, request_id: u64, args: &Vec<String>) -> String {
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
    pub(crate) kind: ClientAction,
    pub(crate) callback: oneshot::Sender<(ClientAction, QueryIO)>,
    pub(crate) results: Vec<QueryIO>,
    pub(crate) num_of_results: usize,
}

impl InputContext {
    pub fn new(kind: ClientAction, callback: oneshot::Sender<(ClientAction, QueryIO)>) -> Self {
        Self { kind, callback, results: Vec::new(), num_of_results: 0 }
    }
    pub(crate) fn append_result(&mut self, result: QueryIO) {
        self.results.push(result);
    }
    pub(crate) fn is_done(&self) -> bool {
        if self.results.len() == self.num_of_results {
            return true;
        }
        false
    }
    pub(crate) fn get_result(&self) -> anyhow::Result<QueryIO> {
        match self.kind {
            | ClientAction::Keys { pattern: _ } | ClientAction::MGet { keys: _ } => {
                let init = QueryIO::Array(Vec::new());
                let result = self.results.iter().fold(init, |acc, item| {
                    let acc =
                        acc.merge(item.clone()).unwrap_or_else(|_| QueryIO::Array(Vec::new()));
                    acc
                });
                Ok(result)
            },
            | ClientAction::Exists { keys: _ } | ClientAction::Delete { keys: _ } => {
                let mut count = 0;
                for result in &self.results {
                    let QueryIO::SimpleString(byte) = result else {
                        return Err(anyhow::anyhow!("Expected SimpleString result"));
                    };
                    let Ok(num) = String::from_utf8(byte.to_vec()) else {
                        return Err(anyhow::anyhow!("Failed to convert byte to string"));
                    };
                    let Ok(num) = num.parse::<u64>() else {
                        return Err(anyhow::anyhow!("Failed to parse string to u64"));
                    };
                    count += num;
                }
                Ok(QueryIO::SimpleString(count.to_string().into()))
            },
            | _ => {
                if self.results.len() != 1 {
                    return Err(anyhow::anyhow!("Expected exactly one result"));
                }
                Ok(self.results[0].clone())
            },
        }
    }
}
