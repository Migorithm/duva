use duva::prelude::anyhow;
use duva::{
    domains::query_io::QueryIO, prelude::tokio::sync::oneshot,
    presentation::clients::request::ClientAction,
};

pub struct CommandToServer {
    pub context: InputContext,
    pub routing_rule: RoutingRule,
}
pub fn separate_command_and_args(args: Vec<&str>) -> (&str, Vec<&str>) {
    // Split the input into command and arguments
    let (cmd, args) = args.split_at(1);
    let cmd = cmd[0];
    let args = args.to_vec();
    (cmd, args)
}

#[derive(Debug)]
pub struct InputContext {
    pub(crate) client_action: ClientAction,
    pub(crate) callback: oneshot::Sender<(ClientAction, QueryIO)>,
    pub(crate) results: Vec<QueryIO>,
    pub(crate) expected_result_cnt: usize,
}
impl InputContext {
    pub fn new(
        client_action: ClientAction,
        callback: oneshot::Sender<(ClientAction, QueryIO)>,
    ) -> Self {
        Self { client_action, callback, results: Vec::new(), expected_result_cnt: 0 }
    }
    pub(crate) fn append_result(&mut self, result: QueryIO) {
        self.results.push(result);
    }

    pub(crate) fn set_expected_result_cnt(&mut self, cnt: usize) {
        self.expected_result_cnt = cnt
    }
    pub(crate) fn is_done(&self) -> bool {
        if self.results.len() == self.expected_result_cnt {
            return true;
        }
        false
    }

    pub(crate) fn get_result(&self) -> anyhow::Result<QueryIO> {
        match self.client_action {
            | ClientAction::Keys { pattern: _ } | ClientAction::MGet { keys: _ } => {
                let init = QueryIO::Array(Vec::new());
                let result = self.results.iter().fold(init, |acc, item| {
                    acc.merge(item.clone()).unwrap_or_else(|_| QueryIO::Array(Vec::new()))
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

#[derive(Debug)]
pub struct CommandEntry {
    pub(crate) key: String,
    value: Option<String>,
    expires_at: Option<i64>,
}

#[derive(Debug, Default)]
pub enum RoutingRule {
    #[default]
    Any,
    // TODO merge Signle and Multi
    Selective(Vec<CommandEntry>),
    BroadCast,
}

impl From<&ClientAction> for RoutingRule {
    fn from(value: &ClientAction) -> Self {
        match value {
            // commands that requires single-key routing
            | ClientAction::Get { key }
            | ClientAction::Ttl { key }
            | ClientAction::Decr { key }
            | ClientAction::Incr { key } => Self::Selective(vec![CommandEntry {
                key: key.clone(),
                value: None,
                expires_at: None,
            }]),
            | ClientAction::Set { key, value } | ClientAction::Append { key, value } => {
                Self::Selective(vec![CommandEntry {
                    key: key.clone(),
                    value: Some(value.clone()),
                    expires_at: None,
                }])
            },
            | ClientAction::IndexGet { key, index } => Self::Selective(vec![CommandEntry {
                key: key.clone(),
                value: Some(index.to_string()),
                expires_at: None,
            }]),
            | ClientAction::SetWithExpiry { key, value, expires_at } => {
                Self::Selective(vec![CommandEntry {
                    key: key.clone(),
                    value: Some(value.clone()),
                    expires_at: Some(*expires_at),
                }])
            },

            // commands thar require multi-key-routings
            | ClientAction::Delete { keys }
            | ClientAction::Exists { keys }
            | ClientAction::MGet { keys } => Self::Selective(
                keys.iter()
                    .map(|key| CommandEntry { key: key.clone(), value: None, expires_at: None })
                    .collect(),
            ),

            // broadcast
            | ClientAction::Keys { pattern } => Self::BroadCast,
            | _ => Self::Any,
        }
    }
}
