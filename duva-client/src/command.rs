use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::replications::LogEntry;
use duva::prelude::BinBytes;
use duva::prelude::anyhow::{self, Context};
use duva::presentation::clients::request::{NonMutatingAction, ServerResponse};
use duva::{
    domains::query_io::QueryIO, prelude::tokio::sync::oneshot,
    presentation::clients::request::ClientAction,
};
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct CommandQueue {
    pub queue: VecDeque<InputContext>,
}
impl CommandQueue {
    pub fn push(&mut self, input_context: InputContext) {
        self.queue.push_back(input_context);
    }

    pub fn pop(&mut self) -> Option<InputContext> {
        self.queue.pop_front()
    }

    pub(crate) fn finalize_or_requeue(
        &mut self,
        query_io: ServerResponse,
        mut context: InputContext,
    ) {
        context.results.push(query_io);

        if context.results.len() != context.expected_result_cnt {
            self.push(context);
            return;
        }

        let result = context
            .get_result()
            .unwrap_or_else(|err| ServerResponse::Err { reason: err.to_string(), request_id: 0 });
        context.callback(result);
    }
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
    pub(crate) callback: oneshot::Sender<(ClientAction, ServerResponse)>,
    pub(crate) results: Vec<ServerResponse>,
    pub(crate) expected_result_cnt: usize,
}
impl InputContext {
    pub fn new(
        client_action: ClientAction,
        callback: oneshot::Sender<(ClientAction, ServerResponse)>,
    ) -> Self {
        Self { client_action, callback, results: Vec::new(), expected_result_cnt: 0 }
    }

    pub(crate) fn callback(self, query_io: ServerResponse) {
        let action_debug = format!("{:?}", self.client_action);
        self.callback.send((self.client_action, query_io)).unwrap_or_else(|_| {
            // Log callback failure for debugging
            tracing::error!(
                action = %action_debug,
                "Failed to send response to input callback"
            );
        });
    }

    pub(crate) fn get_result(&mut self) -> anyhow::Result<ServerResponse> {
        use NonMutatingAction::*;
        let res = std::mem::take(&mut self.results);

        match self.client_action {
            ClientAction::NonMutating(Keys { pattern: _ } | MGet { keys: _ }) => {
                let mut init = QueryIO::Array(Vec::with_capacity(res.len()));

                for item in res {
                    let ServerResponse::ReadRes { res, request_id } = item else {
                        panic!();
                    };
                    init = init.merge(res)?;
                }

                Ok(ServerResponse::ReadRes { res: init, request_id: 0 })
            },
            ClientAction::NonMutating(Exists { keys: _ }) => {
                let mut count = 0;
                for result in res {
                    let ServerResponse::ReadRes { res, request_id } = result else {
                        panic!();
                    };

                    let QueryIO::SimpleString(byte) = res else {
                        return Err(anyhow::anyhow!("Expected SimpleString result"));
                    };
                    let num = String::from_utf8(byte.to_vec())
                        .context("Failed to convert byte to string")?;
                    let num = num.parse::<u64>().context("Failed to parse string to u64")?;

                    count += num;
                }

                Ok(ServerResponse::ReadRes {
                    res: QueryIO::SimpleString(BinBytes::new(count.to_string())),
                    request_id: 0,
                })
            },
            ClientAction::Mutating(LogEntry::Delete { keys: _ }) => {
                let mut count = 0;
                for result in res {
                    let ServerResponse::WriteRes { res, .. } = result else {
                        panic!();
                    };

                    let QueryIO::SimpleString(value) = res else {
                        return Err(anyhow::anyhow!("Expected SimpleString result"));
                    };
                    let decoded_value =
                        IndexedValueCodec::decode_value(String::from_utf8_lossy(&value)).unwrap();

                    count += decoded_value;
                }
                Ok(ServerResponse::WriteRes {
                    res: QueryIO::SimpleString(BinBytes::new(count.to_string())),
                    log_index: 0,
                    request_id: 0,
                })
            },
            _ => {
                if res.len() != 1 {
                    return Err(anyhow::anyhow!("Expected exactly one result"));
                }
                Ok(res[0].clone())
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
    Selective(Vec<CommandEntry>),
    Info,
    BroadCast,
}

impl From<&ClientAction> for RoutingRule {
    fn from(value: &ClientAction) -> Self {
        use LogEntry::*;
        use NonMutatingAction::*;

        match value {
            // commands that requires single-key routing
            ClientAction::NonMutating(Get { key } | Ttl { key }) => {
                Self::Selective(vec![CommandEntry {
                    key: key.clone(),
                    value: None,
                    expires_at: None,
                }])
            },

            ClientAction::NonMutating(IndexGet { key, index }) => {
                Self::Selective(vec![CommandEntry {
                    key: key.clone(),
                    value: Some(index.to_string()),
                    expires_at: None,
                }])
            },

            ClientAction::Mutating(Append { key, value }) => Self::Selective(vec![CommandEntry {
                key: key.clone(),
                value: Some(value.clone()),
                expires_at: None,
            }]),
            ClientAction::Mutating(DecrBy { key, delta: value } | IncrBy { key, delta: value }) => {
                Self::Selective(vec![CommandEntry {
                    key: key.clone(),
                    value: Some(value.to_string()),
                    expires_at: None,
                }])
            },
            ClientAction::Mutating(Set { entry }) => Self::Selective(vec![CommandEntry {
                key: entry.key().into(),
                value: Some(entry.as_str().unwrap_or_default()),
                expires_at: entry.expiry_in_i64(),
            }]),

            // commands thar require multi-key-routings
            ClientAction::Mutating(Delete { keys })
            | ClientAction::NonMutating(Exists { keys } | MGet { keys }) => Self::Selective(
                keys.iter()
                    .map(|key| CommandEntry { key: key.clone(), value: None, expires_at: None })
                    .collect(),
            ),

            ClientAction::NonMutating(Role | Ping | Echo { .. } | Config { .. } | Info) => {
                Self::Info
            },

            // broadcast
            ClientAction::NonMutating(Keys { .. }) => Self::BroadCast,
            _ => Self::Any,
        }
    }
}
