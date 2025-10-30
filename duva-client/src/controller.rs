use crate::broker::Broker;
use crate::broker::BrokerMessage;

use duva::domains::query_io::QueryIO;
use duva::domains::replications::LogEntry;
use duva::prelude::PeerIdentifier;
use duva::prelude::anyhow;
use duva::prelude::bytes::Bytes;
use duva::prelude::tokio;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::presentation::clients::request::ClientAction;
use duva::presentation::clients::request::NonMutatingAction;
use duva::presentation::clients::request::ServerResponse;
use std::fmt::Display;

pub struct ClientController<T> {
    pub broker_tx: Sender<BrokerMessage>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &PeerIdentifier) -> anyhow::Result<Self> {
        let broker = Broker::new(server_addr).await?;
        let broker_tx = broker.tx.clone();
        tokio::spawn(broker.run());
        Ok(Self { broker_tx, target: editor })
    }
}

fn render_return(kind: ClientAction, res: QueryIO) -> Response {
    use ClientAction::*;
    use NonMutatingAction::*;

    match kind {
        NonMutating(
            Ping
            | Get { .. }
            | LIndex { .. }
            | IndexGet { .. }
            | Echo { .. }
            | Config { .. }
            | Info
            | ClusterForget { .. }
            | ReplicaOf { .. }
            | ClusterInfo,
        ) => match res {
            QueryIO::Null => Response::Null,
            QueryIO::BulkString(value) => Response::String(value.into()),

            _err => Response::FormatError,
        },
        Mutating(LogEntry::Delete { .. }) | NonMutating(Exists { .. } | LLen { .. }) => {
            let QueryIO::BulkString(value) = res else {
                return Response::FormatError;
            };
            match str::from_utf8(&value) {
                Ok(int) => Response::Integer(int.to_string().into()),
                Err(_) => Response::Error("ERR value is not an integer or out of range".into()),
            }
        },

        NonMutating(Ttl { .. })
        | Mutating(
            LogEntry::IncrBy { .. }
            | LogEntry::DecrBy { .. }
            | LogEntry::LPush { .. }
            | LogEntry::RPush { .. }
            | LogEntry::LPushX { .. }
            | LogEntry::RPushX { .. },
        ) => match res {
            QueryIO::BulkString(value) => {
                let s = String::from_utf8_lossy(&value);

                Response::Integer(s.to_string().into())
            },

            _ => Response::FormatError,
        },
        NonMutating(Save) => {
            let QueryIO::Null = res else {
                return Response::FormatError;
            };
            Response::Null
        },
        Mutating(LogEntry::Set { .. } | LogEntry::LTrim { .. } | LogEntry::LSet { .. }) => {
            match res {
                QueryIO::BulkString(_) => Response::String("OK".into()),

                _ => Response::FormatError,
            }
        },
        NonMutating(ClusterMeet { .. } | ClusterReshard) => match res {
            QueryIO::Null => Response::String("OK".into()),

            _ => Response::FormatError,
        },
        Mutating(LogEntry::Append { .. }) => match res {
            QueryIO::BulkString(value) => {
                let s = String::from_utf8_lossy(&value);
                Response::String(s.to_string().into())
            },

            _ => Response::FormatError,
        },
        Mutating(LogEntry::LPop { .. } | LogEntry::RPop { .. })
        | NonMutating(Keys { .. } | MGet { .. } | LRange { .. }) => {
            if let QueryIO::Null = res {
                return Response::Null;
            }
            let QueryIO::Array(value) = res else {
                return Response::FormatError;
            };

            let mut keys = Vec::new();
            for (i, item) in value.into_iter().enumerate() {
                let QueryIO::BulkString(value) = item else {
                    return Response::FormatError;
                };
                keys.push(Response::String(
                    format!("{}) \"{}\"", i + 1, String::from_utf8_lossy(&value)).into(),
                ));
            }
            Response::Array(keys)
        },
        NonMutating(Role | ClusterNodes) => match res {
            QueryIO::Array(value) => {
                let mut nodes = Vec::new();
                for item in value {
                    let QueryIO::BulkString(value) = item else {
                        return Response::FormatError;
                    };
                    nodes.push(Response::String(value.into()));
                }
                Response::Array(nodes)
            },
            _ => Response::FormatError,
        },

        ClientAction::Mutating(LogEntry::MSet { .. }) => unimplemented!(),
        ClientAction::Mutating(LogEntry::NoOp) => unreachable!(),
    }
}

pub fn print_res(kind: ClientAction, query_io: ServerResponse) {
    if let ServerResponse::Err { reason: res, .. } = query_io {
        println!("{}", Response::Error(res.into()));
        return;
    }

    if let ServerResponse::ReadRes { res, .. } | ServerResponse::WriteRes { res, .. } = query_io {
        let result = render_return(kind, res);
        println!("{}", result);
    }
}

enum Response {
    Null,
    FormatError,
    String(Bytes),
    Integer(Bytes),
    Error(Bytes),
    Array(Vec<Response>),
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Null => write!(f, "(nil)"),
            Response::FormatError => write!(f, "Unexpected response format"),
            Response::String(value) => {
                write!(f, "{}", String::from_utf8_lossy(value).into_owned())
            },
            Response::Integer(value) => {
                write!(f, "(integer) {}", String::from_utf8_lossy(value).parse::<i64>().unwrap())
            },
            Response::Error(value) => {
                write!(f, "(error) {}", String::from_utf8_lossy(value).into_owned())
            },
            Response::Array(responses) => {
                if responses.is_empty() {
                    return write!(f, "(empty array)");
                }
                let mut iter = responses.iter().peekable();
                while let Some(response) = iter.next() {
                    write!(f, "{response}")?;
                    if iter.peek().is_some() {
                        writeln!(f)?; // Add newline only between items, not at the end
                    }
                }
                Ok(())
            },
        }
    }
}
