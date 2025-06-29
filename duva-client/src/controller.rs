use std::fmt::Display;

use crate::broker::Broker;
use crate::broker::BrokerMessage;

use duva::domains::query_io::QueryIO;
use duva::prelude::anyhow;
use duva::prelude::bytes::Bytes;
use duva::prelude::tokio;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;
use duva::presentation::clients::request::ClientAction;

pub struct ClientController<T> {
    pub broker_tx: Sender<BrokerMessage>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> anyhow::Result<Self> {
        let (r, w, auth_response) = Broker::authenticate(server_addr, None).await?;

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let broker = Broker {
            tx: broker_tx.clone(),
            rx,
            to_server: w.run(),
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            request_id: auth_response.request_id,

            topology: auth_response.topology,
            read_kill_switch: Some(r.run(broker_tx.clone())),
        };
        tokio::spawn(broker.run());
        Ok(Self { broker_tx, target: editor })
    }

    fn render_return(&self, kind: ClientAction, query_io: QueryIO) -> Response {
        use ClientAction::*;
        match kind {
            | Ping
            | Get { .. }
            | IndexGet { .. }
            | Echo { .. }
            | Config { .. }
            | Info
            | ClusterForget { .. }
            | Role
            | ReplicaOf { .. }
            | ClusterInfo => match query_io {
                | QueryIO::Null => Response::Null,
                | QueryIO::SimpleString(value) => Response::String(value),
                | QueryIO::BulkString(value) => Response::String(value),
                | QueryIO::Err(value) => Response::Error(value),
                | _err => Response::FormatError,
            },
            | Delete { .. } | Exists { .. } => {
                let QueryIO::SimpleString(value) = query_io else {
                    return Response::FormatError;
                };

                match str::from_utf8(&value) {
                    | Ok(int) => Response::Integer(int.to_string().into()),
                    | Err(_) => {
                        Response::Error("ERR value is not an integer or out of range".into())
                    },
                }
            },
            | Incr { .. } | Decr { .. } | Ttl { .. } | IncrBy { .. } | DecrBy { .. } => {
                match query_io {
                    | QueryIO::SimpleString(value) => {
                        let s = String::from_utf8_lossy(&value);
                        let s: Option<&str> =
                            s.split('|').next().unwrap_or_default().rsplit(':').next();
                        Response::Integer(s.unwrap().to_string().into())
                    },
                    | QueryIO::Err(value) => Response::Error(value),
                    | QueryIO::BulkString(value) => Response::Integer(value),
                    | _ => Response::FormatError,
                }
            },
            | Save => {
                let QueryIO::Null = query_io else {
                    return Response::FormatError;
                };
                Response::Null
            },
            | Set { .. } | SetWithExpiry { .. } => match query_io {
                | QueryIO::SimpleString(_) => Response::String("OK".into()),
                | QueryIO::Err(value) => Response::Error(value),
                | _ => Response::FormatError,
            },
            | ClusterMeet { .. } | ClusterReshard => match query_io {
                | QueryIO::Null => Response::String("OK".into()),
                | QueryIO::Err(value) => Response::Error(value),
                | _ => Response::FormatError,
            },
            | Append { .. } => match query_io {
                | QueryIO::SimpleString(value) => Response::String(value),
                | QueryIO::Err(value) => Response::Error(value),
                | _ => Response::FormatError,
            },
            | Keys { .. } | MGet { .. } => {
                let QueryIO::Array(value) = query_io else {
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
            | ClusterNodes => {
                let QueryIO::Array(value) = query_io else {
                    return Response::FormatError;
                };
                let mut nodes = Vec::new();
                for item in value {
                    let QueryIO::BulkString(value) = item else {
                        return Response::FormatError;
                    };
                    nodes.push(Response::String(value));
                }
                Response::Array(nodes)
            },
        }
    }

    pub fn print_res(&self, kind: ClientAction, query_io: QueryIO) {
        println!("{}", self.render_return(kind, query_io));
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
            | Response::Null => write!(f, "(nil)"),
            | Response::FormatError => write!(f, "Unexpected response format"),
            | Response::String(value) => {
                write!(f, "{}", String::from_utf8_lossy(value).into_owned())
            },
            | Response::Integer(value) => {
                write!(f, "(integer) {}", String::from_utf8_lossy(&value).parse::<i64>().unwrap())
            },
            | Response::Error(value) => {
                write!(f, "(error) {}", String::from_utf8_lossy(value).into_owned())
            },
            | Response::Array(responses) => {
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
