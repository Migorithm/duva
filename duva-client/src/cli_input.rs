use std::collections::VecDeque;

use duva::{domains::query_parsers::query_io::QueryIO, prelude::tokio::sync::oneshot};

use crate::command::ClientInputKind;
use ClientInputKind::*;

#[derive(Debug, Default)]
pub struct InputQueue {
    pub queue: VecDeque<Input>,
}
impl InputQueue {
    pub fn push(&mut self, input: Input) {
        self.queue.push_back(input);
    }

    pub fn pop(&mut self) -> Option<Input> {
        self.queue.pop_front()
    }
}

#[derive(Debug)]
pub struct Input {
    pub kind: ClientInputKind,
    pub callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
}

impl Input {
    pub fn new(
        input: ClientInputKind,
        callback: oneshot::Sender<(ClientInputKind, QueryIO)>,
    ) -> Self {
        Self { kind: input, callback }
    }
}

pub fn render_return_per_input(kind: ClientInputKind, query_io: QueryIO) {
    match kind {
        Ping | Get | IndexGet | Echo | Config | Save | Info | ClusterForget | Role | ReplicaOf
        | ClusterInfo => match query_io {
            QueryIO::Null => println!("(nil)"),
            QueryIO::SimpleString(value) => println!("{value}"),
            QueryIO::BulkString(value) => println!("{value}"),
            QueryIO::Err(value) => {
                println!("(error) {value}");
            },
            _ => {
                println!("Unexpected response format");
            },
        },
        Del | Exists => {
            let QueryIO::SimpleString(value) = query_io else {
                println!("Unexpected response format");
                return;
            };
            let deleted_count = value.parse::<u64>().unwrap();
            println!("(integer) {}", deleted_count);
        },
        Set => {
            match query_io {
                QueryIO::SimpleString(_) => {
                    println!("OK");
                    return;
                },
                QueryIO::Err(value) => {
                    println!("(error) {value}");
                    return;
                },
                _ => {
                    println!("Unexpected response format");
                    return;
                },
            };
        },
        Keys => {
            let QueryIO::Array(value) = query_io else {
                println!("Unexpected response format");
                return;
            };
            for (i, item) in value.into_iter().enumerate() {
                let QueryIO::BulkString(value) = item else {
                    println!("Unexpected response format");
                    break;
                };
                println!("{i}) \"{value}\"");
            }
        },
        ClusterNodes => {
            let QueryIO::Array(value) = query_io else {
                println!("Unexpected response format");
                return;
            };
            for item in value {
                let QueryIO::BulkString(value) = item else {
                    println!("Unexpected response format");
                    break;
                };
                println!("{value}");
            }
        },
    }
}
