use crate::broker::Broker;
use crate::broker::BrokerMessage;
use crate::command::ClientInputKind;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::prelude::tokio;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;

pub struct ClientController<T> {
    pub broker_tx: Sender<BrokerMessage>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> Self {
        let (r, w, mut auth_response) = Broker::authenticate(server_addr, None).await.unwrap();

        auth_response.cluster_nodes.push(server_addr.to_string().into());
        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let broker = Broker {
            tx: broker_tx.clone(),
            rx,
            to_server: w.run(),
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            request_id: auth_response.request_id,
            latest_known_index: 0,
            cluster_nodes: auth_response.cluster_nodes,
            read_kill_switch: Some(r.run(broker_tx.clone())),
        };
        tokio::spawn(broker.run());
        Self { broker_tx, target: editor }
    }

    #[cfg_attr(not(feature = "cli"), allow(unused))]
    pub fn print_res(&self, kind: ClientInputKind, query_io: QueryIO) {
        use ClientInputKind::*;
        match kind {
            Ping | Get | Echo | Config | Save | Info | ClusterForget | Role | ReplicaOf
            | ClusterInfo => match query_io {
                QueryIO::Null => println!("(nil)"),
                QueryIO::SimpleString(value) => println!("{value}"),
                QueryIO::BulkString(value) => println!("{value}"),
                QueryIO::Err(value) => {
                    println!("(error) {value}");
                },
                _err => {
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
}
