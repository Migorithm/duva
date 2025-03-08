use super::*;
use crate::domains::append_only_files::WriteOperation;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::HeartBeatMessage;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::ReactorKillSwitch;
use crate::domains::cluster_listeners::TListen;
use crate::domains::peers::connected_types::Leader;
use crate::domains::query_parsers::deserialize;
use crate::domains::query_parsers::query_io::parse_replicate;
use std::time::Duration;

impl TListen for ClusterListener<Leader> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_leader() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

impl ClusterListener<Leader> {
    async fn listen_leader(&mut self) {
        loop {
            select! {
                result = self.read_command::<LeaderInput>() => {
                    match result {
                        Ok(cmds) => {
                            self.handle_leader_message(cmds).await;
                        },
                        Err(e)=> {
                            // Most likely connection close case
                            println!("Error reading command: {:?}", e);
                            break;
                        }
                    }
                },

                // ELECTION timeout
                _ =  tokio::time::sleep(Duration::from_millis(rand::random_range(700..1000))) =>{
                    println!("[INFO] leader listener timeout");
                    break;
                }

            };
        }
    }

    async fn handle_leader_message(&mut self, cmds: Vec<LeaderInput>) {
        #[cfg(test)]
        ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        for cmd in cmds {
            match cmd {
                LeaderInput::HeartBeat(state) => {
                    println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
                    let _ = self
                        .cluster_handler
                        .send(ClusterCommand::HandleLeaderHeartBeat(state))
                        .await;
                },
                LeaderInput::FullSync(logs) => {
                    println!("Received full sync logs: {:?}", logs);
                    let _ =
                        self.cluster_handler.send(ClusterCommand::InstallLeaderState(logs)).await;
                },
            }
        }
    }
}

#[derive(Debug)]
pub enum LeaderInput {
    HeartBeat(HeartBeatMessage),
    FullSync(Vec<WriteOperation>),
}

impl TryFrom<QueryIO> for LeaderInput {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(data) => {
                let data = data.into();
                let Ok((QueryIO::Array(array), _)) = deserialize(data) else {
                    return Err(anyhow::anyhow!("Invalid data"));
                };
                let mut ops = Vec::new();
                for str in array {
                    let QueryIO::BulkString(ops_data) = str else {
                        return Err(anyhow::anyhow!("Invalid data"));
                    };
                    let Ok((QueryIO::ReplicateLog(log), _)) = parse_replicate(ops_data.into())
                    else {
                        return Err(anyhow::anyhow!("Invalid data"));
                    };
                    ops.push(log);
                }
                Ok(Self::FullSync(ops))
            },
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domains::peers::connected_types::ReadConnected, services::interface::TWrite};
    use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};

    async fn create_server_listener_client_writer() -> (OwnedReadHalf, OwnedWriteHalf) {
        // Create listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Connect client to listener
        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (_, client_write) = client_stream.into_split();

        // Accept the connection on the server side
        let (server_stream, _) = listener.accept().await.unwrap();
        let (server_read, _) = server_stream.into_split();

        (server_read, client_write)
    }

    #[tokio::test]
    async fn leader_listener_should_break_loop_when_timeout() {
        //GIVEN
        let (server_read, mut client_write) = create_server_listener_client_writer().await;

        let (cluster_tx, _) = tokio::sync::mpsc::channel(1);
        let mut listener = ClusterListener {
            read_connected: ReadConnected::<Leader>::new(server_read),
            cluster_handler: cluster_tx,
        };

        // - run listener
        let task = tokio::spawn(async move {
            listener.listen_leader().await;
        });

        // - simulate heartbeat
        let sending_task = tokio::spawn(async move {
            let msg = QueryIO::HeartBeat(HeartBeatMessage::default());
            for i in 0..20 {
                client_write.write(msg.clone().serialize()).await.unwrap();

                tokio::time::sleep(Duration::from_millis(100 * i)).await;
            }
        });
        task.await.unwrap();
        sending_task.abort();

        //THEN

        assert!(ATOMIC.load(std::sync::atomic::Ordering::Relaxed) > 7);
    }
}
