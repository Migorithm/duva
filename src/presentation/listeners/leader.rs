use crate::{
    SnapshotLoader,
    domains::{
        cluster_actors::{commands::ClusterCommand, replication::HeartBeatMessage},
        cluster_listeners::{ClusterListener, ReactorKillSwitch, TListen},
        peers::connected_types::Leader,
    },
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::timeout;

use super::*;

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

impl ClusterListener<Leader> {
    async fn listen_leader(&mut self) {
        loop {
            select! {
                result = self.read_command::<LeaderInput>() => {
                    match result {
                        Ok(cmds) => {
                            println!("[INFO] leader listener received {} commands", cmds.len());
                            for cmd in cmds {
                                match cmd {
                                    LeaderInput::HeartBeat(state) => {
                                        self.handle_leader_heartbeat(state).await;
                                    },
                                    LeaderInput::FullSync(data) => {
                                        let Ok(snapshot) = SnapshotLoader::load_from_bytes(&data) else {
                                            println!("[ERROR] Failed to load snapshot from leader");
                                            continue;
                                        };
                                        let Ok(_) = self.snapshot_applier.apply_snapshot(snapshot).await else {
                                            println!("[ERROR] Failed to apply snapshot from leader");
                                            continue;
                                        };
                                    }
                                }
                            }
                        },
                        Err(e)=> {
                            println!("Error reading command: {:?}", e);
                        }
                    }
                },
                _ =  tokio::time::sleep(Duration::from_millis(rand::random_range(700..1000))) =>{
                    println!("[INFO] leader listener timeout");
                    break;
                }

            };
        }
    }
    pub(crate) async fn handle_leader_heartbeat(&mut self, state: HeartBeatMessage) {
        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
        let _ = self.cluster_handler.send(ClusterCommand::HandleLeaderHeartBeat(state)).await;
    }
}

#[derive(Debug)]
pub enum LeaderInput {
    HeartBeat(HeartBeatMessage),
    FullSync(Bytes),
}

impl TryFrom<QueryIO> for LeaderInput {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(data) => Ok(Self::FullSync(data)),
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            // TODO term info should be included?
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domains::{
            caches::cache_manager::CacheManager,
            peers::{connected_types::ReadConnected, identifier::PeerIdentifier},
            saves::snapshot::snapshot_applier::SnapshotApplier,
        },
        services::interface::TWrite,
    };
    use std::{sync::atomic::AtomicI16, time::SystemTime};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn leader_listener_should_break_loop_when_timeout() {
        //GIVEN
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (r, mut x) = stream.into_split();
        let (cluster_tx, _) = tokio::sync::mpsc::channel(1);
        let mut listener = ClusterListener {
            read_connected: ReadConnected::<Leader>::new(r),
            cluster_handler: cluster_tx,
            self_id: PeerIdentifier::new("localhost", 1),
            snapshot_applier: SnapshotApplier::new(
                CacheManager { inboxes: vec![] },
                SystemTime::now(),
            ),
        };
        //WHEN
        static ATOMIC: AtomicI16 = AtomicI16::new(0);

        // - run listener
        let task = tokio::spawn(async move {
            listener.listen_leader().await;
        });

        // - simulate heartbeat
        tokio::spawn(async move {
            let msg = QueryIO::HeartBeat(HeartBeatMessage::default());
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                // heartbeat

                x.write_io(msg.clone()).await.unwrap();
                ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        task.await.unwrap();

        //THEN
        assert_eq!(5, ATOMIC.load(std::sync::atomic::Ordering::Relaxed))
    }
}
