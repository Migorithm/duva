use super::response::ConnectionResponse;
use crate::domains::QueryIO;
use crate::domains::cluster_actors::ConnectionMessage;
use crate::domains::cluster_actors::actor::ClusterCommandHandler;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::interface::TRead;
use crate::domains::interface::TWrite;
use crate::domains::peers::connections::connection_types::ConnectedPeerInfo;
use crate::domains::peers::connections::connection_types::WriteConnected;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::identifier::TPeerAddress;
use crate::domains::peers::peer::Peer;
use crate::domains::peers::service::PeerListener;
use crate::write_array;
use anyhow::Context;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::trace;

// The following is used only when the node is in follower mode
pub(crate) struct OutboundStream {
    r: OwnedReadHalf,
    w: OwnedWriteHalf,
    my_repl_info: ReplicationState,
    connected_node_info: Option<ConnectedPeerInfo>,
}

impl OutboundStream {
    pub(crate) async fn new(
        connect_to: PeerIdentifier,
        my_repl_info: ReplicationState,
    ) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(&connect_to.cluster_bind_addr()).await?;
        let (read, write) = stream.into_split();
        Ok(OutboundStream { r: read, w: write, my_repl_info, connected_node_info: None })
    }
    async fn make_handshake(&mut self, self_port: u16) -> anyhow::Result<()> {
        self.w.write(write_array!("PING")).await?;
        let mut ok_count = 0;
        let mut connection_info = ConnectedPeerInfo {
            id: Default::default(),
            replid: Default::default(),
            hwm: Default::default(),
        };

        loop {
            let res = self.r.read_values().await?;
            trace!(?res, "Received handshake response");
            for query in res {
                match ConnectionResponse::try_from(query)? {
                    | ConnectionResponse::Pong => {
                        let msg = write_array!("REPLCONF", "listening-port", self_port.to_string());
                        self.w.write(msg).await?
                    },
                    | ConnectionResponse::Ok => {
                        ok_count += 1;
                        let msg = {
                            match ok_count {
                                | 1 => Ok(write_array!("REPLCONF", "capa", "psync2")),
                                // "?" here means the server is undecided about their leader. and -1 is the offset that follower is aware of
                                | 2 => Ok(write_array!(
                                    "PSYNC",
                                    self.my_repl_info.replid.clone(),
                                    self.my_repl_info.hwm.load(Ordering::Acquire).to_string()
                                )),
                                | _ => Err(anyhow::anyhow!("Unexpected OK count")),
                            }
                        }?;
                        self.w.write(msg).await?
                    },
                    | ConnectionResponse::FullResync { id, repl_id, offset } => {
                        connection_info.replid = ReplicationId::Key(repl_id);
                        connection_info.hwm = offset;
                        connection_info.id = id.into();
                        self.connected_node_info = Some(connection_info);
                        self.reply_with_ok().await?;
                        return Ok(());
                    },
                }
            }
        }
    }

    async fn reply_with_ok(&mut self) -> anyhow::Result<()> {
        self.w.write(QueryIO::SimpleString("OK".to_string())).await?;
        Ok(())
    }

    pub(crate) async fn add_peer(
        mut self,
        self_port: u16,
        cluster_handler: ClusterCommandHandler,
        optional_callback: Option<tokio::sync::oneshot::Sender<anyhow::Result<()>>>,
    ) -> anyhow::Result<()> {
        self.make_handshake(self_port).await?;
        let connection_info =
            self.connected_node_info.take().context("Connected node info not found")?;

        if self.my_repl_info.replid == ReplicationId::Undecided {
            let _ = cluster_handler
                .send(ConnectionMessage::FollowerSetReplId(connection_info.replid.clone()))
                .await;
        }
        let peer_state = connection_info.decide_peer_kind(&self.my_repl_info.replid);

        let kill_switch =
            PeerListener::spawn(self.r, cluster_handler.clone(), peer_state.addr.clone());
        let peer = Peer::new(WriteConnected(Box::new(self.w)), peer_state, kill_switch);

        let _ = cluster_handler.send(ConnectionMessage::AddPeer(peer, optional_callback)).await;
        Ok(())
    }
}
