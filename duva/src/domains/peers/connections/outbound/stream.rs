use super::response::ConnectionResponse;
use crate::domains::QueryIO;
use crate::domains::cluster_actors::ConnectionMessage;
use crate::domains::cluster_actors::queue::ClusterActorSender;

use crate::domains::peers::connections::connection_types::ReadConnected;
use crate::domains::peers::connections::connection_types::WriteConnected;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;
use crate::domains::peers::service::PeerListener;
use crate::domains::replications::*;
use crate::types::BinBytes;
use crate::types::Callback;
use crate::write_array;
use anyhow::Context;
use bytes::Bytes;

use tracing::trace;

// The following is used only when the node is in follower mode
pub(crate) struct OutboundStream {
    pub(crate) r: ReadConnected,
    pub(crate) w: WriteConnected,
    pub(crate) self_state: ReplicationState,
    pub(crate) peer_state: Option<ReplicationState>,
}

impl OutboundStream {
    async fn make_handshake(&mut self, self_port: u16) -> anyhow::Result<()> {
        self.w.write(write_array!("PING")).await?;
        let mut ok_count = 0;
        let mut peer_state = ReplicationState::default();

        loop {
            let res = self.r.read_values().await?;
            trace!(?res, "Received handshake response");
            for query in res {
                match ConnectionResponse::try_from(query)? {
                    ConnectionResponse::Pong => {
                        let msg = write_array!("REPLCONF", "listening-port", self_port.to_string());
                        self.w.write(msg).await?
                    },
                    ConnectionResponse::Ok => {
                        ok_count += 1;
                        let msg = {
                            match ok_count {
                                1 => Ok(write_array!("REPLCONF", "capa", "psync2")),
                                // "?" here means the server is undecided about their leader. and -1 is the offset that follower is aware of
                                2 => Ok(write_array!(
                                    "PSYNC",
                                    self.self_state.replid.clone(),
                                    self.self_state.last_log_index.to_string(),
                                    self.self_state.role.clone()
                                )),
                                _ => Err(anyhow::anyhow!("Unexpected OK count")),
                            }
                        }?;
                        self.w.write(msg).await?
                    },
                    ConnectionResponse::FullResync { id, repl_id, offset, role } => {
                        peer_state.replid = ReplicationId::Key(repl_id);
                        peer_state.last_log_index = offset;
                        peer_state.node_id = PeerIdentifier(id);
                        peer_state.role = role;
                        self.peer_state = Some(peer_state);

                        self.reply_with_ok().await?;
                        return Ok(());
                    },
                }
            }
        }
    }

    async fn reply_with_ok(&mut self) -> anyhow::Result<()> {
        self.w.write(QueryIO::SimpleString(BinBytes::new("ok"))).await?;
        Ok(())
    }

    pub(crate) async fn add_peer(
        mut self,
        self_port: u16,
        cluster_handler: ClusterActorSender,
        optional_callback: Option<Callback<anyhow::Result<()>>>,
    ) -> anyhow::Result<()> {
        self.make_handshake(self_port).await?;
        let connection_info = self.peer_state.take().context("Connected node info not found")?;

        if self.self_state.replid == ReplicationId::Undecided {
            cluster_handler
                .send(ConnectionMessage::FollowerSetReplId(
                    connection_info.replid.clone(),
                    connection_info.node_id.clone(),
                ))
                .await?;
        }
        let peer_state = connection_info.decide_peer_state(&self.self_state.replid);

        let kill_switch =
            PeerListener::spawn(self.r, cluster_handler.clone(), peer_state.id().clone());
        let peer = Peer::new(self.w, peer_state, kill_switch);

        cluster_handler.send(ConnectionMessage::AddPeer(peer, optional_callback)).await?;
        Ok(())
    }
}
