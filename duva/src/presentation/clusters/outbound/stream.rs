use std::sync::atomic::Ordering;

use super::response::ConnectionResponse;
use crate::domains::cluster_actors::commands::AddPeer;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::peers::connected_peer_info::ConnectedPeerInfo;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::PeerState;
use crate::presentation::clusters::connection_manager::ClusterConnectionManager;

use crate::presentation::clusters::listeners::create_peer;
use crate::services::interface::TRead;
use crate::services::interface::TWrite;
use crate::{make_smart_pointer, write_array};
use anyhow::Context;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

// The following is used only when the node is in follower mode
pub(crate) struct OutboundStream {
    pub(crate) stream: TcpStream,
    pub(crate) my_repl_info: ReplicationState,

    connected_node_info: Option<ConnectedPeerInfo>,
    connect_to: PeerIdentifier,
}
make_smart_pointer!(OutboundStream, TcpStream => stream);

impl OutboundStream {
    pub(crate) async fn new(
        connect_to: PeerIdentifier,
        my_repl_info: ReplicationState,
    ) -> anyhow::Result<Self> {
        Ok(OutboundStream {
            stream: TcpStream::connect(&connect_to.cluster_bind_addr()).await?,
            my_repl_info,
            connected_node_info: None,
            connect_to: connect_to.to_string().into(),
        })
    }
    pub async fn establish_connection(mut self, self_port: u16) -> anyhow::Result<Self> {
        // Trigger
        self.write(write_array!("PING")).await?;
        let mut ok_count = 0;
        let mut connection_info = ConnectedPeerInfo {
            id: Default::default(),
            replid: Default::default(),
            hwm: Default::default(),
            peer_list: Default::default(),
        };

        loop {
            let res = self.read_values().await?;
            for query in res {
                match ConnectionResponse::try_from(query)? {
                    ConnectionResponse::PONG => {
                        let msg = write_array!("REPLCONF", "listening-port", self_port.to_string());
                        self.write(msg).await?
                    },
                    ConnectionResponse::OK => {
                        ok_count += 1;
                        let msg = {
                            match ok_count {
                                1 => Ok(write_array!("REPLCONF", "capa", "psync2")),
                                // "?" here means the server is undecided about their leader. and -1 is the offset that follower is aware of
                                2 => Ok(write_array!(
                                    "PSYNC",
                                    self.my_repl_info.replid.clone(),
                                    self.my_repl_info.hwm.load(Ordering::Acquire).to_string()
                                )),
                                _ => Err(anyhow::anyhow!("Unexpected OK count")),
                            }
                        }?;
                        self.write(msg).await?
                    },
                    ConnectionResponse::FULLRESYNC { id, repl_id, offset } => {
                        connection_info.replid = ReplicationId::Key(repl_id.into());
                        connection_info.hwm = offset;
                        connection_info.id = id.into();
                        println!("[INFO] Three-way handshake completed")
                    },
                    ConnectionResponse::PEERS(peer_list) => {
                        println!("[INFO] Received peer list: {:?}", peer_list);
                        connection_info.peer_list = peer_list;
                        self.connected_node_info = Some(connection_info);
                        return Ok(self);
                    },
                }
            }
        }
    }

    pub(crate) async fn set_replication_info(
        self,
        cluster_manager: &ClusterConnectionManager,
    ) -> anyhow::Result<Self> {
        if self.my_repl_info.replid == ReplicationId::Undecided {
            let connected_node_info = self
                .connected_node_info
                .as_ref()
                .context("Connected node info not found. Cannot set replication id")?;

            cluster_manager
                .send(ClusterCommand::SetReplicationInfo {
                    replid: connected_node_info.replid.clone(),
                    hwm: 0,
                })
                .await?;
        }
        Ok(self)
    }
    pub(crate) fn create_peer_cmd(
        self,
        cluster_actor_handler: Sender<ClusterCommand>,
    ) -> anyhow::Result<(ClusterCommand, Vec<PeerIdentifier>)> {
        let mut connection_info =
            self.connected_node_info.context("Connected node info not found")?;
        let peer_list = connection_info.list_peer_binding_addrs();

        let peer = create_peer(
            (*self.connect_to).clone(),
            self.stream,
            PeerState::decide_peer_kind(&self.my_repl_info.replid, &connection_info),
            cluster_actor_handler,
        );

        Ok((ClusterCommand::AddPeer(AddPeer { peer_id: self.connect_to, peer }), peer_list))
    }
}
