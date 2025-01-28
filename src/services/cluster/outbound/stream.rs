use std::ops::Add;

use crate::services::cluster::actors::command::{AddPeer, ClusterCommand};
use crate::services::cluster::actors::replication::Replication;
use crate::services::cluster::actors::types::{PeerAddr, PeerKind};
use crate::services::cluster::manager::ClusterManager;
use crate::services::cluster::outbound::response::ConnectionResponse;
use crate::services::interface::{TRead, TStream};

use crate::{make_smart_pointer, write_array};
use anyhow::Context;
use tokio::net::TcpStream;

// The following is used only when the node is in slave mode
pub(crate) struct OutboundStream {
    pub(crate) stream: TcpStream,
    pub(crate) repl_info: Replication,
    connected_node_info: Option<ConnectedNodeInfo>,
    connect_to: PeerAddr,
}
make_smart_pointer!(OutboundStream, TcpStream => stream);

impl OutboundStream {
    pub(crate) async fn new(connect_to: PeerAddr, repl_info: Replication) -> anyhow::Result<Self> {
        Ok(OutboundStream {
            stream: TcpStream::connect(&connect_to.0).await?,
            repl_info,
            connected_node_info: None,
            connect_to,
        })
    }
    pub async fn establish_connection(mut self, self_port: u16) -> anyhow::Result<Self> {
        // Trigger
        self.write(write_array!("PING")).await?;
        let mut ok_count = 0;
        let mut connection_info = ConnectedNodeInfo::default();

        loop {
            let res = self.read_values().await?;
            for query in res {
                match ConnectionResponse::try_from(query)? {
                    ConnectionResponse::PONG => {
                        let msg = write_array!("REPLCONF", "listening-port", self_port.to_string());

                        self.write(msg).await?
                    }
                    ConnectionResponse::OK => {
                        ok_count += 1;
                        let msg = {
                            match ok_count {
                                1 => Ok(write_array!("REPLCONF", "capa", "psync2")),
                                // "?" here means the server is undecided about their master. and -1 is the offset that slave is aware of
                                2 => Ok(write_array!("PSYNC", "?", "-1")),
                                _ => Err(anyhow::anyhow!("Unexpected OK count")),
                            }
                        }?;
                        self.write(msg).await?
                    }
                    ConnectionResponse::FULLRESYNC { repl_id, offset } => {
                        connection_info.repl_id = repl_id;
                        connection_info.offset = offset;
                        println!("[INFO] Three-way handshake completed")
                    }
                    ConnectionResponse::PEERS(peer_list) => {
                        println!("[INFO] Received peer list: {:?}", peer_list);
                        connection_info.peer_list = peer_list;
                        self.connected_node_info = Some(connection_info);
                        return Ok(self);
                    }
                }
            }
        }
    }

    pub(crate) async fn set_replication_info(
        self,
        cluster_manager: &ClusterManager,
    ) -> anyhow::Result<Self> {
        if self.repl_info.master_replid == "?" {
            let connected_node_info = self
                .connected_node_info
                .as_ref()
                .context("Connected node info not found. Cannot set replication id")?;

            cluster_manager
                .send(ClusterCommand::SetReplicationInfo {
                    master_repl_id: connected_node_info.repl_id.clone(),
                    // TODO offset setting here may want to be revisited once we implement synchronization - echo
                    offset: connected_node_info.offset,
                })
                .await?;
        }
        Ok(self)
    }
    pub(crate) fn deconstruct(self) -> anyhow::Result<(ClusterCommand, ConnectedNodeInfo)> {
        let connection_info = self.connected_node_info.context("Connected node info not found")?;

        Ok((
            ClusterCommand::AddPeer(AddPeer {
                peer_kind: PeerKind::connected_peer_kind(&self.repl_info, &connection_info.repl_id),
                peer_addr: self.connect_to,
                stream: self.stream,
            }),
            connection_info,
        ))
    }
}

#[derive(Debug, Default)]
pub(crate) struct ConnectedNodeInfo {
    // TODO repl_id here is the master_replid from connected server.
    // TODO Set repl_id if given server's repl_id is "?" otherwise, it means that now it's connected to peer.
    pub(crate) repl_id: String,
    pub(crate) offset: u64,
    pub(crate) peer_list: Vec<String>,
}

impl ConnectedNodeInfo {
    pub(crate) fn list_peer_binding_addrs(&self) -> Vec<PeerAddr> {
        self.peer_list
            .iter()
            .flat_map(|peer| {
                if let Some((ip, port)) = peer.rsplit_once(':') {
                    Some(
                        (ip.to_string()
                            + ":"
                            + (port.parse::<u16>().unwrap() + 10000).to_string().as_str())
                        .into(),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
