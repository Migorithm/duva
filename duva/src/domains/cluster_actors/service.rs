use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::ClientMessage;
use crate::domains::cluster_actors::ClusterActor;
use crate::domains::cluster_actors::ClusterCommand;
use crate::domains::cluster_actors::ConnectionMessage;
use crate::domains::cluster_actors::SchedulerMessage;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::peers::PeerMessage;

use crate::prelude::PeerIdentifier;
use crate::res_err;
use tokio::net::TcpStream;
use tracing::{instrument, trace};

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(super) async fn handle(mut self, cache_manager: CacheManager) -> anyhow::Result<Self> {
        while let Some(command) = self.receiver.recv().await {
            trace!(?command, "Cluster command received");
            match command {
                | ClusterCommand::Scheduler(msg) => {
                    self.process_scheduler_message(msg, &cache_manager).await;
                },
                | ClusterCommand::Client(client_message) => {
                    self.process_client_message(&cache_manager, client_message).await
                },
                | ClusterCommand::Peer(peer_message) => {
                    self.process_peer_message(&cache_manager, peer_message.msg, peer_message.from)
                        .await;
                },
                | ClusterCommand::ConnectionReq(conn_msg) => {
                    self.process_connection_message(conn_msg).await;
                },
            }
            trace!("Cluster command processed");
        }
        Ok(self)
    }

    async fn process_scheduler_message(
        &mut self,
        msg: SchedulerMessage,
        cache_manager: &CacheManager,
    ) {
        use SchedulerMessage::*;
        match msg {
            | SendPeriodicHeatBeat => {
                self.send_cluster_heartbeat().await;
            },
            | SendAppendEntriesRPC => {
                self.send_rpc().await;
            },
            | StartLeaderElection => {
                self.run_for_election().await;
            },
            | RebalanceRequest { request_to, lazy_option } => {
                self.rebalance_request(request_to, lazy_option).await;
            },
            | ScheduleMigrationBatch(tasks, callback) => {
                self.migrate_batch(tasks, cache_manager, callback).await;
            },
            | TryUnblockWriteReqs => self.unblock_write_reqs_if_done(),
            | SendBatchAck { batch_id, to } => self.send_batch_ack(batch_id, to).await,
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, cache_manager,client_message))]
    async fn process_client_message(
        &mut self,
        cache_manager: &CacheManager,
        client_message: ClientMessage,
    ) {
        use ClientMessage::*;

        match client_message {
            | GetPeers(callback) => {
                callback.send(self.members.keys().cloned().collect::<Vec<_>>());
            },
            | ClusterNodes(callback) => {
                callback.send(self.cluster_nodes());
            },
            | ReplicationInfo(callback) => {
                callback.send(self.replication.clone());
            },
            | ForgetPeer(peer_addr, callback) => {
                if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                    callback.send(Some(()));
                } else {
                    callback.send(None);
                }
            },
            | LeaderReqConsensus(req) => {
                self.leader_req_consensus(req).await;
            },
            | ReplicaOf(peer_addr, callback) => {
                if self.replication.self_identifier() == peer_addr {
                    callback.send(res_err!("invalid operation: cannot replicate to self"));
                    return;
                }
                cache_manager.drop_cache().await;
                self.replicaof::<TcpStream>(peer_addr, callback).await;
            },
            | ClusterMeet(peer_addr, lazy_option, callback) => {
                self.cluster_meet::<TcpStream>(peer_addr, lazy_option, callback).await;
            },
            | ClusterReshard(sender) => {
                self.start_rebalance(cache_manager).await;
                sender.send(Ok(()));
            },
            | GetRole(callback) => {
                callback.send(self.replication.role.clone());
            },
            | SubscribeToTopologyChange(callback) => {
                callback.send(self.node_change_broadcast.subscribe());
            },
            | GetTopology(callback) => {
                callback.send(self.get_topology());
            },
        };
    }

    async fn process_peer_message(
        &mut self,
        cache_manager: &CacheManager,
        peer_message: PeerMessage,
        from: PeerIdentifier,
    ) {
        use PeerMessage::*;

        match peer_message {
            | ClusterHeartBeat(heartbeat) => {
                self.receive_cluster_heartbeat(heartbeat, cache_manager).await
            },
            | RequestVote(request_vote) => self.vote_election(request_vote).await,
            | AckReplication(repl_res) => self.ack_replication(from, repl_res).await,
            | AppendEntriesRPC(heartbeat) => {
                self.append_entries_rpc(cache_manager, heartbeat).await
            },
            | ElectionVoteReply(request_vote_reply) => {
                self.receive_election_vote(request_vote_reply).await
            },
            | StartRebalance => self.start_rebalance(cache_manager).await,
            | ReceiveBatch(migrate_batch) => {
                self.receive_batch(migrate_batch, cache_manager, from).await
            },
            | MigrationBatchAck(migration_batch_ack) => {
                self.handle_migration_ack(migration_batch_ack, cache_manager).await
            },
        };
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, conn_msg))]
    async fn process_connection_message(&mut self, conn_msg: ConnectionMessage) {
        use ConnectionMessage::*;

        match conn_msg {
            | ConnectToServer { connect_to, callback } => {
                self.connect_to_server::<TcpStream>(connect_to, Some(callback)).await
            },
            | AcceptInboundPeer { stream } => self.accept_inbound_stream(stream),
            | AddPeer(peer, optional_callback) => self.add_peer(peer, optional_callback).await,
            | FollowerSetReplId(replication_id, _leader_id) => self.follower_setup(replication_id),
            | ActivateClusterSync(callback) => self.activate_cluster_sync(callback),
            | RequestClusterSyncAwaiter(callback) => self.send_cluster_sync_awaiter(callback),
        }
    }
}
