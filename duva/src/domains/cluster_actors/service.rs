use crate::domains::cluster_actors::ClientMessage;
use crate::domains::cluster_actors::ClusterActor;
use crate::domains::cluster_actors::ClusterCommand;
use crate::domains::cluster_actors::ConnectionMessage;
use crate::domains::cluster_actors::SchedulerMessage;
use crate::domains::peers::PeerMessage;
use crate::domains::replications::TWriteAheadLog;
use crate::prelude::PeerIdentifier;
use crate::res_err;
use tokio::net::TcpStream;
use tracing::warn;
use tracing::{instrument, trace};

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(super) async fn handle(mut self) -> anyhow::Result<Self> {
        while let Some(command) = self.receiver.recv().await {
            trace!(?command, "Cluster command received");
            match command {
                ClusterCommand::Scheduler(msg) => {
                    self.process_scheduler_message(msg).await;
                },
                ClusterCommand::Client(client_message) => {
                    self.process_client_message(client_message).await
                },
                ClusterCommand::Peer(peer_message) => {
                    self.process_peer_message(peer_message.msg, peer_message.from).await;
                },
                ClusterCommand::ConnectionReq(conn_msg) => {
                    self.process_connection_message(conn_msg).await;
                },
            }
            trace!("Cluster command processed");
        }
        Ok(self)
    }

    // ! Beware scheduler is a separate task that asychrnously sends message, therefore leadership change in between may not be observed.
    async fn process_scheduler_message(&mut self, msg: SchedulerMessage) {
        use SchedulerMessage::*;
        match msg {
            SendPeriodicHeatBeat => {
                self.send_cluster_heartbeat().await;
            },
            SendAppendEntriesRPC => {
                self.send_rpc().await;
            },
            StartLeaderElection => {
                warn!(
                    "{} Running for election term {}",
                    self.replication.self_identifier(),
                    self.log_state().term
                );

                self.run_for_election().await;
            },
            RebalanceRequest { request_to, lazy_option } => {
                self.rebalance_request(request_to, lazy_option).await;
            },
            ScheduleMigrationBatch(tasks, callback) => {
                self.migrate_batch(tasks, callback).await;
            },
            TryUnblockWriteReqs => self.unblock_write_on_migration_done(),
            SendBatchAck { batch_id, to } => self.send_batch_ack(batch_id, to).await,
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, client_message))]
    async fn process_client_message(&mut self, client_message: ClientMessage) {
        use ClientMessage::*;

        match client_message {
            CanEnter(callback) => {
                self.register_awaiter_if_pending(callback);
            },
            GetPeers(callback) => {
                callback.send(self.members.keys().cloned().collect::<Vec<_>>());
            },
            ClusterNodes(callback) => {
                callback.send(self.cluster_nodes());
            },
            ReplicationState(callback) => {
                callback.send(self.replication.clone_state());
            },
            Forget(peer_addr, callback) => {
                if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                    callback.send(Some(()));
                } else {
                    callback.send(None);
                }
            },
            LeaderReqConsensus(req) => {
                self.leader_req_consensus(req).await;
            },
            ReplicaOf(peer_addr, callback) => {
                if self.replication.self_identifier() == peer_addr {
                    callback.send(res_err!("invalid operation: cannot replicate to self"));
                    return;
                }

                self.replicaof::<TcpStream>(peer_addr, callback).await;
            },
            ClusterMeet(peer_addr, lazy_option, callback) => {
                self.cluster_meet::<TcpStream>(peer_addr, lazy_option, callback).await;
            },
            ClusterReshard(sender) => {
                self.start_rebalance().await;
                sender.send(Ok(()));
            },

            GetRoles(callback) => {
                callback.send(self.get_sorted_roles());
            },
            SubscribeToTopologyChange(callback) => {
                callback.send(self.node_change_broadcast.subscribe());
            },
            GetTopology(callback) => {
                callback.send(self.get_topology());
            },
            GetLeaderId(callback) => {
                let leader_id = self.get_leader_id();
                callback.send(leader_id);
            },
        };
    }

    async fn process_peer_message(
        &mut self,
        peer_messages: Vec<PeerMessage>,
        from: PeerIdentifier,
    ) {
        use PeerMessage::*;
        for peer_message in peer_messages {
            match peer_message {
                ClusterHeartBeat(heartbeat) => self.receive_cluster_heartbeat(heartbeat).await,
                RequestVote(request_vote) => self.vote_election(request_vote).await,
                AckReplication(repl_res) => self.ack_replication(&from, repl_res).await,
                AppendEntriesRPC(heartbeat) => self.append_entries_rpc(heartbeat).await,
                ElectionVoteReply(request_vote_reply) => {
                    self.receive_election_vote(&from, request_vote_reply).await
                },
                StartRebalance => self.start_rebalance().await,
                ReceiveBatch(migrate_batch) => self.receive_batch(migrate_batch, &from).await,
                MigrationBatchAck(migration_batch_ack) => {
                    self.handle_migration_ack(migration_batch_ack).await
                },
            };
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, conn_msg))]
    async fn process_connection_message(&mut self, conn_msg: ConnectionMessage) {
        use ConnectionMessage::*;

        match conn_msg {
            ConnectToServer { connect_to, callback } => {
                self.connect_to_server::<TcpStream>(connect_to, Some(callback)).await
            },
            AcceptInboundPeer { read, write, host_ip } => {
                self.accept_inbound_stream(read, write, host_ip);
            },
            AddPeer(peer, optional_callback) => self.add_peer(peer, optional_callback).await,
            FollowerSetReplId(replication_id, _leader_id) => self.follower_setup(replication_id),
            ActivateClusterSync(callback) => self.activate_cluster_sync(callback),
            RequestClusterSyncAwaiter(callback) => self.send_cluster_sync_awaiter(callback),
        }
    }
}
