use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::cluster_actors::{
    ClientMessage, ClusterActor, ConnectionMessage, FANOUT, SchedulerMessage,
};
use crate::domains::cluster_actors::{ClusterCommand, ConsensusClientResponse};
use crate::domains::operation_logs::interfaces::TWriteAheadLog;

use crate::domains::peers::PeerMessage;
use crate::err;
use tracing::{debug, trace};

use super::actor::ClusterCommandHandler;

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(super) async fn handle(mut self, cache_manager: CacheManager) -> anyhow::Result<Self> {
        while let Some(command) = self.receiver.recv().await {
            trace!(?command, "Cluster command received");
            match command {
                | ClusterCommand::Scheduler(msg) => {
                    self.process_scheduler_message(msg).await;
                },
                | ClusterCommand::Client(client_message) => {
                    self.process_client_message(&cache_manager, client_message).await
                },
                | ClusterCommand::Peer(peer_message) => {
                    self.process_peer_message(&cache_manager, peer_message).await;
                },
                | ClusterCommand::ConnectionReq(conn_msg) => {
                    self.process_connection_message(conn_msg).await;
                },
            }
            trace!("Cluster command processed");
        }
        Ok(self)
    }

    async fn process_scheduler_message(&mut self, msg: SchedulerMessage) {
        use SchedulerMessage::*;
        match msg {
            | SendClusterHeatBeat => {
                // ! remove idle peers based on ttl.
                // ! The following may need to be moved else where to avoid blocking the main loop
                self.remove_idle_peers().await;
                let hop_count = Self::hop_count(FANOUT, self.members.len());
                self.send_cluster_heartbeat(hop_count).await;
            },
            | SendAppendEntriesRPC => {
                self.send_rpc().await;
            },
            | StartLeaderElection => {
                self.run_for_election().await;
            },
        }
    }
    async fn process_client_message(
        &mut self,
        cache_manager: &CacheManager,
        client_message: ClientMessage,
    ) {
        use ClientMessage::*;

        match client_message {
            | GetPeers(callback) => {
                let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>());
            },
            | ClusterNodes(callback) => {
                let _ = callback.send(self.cluster_nodes());
            },
            | ReplicationInfo(sender) => {
                let _ = sender.send(self.replication.clone());
            },
            | ForgetPeer(peer_addr, sender) => {
                if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                    let _ = sender.send(Some(()));
                } else {
                    let _ = sender.send(None);
                }
            },
            | LeaderReqConsensus(req) => {
                if !self.replication.is_leader_mode {
                    let _ = req.callback.send(err!("Write given to follower"));
                    return;
                }

                if self.client_sessions.is_processed(&req.session_req) {
                    // TODO mapping between early returned values to client result
                    let _ = req.callback.send(Ok(ConsensusClientResponse::AlreadyProcessed {
                        key: req.request.key(),
                        index: self.logger.last_log_index,
                    }));
                    return;
                };
                self.req_consensus(req).await;
            },

            | ReplicaOf(peer_addr, callback) => {
                if self.replication.self_identifier() == peer_addr {
                    let _ = callback.send(err!("invalid operation: cannot replicate to self"));
                    return;
                }

                cache_manager.drop_cache().await;
                self.replicaof(peer_addr, callback).await;
            },
            | ClusterMeet(peer_addr, lazy_option, callback) => {
                if !self.replication.is_leader_mode
                    || self.replication.self_identifier() == peer_addr
                {
                    let _ = callback
                        .send(err!("wrong address or invalid state for cluster meet command"));
                    return;
                }
                self.cluster_meet(peer_addr, lazy_option, callback).await;
            },
            | GetRole(sender) => {
                let _ = sender.send(self.replication.role.clone());
            },
            | SubscribeToTopologyChange(sender) => {
                let _ = sender.send(self.node_change_broadcast.subscribe());
            },
        }
    }

    async fn process_peer_message(
        &mut self,
        cache_manager: &CacheManager,
        peer_message: PeerMessage,
    ) {
        use PeerMessage::*;

        match peer_message {
            | ClusterHeartBeat(mut heartbeat) => {
                if self.replication.in_ban_list(&heartbeat.from) {
                    debug!("{} in the ban list", heartbeat.from);
                    return;
                }
                self.apply_ban_list(std::mem::take(&mut heartbeat.ban_list)).await;
                self.join_peer_network_if_absent(heartbeat.cluster_nodes).await;
                self.gossip(heartbeat.hop_count).await;
                self.update_on_hertbeat_message(&heartbeat.from, heartbeat.hwm);
            },
            | RequestVote(request_vote) => {
                self.vote_election(request_vote).await;
            },
            | AckReplication(repl_res) => {
                if !repl_res.is_granted() {
                    self.handle_repl_rejection(repl_res).await;
                    return;
                }
                self.update_on_hertbeat_message(&repl_res.from, repl_res.log_idx);
                self.track_replication_progress(repl_res);
            },

            | AppendEntriesRPC(heartbeat) => {
                if self.check_term_outdated(&heartbeat).await {
                    return;
                };

                self.reset_election_timeout(&heartbeat.from);
                self.maybe_update_term(heartbeat.term);
                self.replicate(heartbeat, cache_manager).await;
            },

            | ElectionVoteReply(request_vote_reply) => {
                if !request_vote_reply.vote_granted {
                    return;
                }
                self.tally_vote().await;
            },

            | TriggerRebalance => {
                // self.trigger_rebalance().await;
            },
        }
    }

    async fn process_connection_message(&mut self, conn_msg: ConnectionMessage) {
        use ConnectionMessage::*;

        match conn_msg {
            | ConnectToServer { connect_to, callback } => {
                self.connect_to_server(connect_to, Some(callback)).await;
            },
            | AcceptInboundPeer { stream } => {
                self.accept_inbound_stream(stream).await;
            },

            | AddPeer(peer, optional_callback) => {
                self.add_peer(peer).await;
                if let Some(cb) = optional_callback {
                    let _ = cb.send(Ok(()));
                }
            },
            | FollowerSetReplId(replication_id) => self.set_repl_id(replication_id),
        }
    }

    pub(crate) fn run(
        node_timeout: u128,
        topology_writer: tokio::fs::File,
        heartbeat_interval: u64,
        init_replication: ReplicationState,
        cache_manager: CacheManager,
        wal: T,
    ) -> ClusterCommandHandler {
        let cluster_actor = ClusterActor::new(
            node_timeout,
            init_replication,
            heartbeat_interval,
            topology_writer,
            wal,
        );

        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(cache_manager));
        actor_handler
    }
}
