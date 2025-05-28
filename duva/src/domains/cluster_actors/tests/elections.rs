use super::*;
#[tokio::test]
async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;
    let (tx, rx) = tokio::sync::oneshot::channel();

    let consensus_request = consensus_request_create_helper(tx, None);

    // WHEN
    cluster_actor.req_consensus(consensus_request).await;

    // THEN
    assert_eq!(cluster_actor.consensus_tracker.len(), 0);
    assert_eq!(cluster_actor.logger.last_log_index, 1);
}
