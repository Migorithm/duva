mod common;

#[tokio::test]
#[ignore = "not yet ready"]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    // create master server with fake replica address as peers

    // WHEN - new replica is connecting to master, the newly added server should start sending PING to the other servers attached to the master

    // THEN
}
