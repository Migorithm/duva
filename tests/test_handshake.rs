use common::{init_config_with_free_port, start_test_server};
use redis_starter_rust::adapters::cancellation_token::CancellationToken;
mod common;

#[tokio::test]
async fn test_replication_info() {
    // GIVEN
    //TODO test config should be dynamically configured

    let master_config = Box::leak(Box::new(init_config_with_free_port().await));
    start_test_server::<CancellationToken>(master_config).await;

    let slave_config = init_config_with_free_port().await;

    // WHEN - firing slave node
    slave_config.replication.write().await.master_host = Some("localhost".to_string());
    slave_config.replication.write().await.master_port = Some(master_config.port);
    let slave_config = Box::leak(Box::new(slave_config));
    start_test_server::<CancellationToken>(slave_config).await;
    start_test_server::<CancellationToken>(slave_config).await;
    start_test_server::<CancellationToken>(slave_config).await;
    start_test_server::<CancellationToken>(slave_config).await;

    // THEN
    assert_eq!(master_config.replication.read().await.connected_slaves, 4);
}
