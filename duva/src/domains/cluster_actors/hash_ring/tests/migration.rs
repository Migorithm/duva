use super::*;
use std::collections::HashMap;

use uuid::Uuid;

use crate::ReplicationId;
use crate::prelude::PeerIdentifier;

// Migration plan tests
fn replid_create_helper(repl_id: &str) -> ReplicationId {
    ReplicationId::Key(repl_id.to_string())
}

#[tokio::test]
async fn test_no_migration_when_rings_identical() {
    let ring = HashRing::default();
    let ring = ring
        .add_partition_if_not_exists(
            ReplicationId::Key(Uuid::now_v7().to_string()),
            PeerIdentifier("127.0.0.1:6379".into()),
        )
        .unwrap();

    let keys = vec!["key1".to_string(), "key2".to_string()];
    let tasks = ring.create_migration_tasks(&ring, keys);

    assert!(tasks.is_empty(), "Identical rings should require no migration");
}

#[tokio::test]
async fn test_empty_keys_migration_plan() {
    // Test with empty keys list
    let old_ring = HashRing::default();
    let new_ring = HashRing::default();

    let node1 = PeerIdentifier("127.0.0.1:6379".into());
    let node2 = PeerIdentifier("127.0.0.1:6380".into());
    let repl_id1 = ReplicationId::Key(Uuid::now_v7().to_string());
    let repl_id2 = ReplicationId::Key(Uuid::now_v7().to_string());

    let old_ring = old_ring.add_partition_if_not_exists(repl_id1, node1).unwrap();
    let new_ring = new_ring.add_partition_if_not_exists(repl_id2, node2).unwrap();

    let empty_keys: Vec<String> = Vec::new();
    let tasks = old_ring.create_migration_tasks(&new_ring, empty_keys);

    // Should return empty migration tasks since no keys to migrate
    assert!(tasks.is_empty(), "Empty keys should result in no migration tasks");
}

#[tokio::test]
async fn test_single_node_ownership_change() {
    // Test scenario: 3 nodes in ring, one node (node2) is replaced by node4
    let replid1 = replid_create_helper("node1");
    let replid2 = replid_create_helper("node2");
    let replid3 = replid_create_helper("node3");
    let replid4 = replid_create_helper("node4");

    // Setup old ring
    let old_ring = HashRing::default();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()))
        .unwrap();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()))
        .unwrap();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()))
        .unwrap();

    // Setup new ring
    let new_ring = HashRing::default();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()))
        .unwrap();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into()))
        .unwrap(); // node2 -> node4
    let new_ring = new_ring
        .add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()))
        .unwrap();

    let test_keys: Vec<String> = (0..10000).map(|i| format!("test_key_{}", i)).collect();

    // Identify expected migrations
    let mut expected_migrations = HashMap::<ReplicationId, Vec<String>>::new();
    for key in &test_keys {
        let old_owner = old_ring.get_node_for_key(key).unwrap();
        let new_owner = new_ring.get_node_for_key(key).unwrap();
        if old_owner != new_owner {
            expected_migrations.entry(new_owner.clone()).or_default().push(key.clone());
        }
    }
    expected_migrations.values_mut().for_each(|keys| keys.sort()); // Sort for consistent comparison

    // Create and collect actual migration tasks
    let migration_plans = old_ring.create_migration_tasks(&new_ring, test_keys.clone());
    let mut actual_migrations = HashMap::<ReplicationId, Vec<String>>::new();
    for (replid, tasks) in &migration_plans {
        actual_migrations
            .entry(replid.clone())
            .or_default()
            .extend(tasks.iter().flat_map(|t| t.keys_to_migrate.clone()));
    }
    actual_migrations.values_mut().for_each(|keys| keys.sort()); // Sort for consistent comparison

    // Assertions for migrations
    assert!(!migration_plans.is_empty(), "Should have migration tasks when ownership changes");
    assert_eq!(
        actual_migrations.len(),
        expected_migrations.len(),
        "Number of migration targets should match."
    );
    for (to_node, expected_keys) in &expected_migrations {
        assert_eq!(
            actual_migrations.get(to_node),
            Some(expected_keys),
            "Keys for node {:?} do not match",
            to_node
        );
    }
    for (to_node, actual_keys) in &actual_migrations {
        assert!(
            expected_migrations.contains_key(to_node),
            "Unexpected migration to {} for keys {:?}",
            to_node,
            actual_keys
        );
    }
    let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
    let total_actual: usize = actual_migrations.values().map(|v| v.len()).sum();
    assert_eq!(
        total_actual, total_expected,
        "Total migrated keys should match expected migrations"
    );
    assert!(total_actual > 0, "There should be actual migrations.");

    // Additional node existence verification
    for key in &test_keys {
        if let Some(old_owner) = old_ring.get_node_for_key(key) {
            assert_ne!(*old_owner, replid4, "Node4 shouldn't exist in old ring or own keys");
        }
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring or own keys");
        }
    }
}

#[tokio::test]
async fn test_multiple_ownership_changes() {
    // Test scenario: 4 nodes, replace 2 (node1->node5, node2->node6), node3 and node4 remain
    let replid1 = replid_create_helper("node1");
    let replid2 = replid_create_helper("node2");
    let replid3 = replid_create_helper("node3");
    let replid4 = replid_create_helper("node4");
    let replid5 = replid_create_helper("node5");
    let replid6 = replid_create_helper("node6");

    // Setup old ring
    let old_ring = HashRing::default();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()))
        .unwrap();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()))
        .unwrap();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()))
        .unwrap();
    let old_ring = old_ring
        .add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into()))
        .unwrap();

    // Setup new ring
    let new_ring = HashRing::default();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid5.clone(), PeerIdentifier("peer5".into()))
        .unwrap();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid6.clone(), PeerIdentifier("peer6".into()))
        .unwrap();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()))
        .unwrap();
    let new_ring = new_ring
        .add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into()))
        .unwrap();

    let test_keys: Vec<String> = (0..5000).map(|i| format!("test_key_{}", i)).collect();

    // Identify expected migrations
    let mut expected_migrations = HashMap::<ReplicationId, Vec<String>>::new();
    for key in &test_keys {
        let old_owner = old_ring.get_node_for_key(key).unwrap();
        let new_owner = new_ring.get_node_for_key(key).unwrap();
        if old_owner != new_owner {
            expected_migrations.entry(new_owner.clone()).or_default().push(key.clone());
        }
    }
    expected_migrations.values_mut().for_each(|keys| keys.sort()); // Sort for consistent comparison

    // Create and collect actual migration tasks
    let migration_plans = old_ring.create_migration_tasks(&new_ring, test_keys.clone());
    let mut actual_migrations = HashMap::<ReplicationId, Vec<String>>::new();
    for (replid, tasks) in &migration_plans {
        actual_migrations
            .entry(replid.clone())
            .or_default()
            .extend(tasks.iter().flat_map(|t| t.keys_to_migrate.clone()));
    }
    actual_migrations.values_mut().for_each(|keys| keys.sort()); // Sort for consistent comparison

    // Assertions for migrations
    assert!(!migration_plans.is_empty(), "Should have migration tasks when multiple nodes change");
    assert_eq!(
        actual_migrations.len(),
        expected_migrations.len(),
        "Number of migration targets should match."
    );
    for (to_node, expected_keys) in &expected_migrations {
        assert_eq!(
            actual_migrations.get(to_node),
            Some(expected_keys),
            "Keys for node {:?} do not match",
            to_node
        );
    }
    for (to_node, actual_keys) in &actual_migrations {
        assert!(
            expected_migrations.contains_key(to_node),
            "Unexpected migration to {} for keys {:?}",
            to_node,
            actual_keys
        );
    }
    let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
    let total_actual: usize = actual_migrations.values().map(|v| v.len()).sum();
    assert_eq!(
        total_actual, total_expected,
        "Total migrated keys should match expected migrations"
    );
    assert!(total_actual > 0, "There should be actual migrations.");

    assert!(expected_migrations.len() >= 2, "Expected multiple different migration paths");

    // Additional node existence verification
    for key in &test_keys {
        if let Some(old_owner) = old_ring.get_node_for_key(key) {
            assert_ne!(*old_owner, replid5, "Node5 shouldn't exist in old ring");
            assert_ne!(*old_owner, replid6, "Node6 shouldn't exist in old ring");
        }
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            assert_ne!(*new_owner, replid1, "Node1 shouldn't exist in new ring");
            assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring");
        }
    }

    // Verify that both unchanged nodes (node3, node4) still exist and own some keys
    let mut node_key_counts = HashMap::new();
    for key in &test_keys {
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            *node_key_counts.entry(new_owner.clone()).or_insert(0) += 1;
        }
    }
    assert!(node_key_counts.get(&replid3).unwrap_or(&0) > &0, "Node3 should still own some keys");
    assert!(node_key_counts.get(&replid4).unwrap_or(&0) > &0, "Node4 should still own some keys");

    println!("Migration summary:");
    for (to, keys) in &expected_migrations {
        println!("  to {}: {} keys", to, keys.len());
    }
    println!("  Node3 keys in new ring: {}", node_key_counts.get(&replid3).unwrap_or(&0));
    println!("  Node4 keys in new ring: {}", node_key_counts.get(&replid4).unwrap_or(&0));
    println!("  Total test keys: {}", test_keys.len());
}
