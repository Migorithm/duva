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
    let mut ring = HashRing::default();
    ring.add_partition_if_not_exists(
        ReplicationId::Key(Uuid::now_v7().to_string()),
        PeerIdentifier("127.0.0.1:6379".into()),
    );

    let keys = vec!["key1".to_string(), "key2".to_string()];
    let tasks = ring.create_migration_tasks(&ring, keys).await;

    assert!(tasks.is_empty(), "Identical rings should require no migration");
}

#[tokio::test]
async fn test_empty_keys_migration_plan() {
    // Test with empty keys list
    let mut old_ring = HashRing::default();
    let mut new_ring = HashRing::default();

    let node1 = PeerIdentifier("127.0.0.1:6379".into());
    let node2 = PeerIdentifier("127.0.0.1:6380".into());
    let repl_id1 = ReplicationId::Key(Uuid::now_v7().to_string());
    let repl_id2 = ReplicationId::Key(Uuid::now_v7().to_string());

    old_ring.add_partition_if_not_exists(repl_id1, node1);
    new_ring.add_partition_if_not_exists(repl_id2, node2);

    let empty_keys: Vec<String> = Vec::new();
    let tasks = old_ring.create_migration_tasks(&new_ring, empty_keys).await;

    // Should return empty migration tasks since no keys to migrate
    assert!(tasks.is_empty(), "Empty keys should result in no migration tasks");
}

#[tokio::test]
async fn test_single_node_ownership_change() {
    // Test scenario: 3 nodes in ring, one node (node2) is replaced by node4
    // node1 and node3 remain unchanged
    let replid1 = replid_create_helper("node1");
    let replid2 = replid_create_helper("node2");
    let replid3 = replid_create_helper("node3");
    let replid4 = replid_create_helper("node4");

    // Create old ring with 3 nodes using realistic hash ring
    let mut old_ring = HashRing::default();
    old_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
    old_ring.add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()));
    old_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));

    // Create new ring where node2 is replaced by node4
    let mut new_ring = HashRing::default();
    new_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
    new_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into())); // node2 -> node4
    new_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));

    // Generate a set of test keys
    let test_keys: Vec<String> = (0..10000).map(|i| format!("test_key_{}", i)).collect();

    // Identify ownership in both rings
    let mut old_ownership = HashMap::new();
    let mut new_ownership = HashMap::new();

    for key in &test_keys {
        old_ownership.insert(key.clone(), old_ring.get_node_for_key(key).unwrap().clone());
        new_ownership.insert(key.clone(), new_ring.get_node_for_key(key).unwrap().clone());
    }

    // Find keys that changed ownership
    let mut expected_migrations = HashMap::<ReplicationId, Vec<String>>::new();

    for key in &test_keys {
        let new_owner = new_ownership.get(key).unwrap();

        if old_ownership.get(key) != new_ownership.get(key) {
            let migration_key = new_owner.clone();
            expected_migrations.entry(migration_key).or_default().push(key.clone());
        }
    }

    // Create migration plan
    let migration_plans = old_ring.create_migration_tasks(&new_ring, test_keys.clone()).await;

    // Verify that we have migration tasks if and only if there are ownership changes
    assert!(!migration_plans.is_empty(), "Should have migration tasks when ownership changes");

    // Collect actual migrations from tasks
    let mut actual_migrations = HashMap::<ReplicationId, Vec<String>>::new();

    for (replid, tasks) in &migration_plans {
        let keys = tasks.iter().map(|t| t.keys_to_migrate.clone()).flatten().collect::<Vec<_>>();
        actual_migrations.entry(replid.clone()).or_default().extend(keys);
    }

    // Verify that actual migrations match expected migrations
    for (to_node, expected_keys) in &expected_migrations {
        let actual_keys = actual_migrations.get(&to_node.clone()).unwrap();
        assert_eq!(actual_keys.len(), expected_keys.len());
        let mut ak = actual_keys.clone();
        let mut ek = expected_keys.clone();
        ak.sort();
        ek.sort();
        assert_eq!(ak, ek);
    }

    // Verify no unexpected migrations
    for (to_node, actual_keys) in &actual_migrations {
        if !expected_migrations.contains_key(&to_node.clone()) {
            panic!("Unexpected migration  to {} for keys {:?}", to_node, actual_keys);
        }
    }

    // Verify total count
    let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
    let total_actual: usize = migration_plans
        .iter()
        .map(|(_, tasks)| tasks.iter().map(|t| t.keys_to_migrate.len()).sum::<usize>())
        .sum();
    assert_eq!(
        total_actual, total_expected,
        "Total migrated keys should match expected migrations"
    );

    // Additional verification: node2 should not exist in new ring, node4 should not exist in old ring
    for key in &test_keys {
        // In old ring, node4 shouldn't own any keys (it doesn't exist)
        if let Some(old_owner) = old_ring.get_node_for_key(key) {
            assert_ne!(*old_owner, replid4, "Node4 shouldn't exist in old ring");
        }

        // In new ring, node2 shouldn't own any keys (it was removed)
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring");
        }
    }
}

#[tokio::test]
async fn test_multiple_ownership_changes() {
    // Test scenario: 4 nodes in ring, replace 2 nodes simultaneously
    // node1 -> node5, node2 -> node6, node3 and node4 remain unchanged
    let replid1 = replid_create_helper("node1");
    let replid2 = replid_create_helper("node2");
    let replid3 = replid_create_helper("node3");
    let replid4 = replid_create_helper("node4");
    let replid5 = replid_create_helper("node5");
    let replid6 = replid_create_helper("node6");

    // Create old ring with 4 nodes
    let mut old_ring = HashRing::default();
    old_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
    old_ring.add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()));
    old_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));
    old_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into()));

    // Create new ring where node1->node5 and node2->node6, but node3 and node4 remain
    let mut new_ring = HashRing::default();
    new_ring.add_partition_if_not_exists(replid5.clone(), PeerIdentifier("peer5".into())); // node1 -> node5
    new_ring.add_partition_if_not_exists(replid6.clone(), PeerIdentifier("peer6".into())); // node2 -> node6
    new_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into())); // node3 stays
    new_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into())); // node4 stays

    // Generate a large set of test keys for comprehensive testing
    let test_keys: Vec<String> = (0..5000).map(|i| format!("test_key_{}", i)).collect();

    // Identify ownership in both rings
    let mut old_ownership = HashMap::new();
    let mut new_ownership = HashMap::new();

    for key in &test_keys {
        old_ownership.insert(key.clone(), old_ring.get_node_for_key(key).unwrap().clone());
        new_ownership.insert(key.clone(), new_ring.get_node_for_key(key).unwrap().clone());
    }

    // Find keys that changed ownership
    let mut expected_migrations = HashMap::<ReplicationId, Vec<String>>::new();

    for key in &test_keys {
        let new_owner = new_ownership.get(key).unwrap();

        if old_ownership.get(key) != new_ownership.get(key) {
            let migration_key = new_owner.clone();
            expected_migrations.entry(migration_key).or_default().push(key.clone());
        }
    }

    // Create migration plan
    let migration_plans = old_ring.create_migration_tasks(&new_ring, test_keys.clone()).await;

    // Should have migration tasks since we replaced multiple nodes
    assert!(!migration_plans.is_empty(), "Should have migration tasks when multiple nodes change");

    // Collect actual migrations from tasks
    let mut actual_migrations = HashMap::<ReplicationId, Vec<String>>::new();

    for (replid, tasks) in &migration_plans {
        let keys = tasks.iter().map(|t| t.keys_to_migrate.clone()).flatten().collect::<Vec<_>>();
        actual_migrations.entry(replid.clone()).or_default().extend(keys);
    }

    // Verify that actual migrations match expected migrations exactly
    for (to_node, expected_keys) in &expected_migrations {
        let actual_keys = actual_migrations.get(&to_node.clone()).unwrap();

        assert_eq!(actual_keys.len(), expected_keys.len(),);

        // Sort both lists and compare for exact match
        let mut actual_sorted = actual_keys.clone();
        let mut expected_sorted = expected_keys.clone();
        actual_sorted.sort();
        expected_sorted.sort();

        assert_eq!(actual_sorted, expected_sorted,);
    }

    // Verify no unexpected migrations
    for (to_node, _actual_keys) in &actual_migrations {
        assert!(expected_migrations.contains_key(&to_node.clone()));
    }

    // Verify total count
    let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
    let total_actual: usize = migration_plans
        .iter()
        .map(|(_, tasks)| tasks.iter().map(|t| t.keys_to_migrate.len()).sum::<usize>())
        .sum();
    assert_eq!(total_actual, total_expected,);

    // Verify that we actually have multiple different migration paths (multiple ownership changes)
    assert!(expected_migrations.len() >= 2,);

    // Additional verification: removed nodes shouldn't exist in new ring, added nodes shouldn't exist in old ring
    for key in &test_keys {
        // In old ring, new nodes shouldn't own any keys (they don't exist)
        if let Some(old_owner) = old_ring.get_node_for_key(key) {
            assert_ne!(*old_owner, replid5, "Node5 shouldn't exist in old ring");
            assert_ne!(*old_owner, replid6, "Node6 shouldn't exist in old ring");
        }

        // In new ring, removed nodes shouldn't own any keys
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            assert_ne!(*new_owner, replid1, "Node1 shouldn't exist in new ring");
            assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring");
        }
    }

    // Verify that both unchanged nodes (node3, node4) still exist and own some keys in the new ring
    let mut node3_key_count = 0;
    let mut node4_key_count = 0;

    for key in &test_keys {
        if let Some(new_owner) = new_ring.get_node_for_key(key) {
            if *new_owner == replid3 {
                node3_key_count += 1;
            } else if *new_owner == replid4 {
                node4_key_count += 1;
            }
        }
    }

    // Both unchanged nodes should still own some keys (they weren't removed)
    assert!(node3_key_count > 0, "Node3 should still own some keys in new ring");
    assert!(node4_key_count > 0, "Node4 should still own some keys in new ring");

    println!("Migration summary:");
    for (to, keys) in &expected_migrations {
        println!("  to {}: {} keys", to, keys.len());
    }
    println!("  Node3 keys in new ring: {}", node3_key_count);
    println!("  Node4 keys in new ring: {}", node4_key_count);
    println!("  Total test keys: {}", test_keys.len());
}
