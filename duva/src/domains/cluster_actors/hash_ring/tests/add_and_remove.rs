use super::*;

#[test]
fn test_add_and_remove_node() {
    let ring = HashRing::default();
    let modified_time = ring.last_modified;
    let node = PeerIdentifier("127.0.0.1:6379".into());
    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

    let partitions = vec![(repl_id.clone(), node.clone())];
    let ring = ring.set_partitions(partitions).unwrap();
    let modified_time_after_add = ring.last_modified;
    assert_eq!(ring.get_pnode_count(), 1);
    assert_eq!(ring.get_vnode_count(), 256);
    assert!(modified_time < modified_time_after_add);

    sleep(Duration::from_millis(1)); // Ensure time has changed
    let ring = ring.set_partitions(vec![]).unwrap();
    assert_eq!(ring.get_pnode_count(), 0);
    assert_eq!(ring.get_vnode_count(), 0);
    assert!(ring.last_modified > modified_time_after_add);
}

#[test]
fn test_get_node_for_key() {
    let ring = HashRing::default();
    let node = PeerIdentifier("127.0.0.1:6379".into());
    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let ring = ring.set_partitions(vec![(repl_id.clone(), node.clone())]).unwrap();

    let key = "test_key";
    let node = ring.get_node_for_key(key);
    assert!(node.is_some());
    assert_eq!(*node.unwrap(), repl_id);
}

#[test]
fn test_set_partitions_multiple_partitions() {
    let ring = HashRing::default();

    let ring = ring
        .set_partitions(vec![
            replid_and_nodeid(6379),
            replid_and_nodeid(6380),
            replid_and_nodeid(6381),
        ])
        .unwrap();
    assert_eq!(ring.get_pnode_count(), 3);
    assert_eq!(ring.get_vnode_count(), 768); // 3 * 256
}

#[test]
fn test_set_partitions_empty_list() {
    let ring = HashRing::default();
    let empty_partitions: Vec<(ReplicationId, PeerIdentifier)> = vec![];

    let result = ring.set_partitions(empty_partitions);
    assert!(result.is_none()); // Should return None for empty list
}

#[test]
fn test_consistent_hashing() {
    let ring = HashRing::default();
    let ring = ring
        .set_partitions(vec![
            replid_and_nodeid(6379),
            replid_and_nodeid(6380),
            replid_and_nodeid(6381),
        ])
        .unwrap();

    // Test key distribution consistency
    let key = "test_key";
    let node_got1 = ring.get_node_for_key(key);
    let node_got2 = ring.get_node_for_key(key);

    assert_eq!(node_got1, node_got2);
    assert!(node_got1.is_some());
}

#[test]
fn test_set_partitions_virtual_node_consistency() {
    let ring = HashRing::default();

    let (repl_id, node) = replid_and_nodeid(6379);

    let partitions = vec![(repl_id.clone(), node.clone())];
    let ring = ring.set_partitions(partitions).unwrap();

    let virtual_nodes = ring.get_virtual_nodes();
    assert_eq!(virtual_nodes.len(), 256);

    // remove duplicate
    let physical_nodes: HashSet<&ReplicationId> =
        virtual_nodes.iter().map(|(_, peer_id)| peer_id.as_ref()).collect();
    assert_eq!(physical_nodes.len(), 1);
    assert!(physical_nodes.contains::<ReplicationId>(&repl_id));
}

#[test]
fn test_set_partitions_idempotent_addition() {
    let ring = HashRing::default();
    let (repl_id, node) = replid_and_nodeid(6379);

    let new_ring = ring.set_partitions(vec![(repl_id.clone(), node.clone())]);
    assert!(new_ring.is_some());
    let ring = new_ring.unwrap();

    let ring_to_compare = ring.clone(); // Clone to avoid borrowing issues

    // Adding the same partition again should not change the ring
    let partitions = vec![(repl_id.clone(), node.clone())];
    let is_added = ring.set_partitions(partitions);
    assert!(is_added.is_none());

    assert_eq!(ring, ring_to_compare);
}

#[test]
fn test_node_removal_redistribution() {
    // GIVEN: Create a hash ring with 3 nodes
    let ring = HashRing::default();
    let (repl_id, node1) = replid_and_nodeid(6379);
    let (repl_id2, node2) = replid_and_nodeid(6380);
    let (repl_id3, node3) = replid_and_nodeid(6381);

    let ring = ring
        .set_partitions(vec![
            (repl_id.clone(), node1.clone()),
            (repl_id2.clone(), node2.clone()),
            (repl_id3.clone(), node3.clone()),
        ])
        .unwrap();

    // Record initial key distribution
    let mut before_removal = Vec::new();
    for i in 0..100 {
        let key = format!("key{}", i);
        if let Some(node) = ring.get_node_for_key(&key) {
            before_removal.push((key, node.clone()));
        }
    }

    // WHEN Remove one node
    let ring = ring
        .clone()
        .set_partitions(vec![
            (repl_id2.clone(), node2.clone()), // Keep node2
            (repl_id3.clone(), node3.clone()), // Keep node3
        ])
        .unwrap();

    // keys are accessed again
    let mut redistributed = 0;
    for (key, old_addr) in before_removal {
        if let Some(new_node) = ring.get_node_for_key(&key)
            && *new_node != old_addr
        {
            redistributed += 1;
        }
    }

    //THEN
    assert!(redistributed > 0); // Some keys must be redistributed
    assert!(redistributed < 100); // But not all keys should be redistributed
}
