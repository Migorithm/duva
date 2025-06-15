use super::*;

#[test]
fn test_add_and_remove_node() {
    let ring = HashRing::default();
    let modified_time = ring.last_modified;
    let node = PeerIdentifier("127.0.0.1:6379".into());
    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

    let mut ring = ring.add_partition_if_not_exists(repl_id.clone(), node.clone()).unwrap();
    let modified_time_after_add = ring.last_modified;
    assert_eq!(ring.get_pnode_count(), 1);
    assert_eq!(ring.get_vnode_count(), 256);
    assert!(modified_time < modified_time_after_add);

    sleep(Duration::from_millis(1)); // Ensure time has changed
    ring.remove_partition(&repl_id);
    assert_eq!(ring.get_pnode_count(), 0);
    assert_eq!(ring.get_vnode_count(), 0);
    assert!(ring.last_modified > modified_time_after_add);
}

#[test]
fn test_get_node_for_key() {
    let ring = HashRing::default();
    let node = PeerIdentifier("127.0.0.1:6379".into());
    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let ring = ring.add_partition_if_not_exists(repl_id.clone(), node.clone()).unwrap();

    let key = "test_key";
    let node = ring.get_node_for_key(key);
    assert!(node.is_some());
    assert_eq!(*node.unwrap(), repl_id);
}

#[test]
fn test_multiple_nodes() {
    let ring = HashRing::default();
    let node1 = PeerIdentifier("127.0.0.1:6379".into());
    let repl_id1 = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let node2 = PeerIdentifier("127.0.0.1:6380".into());
    let repl_id2 = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

    let ring = ring.add_partition_if_not_exists(repl_id1, node1).unwrap();
    let ring = ring.add_partition_if_not_exists(repl_id2, node2).unwrap();

    assert_eq!(ring.get_pnode_count(), 2);
    assert_eq!(ring.get_vnode_count(), 512);
}

#[test]
fn test_consistent_hashing() {
    let ring = HashRing::default();
    let node1 = PeerIdentifier("127.0.0.1:6379".into());
    let node2 = PeerIdentifier("127.0.0.1:6380".into());
    let node3 = PeerIdentifier("127.0.0.1:6389".into());

    let ring = ring
        .add_partition_if_not_exists(ReplicationId::Key(uuid::Uuid::now_v7().to_string()), node1)
        .unwrap();
    let ring = ring
        .add_partition_if_not_exists(ReplicationId::Key(uuid::Uuid::now_v7().to_string()), node2)
        .unwrap();
    let ring = ring
        .add_partition_if_not_exists(ReplicationId::Key(uuid::Uuid::now_v7().to_string()), node3)
        .unwrap();

    let key = "test_key";
    let node_got1 = ring.get_node_for_key(key);
    let node_got2 = ring.get_node_for_key(key);

    assert_eq!(node_got1, node_got2);
}

#[test]
fn test_node_removal_redistribution() {
    // GIVEN: Create a hash ring with 3 nodes
    let ring = HashRing::default();
    let node1 = PeerIdentifier("127.0.0.1:6379".into());
    let node2 = PeerIdentifier("127.0.0.1:6380".into());
    let node3 = PeerIdentifier("127.0.0.1:6381".into());

    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let ring = ring.add_partition_if_not_exists(repl_id.clone(), node1).unwrap();
    let ring = ring
        .add_partition_if_not_exists(ReplicationId::Key(uuid::Uuid::now_v7().to_string()), node2)
        .unwrap();
    let mut ring = ring
        .add_partition_if_not_exists(ReplicationId::Key(uuid::Uuid::now_v7().to_string()), node3)
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
    ring.remove_partition(&repl_id);

    // keys are accessed again
    let mut redistributed = 0;
    for (key, old_addr) in before_removal {
        if let Some(new_node) = ring.get_node_for_key(&key) {
            if *new_node != old_addr {
                redistributed += 1;
            }
        }
    }

    //THEN
    assert!(redistributed > 0); // Some keys must be redistributed
    assert!(redistributed < 100); // But not all keys should be redistributed
}

#[test]
fn test_virtual_node_consistency() {
    let ring = HashRing::default();
    let node = PeerIdentifier("127.0.0.1:6379".into());

    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let ring = ring.add_partition_if_not_exists(repl_id.clone(), node.clone()).unwrap();

    let virtual_nodes = ring.get_virtual_nodes();
    assert_eq!(virtual_nodes.len(), 256);

    // remove duplicate
    let physical_nodes: HashSet<&ReplicationId> =
        virtual_nodes.iter().map(|(_, peer_id)| peer_id.as_ref()).collect();
    assert_eq!(physical_nodes.len(), 1);
    assert!(physical_nodes.contains::<ReplicationId>(&repl_id));
}

#[test]
fn test_empty_ring() {
    let ring = HashRing::default();
    assert_eq!(ring.get_pnode_count(), 0);
    assert_eq!(ring.get_vnode_count(), 0);
    assert!(ring.get_node_for_key("test").is_none());
}

#[test]
fn test_idempotent_addition() {
    let ring = HashRing::default();
    let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let node = PeerIdentifier("127.0.0.1:3499".to_string());
    let new_ring = ring.add_partition_if_not_exists(repl_id.clone(), node.clone());
    assert!(new_ring.is_some());
    let ring = new_ring.unwrap();

    let ring_to_compare = ring.clone(); // Clone to avoid borrowing issues

    // Adding the same node again should not change the ring
    let is_added = ring.add_partition_if_not_exists(repl_id.clone(), node.clone());
    assert!(is_added.is_none());

    assert_eq!(ring, ring_to_compare);
}

#[test]
fn test_eq_works_deterministically() {
    let ring = HashRing::default();
    let repl_id = ReplicationId::Key("dsdsdds".to_string());
    let node = PeerIdentifier("127.0.0.1:3499".to_string());
    let mut ring = ring.add_partition_if_not_exists(repl_id.clone(), node.clone()).unwrap();

    let ring_to_compare =
        HashRing::default().add_partition_if_not_exists(repl_id.clone(), node.clone()).unwrap();
    assert_eq!(ring, ring_to_compare);

    ring.remove_partition(&repl_id);
    assert_ne!(ring, ring_to_compare);

    let mut ring_to_compare = ring_to_compare;
    ring_to_compare.remove_partition(&repl_id);
    assert_eq!(ring, ring_to_compare);
}
