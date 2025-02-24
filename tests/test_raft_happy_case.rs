mod common;
use common::{FileName, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_set_operation_reaches_to_all_replicas() {
    // GIVEN

    let file_name: FileName = FileName(None);
    // loads the leader/follower processes
    let mut leader_p = spawn_server_process(None, &file_name);
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let repl_file_name = FileName("follower_dbfilename".to_string().into());
    let mut repl_p = spawn_server_process(leader_p.bind_addr().into(), &repl_file_name);

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    // WHEN -- set operation is made
    client_handler.send(&common::array(vec!["SET", "foo", "bar"])).await;

    //THEN - run the following together
    let h = std::thread::spawn(move || {
        repl_p.timed_wait_for_message(
            vec![
                "[INFO] Received log entry with log index num 1: Set { key: \"foo\", value: \"bar\" }",
                "[INFO] Received commit offset 1"
            ],
            1,
            4,
        )
    });

    let h2 = std::thread::spawn(move || {
        leader_p.timed_wait_for_message(
            vec!["[INFO] Received acks for log index num: 1", "[INFO] Sending commit request on 1"],
            1,
            4,
        )
    });

    h.join().unwrap().unwrap();
    h2.join().unwrap().unwrap();
}
