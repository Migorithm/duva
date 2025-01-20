use crate::common::{spawn_server_as_slave, spawn_server_process, wait_for_and_get_message};

mod common;

#[tokio::test]
async fn test_send_sync_and_receive_sync() {
    let mut master = spawn_server_process();

    let mut replica = spawn_server_as_slave(&master);

    let mut stdout = master.stdout.take().unwrap();

    let master_message = "[INFO] Sent sync to slave ";
    let message = wait_for_and_get_message(&mut stdout, master_message, 1);

    let file: String = message.split_inclusive(master_message).skip(1).collect();

    let mut repl_stdout = replica.stdout.take().unwrap();

    let slave_message = "[INFO] Received sync from master ";
    let message = wait_for_and_get_message(&mut repl_stdout, slave_message, 1);

    let repl_file: String = message.split_inclusive(slave_message).skip(1).collect();

    assert_eq!(file, repl_file);
}
