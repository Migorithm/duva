/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_set_get(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET somanyrand bar PX 300"), "OK");
    let res = h.send_and_get("GET somanyrand");
    assert_eq!(res, "bar");

    std::thread::sleep(std::time::Duration::from_millis(300));

    // THEN
    let res = h.send_and_get("GET somanyrand");
    assert_eq!(res, "(nil)");

    Ok(())
}

#[test]
fn test_set_get() -> anyhow::Result<()> {
    for env in [ServerEnv::default().with_append_only(true)] {
        run_set_get(env)?;
    }

    Ok(())
}
