/// The following is to test out the set operation with expiry
/// Firstly, we set a key with a value and an expiry of 300ms
/// Then we get the key and check if the value is returned
/// After 300ms, we get the key again and check if the value is not returned (-1)
use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_del(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false)?;

    let mut h = Client::new(process.port);
    assert_eq!(h.send_and_get("SET a b"), "OK");
    assert_eq!(h.send_and_get("SET c d"), "OK");

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("del a c d"), "(integer) 2");

    assert_eq!(h.send_and_get("get a"), "(nil)");

    // THEN
    let res = h.send_and_get("GET somanyrand");
    assert_eq!(res, "(nil)");

    Ok(())
}

#[test]
fn test_del() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_del(env)?;
    }

    Ok(())
}
