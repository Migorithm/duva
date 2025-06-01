/// The following is to test out the append operation
/// Firstly, we append a key (which not exists before) with a value
/// Then we get the key and check if the value is returned (it act like set on first time using unexisting key)
/// After immediately, we append latter string with used key before and check if it's result same as concatted one
use crate::common::{Client, ServerEnv, spawn_server_process};

#[test]
fn test_append() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    let first = "Hello";
    let second = "World!";

    // WHEN - append first value
    assert_eq!(h.send_and_get(format!("APPEND appended_one {first}")), first.len().to_string());
    // THEN
    let res = h.send_and_get("GET appended_one");
    assert_eq!(res, "Hello");

    // WHEN - append second value
    assert_eq!(
        h.send_and_get(format!("APPEND appended_one {second}")),
        (first.len() + second.len()).to_string()
    );
    // THEN
    let res = h.send_and_get("GET appended_one");
    assert_eq!(res, "HelloWorld!");

    Ok(())
}
