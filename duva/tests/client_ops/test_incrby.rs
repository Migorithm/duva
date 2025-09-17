use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_incrby(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;
    let mut h = Client::new(process.port);

    // WHEN: Basic increment
    assert_eq!(h.send_and_get("SET a 10"), "OK");
    assert_eq!(h.send_and_get("INCRBY a 5"), "(integer) 15");

    // THEN
    assert_eq!(h.send_and_get("GET a"), "15");

    // WHEN: Increment existing value
    assert_eq!(h.send_and_get("SET b 10"), "OK");
    assert_eq!(h.send_and_get("INCRBY b 5"), "(integer) 15");
    assert_eq!(h.send_and_get("INCRBY b 5"), "(integer) 20");

    // THEN
    assert_eq!(h.send_and_get("GET b"), "20");

    // WHEN: Try to increment non-integer value
    assert_eq!(h.send_and_get("SET c not_a_number"), "OK");

    // THEN
    assert_eq!(h.send_and_get("INCRBY c 1"), "(error) ERR value is not an integer or out of range");

    // WHEN: Increment with negative value (should decrement)
    assert_eq!(h.send_and_get("SET d 5"), "OK");
    assert_eq!(h.send_and_get("INCRBY d -3"), "(integer) 2");

    // THEN
    assert_eq!(h.send_and_get("GET d"), "2");

    Ok(())
}

#[test]
fn test_incrby() -> anyhow::Result<()> {
    for env in [ServerEnv::default().with_append_only(true)] {
        run_incrby(env)?;
    }
    Ok(())
}
