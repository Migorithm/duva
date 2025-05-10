use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_incrby(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false)?;
    let mut h = Client::new(process.port);

    // WHEN: Basic increment
    assert_eq!(h.send_and_get("SET a 10", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("INCRBY a 5", 1), vec!["(integer) 15"]);

    // THEN
    assert_eq!(h.send_and_get("GET a", 1), vec!["15"]);

    // WHEN: Increment existing value
    assert_eq!(h.send_and_get("SET b 10", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("INCRBY b 5", 1), vec!["(integer) 15"]);

    // THEN
    assert_eq!(h.send_and_get("GET b", 1), vec!["15"]);

    // WHEN: Try to increment non-integer value
    assert_eq!(h.send_and_get("SET c not_a_number", 1), vec!["OK"]);

    // THEN
    assert_eq!(
        h.send_and_get("INCRBY c 1", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN: Increment with negative value (should decrement)
    assert_eq!(h.send_and_get("SET d 5", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("INCRBY d -3", 1), vec!["(integer) 2"]);

    // THEN
    assert_eq!(h.send_and_get("GET d", 1), vec!["2"]);

    Ok(())
}

#[test]
fn test_incrby() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_incrby(env)?;
    }
    Ok(())
}
