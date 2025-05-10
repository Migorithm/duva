use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_decrby(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false)?;
    let mut h = Client::new(process.port);

    // WHEN: Basic decrement
    assert_eq!(h.send_and_get("SET a 10", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("DECRBY a 5", 1), vec!["(integer) 5"]);

    // THEN
    assert_eq!(h.send_and_get("GET a", 1), vec!["5"]);

    // WHEN: Decrement existing value
    assert_eq!(h.send_and_get("SET b 10", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("DECRBY b 5", 1), vec!["(integer) 5"]);

    // THEN
    assert_eq!(h.send_and_get("GET b", 1), vec!["5"]);

    // WHEN: Try to decrement non-integer value
    assert_eq!(h.send_and_get("SET c not_a_number", 1), vec!["OK"]);

    // THEN
    assert_eq!(
        h.send_and_get("DECRBY c 1", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN: Decrement with negative value (should increment)
    assert_eq!(h.send_and_get("SET d 5", 1), vec!["OK"]);
    assert_eq!(h.send_and_get("DECRBY d -3", 1), vec!["(integer) 8"]);

    // THEN
    assert_eq!(h.send_and_get("GET d", 1), vec!["8"]);

    Ok(())
}

#[test]
fn test_decrby() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_decrby(env)?;
    }
    Ok(())
} 
