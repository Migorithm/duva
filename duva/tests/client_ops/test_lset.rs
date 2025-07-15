use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_lset(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);
    let res = h.send_and_get(format!("RPUSH test one two three"));
    assert_eq!(res, "(integer) 5");

    //WHEN & ASSERT
    assert_eq!(h.send_and_get(format!("LSET test 0 four")), "OK");
    assert_eq!(h.send_and_get(format!("LSET test -2 five")), "OK");
    assert_eq!(
        h.send_and_get(format!("LRANGE test 0 -1")),
        "1) \"four\"\n2) \"five\"\n3) \"three\""
    );

    // ERROR CASE
    assert_eq!(h.send_and_get(format!("LSET x 1 2")), "(error) ERR no such key");

    assert_eq!(h.send_and_get(format!("SET x 1 2")), "OK");
    assert_eq!(
        h.send_and_get(format!("LSET x 0 2")),
        "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
    );

    Ok(())
}

#[test]
fn test_lset() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_lset(env)?;
    }

    Ok(())
}
