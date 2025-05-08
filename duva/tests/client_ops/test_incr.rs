use crate::common::{Client, ServerEnv, spawn_server_process};

async fn run_incr(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false).await?;

    let mut h = Client::new(process.port);

    // WHEN
    assert_eq!(h.send_and_get("INCR a", 1).await, vec!["(integer) 1"]);
    assert_eq!(h.send_and_get("INCR a", 1).await, vec!["(integer) 2"]);
    assert_eq!(h.send_and_get("INCR a", 1).await, vec!["(integer) 3"]);

    // THEN
    assert_eq!(h.send_and_get("GET a", 1).await, vec!["3"]);
    assert_eq!(h.send_and_get("INCR b", 1).await, vec!["(integer) 1"]);
    assert_eq!(h.send_and_get("INCR b", 1).await, vec!["(integer) 2"]);

    // WHEN
    assert_eq!(h.send_and_get("SET c adsds", 1).await, vec!["OK"]);

    // THEN
    assert_eq!(
        h.send_and_get("INCR c", 1).await,
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN - out of range
    assert_eq!(h.send_and_get("SET d 92233720368547332375808", 1).await, vec!["OK"]);
    // THEN
    assert_eq!(
        h.send_and_get("INCR d", 1).await,
        vec!["(error) ERR value is not an integer or out of range"]
    );

    Ok(())
}

#[tokio::test]
async fn test_incr() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_incr(env).await?;
    }

    Ok(())
}
