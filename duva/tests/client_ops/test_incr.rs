use crate::common::{Client, ServerEnv, form_cluster};

fn run_incr(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut env2 = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, follower_p] = form_cluster([&mut env, &mut env2], false);

    let mut h = Client::new(leader_p.port);
    let mut h2 = Client::new(follower_p.port);

    // WHEN & THEN
    assert_eq!(h.send_and_get("INCR a", 1), vec!["(integer) 1"]);
    assert_eq!(h.send_and_get("INCR a", 1), vec!["(integer) 2"]);
    assert_eq!(h.send_and_get("INCR a", 1), vec!["(integer) 3"]);
    assert_eq!(h.send_and_get("GET a", 1), vec!["3"]);

    // WHEN & THEN- set a string and apply incr on the same key
    assert_eq!(h.send_and_get("SET c adsds", 1), vec!["OK"]);
    assert_eq!(
        h.send_and_get("INCR c", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN - out of range
    assert_eq!(h.send_and_get("SET d 92233720368547332375808", 1), vec!["OK"]);
    // THEN
    assert_eq!(
        h.send_and_get("INCR d", 1),
        vec!["(error) ERR value is not an integer or out of range"]
    );

    // WHEN&THEN- getting values from follower
    assert_eq!(h2.send_and_get("GET a", 1), vec!["3"]);

    Ok(())
}

#[test]
fn test_incr() -> anyhow::Result<()> {
    run_incr(false)?;
    run_incr(true)?;

    Ok(())
}
