use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const LENGTH: usize = 40;

pub(crate) fn generate_replid() -> String {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64;

    let pid = process::id() as u64;

    // each entropy source occupies distinct bit regions, reducing the chance of collisions.
    let seed = (pid << 32) ^ time;

    let mut rng = StdRng::seed_from_u64(seed);
    let random_string: String = (0..LENGTH)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();

    random_string
}

#[test]
fn test_generate_replid() {
    let replid = generate_replid();
    assert_eq!(replid.len(), 40);
    println!("{}", replid);
}