use std::hash::{DefaultHasher, Hasher};

pub fn get_hash(s: &str) -> Hash {
    let mut hasher = DefaultHasher::new();
    std::hash::Hash::hash(&s, &mut hasher);
    Hash(hasher.finish() as usize)
}

pub struct Hash(usize);

impl Hash {
    pub fn shard_key(self, num_shards: usize) -> usize {
        self.0 % num_shards
    }
}
