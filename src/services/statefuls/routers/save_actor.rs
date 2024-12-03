use crate::services::{interfaces::endec::TEncodeData, CacheEntry};
use tokio::sync::mpsc::Sender;

pub enum SaveActorCommand {
    LocalShardSize {
        table_size: usize,
        expiry_size: usize,
    },
    SaveChunk(Vec<CacheEntry>),
    StopSentinel,
}

pub struct SaveActor;

impl SaveActor {
    pub fn run<T: TEncodeData>(
        filepath: String,
        num_of_cache_actors: usize,
        // TODO encoder seems to work as actual save actor.
        encoder: T,
    ) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            encoder
                .encode_data(&filepath, inbox, num_of_cache_actors)
                .await
        });
        outbox
    }
}
