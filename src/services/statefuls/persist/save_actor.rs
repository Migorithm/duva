use crate::services::{statefuls::persist::endec::TEncodingProcessor, CacheEntry};
use tokio::sync::mpsc::{Receiver, Sender};

use super::endec::TEncodeData;

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
    pub fn run(
        filepath: String,
        num_of_cache_actors: usize,
        // TODO encoder seems to work as actual save actor.
        encoder: impl TEncodeData,
    ) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(Self::handle(encoder, filepath, num_of_cache_actors, inbox));
        outbox
    }

    async fn handle(
        encoder: impl TEncodeData,
        filepath: String,
        number_of_cache_actors: usize,
        mut inbox: Receiver<SaveActorCommand>,
    ) -> anyhow::Result<()> {
        let mut processor = encoder
            .create_encoding_processor(&filepath, number_of_cache_actors)
            .await?;

        processor.add_meta().await?;
        while let Some(command) = inbox.recv().await {
            match processor.handle_cmd(command).await {
                Ok(should_break) => {
                    if should_break {
                        break;
                    }
                }
                Err(err) => {
                    eprintln!("error while encoding: {:?}", err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}
