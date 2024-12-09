use crate::services::statefuls::{cache::CacheEntry, persist::endec::TEncodingProcessor};
use tokio::sync::mpsc::{Receiver, Sender};

use super::{
    endec::{
        decoder::{byte_decoder::BytesDecoder, states::DecoderInit},
        encoder::encoding_processor::{EncodingMeta, EncodingProcessor},
    },
    DumpFile,
};

pub enum SaveActorCommand {
    LocalShardSize {
        table_size: usize,
        expiry_size: usize,
    },
    SaveChunk(Vec<CacheEntry>),
    StopSentinel,
}

pub struct Load;
pub struct Save;
pub struct PersistActor;

impl PersistActor {
    pub(crate) async fn dump(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes = tokio::fs::read(filepath).await?;

        let decoder: BytesDecoder<DecoderInit> = bytes.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
    pub fn run_save(
        filepath: String,
        num_of_cache_actors: usize,
        // TODO encoder seems to work as actual save actor.
    ) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(Self::save(filepath, num_of_cache_actors, inbox));
        outbox
    }

    async fn save(
        filepath: String,
        number_of_cache_actors: usize,
        mut inbox: Receiver<SaveActorCommand>,
    ) -> anyhow::Result<()> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await?;
        let mut processor = EncodingProcessor::new(file, EncodingMeta::new(number_of_cache_actors));

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
