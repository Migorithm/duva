use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use super::endec::encoder::encoding_processor::EncodingMeta;
use super::endec::encoder::encoding_processor::EncodingProcessor;
use super::DumpFile;
use crate::services::query_manager::interface::TWriterFactory;
use crate::services::statefuls::cache::CacheEntry;
use crate::services::statefuls::persist::endec::TEncodingProcessor;
use tokio::sync::mpsc::{Receiver, Sender};

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
pub struct PersistActor<T> {
    processor: T,
}

impl PersistActor<Load> {
    pub(crate) async fn dump(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes = tokio::fs::read(filepath).await?;

        let decoder: BytesDecoder<DecoderInit> = bytes.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}

impl<T> PersistActor<EncodingProcessor<T>>
where
    T: TWriterFactory,
{
    pub async fn run(
        filepath: String,
        num_of_cache_actors: usize,
    ) -> anyhow::Result<Sender<SaveActorCommand>> {
        // * Propagate error to caller before sending it to the background
        let file = T::create_writer(filepath).await?;

        let processor = EncodingProcessor::new(file, EncodingMeta::new(num_of_cache_actors));

        let actor = Self { processor };

        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(actor.save(inbox));
        Ok(outbox)
    }

    async fn save(mut self, mut inbox: Receiver<SaveActorCommand>) -> anyhow::Result<()> {
        self.processor.add_meta().await?;
        while let Some(command) = inbox.recv().await {
            match self.processor.handle_cmd(command).await {
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
