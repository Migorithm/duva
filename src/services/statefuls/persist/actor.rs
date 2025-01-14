use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use super::endec::encoder::encoding_processor::InMemory;
use super::endec::encoder::encoding_processor::SaveMeta;
use super::endec::encoder::encoding_processor::SavingProcessor;
use super::DumpFile;
use tokio::fs::File;

use crate::services::interface::TWriterFactory;
use crate::services::statefuls::persist::save_command::SaveCommand;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Load;

pub(crate) struct PersistActor<T> {
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

impl PersistActor<SavingProcessor<InMemory>> {
    pub async fn run(in_memory: InMemory, num_of_cache_actors: usize) -> anyhow::Result<(Sender<SaveCommand>, tokio::task::JoinHandle<anyhow::Result<InMemory>>)> {
        let processor = SavingProcessor::new(in_memory, SaveMeta::new(num_of_cache_actors));
        let persist_actor = Self { processor };

        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let handle = tokio::spawn(async move {
            persist_actor.save(inbox).await
        });
        Ok((outbox, handle))
    }
    async fn save(mut self, mut inbox: Receiver<SaveCommand>) -> anyhow::Result<InMemory> {
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
        Ok(self.processor.into_inner())
    }
}

impl PersistActor<SavingProcessor<File>> {
    pub async fn run(
        filepath: String,
        num_of_cache_actors: usize,
    ) -> anyhow::Result<Sender<SaveCommand>> {
        // * Propagate error to caller before sending it to the background
        let file = tokio::fs::File::create_writer(filepath).await?;

        let processor = SavingProcessor::new(file, SaveMeta::new(num_of_cache_actors));

        let persist_actor = Self { processor };

        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(persist_actor.save(inbox));
        Ok(outbox)
    }
    async fn save(mut self, mut inbox: Receiver<SaveCommand>) -> anyhow::Result<()> {
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
