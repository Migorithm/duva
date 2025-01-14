use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use super::endec::encoder::encoding_processor::EncodingProcessor;
use super::endec::encoder::encoding_processor::SaveMeta;
use super::DumpFile;

use crate::services::interface::TWriterFactory;
use crate::services::statefuls::persist::save_command::EncodingCommand;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Load;

pub(crate) struct PersistActor<T> {
    processor: T,
}

impl PersistActor<Load> {
    pub(crate) async fn load_file(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes = tokio::fs::read(filepath).await?;
        let decoder: BytesDecoder<DecoderInit> = bytes.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}

impl PersistActor<EncodingProcessor> {
    pub async fn run(
        filepath: String,
        num_of_cache_actors: usize,
    ) -> anyhow::Result<Sender<EncodingCommand>> {
        // * Propagate error to caller before sending it to the background
        let file = tokio::fs::File::create_writer(filepath).await?;

        let processor = EncodingProcessor::with_file(file, SaveMeta::new(num_of_cache_actors));

        let persist_actor = Self { processor };

        let (outbox, inbox) = tokio::sync::mpsc::channel(100);

        tokio::spawn(persist_actor.save(inbox));
        Ok(outbox)
    }

    async fn save(mut self, mut inbox: Receiver<EncodingCommand>) -> anyhow::Result<()> {
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
