use tokio::sync::mpsc::Receiver;

use crate::services::statefuls::persist::{save_actor::SaveActorCommand, RdbFile};

use super::ThreadSafeCloneable;

pub trait TEnDecoder: TDecodeData + TEncodeData {}
impl<T: TDecodeData + TEncodeData> TEnDecoder for T {}
pub trait TDecodeData: ThreadSafeCloneable {
    fn decode_data(&self, bytes: Vec<u8>) -> anyhow::Result<RdbFile>;
}

pub trait TEncodeData: ThreadSafeCloneable {
    /// ** Template method pattern
    ///    `create_on_path` and its return will determine where the file will be saved
    fn encode_data(
        &self,
        filepath: &str,
        mut inbox: Receiver<SaveActorCommand>,
        number_of_cache_actors: usize,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        async move {
            let mut processor = self
                .create_encoding_processor(filepath, number_of_cache_actors)
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
    fn create_encoding_processor(
        &self,
        filepath: &str,
        number_of_cache_actors: usize,
    ) -> impl std::future::Future<Output = anyhow::Result<impl TEncodingProcessor>> + Send;
}

pub trait TEncodingProcessor: Send + Sync {
    fn add_meta(&mut self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
    fn handle_cmd(
        &mut self,
        cmd: SaveActorCommand,
    ) -> impl std::future::Future<Output = anyhow::Result<bool>> + Send;
}
