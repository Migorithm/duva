use tokio::sync::mpsc::Receiver;

use crate::services::{
    interfaces::ThreadSafeCloneable,
    statefuls::persist::{save_actor::SaveActorCommand, RdbFile},
};

pub trait TEnDecoder: TDecodeData + TEncodeData {}
impl<T: TDecodeData + TEncodeData> TEnDecoder for T {}
pub trait TDecodeData: ThreadSafeCloneable {
    fn decode_data(&self, bytes: Vec<u8>) -> anyhow::Result<RdbFile>;
}

pub trait TEncodeData: ThreadSafeCloneable {
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
