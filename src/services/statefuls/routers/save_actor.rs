use super::cache_actor::CacheChunk;
use crate::services::interfaces::endec::TEncodeData;
use tokio::sync::mpsc::Sender;

pub enum SaveActorCommand {
    SaveTableSize(usize, usize),
    SaveChunk(CacheChunk),
    StopSentinel,
}

pub struct SaveActor<T: TEncodeData> {
    filepath: String,
    pub num_of_cache_actors: usize,
    pub inbox: tokio::sync::mpsc::Receiver<SaveActorCommand>,
    encoder: T,
}

impl<T: TEncodeData> SaveActor<T> {
    pub fn run(
        filepath: String,
        num_of_cache_actors: usize,
        encoder: T,
    ) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let actor = Self {
            filepath,
            inbox,
            num_of_cache_actors,
            encoder,
        };
        tokio::spawn(actor.handle());
        outbox
    }

    pub async fn handle(mut self) -> anyhow::Result<()> {
        self.encoder
            .encode_data(&self.filepath, &mut self.inbox, self.num_of_cache_actors)
            .await?;
        Ok(())
    }
}
