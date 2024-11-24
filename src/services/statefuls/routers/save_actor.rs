use tokio::sync::mpsc::Sender;

use super::cache_actor::CacheChunk;

pub enum SaveActorCommand {
    SaveChunk(CacheChunk),
    StopSentinel,
}

pub struct SaveActor {
    filepath: String,
    pub num_of_cache_actors: usize,
    pub inbox: tokio::sync::mpsc::Receiver<SaveActorCommand>,
}

impl SaveActor {
    pub fn run(filepath: String, num_of_cache_actors: usize) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let actor = Self {
            filepath,
            inbox,
            num_of_cache_actors,
        };
        tokio::spawn(actor.handle());
        outbox
    }

    pub async fn handle(mut self) {
        while let Some(command) = self.inbox.recv().await {
            match command {
                SaveActorCommand::SaveChunk(chunk) => {
                    for (k, v) in chunk.0 {
                        //SAVE operation
                    }
                }
                SaveActorCommand::StopSentinel => {
                    self.num_of_cache_actors -= 1;
                    if self.num_of_cache_actors == 0 {
                        break;
                    }
                }
            }
        }
    }
}
