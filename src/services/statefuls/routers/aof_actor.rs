use super::cache_actor::CacheDb;

use tokio::sync::mpsc::Sender;
use tokio::time;
use tokio::time::interval;

pub enum SaveActorCommand {
    SaveChunk(Vec<(String, String)>),
    StopSentinel,
}

pub struct SaveActor {
    pub num_of_cache_actors: usize,
    pub inbox: tokio::sync::mpsc::Receiver<SaveActorCommand>,
}

impl SaveActor {
    pub fn run(num_of_cache_actors: usize) -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let actor = Self {
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
                    for (k, v) in chunk {
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
