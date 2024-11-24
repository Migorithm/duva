use super::cache_actor::CacheDb;

use tokio::sync::mpsc::Sender;
use tokio::time;
use tokio::time::interval;

#[derive(Default)]
pub struct AOFBuffer {
    pub buffer: CacheDb,
}

enum SaveActorCommand {}

pub struct SaveActor {
    pub aof_buffer: AOFBuffer,
    pub inbox: tokio::sync::mpsc::Receiver<SaveActorCommand>,
}

impl SaveActor {
    pub fn run() -> Sender<SaveActorCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let actor = Self {
            aof_buffer: AOFBuffer::default(),
            inbox,
        };
        tokio::spawn(actor.handle());
        outbox
    }

    pub async fn handle(self) {
        self.dump_manager();
    }

    pub fn dump_manager(&self) {
        tokio::spawn(async move {
            let mut dump_internal = interval(time::Duration::from_secs(1));
            loop {
                dump_internal.tick().await;
                // dump to disk using actor_id
            }
        });
    }
}
