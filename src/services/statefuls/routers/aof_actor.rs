use super::cache_actor::CacheDb;
use crate::services::statefuls::command::AOFCommand;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tokio::time::interval;

#[derive(Default)]
pub struct AOFBuffer {
    pub buffer: CacheDb,
}

pub struct AOFActor {
    pub actor_ref_id: usize,
    pub aof_buffer: AOFBuffer,
    pub inbox: tokio::sync::mpsc::Receiver<AOFCommand>,
}

impl AOFActor {
    pub fn run(actor_ref_id: usize) -> Sender<AOFCommand> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let actor = Self {
            actor_ref_id,
            aof_buffer: AOFBuffer::default(),
            inbox,
        };
        tokio::spawn(actor.handle());
        outbox
    }

    pub async fn handle(self) {
        self.dump_manager();
        self.aof_buffer_manager();
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
    pub fn aof_buffer_manager(mut self) {
        tokio::spawn(async move {
            let mut aof_buffer = AOFBuffer::default();
            // TODO command needs to be changed..?
            while let Some(command) = self.inbox.recv().await {
                match command {
                    AOFCommand::Set { key, value, expiry } => {}
                    AOFCommand::Delete(key) => {}
                }
            }
        });
    }
}
