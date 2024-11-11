use tokio::time::{self, interval};

use crate::services::statefuls::{command::AOFCommand, CacheDb};

#[derive(Default)]
pub struct AOFBuffer {
    pub buffer: CacheDb,
}

pub async fn run_aof_actor(mut recv: tokio::sync::mpsc::Receiver<AOFCommand>, actor_id: usize) {
    tokio::spawn(async move {
        let mut dump_internal = interval(time::Duration::from_secs(1));
        loop {
            dump_internal.tick().await;
            // dump to disk using actor_id
        }
    });

    tokio::spawn(async move {
        let mut aof_buffer = AOFBuffer::default();
        // TODO command needs to be changed..?
        while let Some(command) = recv.recv().await {
            match command {
                AOFCommand::Set { key, value, expiry } => {}
                AOFCommand::Delete(key) => {}
            }
        }
    });
}
