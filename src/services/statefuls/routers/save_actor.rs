use super::cache_actor::CacheChunk;
use crate::adapters::persistence::byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};
use std::collections::VecDeque;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Sender;

pub enum SaveActorCommand {
    SaveTableSize(usize, usize),
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

    pub async fn handle(mut self) -> anyhow::Result<()> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.filepath)
            .await?;

        // TODO move to adapter logic as it pertains to IO
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];

        file.write_all(&meta.concat()).await?;

        let mut num_of_saved_table_size_actor = self.num_of_cache_actors;
        let mut total_key_value_table_size = 0;
        let mut total_expires_table_size = 0;
        let mut chunk_queue = VecDeque::new();

        while let Some(command) = self.inbox.recv().await {
            match command {
                SaveActorCommand::SaveTableSize(key_value_table_size, expires_table_size) => {
                    num_of_saved_table_size_actor -= 1;
                    if num_of_saved_table_size_actor == 0 {
                        file.write_all(&encode_database_table_size(
                            total_key_value_table_size,
                            total_expires_table_size,
                        )?)
                        .await?;
                    } else {
                        total_key_value_table_size += key_value_table_size;
                        total_expires_table_size += expires_table_size;
                    }
                }
                SaveActorCommand::SaveChunk(chunk) => {
                    if num_of_saved_table_size_actor != 0 {
                        chunk_queue.push_back(chunk);
                    } else {
                        chunk_queue.push_back(chunk);
                        while let Some(chunk) = chunk_queue.pop_front() {
                            let chunk = chunk.0;
                            for (key, value) in chunk {
                                let encoded_chunk = value.encode_with_key(&key)?;
                                file.write_all(&encoded_chunk).await?;
                            }
                        }
                    }
                }
                SaveActorCommand::StopSentinel => {
                    self.num_of_cache_actors -= 1;
                    if self.num_of_cache_actors == 0 {
                        while let Some(chunk) = chunk_queue.pop_front() {
                            let chunk = chunk.0;
                            for (key, value) in chunk {
                                let encoded_chunk = value.encode_with_key(&key)?;
                                file.write_all(&encoded_chunk).await?;
                            }
                        }
                        let checksum = encode_checksum(&[0; 8])?;
                        file.write_all(&checksum).await?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
