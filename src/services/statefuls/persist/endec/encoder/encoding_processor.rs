use crate::services::{interface::TWrite, statefuls::cache::CacheEntry};

use crate::services::statefuls::persist::save_command::SaveCommand;
use anyhow::Result;

use std::collections::VecDeque;

use super::byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};

pub struct SavingProcessor<T: TWrite> {
    pub(super) writer: T,
    pub(super) meta: SaveMeta,
}

impl<T: TWrite> SavingProcessor<T> {
    pub fn new(file: T, meta: SaveMeta) -> Self {
        Self { writer: file, meta }
    }

    pub async fn add_meta(&mut self) -> Result<()> {
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];
        self.writer.write(&meta.concat()).await?;
        Ok(())
    }
    pub async fn handle_cmd(&mut self, cmd: SaveCommand) -> Result<bool> {
        match cmd {
            SaveCommand::LocalShardSize {
                table_size,
                expiry_size,
            } => {
                self.meta.total_key_value_table_size += table_size;
                self.meta.total_expires_table_size += expiry_size;
                self.meta.num_of_saved_table_size_actor -= 1;
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.writer
                        .write(&encode_database_table_size(
                            self.meta.total_key_value_table_size,
                            self.meta.total_expires_table_size,
                        )?)
                        .await?;
                }
            }
            SaveCommand::SaveChunk(chunk) => {
                self.meta.chunk_queue.push_back(chunk);
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.encode_chunk_queue().await?;
                }
            }
            SaveCommand::StopSentinel => {
                self.meta.num_of_cache_actors -= 1;
                if self.meta.num_of_cache_actors == 0 {
                    self.encode_chunk_queue().await?;
                    let checksum = encode_checksum(&[0; 8])?;
                    self.writer.write(&checksum).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn encode_chunk_queue(&mut self) -> Result<()> {
        while let Some(chunk) = self.meta.chunk_queue.pop_front() {
            for kvs in chunk {
                let encoded_chunk = kvs.encode_with_key()?;
                self.writer.write(&encoded_chunk).await?;
            }
        }
        Ok(())
    }
}

pub struct SaveMeta {
    num_of_saved_table_size_actor: usize,
    total_key_value_table_size: usize,
    total_expires_table_size: usize,
    chunk_queue: VecDeque<Vec<CacheEntry>>,
    num_of_cache_actors: usize,
}

impl SaveMeta {
    pub fn new(num_of_cache_actors: usize) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
        }
    }
}
