use crate::adapters::endec::byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};
use crate::services::query_manager::interface::TWrite;
use crate::services::statefuls::cache::CacheEntry;
use crate::services::statefuls::persist::endec::TEncodingProcessor;
use crate::services::statefuls::persist::save_actor::SaveActorCommand;
use anyhow::Result;

use std::collections::VecDeque;

pub struct EncodingProcessor<T: TWrite> {
    pub(super) writer: T,
    pub(super) meta: EncodingMeta,
}

pub(crate) struct EncodingMeta {
    num_of_saved_table_size_actor: usize,
    total_key_value_table_size: usize,
    total_expires_table_size: usize,
    chunk_queue: VecDeque<Vec<CacheEntry>>,
    num_of_cache_actors: usize,
}

impl<T: TWrite> TEncodingProcessor for EncodingProcessor<T> {
    async fn add_meta(&mut self) -> Result<()> {
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];
        self.writer.write_all(&meta.concat()).await?;
        Ok(())
    }
    async fn handle_cmd(&mut self, cmd: SaveActorCommand) -> Result<bool> {
        match cmd {
            SaveActorCommand::LocalShardSize {
                table_size,
                expiry_size,
            } => {
                self.meta.total_key_value_table_size += table_size;
                self.meta.total_expires_table_size += expiry_size;
                self.meta.num_of_saved_table_size_actor -= 1;
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.writer
                        .write_all(&encode_database_table_size(
                            self.meta.total_key_value_table_size,
                            self.meta.total_expires_table_size,
                        )?)
                        .await?;
                }
            }
            SaveActorCommand::SaveChunk(chunk) => {
                self.meta.chunk_queue.push_back(chunk);
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.encode_chunk_queue().await?;
                }
            }
            SaveActorCommand::StopSentinel => {
                self.meta.num_of_cache_actors -= 1;
                if self.meta.num_of_cache_actors == 0 {
                    self.encode_chunk_queue().await?;
                    let checksum = encode_checksum(&[0; 8])?;
                    self.writer.write_all(&checksum).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl<T: TWrite> EncodingProcessor<T> {
    pub fn new(file: T, meta: EncodingMeta) -> Self {
        Self { writer: file, meta }
    }

    async fn encode_chunk_queue(&mut self) -> Result<()> {
        while let Some(chunk) = self.meta.chunk_queue.pop_front() {
            for kvs in chunk {
                let encoded_chunk = kvs.encode_with_key()?;
                self.writer.write_all(&encoded_chunk).await?;
            }
        }
        Ok(())
    }
}

impl EncodingMeta {
    pub(in crate::adapters::endec) fn new(num_of_cache_actors: usize) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
        }
    }
}
