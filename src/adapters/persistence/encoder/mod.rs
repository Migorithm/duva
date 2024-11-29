use crate::services::{
    interfaces::endec::Processable,
    statefuls::routers::{cache_actor::CacheChunk, save_actor::SaveActorCommand},
};
use byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};
use std::collections::VecDeque;
use tokio::io::AsyncWriteExt;

pub mod byte_encoder;

pub struct EncodingProcessor {
    pub(super) file: tokio::fs::File,
    pub(super) meta: EncodingMeta,
}

pub(super) struct EncodingMeta {
    num_of_saved_table_size_actor: usize,
    total_key_value_table_size: usize,
    total_expires_table_size: usize,
    chunk_queue: VecDeque<CacheChunk>,
    num_of_cache_actors: usize,
}
impl EncodingMeta {
    pub(super) fn new(num_of_cache_actors: usize) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
        }
    }
}

impl Processable for EncodingProcessor {
    async fn add_meta(&mut self) -> anyhow::Result<()> {
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];

        self.file.write_all(&meta.concat()).await?;
        Ok(())
    }
    async fn handle_cmd(&mut self, cmd: SaveActorCommand) -> anyhow::Result<bool> {
        match cmd {
            SaveActorCommand::LocalShardSize {
                total_size,
                expiry_size,
            } => {
                self.meta.total_key_value_table_size += total_size;
                self.meta.total_expires_table_size += expiry_size;
                self.meta.num_of_saved_table_size_actor -= 1;
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.file
                        .write_all(&encode_database_table_size(
                            self.meta.total_key_value_table_size,
                            self.meta.total_expires_table_size,
                        )?)
                        .await?;
                }
            }
            SaveActorCommand::SaveChunk(chunk) => {
                if self.meta.num_of_saved_table_size_actor != 0 {
                    self.meta.chunk_queue.push_back(chunk);
                } else {
                    self.meta.chunk_queue.push_back(chunk);
                    while let Some(chunk) = self.meta.chunk_queue.pop_front() {
                        let chunk = chunk.0;
                        for (key, value) in chunk {
                            let encoded_chunk = value.encode_with_key(&key)?;
                            self.file.write_all(&encoded_chunk).await?;
                        }
                    }
                }
            }
            SaveActorCommand::StopSentinel => {
                self.meta.num_of_cache_actors -= 1;
                if self.meta.num_of_cache_actors == 0 {
                    while let Some(chunk) = self.meta.chunk_queue.pop_front() {
                        let chunk = chunk.0;
                        for (key, value) in chunk {
                            let encoded_chunk = value.encode_with_key(&key)?;
                            self.file.write_all(&encoded_chunk).await?;
                        }
                    }
                    let checksum = encode_checksum(&[0; 8])?;
                    self.file.write_all(&checksum).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
