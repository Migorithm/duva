use crate::adapters::persistence::byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};
use crate::services::interfaces::endec::TEncodingProcessor;
use crate::services::statefuls::routers::save_actor::SaveActorCommand;
use crate::services::CacheEntry;
use anyhow::Result;
use std::collections::VecDeque;
use tokio::io::AsyncWriteExt;

pub struct FileEncodingProcessor {
    pub(super) file: tokio::fs::File,
    pub(super) meta: EncodingMeta,
}

pub(crate) struct EncodingMeta {
    num_of_saved_table_size_actor: usize,
    total_key_value_table_size: usize,
    total_expires_table_size: usize,
    chunk_queue: VecDeque<Vec<CacheEntry>>,
    num_of_cache_actors: usize,
}

impl TEncodingProcessor for FileEncodingProcessor {
    async fn add_meta(&mut self) -> Result<()> {
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];

        self.file.write_all(&meta.concat()).await?;
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
                    self.file
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
                    self.file.write_all(&checksum).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

impl FileEncodingProcessor {
    pub fn new(file: tokio::fs::File, meta: EncodingMeta) -> Self {
        Self { file, meta }
    }

    async fn encode_chunk_queue(&mut self) -> Result<()> {
        while let Some(chunk) = self.meta.chunk_queue.pop_front() {
            for kvs in chunk {
                let encoded_chunk = kvs.encode_with_key()?;
                self.file.write_all(&encoded_chunk).await?;
            }
        }
        Ok(())
    }
}

impl EncodingMeta {
    pub(in crate::adapters::persistence) fn new(num_of_cache_actors: usize) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
        }
    }
}
