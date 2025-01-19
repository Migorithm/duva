use crate::services::{interface::TWrite, statefuls::cache::CacheEntry};

use crate::services::statefuls::persist::encoding_command::EncodingCommand;
use anyhow::Result;

use super::byte_encoder::encode_checksum;
use super::byte_encoder::encode_database_info;
use super::byte_encoder::encode_database_table_size;
use super::byte_encoder::encode_header;
use super::byte_encoder::encode_metadata;
use crate::services::error::IoError;
use std::collections::VecDeque;
use tokio::io::AsyncWriteExt;

pub enum EncodingTarget {
    File(tokio::fs::File),
    InMemory(Vec<u8>),
}

impl EncodingTarget {
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        match self {
            EncodingTarget::File(f) => {
                f.write_all(buf).await
                    .map_err(|e| e.kind().into())
            }
            EncodingTarget::InMemory(v) => {
                Ok(v.extend_from_slice(buf))
            }
        }
    }
}

pub struct EncodingProcessor {
    pub(super) target: EncodingTarget,
    pub(super) meta: SaveMeta,
}

impl EncodingProcessor {
    pub fn with_file(file: tokio::fs::File, num_of_cache_actor: usize) -> Self {
        Self { target: EncodingTarget::File(file), meta: SaveMeta::new(num_of_cache_actor) }
    }

    pub fn with_vec(num_of_cache_actor: usize) -> Self {
        Self { target: EncodingTarget::InMemory(Vec::new()), meta: SaveMeta::new(num_of_cache_actor) }
    }

    pub async fn add_meta(&mut self) -> Result<()> {
        let meta = [
            encode_header("0011")?,
            encode_metadata(Vec::from([("redis-ver", "6.0.16")]))?,
            encode_database_info(0)?,
        ];
        self.target.write(&meta.concat()).await?;
        Ok(())
    }
    pub async fn handle_cmd(&mut self, cmd: EncodingCommand) -> Result<bool> {
        match cmd {
            EncodingCommand::LocalShardSize { table_size, expiry_size } => {
                self.meta.total_key_value_table_size += table_size;
                self.meta.total_expires_table_size += expiry_size;
                self.meta.num_of_saved_table_size_actor -= 1;
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.target
                        .write(&encode_database_table_size(
                            self.meta.total_key_value_table_size,
                            self.meta.total_expires_table_size,
                        )?)
                        .await?;
                }
            }
            EncodingCommand::SaveChunk(chunk) => {
                self.meta.chunk_queue.push_back(chunk);
                if self.meta.num_of_saved_table_size_actor == 0 {
                    self.encode_chunk_queue().await?;
                }
            }
            EncodingCommand::StopSentinel => {
                self.meta.num_of_cache_actors -= 1;
                if self.meta.num_of_cache_actors == 0 {
                    self.encode_chunk_queue().await?;
                    let checksum = encode_checksum(&[0; 8])?;
                    self.target.write(&checksum).await?;
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
                self.target.write(&encoded_chunk).await?;
            }
        }
        Ok(())
    }

    pub fn into_inner(self) -> Vec<u8> {
        if let EncodingTarget::InMemory(v) = self.target {
            v
        } else {
            panic!("cannot return inner for non InMemory type target")
        }
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