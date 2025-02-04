use crate::services::cluster::replications::replication::Replication;
use crate::services::interface::TWriterFactory;
use crate::services::statefuls::cache::CacheEntry;

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

pub enum SaveTarget {
    File(String),
    InMemory(Vec<u8>),
}

impl SaveTarget {
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        match self {
            SaveTarget::File(f) => {
                let mut f = tokio::fs::File::create_writer(f.clone()).await.map_err(|e| {
                    println!("{e}");
                    IoError::Unknown
                })?;
                f.write_all(buf).await.map_err(|e| e.kind().into())
            }
            SaveTarget::InMemory(v) => {
                v.extend_from_slice(buf);
                Ok(())
            }
        }
    }
}

pub struct SaveActor {
    pub(super) target: SaveTarget,
    pub(super) meta: SaveMeta,
}

impl SaveActor {
    pub async fn new(
        target: SaveTarget,
        num_of_shards: usize,
        repl_info: Replication,
    ) -> Result<Self> {
        let meta = SaveMeta::new(
            num_of_shards,
            repl_info.master_replid.clone(),
            repl_info.master_repl_offset.to_string(),
        );
        let mut processor = Self { target, meta };
        processor.encode_meta().await?;
        Ok(processor)
    }

    pub async fn run(
        mut self,
        mut inbox: tokio::sync::mpsc::Receiver<EncodingCommand>,
    ) -> Result<Self> {
        while let Some(cmd) = inbox.recv().await {
            match self.handle_cmd(cmd).await {
                Ok(should_break) => {
                    if should_break {
                        break;
                    }
                }
                Err(err) => {
                    eprintln!("error while encoding: {:?}", err);
                    return Err(err);
                }
            }
        }
        Ok(self)
    }

    pub async fn encode_meta(&mut self) -> Result<()> {
        let mut metadata = vec![("redis-ver", "6.0.16")];
        if &self.meta.repl_id != "?" {
            metadata.push(("repl-id", &self.meta.repl_id));
            metadata.push(("repl-offset", &self.meta.offset));
        }

        let meta = [encode_header("0011")?, encode_metadata(metadata)?, encode_database_info(0)?];
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
        if let SaveTarget::InMemory(v) = self.target {
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
    repl_id: String,
    offset: String,
}

impl SaveMeta {
    pub fn new(num_of_cache_actors: usize, repl_id: String, offset: String) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
            repl_id,
            offset,
        }
    }
}
