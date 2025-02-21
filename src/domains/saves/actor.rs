use std::collections::VecDeque;

use crate::domains::{IoError, caches::cache_objects::CacheEntry};
use tokio::io::AsyncWriteExt;

use super::{
    command::SaveCommand,
    endec::encoder::byte_encoder::{
        encode_checksum, encode_database_info, encode_database_table_size, encode_header,
        encode_metadata,
    },
};

pub struct SaveActor {
    pub(crate) target: SaveTarget,
    pub(crate) meta: SaveMeta,
}

impl SaveActor {
    pub async fn new(
        target: SaveTarget,
        num_of_shards: usize,
        repl_id: String,
        current_offset: u64,
    ) -> anyhow::Result<Self> {
        let meta = SaveMeta::new(num_of_shards, repl_id, current_offset.to_string());
        let mut processor = Self { target, meta };
        processor.encode_meta().await?;
        Ok(processor)
    }

    pub async fn encode_meta(&mut self) -> anyhow::Result<()> {
        let mut metadata: Vec<(&str, &str)> = vec![];
        if &self.meta.repl_id != "?" {
            metadata.push(("repl-id", &self.meta.repl_id));
            metadata.push(("repl-offset", &self.meta.offset));
        }

        let meta = [encode_header("0011")?, encode_metadata(metadata)?, encode_database_info(0)?];
        self.target.write(&meta.concat()).await?;
        Ok(())
    }
    pub async fn handle_cmd(&mut self, cmd: SaveCommand) -> anyhow::Result<bool> {
        match cmd {
            SaveCommand::LocalShardSize { table_size, expiry_size } => {
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
                    self.target.write(&checksum).await?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn encode_chunk_queue(&mut self) -> anyhow::Result<()> {
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

pub enum SaveTarget {
    File(tokio::fs::File),
    InMemory(Vec<u8>),
}

impl SaveTarget {
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        match self {
            SaveTarget::File(f) => f.write_all(buf).await.map_err(|e| e.kind().into()),
            SaveTarget::InMemory(v) => {
                v.extend_from_slice(buf);
                Ok(())
            }
        }
    }
}

pub struct SaveMeta {
    pub(crate) num_of_saved_table_size_actor: usize,
    pub(crate) total_key_value_table_size: usize,
    pub(crate) total_expires_table_size: usize,
    pub(crate) chunk_queue: VecDeque<Vec<CacheEntry>>,
    pub(crate) num_of_cache_actors: usize,
    pub(crate) repl_id: String,
    pub(crate) offset: String,
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
