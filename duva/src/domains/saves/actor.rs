use super::{
    command::SaveCommand,
    endec::encoder::{
        encode_checksum, encode_database_info, encode_database_table_size, encode_header,
        encode_metadata,
    },
};
use crate::domains::saves::snapshot::Metadata;
use crate::domains::{
    IoError, caches::cache_objects::CacheEntry, cluster_actors::replication::ReplicationId,
};
use std::collections::VecDeque;
use tokio::io::AsyncWriteExt;

pub struct SaveActor {
    pub(crate) target: SaveTarget,
    pub(crate) meta: SaveMeta,
}

impl SaveActor {
    pub(crate) async fn new(
        target: SaveTarget,
        num_of_shards: usize,
        repl_id: ReplicationId,
        current_offset: u64,
    ) -> anyhow::Result<Self> {
        let meta = SaveMeta::new(num_of_shards, repl_id, current_offset);
        let mut processor = Self { target, meta };
        processor.encode_meta().await?;
        Ok(processor)
    }

    pub async fn encode_meta(&mut self) -> anyhow::Result<()> {
        let metadata = Metadata { repl_id: self.meta.repl_id.clone(), log_idx: self.meta.offset };
        let meta = [encode_header()?, encode_metadata(metadata)?, encode_database_info(0)?];
        self.target.write(&meta.concat()).await?;
        Ok(())
    }
    pub async fn handle_cmd(&mut self, cmd: SaveCommand) -> anyhow::Result<bool> {
        match cmd {
            | SaveCommand::LocalShardSize { table_size, expiry_size } => {
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
                    self.encode_chunk_queue().await?;
                }
            },
            | SaveCommand::SaveChunk(chunk) => {
                self.meta.chunk_queue.push_back(chunk);
            },
            | SaveCommand::StopSentinel => {
                self.meta.num_of_cache_actors -= 1;
                if self.meta.num_of_cache_actors == 0 {
                    self.encode_chunk_queue().await?;
                    let checksum = encode_checksum(&[0; 8])?;
                    self.target.write(&checksum).await?;
                    return Ok(true);
                }
            },
        }
        Ok(false)
    }

    async fn encode_chunk_queue(&mut self) -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        while let Some(chunk) = self.meta.chunk_queue.pop_front() {
            for kvs in chunk {
                let encoded_chunk = kvs.encode_with_key()?;
                buffer.extend_from_slice(&encoded_chunk);
            }
        }
        if !buffer.is_empty() {
            self.target.write(&buffer).await?;
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
            | SaveTarget::File(f) => f.write_all(buf).await.map_err(|e| e.kind().into()),
            | SaveTarget::InMemory(v) => {
                v.extend_from_slice(buf);
                Ok(())
            },
        }
    }
}

pub struct SaveMeta {
    pub(crate) num_of_saved_table_size_actor: usize,
    pub(crate) total_key_value_table_size: usize,
    pub(crate) total_expires_table_size: usize,
    pub(crate) chunk_queue: VecDeque<Vec<CacheEntry>>,
    pub(crate) num_of_cache_actors: usize,
    pub(crate) repl_id: ReplicationId,
    pub(crate) offset: u64,
}

impl SaveMeta {
    pub(crate) fn new(num_of_cache_actors: usize, repl_id: ReplicationId, offset: u64) -> Self {
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
