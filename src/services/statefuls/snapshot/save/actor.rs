use crate::domains::save::actor::SaveActor;
use crate::domains::save::actor::SaveMeta;
use crate::domains::save::actor::SaveTarget;
use crate::domains::storage::cache_objects::CacheEntry;
use crate::services::error::IoError;

use crate::services::statefuls::snapshot::endec::encoder::byte_encoder::encode_checksum;
use crate::services::statefuls::snapshot::endec::encoder::byte_encoder::encode_database_info;
use crate::services::statefuls::snapshot::endec::encoder::byte_encoder::encode_database_table_size;
use crate::services::statefuls::snapshot::endec::encoder::byte_encoder::encode_header;
use crate::services::statefuls::snapshot::endec::encoder::byte_encoder::encode_metadata;
use crate::services::statefuls::snapshot::save::command::SaveCommand;
use anyhow::Result;

impl SaveActor {
    pub async fn new(
        target: SaveTarget,
        num_of_shards: usize,
        repl_id: String,
        current_offset: u64,
    ) -> Result<Self> {
        let meta = SaveMeta::new(num_of_shards, repl_id, current_offset.to_string());
        let mut processor = Self { target, meta };
        processor.encode_meta().await?;
        Ok(processor)
    }

    pub async fn run(
        mut self,
        mut inbox: tokio::sync::mpsc::Receiver<SaveCommand>,
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
        let mut metadata: Vec<(&str, &str)> = vec![];
        if &self.meta.repl_id != "?" {
            metadata.push(("repl-id", &self.meta.repl_id));
            metadata.push(("repl-offset", &self.meta.offset));
        }

        let meta = [encode_header("0011")?, encode_metadata(metadata)?, encode_database_info(0)?];
        self.target.write(&meta.concat()).await?;
        Ok(())
    }
    pub async fn handle_cmd(&mut self, cmd: SaveCommand) -> Result<bool> {
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
