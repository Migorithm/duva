use crate::services::{
    interfaces::endec::{Processable, TEncodeData},
    statefuls::routers::{cache_actor::CacheChunk, save_actor::SaveActorCommand},
};
use byte_encoder::{
    encode_checksum, encode_database_info, encode_database_table_size, encode_header,
    encode_metadata,
};
use std::collections::VecDeque;
use tokio::io::AsyncWriteExt;

pub mod byte_encoder;

pub struct FileProcessor {
    file: tokio::fs::File,
    num_of_saved_table_size_actor: usize,
    total_key_value_table_size: usize,
    total_expires_table_size: usize,
    chunk_queue: VecDeque<CacheChunk>,
    num_of_cache_actors: usize,
}

impl Processable for FileProcessor {
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
            SaveActorCommand::SaveTableSize(key_value_table_size, expires_table_size) => {
                self.num_of_saved_table_size_actor -= 1;
                if self.num_of_saved_table_size_actor == 0 {
                    self.file
                        .write_all(&encode_database_table_size(
                            self.total_key_value_table_size,
                            self.total_expires_table_size,
                        )?)
                        .await?;
                } else {
                    self.total_key_value_table_size += key_value_table_size;
                    self.total_expires_table_size += expires_table_size;
                }
            }
            SaveActorCommand::SaveChunk(chunk) => {
                if self.num_of_saved_table_size_actor != 0 {
                    self.chunk_queue.push_back(chunk);
                } else {
                    self.chunk_queue.push_back(chunk);
                    while let Some(chunk) = self.chunk_queue.pop_front() {
                        let chunk = chunk.0;
                        for (key, value) in chunk {
                            let encoded_chunk = value.encode_with_key(&key)?;
                            self.file.write_all(&encoded_chunk).await?;
                        }
                    }
                }
            }
            SaveActorCommand::StopSentinel => {
                self.num_of_cache_actors -= 1;
                if self.num_of_cache_actors == 0 {
                    while let Some(chunk) = self.chunk_queue.pop_front() {
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

impl TEncodeData for tokio::fs::OpenOptions {
    async fn create_on_path(
        filepath: &str,
        num_of_cache_actors: usize,
    ) -> anyhow::Result<FileProcessor> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await?;
        Ok(FileProcessor {
            file,
            num_of_saved_table_size_actor: Default::default(),
            total_key_value_table_size: Default::default(),
            total_expires_table_size: Default::default(),
            chunk_queue: Default::default(),
            num_of_cache_actors,
        })
    }
}
