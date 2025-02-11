use super::actor::CacheActor;
use super::actor::CacheCommand;
use super::actor::CacheCommandSender;
use super::CacheEntry;
use crate::services::cluster::replications::replication::ReplicationInfo;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::ttl::actor::TtlActor;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::snapshot::save::actor::SaveActor;
use crate::services::statefuls::snapshot::save::actor::SaveTarget;
use crate::services::statefuls::snapshot::dump_file::DumpFile;
use anyhow::Result;
use std::time::SystemTime;
use std::{hash::Hasher, iter::Zip};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone)]
pub(crate) struct CacheManager {
    pub(crate) inboxes: Vec<CacheCommandSender>,
}

impl CacheManager {
    pub(crate) async fn dump_cache(
        &self,
        dump: DumpFile,
        ttl_inbox: TtlSchedulerInbox,
        startup_time: SystemTime,
    ) -> Result<()> {
        let startup_time = &startup_time;
        for kvs in dump.key_values().into_iter().filter(|kvs| kvs.is_valid(startup_time)) {
            self.route_set(kvs, ttl_inbox.clone()).await?;
        }

        Ok(())
    }

    pub(crate) async fn route_get(&self, key: String) -> Result<QueryIO> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key).send(CacheCommand::Get { key, sender: tx }).await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(
        &self,
        kvs: CacheEntry,
        ttl_sender: TtlSchedulerInbox,
    ) -> Result<QueryIO> {
        self.select_shard(kvs.key())
            .send(CacheCommand::Set { cache_entry: kvs, ttl_sender })
            .await?;
        Ok(QueryIO::SimpleString("OK".to_string().into()))
    }

    pub(crate) async fn route_save(
        &self,
        save_target: SaveTarget,
        repl_info: ReplicationInfo,
    ) -> Result<JoinHandle<anyhow::Result<SaveActor>>> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let save_actor = SaveActor::new(save_target, self.inboxes.len(), repl_info).await?;

        // get all the handlers to cache actors
        for cache_handler in self.inboxes.iter().map(Clone::clone) {
            let outbox = outbox.clone();
            tokio::spawn(async move {
                let _ = cache_handler.send(CacheCommand::Save { outbox }).await;
            });
        }

        //* defaults to BGSAVE but optionally waitable
        Ok(tokio::spawn(save_actor.run(inbox)))
    }

    // Send recv handler firstly to the background and return senders and join handlers for receivers
    fn ontshot_channels<T: Send + Sync + 'static>(
        &self,
    ) -> (Vec<OneShotSender<T>>, Vec<OneShotReceiverJoinHandle<T>>) {
        (0..self.inboxes.len())
            .map(|_| {
                let (sender, recv) = tokio::sync::oneshot::channel();
                let recv_handler = tokio::spawn(recv);
                (sender, recv_handler)
            })
            .unzip()
    }

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<QueryIO> {
        let (senders, receivers) = self.ontshot_channels();

        // send keys to shards
        self.chain(senders).for_each(|(shard, sender)| {
            tokio::spawn(Self::send_keys_to_shard(shard.clone(), pattern.clone(), sender));
        });
        let mut keys = Vec::new();
        for v in receivers {
            if let Ok(QueryIO::Array(v)) = v.await? {
                keys.extend(v)
            }
        }
        Ok(QueryIO::Array(keys))
    }

    fn chain<T>(
        &self,
        senders: Vec<Sender<T>>,
    ) -> Zip<std::slice::Iter<'_, CacheCommandSender>, std::vec::IntoIter<Sender<T>>> {
        self.inboxes.iter().zip(senders)
    }

    // stateless function to send keys
    async fn send_keys_to_shard(
        shard: CacheCommandSender,
        pattern: Option<String>,
        tx: OneShotSender<QueryIO>,
    ) -> Result<()> {
        Ok(shard.send(CacheCommand::Keys { pattern: pattern.clone(), sender: tx }).await?)
    }

    pub(crate) fn select_shard(&self, key: &str) -> &CacheCommandSender {
        let shard_key = self.take_shard_key_from_str(key);
        &self.inboxes[shard_key]
    }

    fn take_shard_key_from_str(&self, s: &str) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        hasher.finish() as usize % self.inboxes.len()
    }

    pub fn run_cache_actors() -> (CacheManager, TtlSchedulerInbox) {
        const NUM_OF_PERSISTENCE: usize = 10;

        let cache_dispatcher = CacheManager {
            inboxes: (0..NUM_OF_PERSISTENCE).map(|_| CacheActor::run()).collect::<Vec<_>>(),
        };

        let ttl_inbox = cache_dispatcher.run_ttl_actors();
        (cache_dispatcher, ttl_inbox)
    }

    fn run_ttl_actors(&self) -> TtlSchedulerInbox {
        TtlActor::run(self.clone())
    }
}
