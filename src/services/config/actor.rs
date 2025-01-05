use std::sync::atomic::{AtomicBool, Ordering};

use super::{
    command::{ConfigMessage, ConfigResource, ConfigResponse},
    ConfigCommand,
};
use crate::{env_var, services::config::replication::Replication};
use tokio::sync::mpsc::Receiver;

pub static IS_LEADER_MODE: AtomicBool = AtomicBool::new(true);

#[derive(Clone)]
pub struct ConfigActor {
    pub(crate) dir: &'static str,
    pub dbfilename: &'static str,
    pub replication: Replication,
}

impl ConfigActor {
    pub fn handle(
        mut self,
        mut inbox: Receiver<ConfigMessage>,
    ) -> tokio::sync::watch::Receiver<bool> {
        // inner state
        let (notifier, watcher) =
            tokio::sync::watch::channel(IS_LEADER_MODE.load(Ordering::Relaxed));

        tokio::spawn(async move {
            let notifier = notifier;

            while let Some(msg) = inbox.recv().await {
                match msg {
                    ConfigMessage::Query(query) => match query.resource {
                        ConfigResource::Dir => {
                            let _ = query.respond_with(ConfigResponse::Dir(self.dir.into()));
                        }
                        ConfigResource::DbFileName => {
                            let _ = query
                                .respond_with(ConfigResponse::DbFileName(self.dbfilename.into()));
                        }
                        ConfigResource::FilePath => {
                            let _ =
                                query.respond_with(ConfigResponse::FilePath(self.get_filepath()));
                        }
                        ConfigResource::ReplicationInfo => {
                            let _ = query.respond_with(ConfigResponse::ReplicationInfo(
                                self.replication.clone(),
                            ));
                        }
                    },
                    ConfigMessage::Command(config_command) => match config_command {
                        ConfigCommand::ReplicaPing => {
                            // ! Deprecated
                            self.replication.connected_slaves += 1;
                        }
                        ConfigCommand::SetDbFileName(new_file_name) => {
                            self.set_dbfilename(&new_file_name);
                        }
                    },
                }
            }
        });
        watcher
    }

    pub fn set_dir(&mut self, dir: &str) {
        unsafe {
            // Get the pointer to the str
            let ptr: *const str = self.dir;
            // Recreate the Box from the pointer and drop it
            let _reclaimed_box: Box<str> = Box::from_raw(ptr as *mut str);
            self.dir = Box::leak(dir.into());
        }
    }
    pub fn set_dbfilename(&mut self, dbfilename: &str) {
        unsafe {
            // Get the pointer to the str
            let ptr: *const str = self.dbfilename;
            // Recreate the Box from the pointer and drop it
            let _reclaimed_box: Box<str> = Box::from_raw(ptr as *mut str);
            self.dbfilename = Box::leak(dbfilename.into());
        }
    }

    pub fn get_filepath(&self) -> String {
        format!("{}/{}", self.dir, self.dbfilename)
    }
}

impl Default for ConfigActor {
    fn default() -> Self {
        env_var!(
            {
                replicaof
            }
            {
                dir = ".".to_string(),
                dbfilename = "dump.rdb".to_string()
            }
        );

        let replicaof = replicaof.map(|host_port| {
            host_port
                .split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .into_iter()
                .collect::<(_, _)>()
        });

        let replication = Replication::new(replicaof);

        IS_LEADER_MODE.store(
            replication.master_port.is_none(),
            std::sync::atomic::Ordering::Relaxed,
        );

        ConfigActor {
            dir: Box::leak(dir.into_boxed_str()),
            dbfilename: Box::leak(dbfilename.into_boxed_str()),
            replication,
        }
    }
}

#[test]
fn test_set_dir() {
    let mut config = ConfigActor::default();
    config.set_dir("test");
    assert_eq!(config.dir, "test");
    config.set_dir("test2");
    assert_eq!(config.dir, "test2");
}
