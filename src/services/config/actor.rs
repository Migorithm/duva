use super::command::ConfigMessage;
use super::command::ConfigResource;
use super::command::ConfigResponse;

use super::init::get_env;
use super::ConfigCommand;

use tokio::sync::mpsc::Receiver;

#[derive(Clone)]
pub struct ConfigActor {
    pub(crate) dir: &'static str,
    pub dbfilename: &'static str,
}

impl ConfigActor {
    pub fn handle(mut self, mut inbox: Receiver<ConfigMessage>) {
        tokio::spawn(async move {
            while let Some(msg) = inbox.recv().await {
                match msg {
                    ConfigMessage::Query(query) => match query.resource {
                        ConfigResource::Dir => {
                            query.respond_with(ConfigResponse::Dir(self.dir.into()));
                        }
                        ConfigResource::DbFileName => {
                            query.respond_with(ConfigResponse::DbFileName(self.dbfilename.into()));
                        }
                        ConfigResource::FilePath => {
                            query.respond_with(ConfigResponse::FilePath(self.get_filepath()));
                        }
                    },
                    ConfigMessage::Command(config_command) => match config_command {
                        ConfigCommand::SetDbFileName(new_file_name) => {
                            self.set_dbfilename(&new_file_name);
                        }
                    },
                }
            }
        });
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
        let env = get_env();
        ConfigActor {
            dir: Box::leak(env.dir.clone().into_boxed_str()),
            dbfilename: Box::leak(env.dbfilename.clone().into_boxed_str()),
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
