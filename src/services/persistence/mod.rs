use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

pub enum PersistEnum {
    Set(String, String),
    Get(String, Sender<Option<String>>),
    StopSentinel,
}

async fn persist_handler(mut recv: Receiver<PersistEnum>) -> Result<()> {
    // inner state
    let mut db = HashMap::<String, String>::new();

    while let Some(command) = recv.recv().await {
        match command {
            PersistEnum::StopSentinel => break,
            PersistEnum::Set(k, v) => {
                db.insert(k, v);
            }
            PersistEnum::Get(key, sender) => match sender.send(db.get(key.as_str()).cloned()) {
                Ok(_) => {}
                Err(err) => println!("Error: {:?}", err),
            },
        }
    }
    Ok(())
}
