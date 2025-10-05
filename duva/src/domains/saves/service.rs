use crate::domains::saves::{actor::SaveActor, command::SaveCommand};
use tokio::sync::mpsc::Receiver;

impl SaveActor {
    pub async fn run(mut self, mut inbox: Receiver<SaveCommand>) -> anyhow::Result<Self> {
        while let Some(cmd) = inbox.recv().await {
            match self.handle_cmd(cmd).await {
                Ok(should_break) => {
                    if should_break {
                        break;
                    }
                },
                Err(err) => {
                    eprintln!("error while encoding: {err:?}");
                    return Err(err);
                },
            }
        }
        Ok(self)
    }
}
