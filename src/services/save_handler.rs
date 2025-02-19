use crate::domains::saves::{actor::SaveActor, command::SaveCommand};

impl SaveActor {
    pub async fn run(
        mut self,
        mut inbox: tokio::sync::mpsc::Receiver<SaveCommand>,
    ) -> anyhow::Result<Self> {
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
}
