use super::{WriteOperation, WriteRequest, interfaces::TWriteAheadLog};

pub(crate) struct ReplicatedLogs<T: TWriteAheadLog> {
    pub(crate) target: T,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64,
}

impl<T: TWriteAheadLog> ReplicatedLogs<T> {
    pub fn new(target: T, last_log_index: u64, last_log_term: u64) -> Self {
        Self { target, last_log_index, last_log_term }
    }

    pub(crate) async fn create_log_entries(
        &mut self,
        log: &WriteRequest,
        low_watermark: Option<u64>,
        term: u64,
    ) -> anyhow::Result<Vec<WriteOperation>> {
        let current_idx = self.last_log_index;
        self.write_log_entry(log, term).await?;

        if low_watermark.is_none() {
            return Ok(self.from(current_idx));
        }

        let mut logs = Vec::with_capacity((self.last_log_index - low_watermark.unwrap()) as usize);
        logs.extend(self.from(low_watermark.unwrap()));

        // ! Last log term must be updated because
        // ! log consistency check is based on previous log term and index
        self.last_log_term = term;
        Ok(logs)
    }

    pub(crate) async fn write_log_entry(
        &mut self,
        log: &WriteRequest,
        term: u64,
    ) -> anyhow::Result<()> {
        let op =
            WriteOperation { request: log.clone(), log_index: (self.last_log_index + 1), term };
        self.target.append(op).await?;
        self.last_log_index += 1;
        Ok(())
    }

    pub(crate) async fn write_log_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
    ) -> anyhow::Result<u64> {
        // Filter and append entries in a single operation
        let new_entries: Vec<_> =
            append_entries.into_iter().filter(|log| log.log_index > self.last_log_index).collect();

        let cnt = new_entries.len();
        self.target.append_many(new_entries).await?;
        self.last_log_index += cnt as u64;

        println!("[INFO] Received log entry with log index up to {}", self.last_log_index);
        Ok(self.last_log_index)
    }

    pub(crate) async fn overwrite(&mut self, ops: Vec<WriteOperation>) -> anyhow::Result<()> {
        let last_index = ops.len() as u64;
        self.target.overwrite(ops).await?;
        self.last_log_index = last_index;
        Ok(())
    }
    pub(crate) fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, end_inclusive)
    }

    fn from(&self, start_exclusive: u64) -> Vec<WriteOperation> {
        self.target.range(start_exclusive, self.last_log_index)
    }

    pub(crate) async fn read_at(&self, prev_log_index: u64) -> Option<WriteOperation> {
        self.target.read_at(prev_log_index).await
    }

    pub(crate) fn log_start_index(&self) -> u64 {
        self.target.log_start_index()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.target.is_empty()
    }

    pub(crate) async fn truncate_after(&mut self, log_index: u64) {
        self.target.truncate_after(log_index).await;
    }
}
