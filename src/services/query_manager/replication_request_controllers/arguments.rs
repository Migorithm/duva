use crate::make_smart_pointer;
use crate::services::query_manager::query_io::QueryIO;

#[derive(Debug, Clone)]
pub struct ReplicationRequestArguments(Vec<QueryIO>);

make_smart_pointer!(ReplicationRequestArguments, Vec<QueryIO>);

impl ReplicationRequestArguments {
    pub fn new(values: Vec<QueryIO>) -> Self {
        Self(values)
    }

    // take replica port info
    pub(crate) fn take_replica_port(&self) -> anyhow::Result<String> {
        let port = self.get(1).ok_or(anyhow::anyhow!("No value"))?;

        let (QueryIO::BulkString(port)) = port
        else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok(port.to_string())
    }
}
