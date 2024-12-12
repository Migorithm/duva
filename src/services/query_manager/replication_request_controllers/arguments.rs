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
    pub(crate) fn take_replica_port(self) -> anyhow::Result<String> {
        let port = self.get(1).ok_or(anyhow::anyhow!("No value"))?;

        let (QueryIO::BulkString(port)) = port else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok(port.to_string())
    }
    pub(crate) fn take_capabilities(self) -> anyhow::Result<Vec<(String, String)>> {
        let mut capabilities = Vec::new();
        for i in 0..self.len() {
            if i % 2 == 1 {
                continue;
            }
            let capa = self.get(i).ok_or(anyhow::anyhow!("No value"))?;
            let QueryIO::BulkString(capa) = capa else {
                return Err(anyhow::anyhow!("Invalid arguments"));
            };
            if capa != "capa" {
                return Err(anyhow::anyhow!("Invalid arguments"));
            }
            let Some(QueryIO::BulkString(value)) = self.get(i + 1) else {
                return Err(anyhow::anyhow!("Invalid arguments"));
            };
            // last value must be psync2
            if i == self.len() - 2 && value != "psync2" {
                return Err(anyhow::anyhow!("Invalid arguments"));
            }
            capabilities.push((capa.to_string(), value.to_string()));
        }
        Ok(capabilities)
    }
    pub(crate) fn take_psync(self) -> anyhow::Result<(String, i64)> {
        let replica_id = self
            .get(0)
            .map(|v| v.clone().unpack_bulk_str())
            .ok_or(anyhow::anyhow!("No replica id"))??;
        let offset = self
            .get(1)
            .map(|v| v.clone().unpack_bulk_str().map(|s| s.parse::<i64>()))
            .ok_or(anyhow::anyhow!("No offset"))???;
        Ok((replica_id, offset))
    }
}
