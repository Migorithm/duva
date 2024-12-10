use crate::make_smart_pointer;
use crate::services::query_manager::query_io::QueryIO;

#[derive(Debug, Clone)]
pub struct ReplicationRequestArguments(Vec<QueryIO>);

make_smart_pointer!(ReplicationRequestArguments, Vec<QueryIO>);

impl ReplicationRequestArguments {
    pub fn new(values: Vec<QueryIO>) -> Self {
        Self(values)
    }
}
