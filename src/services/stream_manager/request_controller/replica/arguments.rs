use crate::make_smart_pointer;
use crate::services::stream_manager::query_io::QueryIO;

#[derive(Debug, Clone)]
pub struct QueryArguments(pub Vec<QueryIO>);

make_smart_pointer!(QueryArguments, Vec<QueryIO>);

impl QueryArguments {
    pub fn new(values: Vec<QueryIO>) -> Self {
        Self(values)
    }
}
