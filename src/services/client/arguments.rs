use crate::make_smart_pointer;
use crate::services::query_io::QueryIO;

#[derive(Debug, Clone, Default)]
pub struct ClientRequestArguments(Vec<QueryIO>);

make_smart_pointer!(ClientRequestArguments, Vec<QueryIO>);

impl ClientRequestArguments {
    pub fn new(values: Vec<QueryIO>) -> Self {
        Self(values)
    }
}
