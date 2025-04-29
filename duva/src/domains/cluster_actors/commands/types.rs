use crate::{domains::operation_logs::WriteOperation, from_to};

#[derive(Debug)]
pub(crate) struct SyncLogs(pub(crate) Vec<WriteOperation>);
from_to!(Vec<WriteOperation>, SyncLogs);
