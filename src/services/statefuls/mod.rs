pub mod routers;
pub mod ttl_handlers;

use super::value::Value;

impl From<Option<String>> for Value {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(v) => Value::BulkString(v),
            None => Value::Null,
        }
    }
}
