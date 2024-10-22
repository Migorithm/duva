use super::value::Value;
use anyhow::Result;

pub struct Args(pub(crate) Vec<Value>);
impl Args {
    pub(crate) fn extract_command(value: Value) -> Result<(String, Self)> {
        match value {
            Value::Array(a) => Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                Self(a.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
    pub(crate) fn first(&self) -> Result<Value> {
        self.0.first().cloned().ok_or(anyhow::anyhow!("No value"))
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s.to_lowercase()),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
