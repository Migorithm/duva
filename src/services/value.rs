use anyhow::Result;

use crate::make_smart_pointer;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    Null,
    Err(String),
}
impl Value {
    pub fn serialize(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Array(a) => {
                let mut result = format!("*{}\r\n", a.len());
                for v in a {
                    result.push_str(&v.serialize());
                }
                result
            }
            Value::Null => "$-1\r\n".to_string(),
            Value::Err(e) => format!("-{}\r\n", e),
        }
    }
    pub fn extract_expiry(&self) -> anyhow::Result<u64> {
        match self {
            Value::BulkString(expiry) => Ok(expiry.parse::<u64>()?),
            _ => Err(anyhow::anyhow!("Invalid expiry")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Values(Vec<Value>);
impl Values {
    pub fn new(values: Vec<Value>) -> Self {
        Self(values)
    }
    pub(crate) fn extract_query(value: Value) -> Result<(String, Self)> {
        match value {
            Value::Array(value_array) => Ok((
                unpack_bulk_str(value_array.first().unwrap().clone())?,
                Self(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
    pub(crate) fn first(&self) -> Result<Value> {
        self.0.first().cloned().ok_or(anyhow::anyhow!("No value"))
    }

    pub(crate) fn take_set_args(&self) -> Result<(Value, &Value, Option<&Value>)> {
        let key = self.first()?;
        let value = self.0.get(1).ok_or(anyhow::anyhow!("No value"))?;
        //expire sig must be px or PX
        match (self.0.get(2), self.0.get(3)) {
            (Some(Value::BulkString(sig)), Some(expiry)) => {
                if sig.to_lowercase() != "px" {
                    return Err(anyhow::anyhow!("Invalid arguments"));
                }
                Ok((key, value, Some(expiry)))
            }
            (None, _) => Ok((key, value, None)),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s.to_lowercase()),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}

make_smart_pointer!(Values, Vec<Value>);
