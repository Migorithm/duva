use anyhow::Result;

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

    pub fn unpack_bulk_str(self) -> Result<String> {
        match self {
            Value::BulkString(s) => Ok(s.to_lowercase()),
            _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
        }
    }
    pub fn extract_expiry(&self) -> anyhow::Result<u64> {
        match self {
            Value::BulkString(expiry) => Ok(expiry.parse::<u64>()?),
            _ => Err(anyhow::anyhow!("Invalid expiry")),
        }
    }
}
