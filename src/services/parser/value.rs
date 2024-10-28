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

            Value::Err(s) => format!("-{}\r\n", s),
        }
    }
    pub fn extract_expiry(&self) -> anyhow::Result<u64> {
        match self {
            Value::BulkString(expiry) => Ok(expiry.parse::<u64>()?),
            _ => Err(anyhow::anyhow!("Invalid expiry")),
        }
    }
}

//TODO move to a separate file
pub enum TtlCommand {
    Expiry { expiry: u64, key: String },
    StopSentinel,
}
