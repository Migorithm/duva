use crate::make_smart_pointer;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::CacheEntry;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct ClientRequestArguments(Vec<QueryIO>);

make_smart_pointer!(ClientRequestArguments, Vec<QueryIO>);

impl ClientRequestArguments {
    pub fn new(values: Vec<QueryIO>) -> Self {
        Self(values)
    }

    pub(crate) fn take_get_args(&self) -> anyhow::Result<String> {
        let QueryIO::BulkString(key) = self.first().ok_or(anyhow::anyhow!("Not exists"))? else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok(key.to_string())
    }
    pub(crate) fn take_set_args(&self) -> anyhow::Result<CacheEntry> {
        let (QueryIO::BulkString(key), QueryIO::BulkString(value)) = (
            self.first().ok_or(anyhow::anyhow!("Not exists"))?,
            self.get(1).ok_or(anyhow::anyhow!("No value"))?,
        ) else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        //expire sig must be px or PX
        match (self.0.get(2), self.0.get(3)) {
            (Some(QueryIO::BulkString(sig)), Some(expiry)) => {
                if sig.to_lowercase() != "px" {
                    return Err(anyhow::anyhow!("Invalid arguments"));
                }
                Ok(CacheEntry::KeyValueExpiry(
                    key.to_string(),
                    value.to_string(),
                    expiry.extract_expiry()?,
                ))
            }
            (None, _) => Ok(CacheEntry::KeyValue(key.to_owned(), value.to_string())),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }

    pub(crate) fn take_config_args(&self) -> Result<(String, String)> {
        let sub_command = self.first().ok_or(anyhow::anyhow!("Not exists"))?;
        let args = &self[1..];

        let (QueryIO::BulkString(command), [QueryIO::BulkString(key), ..]) = (&sub_command, args)
        else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok((command.into(), key.into()))
    }

    // Pattern is passed with escape characters \" wrapping the pattern in question.
    pub(crate) fn take_keys_pattern(&self) -> anyhow::Result<Option<String>> {
        let QueryIO::BulkString(pattern) = self.first().ok_or(anyhow::anyhow!("Not exists"))?
        else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        let pattern = pattern.trim_matches('\"');
        match pattern {
            "*" => Ok(None),
            "" => Err(anyhow::anyhow!("Invalid pattern")),
            pattern => Ok(Some(pattern.to_string())),
        }
    }
}
