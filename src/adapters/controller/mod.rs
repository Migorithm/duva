pub mod interface;
pub mod request;
use anyhow::Result;
use bytes::BytesMut;
use interface::{TRead, TWriteBuf};
use request::UserRequest::{self, *};
use std::str::FromStr;

use crate::{
    make_smart_pointer,
    services::{
        config_handler::{command::ConfigCommand, ConfigHandler},
        statefuls::{routers::inmemory_router::CacheDispatcher, ttl_handlers::set::TtlInbox},
        value::Value,
    },
};

/// Controller is a struct that will be used to read and write values to the client.
pub struct QueryManager<T: TWriteBuf + TRead> {
    pub(crate) stream: T,
    buffer: BytesMut,
}

impl<U: TWriteBuf + TRead> QueryManager<U> {
    pub(crate) async fn handle(
        &mut self,
        persistence_router: &CacheDispatcher,
        ttl_sender: TtlInbox,
        mut config_handler: ConfigHandler,
    ) -> Result<()> {
        let Some((cmd, args)) = self.read_value().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            Ping => Value::SimpleString("PONG".to_string()),
            Echo => args.first().ok_or(anyhow::anyhow!("Not exists"))?.clone(),
            Set => {
                let (key, value, expiry) = args.take_set_args()?;
                persistence_router
                    .route_set(key, value, expiry, ttl_sender)
                    .await?
            }
            // TODO
            Save => {
                // spawn save actor
                persistence_router.run_save_actor(config_handler.conf.get_filepath());
                // set return type
                Value::Null
            }
            Get => {
                let key = args.take_get_args()?;
                persistence_router.route_get(key).await?
            }
            Keys => {
                let pattern = args.take_keys_pattern()?;
                persistence_router.route_keys(pattern).await?
            }
            // modify we have to add a new command
            Config => {
                let cmd = args.take_config_args()?;
                config_handler.handle_config(cmd)?
            }

            Delete => panic!("Not implemented"),
        };

        self.write_value(response).await?;
        Ok(())
    }
}

impl<T: TWriteBuf + TRead> QueryManager<T> {
    pub fn new(stream: T) -> Self {
        QueryManager {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    // crlf
    pub async fn read_value(&mut self) -> Result<Option<(UserRequest, InputValues)>> {
        let bytes_read = self.stream.read(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse(self.buffer.split())?;

        let (str_cmd, values) = Self::extract_query(v)?;

        Ok(Some((FromStr::from_str(&str_cmd)?, values)))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write_buf(value.serialize().as_bytes()).await?;
        Ok(())
    }

    pub(crate) fn extract_query(value: Value) -> Result<(String, InputValues)> {
        match value {
            Value::Array(value_array) => Ok((
                value_array.first().unwrap().clone().unpack_bulk_str()?,
                InputValues::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
}

pub(super) fn parse(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (line, len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid simple string"))?;
    let string = String::from_utf8(line.to_vec())?;
    Ok((Value::SimpleString(string), len + 1))
}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    len += 1;

    let len_of_array = TryInto::<usize>::try_into(ConversionWrapper(line))?;
    let mut bulk_strings = Vec::with_capacity(len_of_array);

    for _ in 0..len_of_array {
        let (value, l) = parse(BytesMut::from(&buffer[len..]))?;
        bulk_strings.push(value);
        len += l;
    }

    return Ok((Value::Array(bulk_strings), len));
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let bulk_str_len = usize::try_from(ConversionWrapper(line))?;

    // Extract the bulk string from the buffer
    let bulk_str = &buffer[len..len + bulk_str_len];
    let bulk_string_value = String::from_utf8(bulk_str.to_vec())?;

    // Return the bulk string value and adjusted length to account for CRLF
    Ok((Value::BulkString(bulk_string_value), len + bulk_str_len + 2))
}

#[derive(Debug, Clone)]
pub struct InputValues(Vec<Value>);
impl InputValues {
    pub fn new(values: Vec<Value>) -> Self {
        Self(values)
    }

    pub(crate) fn take_get_args(&self) -> Result<String> {
        let Value::BulkString(key) = self.first().ok_or(anyhow::anyhow!("Not exists"))? else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok(key.to_string())
    }
    pub(crate) fn take_set_args(&self) -> Result<(String, String, Option<u64>)> {
        let (Value::BulkString(key), Value::BulkString(value)) = (
            self.first().ok_or(anyhow::anyhow!("Not exists"))?,
            self.get(1).ok_or(anyhow::anyhow!("No value"))?,
        ) else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        //expire sig must be px or PX
        match (self.0.get(2), self.0.get(3)) {
            (Some(Value::BulkString(sig)), Some(expiry)) => {
                if sig.to_lowercase() != "px" {
                    return Err(anyhow::anyhow!("Invalid arguments"));
                }
                Ok((
                    key.to_owned(),
                    value.to_string(),
                    Some(expiry.extract_expiry()?),
                ))
            }
            (None, _) => Ok((key.to_owned(), value.to_string(), None)),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }

    fn take_config_args(&self) -> Result<ConfigCommand> {
        let sub_command = self.first().ok_or(anyhow::anyhow!("Not exists"))?;
        let args = &self[1..];

        let (Value::BulkString(command), [Value::BulkString(key), ..]) = (&sub_command, args)
        else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };
        Ok((command.as_str(), key.as_str()).try_into()?)
    }

    // Pattern is passed with escape characters \" wrapping the pattern in question.
    fn take_keys_pattern(&self) -> Result<Option<String>> {
        let Value::BulkString(pattern) = self.first().ok_or(anyhow::anyhow!("Not exists"))? else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        let pattern = pattern.trim_matches('\"');
        match pattern {
            pattern if pattern == "*" => Ok(None),
            pattern if pattern.is_empty() => Err(anyhow::anyhow!("Invalid pattern")),
            pattern => Ok(Some(pattern.to_string())),
        }
    }
}
make_smart_pointer!(InputValues, Vec<Value>);

pub struct ConversionWrapper<T>(pub(crate) T);
impl TryFrom<ConversionWrapper<&[u8]>> for usize {
    type Error = anyhow::Error;

    fn try_from(value: ConversionWrapper<&[u8]>) -> Result<Self> {
        let string = String::from_utf8(value.0.to_vec())?;
        Ok(string.parse()?)
    }
}

#[test]
fn test_parse_simple_string() {
    // GIVEN
    let buffer = BytesMut::from("+OK\r\n");

    // WHEN
    let (value, len) = parse_simple_string(buffer).unwrap();

    // THEN
    assert_eq!(len, 5);
    assert_eq!(value, Value::SimpleString("OK".to_string()));
}

#[test]
fn test_parse_simple_string_ping() {
    // GIVEN
    let buffer = BytesMut::from("+PING\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 7);
    assert_eq!(value, Value::SimpleString("PING".to_string()));
}

#[test]
fn test_parse_bulk_string() {
    // GIVEN
    let buffer = BytesMut::from("$5\r\nhello\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 11);
    assert_eq!(value, Value::BulkString("hello".to_string()));
}

#[test]
fn test_parse_bulk_string_empty() {
    // GIVEN
    let buffer = BytesMut::from("$0\r\n\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 6);
    assert_eq!(value, Value::BulkString("".to_string()));
}

#[test]
fn test_parse_array() {
    // GIVEN
    let buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 26);
    assert_eq!(
        value,
        Value::Array(vec![
            Value::BulkString("hello".to_string()),
            Value::BulkString("world".to_string()),
        ])
    );
}
