pub mod command;
pub mod interface;

use std::str::FromStr;

use anyhow::Result;
use bytes::BytesMut;
use command::ControllerCommand::{self, *};
use interface::{TRead, TWriteBuf};

use crate::services::{
    config_handler::ConfigHandler,
    persistence::{router::PersistenceRouter, ttl_handlers::set::TtlSetter},
    value::{Value, Values},
};

/// Controller is a struct that will be used to read and write values to the client.
pub struct Controller<T: TWriteBuf + TRead> {
    pub(crate) stream: T,
    buffer: BytesMut,
}

impl<U: TWriteBuf + TRead> Controller<U> {
    pub(crate) async fn handle(
        &mut self,
        persistence_router: &PersistenceRouter,
        ttl_sender: TtlSetter,
        mut config_handler: ConfigHandler,
    ) -> Result<()> {
        let Some((cmd, args)) = self.read_value().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            Ping => Value::SimpleString("PONG".to_string()),
            Echo => args.first()?,
            Set => {
                persistence_router
                    .route_set(&args, ttl_sender.clone())
                    .await?
            }
            Get => persistence_router.route_get(&args).await?,
            // modify we have to add a new command
            Config => config_handler.handle_config(&args)?,
            Delete => panic!("Not implemented"),
        };

        self.write_value(response).await?;
        Ok(())
    }
}

impl<T: TWriteBuf + TRead> Controller<T> {
    pub fn new(stream: T) -> Self {
        Controller {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<(ControllerCommand, Values)>> {
        let bytes_read = self.stream.read(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse(self.buffer.split())?;

        let (str_cmd, values) = Values::extract_query(v)?;
        Ok(Some((FromStr::from_str(&str_cmd)?, values)))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write_buf(value.serialize().as_bytes()).await?;
        Ok(())
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
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec())?;
        Ok((Value::SimpleString(string), len + 1))
    } else {
        Err(anyhow::anyhow!("Invalid simple string"))
    }
}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let Some((line, mut len)) = read_until_crlf(&buffer[1..]) else {
        return Err(anyhow::anyhow!("Invalid bulk string"));
    };

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
    let Some((line, mut len)) = read_until_crlf(&buffer[1..]) else {
        return Err(anyhow::anyhow!("Invalid bulk string"));
    };

    // add 1 to len to account for the first line
    len += 1;

    let bulk_str_len = TryInto::<usize>::try_into(ConversionWrapper(line))?;

    let bulk_str = &buffer[len..bulk_str_len + len];
    Ok((
        Value::BulkString(String::from_utf8(bulk_str.to_vec())?),
        len + bulk_str_len + 2, // to account for crlf, add 2
    ))
}

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
