use super::value::Value;
use anyhow::Result;
use bytes::BytesMut;

pub fn parse(buffer: BytesMut) -> Result<(Value, usize)> {
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

struct ConversionWrapper<T>(T);
impl TryFrom<ConversionWrapper<&[u8]>> for usize {
    type Error = anyhow::Error;

    fn try_from(value: ConversionWrapper<&[u8]>) -> Result<Self> {
        let string = String::from_utf8(value.0.to_vec())?;
        Ok(string.parse()?)
    }
}
