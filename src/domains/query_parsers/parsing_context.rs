use bytes::BytesMut;

use super::{QueryIO, deserialize};

pub(super) struct ParseContext {
    offset: usize,
    pub(super) buffer: BytesMut,
}

impl ParseContext {
    pub(super) fn new(buffer: BytesMut) -> Self {
        Self { offset: 0, buffer }
    }

    pub(super) fn advance(&mut self, len: usize) {
        self.offset += len;
    }

    pub(super) fn parse_next(&mut self) -> anyhow::Result<QueryIO> {
        let (result, len) = deserialize(BytesMut::from(&self.buffer[self.offset..]))?;
        self.advance(len);
        Ok(result)
    }

    pub(super) fn offset(&self) -> usize {
        self.offset
    }
}
