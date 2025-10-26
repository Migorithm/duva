use bincode::{
    BorrowDecode, Decode,
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;

use crate::{from_to, make_smart_pointer};

#[derive(Debug)]
pub(crate) struct Callback<T>(pub(crate) tokio::sync::oneshot::Sender<T>);

impl<T> Callback<T> {
    pub(crate) fn create() -> (Self, CallbackAwaiter<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Callback(tx), CallbackAwaiter(rx))
    }

    pub(crate) fn send(self, value: T) {
        let _ = self.0.send(value);
    }
}

#[derive(Debug)]
pub(crate) struct CallbackAwaiter<T>(pub(crate) tokio::sync::oneshot::Receiver<T>);

impl<T> CallbackAwaiter<T> {
    pub(crate) async fn recv(self) -> T {
        self.0.await.expect("Channel Closed")
    }

    pub(crate) async fn wait(self) {
        let _ = self.0.await;
    }
}

impl<T> PartialEq for Callback<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for Callback<T> {}

#[derive(Clone, Debug, PartialEq, Default, Eq)]
pub struct BinBytes(pub Bytes);

make_smart_pointer!(BinBytes, Bytes);
from_to!(Bytes, BinBytes);

impl bincode::Encode for BinBytes {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.0.as_ref().encode(encoder)
    }
}
impl<Ctx> Decode<Ctx> for BinBytes {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let vec: Vec<u8> = Decode::decode(decoder)?;
        Ok(BinBytes(Bytes::from(vec)))
    }
}
impl<'de, Ctx> BorrowDecode<'de, Ctx> for BinBytes {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let slice: &'de [u8] = BorrowDecode::borrow_decode(decoder)?;
        Ok(BinBytes(Bytes::copy_from_slice(slice)))
    }
}

impl BinBytes {
    pub fn new(arg: impl Into<Bytes>) -> Self {
        let bytes = arg.into();
        Self(bytes)
    }
}
