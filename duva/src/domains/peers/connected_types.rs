use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::{from_to, make_smart_pointer};

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) OwnedWriteHalf);
make_smart_pointer!(WriteConnected, OwnedWriteHalf);
from_to!(OwnedWriteHalf, WriteConnected);

#[derive(Debug)]
pub(crate) struct ReadConnected(pub(crate) OwnedReadHalf);

make_smart_pointer!(ReadConnected, OwnedReadHalf);
from_to!(OwnedReadHalf, ReadConnected);
