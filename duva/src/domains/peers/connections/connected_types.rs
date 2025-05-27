use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::domains::{TRead, TWrite};

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) Box<dyn TWrite>);
impl std::ops::Deref for WriteConnected {
    type Target = Box<dyn TWrite>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for WriteConnected {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl From<Box<dyn TWrite>> for WriteConnected {
    fn from(value: Box<dyn TWrite>) -> Self {
        Self(value)
    }
}

impl From<OwnedWriteHalf> for WriteConnected {
    fn from(value: OwnedWriteHalf) -> Self {
        Self(Box::new(value))
    }
}

#[derive(Debug)]
pub(crate) struct ReadConnected(pub(crate) Box<dyn TRead>);

impl std::ops::Deref for ReadConnected {
    type Target = Box<dyn TRead>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ReadConnected {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<OwnedReadHalf> for ReadConnected {
    fn from(value: OwnedReadHalf) -> Self {
        Self(Box::new(value))
    }
}
