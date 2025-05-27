use crate::{
    domains::{TRead, TWrite},
    make_smart_pointer,
};

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) Box<dyn TWrite>);
make_smart_pointer!(WriteConnected, Box<dyn TWrite>);

impl<T: TWrite> From<T> for WriteConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

#[derive(Debug)]
pub(crate) struct ReadConnected(pub(crate) Box<dyn TRead>);
make_smart_pointer!(ReadConnected, Box<dyn TRead>);

impl<T: TRead> From<T> for ReadConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}
