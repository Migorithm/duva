use crate::domains::TSerdeDynamicRead;
use crate::domains::TSerdeDynamicWrite;
use crate::make_smart_pointer;

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) Box<dyn TSerdeDynamicWrite>);
make_smart_pointer!(WriteConnected, Box<dyn TSerdeDynamicWrite>);

impl<T: TSerdeDynamicWrite> From<T> for WriteConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl PartialEq for WriteConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for WriteConnected {}

#[derive(Debug)]
pub(crate) struct ReadConnected(pub(crate) Box<dyn TSerdeDynamicRead>);

make_smart_pointer!(ReadConnected, Box<dyn TSerdeDynamicRead>);

impl<T: TSerdeDynamicRead> From<T> for ReadConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl PartialEq for ReadConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ReadConnected {}
