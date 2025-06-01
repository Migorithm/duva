use std::ptr::NonNull;

use crate::{domains::caches::cache_objects::CacheValue, from_to, make_smart_pointer};

#[derive(Clone)]
pub(super) struct CacheNode {
    pub(super) key: String,
    pub(super) value: Box<CacheValue>,
    pub(super) prev: Option<NodeRawPtr>,
    pub(super) next: Option<NodeRawPtr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NodeRawPtr(NonNull<CacheNode>);
from_to!(NonNull<CacheNode>, NodeRawPtr);
make_smart_pointer!(NodeRawPtr, NonNull<CacheNode>);

impl From<*mut CacheNode> for NodeRawPtr {
    fn from(ptr: *mut CacheNode) -> Self {
        // SAFETY: The pointer obtained from `Pin<Box<UnsafeCell<CacheNode>>>.get()`
        // is guaranteed to be non-null because it originates from a Box.
        // If ptr were from a source that could be null, `NonNull::new(ptr).expect(...)`
        // would be a safer alternative.
        unsafe { NodeRawPtr(NonNull::new_unchecked(ptr)) }
    }
}

impl NodeRawPtr {
    #[inline]
    pub(super) fn set_node_prev_link(self, new_prev: Option<NodeRawPtr>) {
        unsafe {
            (*self.0.as_ptr()).prev = new_prev;
        }
    }
    #[inline]
    pub(super) fn set_node_next_link(self, new_next: Option<NodeRawPtr>) {
        unsafe {
            (*self.0.as_ptr()).next = new_next;
        }
    }

    #[inline]
    pub(super) fn get_node_prev_link(self) -> Option<NodeRawPtr> {
        unsafe { (*(self.0.as_ptr())).prev.clone() }
    }

    #[inline]
    pub(super) fn get_node_next_link(self) -> Option<NodeRawPtr> {
        unsafe { (*self.0.as_ptr()).next.clone() }
    }
    #[inline]
    pub(super) fn as_ref(&self) -> &CacheNode {
        unsafe { &*self.0.as_ref() }
    }

    // ! SAFETY: The caller guarantees `ptr` is valid and non-aliased for a mutable reference.
    // ! The `'static` lifetime is a lie, but necessary for the compiler to accept the returned
    // ! reference. The true lifetime must be managed by the caller.(This pattern is common
    // ! when encapsulating raw pointer usage.)
    #[inline]
    pub(super) fn value_mut(&mut self) -> &'static mut CacheValue {
        unsafe { (&mut *self.0.as_ptr()).value.as_mut() }
    }
}
