use std::{cell::UnsafeCell, pin::Pin};

use crate::domains::caches::cache_objects::CacheValue;

use super::{CacheDb, CacheNode};

pub(crate) struct Iter<'a> {
    pub(super) inner: std::collections::hash_map::Iter<'a, String, Pin<Box<UnsafeCell<CacheNode>>>>,
    pub(super) parent: &'a CacheDb,
}
pub(crate) struct IterMut<'a> {
    pub(super) inner:
        std::collections::hash_map::IterMut<'a, String, Pin<Box<UnsafeCell<CacheNode>>>>,
    pub(super) parent: &'a mut CacheDb,
}
pub(crate) struct Values<'a> {
    pub(super) inner: Iter<'a>,
}
pub(crate) struct ValuesMut<'a> {
    pub(super) inner: IterMut<'a>,
}
pub(crate) struct Keys<'a> {
    pub(super) inner: Iter<'a>,
}
pub(crate) struct KeysMut<'a> {
    pub(super) inner: IterMut<'a>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a String, &'a CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            // SAFETY: Access data of UnsafeCell
            let value = unsafe { &*v.get() };
            (k, value.value.as_ref())
        })
    }
}

impl<'a> Iterator for IterMut<'a> {
    type Item = (&'a String, &'a mut CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k, v.get_mut().value.as_mut()))
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = &'a CacheValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a> Iterator for ValuesMut<'a> {
    type Item = &'a mut CacheValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = &'a String;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}

impl<'a> Iterator for KeysMut<'a> {
    type Item = &'a String;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}
