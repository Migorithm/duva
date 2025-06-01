use std::{cell::UnsafeCell, pin::Pin};

use crate::domains::caches::cache_objects::CacheValue;

use super::{CacheDb, CacheNode, NodeRawPtr};

#[allow(private_interfaces)]
pub(crate) enum Entry<'a> {
    Occupied { key: String, value: NodeRawPtr },
    Vacant { key: String, parent: &'a mut CacheDb },
}

impl<'a> Entry<'a> {
    #[inline]
    fn insert_parent(
        parent: &'a mut CacheDb,
        k: String,
        v: CacheValue,
    ) -> &'a mut Pin<Box<UnsafeCell<CacheNode>>> {
        let parent_copy = unsafe { std::mem::transmute::<&mut CacheDb, &mut CacheDb>(parent) };
        if let Some(existing_node) = parent.map.get_mut(&k) {
            existing_node.get_mut().value = Box::new(v);
            parent_copy.move_to_head(existing_node.get().into());
            existing_node
        } else {
            // Remove tail if exceeding capacity
            if parent_copy.map.len() >= parent.capacity {
                parent_copy.remove_tail();
            }
            if v.has_expiry() {
                parent.keys_with_expiry += 1;
            }
            let v = Box::pin(UnsafeCell::new(CacheNode {
                key: k.clone(),
                value: Box::new(v),
                prev: None,
                next: None,
            }));
            parent_copy.push_front(v.get().into());
            parent_copy.map.entry(k).or_insert(v)
        }
    }
    #[inline]
    pub fn key(&self) -> &String {
        match self {
            | Entry::Occupied { key, .. } | Entry::Vacant { key, .. } => key,
        }
    }

    #[inline]
    pub fn or_insert(self, default: CacheValue) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { mut value, .. } => value.value_mut(),
            | Entry::Vacant { key, parent } => {
                Self::insert_parent(parent, key, default).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with<F: FnOnce() -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { mut value, .. } => value.value_mut(),
            | Entry::Vacant { key, parent } => {
                let val = f();
                Self::insert_parent(parent, key, val).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&String) -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { mut value, .. } => value.value_mut(),
            | Entry::Vacant { key, parent } => {
                let val = f(&key);
                Self::insert_parent(parent, key, val).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn and_modify<F: FnOnce(&mut CacheValue)>(self, f: F) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, mut value } => {
                f(value.value_mut());
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => Entry::Vacant { key, parent },
        }
    }

    #[inline]
    pub fn insert_entry(self, new_val: CacheValue) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, mut value } => {
                *value.value_mut() = new_val;
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => {
                let v = Self::insert_parent(parent, key.clone(), new_val);
                Entry::Occupied { key, value: v.get().into() }
            },
        }
    }
}
