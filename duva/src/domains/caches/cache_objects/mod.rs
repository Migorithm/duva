pub(crate) mod entry;
pub(crate) mod value;

pub(crate) use entry::CacheEntry;
pub(crate) use value::CacheValue;
pub(crate) use value::TypedValue;

pub(crate) trait THasExpiry {
    fn has_expiry(&self) -> bool;
}
