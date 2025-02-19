use crate::{domains::caches::cache_manager::CacheManager, make_smart_pointer};

pub struct TtlActor(pub(crate) CacheManager);
make_smart_pointer!(TtlActor, CacheManager);
