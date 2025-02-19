use crate::{domains::cache::cache_manager::CacheManager, make_smart_pointer};

pub struct TtlActor(pub(crate) CacheManager);
make_smart_pointer!(TtlActor, CacheManager);
