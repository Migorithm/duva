use crate::{make_smart_pointer, services::cache_manager::CacheManager};

pub struct TtlActor(pub(crate) CacheManager);
make_smart_pointer!(TtlActor, CacheManager);
