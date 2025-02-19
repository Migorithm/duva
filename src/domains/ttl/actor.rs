use crate::make_smart_pointer;
use crate::services::statefuls::cache::cache_manager::CacheManager;
pub struct TtlActor(pub(crate) CacheManager);
make_smart_pointer!(TtlActor, CacheManager);
