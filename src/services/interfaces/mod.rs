// marker trait for types that are thread-safe and cloneable
pub trait ThreadSafeCloneable: 'static + Send + Sync + Clone {}
impl<T: 'static + Send + Sync + Clone> ThreadSafeCloneable for T {}
