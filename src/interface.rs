use crate::protocol::value::Value;

pub trait Database {
    fn set(&self, key: String, value: String) -> impl std::future::Future<Output = ()> + Send;

    fn set_with_expiration(
        &self,
        key: String,
        value: String,
        expire_at: &Value,
    ) -> impl std::future::Future<Output = ()> + Send;

    fn get(&self, key: &str) -> impl std::future::Future<Output = Option<String>> + Send;
}
