pub trait Database {
    fn set(&self, key: String, value: String) -> impl std::future::Future<Output = ()> + Send;
    fn get(&self, key: &str) -> impl std::future::Future<Output = Option<String>> + Send;
}
