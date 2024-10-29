#[macro_export]
macro_rules! make_smart_pointer {
    ($name:ident, $target:ty) => {
        impl std::ops::Deref for $name {
            type Target = $target;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}
