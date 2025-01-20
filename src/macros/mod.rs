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
    ($name:ident$(<'a>)?, $target:ty) => {
        impl<'a> std::ops::Deref for $name<'a> {
            type Target = $target;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl<'a> std::ops::DerefMut for $name<'a> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };

    ($name:ident, $target:ty => $field:ident) => {
        impl std::ops::Deref for $name {
            type Target = $target;
            fn deref(&self) -> &Self::Target {
                &self.$field
            }
        }
        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.$field
            }
        }
    };
}

#[macro_export]
macro_rules! from_to {
    ($from:ty, $to:ty) => {
        impl From<$from> for $to {
            fn from(value: $from) -> Self {
                Self(value)
            }
        }
        impl From<$to> for $from {
            fn from(value: $to) -> Self {
                value.0
            }
        }
    };
}
