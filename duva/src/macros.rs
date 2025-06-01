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
#[macro_export]
macro_rules! env_var {
    (
        defaults: {
            $($name:ident : $type:ty = $default:expr),* $(,)?
        },
        optional: {
            $($opt_name:ident),* $(,)?
        }
    ) => {
        $(
            let mut $name: $type = $default;
        )*

        $(
            let mut $opt_name: Option<String> = std::env::var(stringify!($opt_name)).ok();
        )*

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                $(
                    concat!("--", stringify!($name)) => {
                        if let Some(val) = args.next() {
                            $name = val.parse::<$type>().expect("Failed to parse argument");
                        }
                    }
                )*
                $(
                    concat!("--", stringify!($opt_name)) => {
                        if let Some(val) = args.next() {
                            $opt_name = Some(val);
                        }
                    }
                )*
                _ => eprintln!("Unexpected argument: {}", arg),
            }
        }
    };
}

/// This macro is used to create a new error with a message and log it using tracing::error.
/// It takes a single argument, which is the error message.
#[macro_export]
macro_rules! err {
    ($msg:expr) => {{
        tracing::error!("{}: {}", $msg, file!());
        Err(anyhow::anyhow!($msg))
    }};
}
