#[macro_export]
macro_rules! env_var {
    (
        {
            $($env_name:ident),*
        }
        $({
            $($default:ident = $default_value:expr),*
        })?
    ) => {
        $(
            // Initialize the variable with the environment variable or the default value.
            let mut $env_name = std::env::var(stringify!($env_name))
                .ok();
        )*

        let mut args = std::env::args().skip(1);
        $(
            $(let mut $default = $default_value;)*
        )?

        while let Some(arg) = args.next(){
            match arg.as_str(){
                $(
                    concat!("--", stringify!($env_name)) => {
                    if let Some(value) = args.next(){
                        $env_name = Some(value.parse().unwrap());
                    }
                })*
                $(
                    $(
                        concat!("--", stringify!($default)) => {
                        if let Some(value) = args.next(){
                            $default = value.parse().expect("Default value must be given");
                        }
                    })*
                )?


                _ => {
                    eprintln!("Unexpected argument: {}", arg);
                }
            }
        }
    };
}
