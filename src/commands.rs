use std::str::FromStr;

pub(crate) enum Command {
    Ping,
}

impl FromStr for Command {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_uppercase().as_str() {
            "*1\r\n$4\r\nPING\r\n" => Ok(Command::Ping),
            _ => Err(Self::Err::UnrecognizedCommand),
        }
    }
}
