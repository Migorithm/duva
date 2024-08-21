use std::str::FromStr;

pub(crate) enum Command {
    Ping,
}

impl FromStr for Command {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_uppercase().as_str() {
            "PING" => Ok(Command::Ping),
            _ => Err(Self::Err::UnrecognizedCommand),
        }
    }
}
