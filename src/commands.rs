use crate::error::Error;

pub(crate) enum Command {
    Ping,
    Echo(String),
}
impl TryFrom<&str> for Command {
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "PING" => Ok(Command::Ping),
            echo if echo.starts_with("ECHO ") => Ok(Command::Echo(echo[5..].to_string())),
            e => {
                eprintln!("Unrecognized command: {:?}", e);
                Err(Self::Error::UnrecognizedCommand)
            }
        }
    }

    type Error = Error;
}
