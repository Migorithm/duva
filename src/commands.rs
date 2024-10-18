use crate::error::Error;

pub(crate) enum Command {
    Ping,
}
impl TryFrom<&str> for Command {
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        println!("value: {:?}", value);
        match value {
            "PING" => Ok(Command::Ping),
            e => {
                eprintln!("Unrecognized command: {:?}", e);
                Err(Self::Error::UnrecognizedCommand)
            }
        }
    }

    type Error = Error;
}
