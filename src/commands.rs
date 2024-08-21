use crate::error::Error;

pub(crate) enum Command {
    Ping,
}
impl TryFrom<&[u8]> for Command {
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"*1\\r\\n$4\\r\\nPING\r\n" => Ok(Command::Ping),
            e => {
                eprintln!("Unrecognized command: {:?}", e);
                Err(Self::Error::UnrecognizedCommand)
            }
        }
    }

    type Error = Error;
}
