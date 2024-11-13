pub enum ConfigCommand {
    Get(ConfigResource),
}
pub enum ConfigResource {
    Dir,
    DbFileName,
}

impl TryFrom<(&str, &str)> for ConfigCommand {
    type Error = anyhow::Error;
    fn try_from((cmd, resource): (&str, &str)) -> anyhow::Result<Self> {
        match (
            cmd.to_lowercase().as_str(),
            resource.to_lowercase().as_str(),
        ) {
            ("get", "dir") => Ok(ConfigCommand::Get(ConfigResource::Dir)),
            ("get", "dbfilename") => Ok(ConfigCommand::Get(ConfigResource::DbFileName)),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }
}
