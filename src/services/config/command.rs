pub enum ConfigCommand {
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
            ("get", "dir") => Ok(ConfigCommand::Dir),
            ("get", "dbfilename") => Ok(ConfigCommand::DbFileName),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }
}
