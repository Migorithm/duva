use clap::Parser;

#[derive(Parser)]
#[command(name = "redis-cli", version = "1.0", about = "A simple interactive Redis CLI in Rust")]
#[clap(disable_help_flag = true)]
pub(crate) struct Cli {
    #[arg(short, long, default_value = "6000")]
    port: u16,
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,
}

impl Cli {
    pub(crate) fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
