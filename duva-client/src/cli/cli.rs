use clap::Parser;
use duva::prelude::PeerIdentifier;

#[derive(Parser)]
#[command(name = "redis-cli", version = "1.0", about = "A simple interactive Redis CLI in Rust")]
#[clap(disable_help_flag = true)]
pub(crate) struct Cli {
    #[arg(short, long, default_value = "6000")]
    port: u16,
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long, default_value = "true", help = "Enable OpenTelemetry/Grafana integration (file logging always enabled)")]
    enable_grafana: bool,
}

impl Cli {
    pub(crate) fn address(&self) -> PeerIdentifier {
        PeerIdentifier::new(self.host.as_str(), self.port)
    }

    pub(crate) fn log_level(&self) -> &str {
        &self.log_level
    }

    pub(crate) fn enable_grafana(&self) -> bool {
        self.enable_grafana
    }
}
