use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use std::sync::LazyLock;
use std::time::Duration;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

pub struct ObservabilityConfig {
    pub log_level: String,
}
impl ObservabilityConfig {
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = level.into();
        self
    }
}

fn init_logs() -> SdkLoggerProvider {
    static RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
        Resource::builder_empty()
            .with_service_name("duva-client")
            .with_attribute(KeyValue::new("instance_id", Uuid::now_v7().to_string()))
            .build()
    });

    use opentelemetry_otlp::LogExporter;
    let exporter = LogExporter::builder()
        .with_http()
        .with_endpoint("http://localhost:4318/v1/logs")
        .with_timeout(Duration::from_secs(2))
        .build()
        .expect("Failed to create log exporter");

    SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(RESOURCE.clone())
        .build()
}

fn create_user_filter(log_level: &str) -> Result<EnvFilter, Box<dyn std::error::Error>> {
    Ok(EnvFilter::new(log_level)
        .add_directive("hyper=off".parse()?)
        .add_directive("tonic=off".parse()?)
        .add_directive("h2=off".parse()?)
        .add_directive("reqwest=off".parse()?)
        .add_directive("rustls=off".parse()?)
        .add_directive("tokio=off".parse()?)
        .add_directive("mio=off".parse()?)
        .add_directive("want=off".parse()?)
        .add_directive("tower=off".parse()?)
        .add_directive("opentelemetry=off".parse()?)
        .add_directive("rustyline=off".parse()?)
        .add_directive("duva_client=info".parse()?)
        .add_directive("cli=info".parse()?))
}

pub fn init_observability_with_config(
    config: ObservabilityConfig,
) -> Result<SdkLoggerProvider, Box<dyn std::error::Error>> {
    let logger_provider = init_logs();
    // Create a new OpenTelemetryTracingBridge using the above LoggerProvider.
    let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    let filter_otel = create_user_filter(&config.log_level)?;

    // Add disabled fmt layer for proper tracing subscriber initialization
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(EnvFilter::new("off"));

    tracing_subscriber::registry().with(otel_layer.with_filter(filter_otel)).with(fmt_layer).init();

    Ok(logger_provider)
}
