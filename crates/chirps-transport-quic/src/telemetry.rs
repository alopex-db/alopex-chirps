use std::sync::{Once, OnceLock};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Clone, Debug)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    pub log_format: LogFormat,
    pub env_filter: String,
    pub enable_ansi: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        TelemetryConfig {
            log_format: LogFormat::Pretty,
            env_filter: "info".into(),
            enable_ansi: true,
        }
    }
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Initialize tracing subscriber with given config. Idempotent.
pub fn init_tracing(config: TelemetryConfig) {
    TRACING_INIT.get_or_init(|| {
        let env_filter = EnvFilter::try_new(config.env_filter.clone())
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let fmt_layer = match config.log_format {
            LogFormat::Json => fmt::layer().json().with_ansi(config.enable_ansi).boxed(),
            LogFormat::Pretty => fmt::layer().with_ansi(config.enable_ansi).boxed(),
        };

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    });
}

/// Initialize tracing for tests; ignores double-init errors.
pub fn init_test_tracing() {
    let _ = TRACING_INIT.get_or_init(|| {
        let _ = fmt()
            .with_test_writer()
            .with_env_filter(EnvFilter::new("info"))
            .try_init();
    });
}

#[cfg(feature = "metrics-export")]
pub struct MetricsRegistry {
    pub handle: metrics_exporter_prometheus::PrometheusHandle,
}

#[cfg(not(feature = "metrics-export"))]
pub struct MetricsRegistry;

static METRICS_INIT: OnceLock<()> = OnceLock::new();
static METRICS_NOOP: Once = Once::new();

struct NoopRecorder;

impl Recorder for NoopRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

/// Ensure at least a no-op recorder is installed to avoid panics from metrics macros.
pub fn ensure_metrics_recorder() {
    METRICS_NOOP.call_once(|| {
        let _ = metrics::set_global_recorder(NoopRecorder);
    });
}

/// Initialize global metrics recorder. When `metrics-export` feature is enabled,
/// optionally install a Prometheus exporter and return its handle.
pub fn init_metrics(_enable_prometheus: bool) -> Option<MetricsRegistry> {
    // Ensure idempotent initialization.
    if METRICS_INIT.set(()).is_err() {
        return None;
    }

    #[cfg(feature = "metrics-export")]
    {
        if _enable_prometheus {
            let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
            let handle = builder.install_recorder().expect("set prometheus recorder");
            return Some(MetricsRegistry { handle });
        }
    }

    // Fallback to no-op recorder so that metrics macros do not panic if init_metrics is invoked.
    ensure_metrics_recorder();
    None
}
