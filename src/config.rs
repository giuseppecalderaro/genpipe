//! TOML-driven pipeline configuration.
//!
//! A config file describes one or more services — each with its sources,
//! processors, and sinks — so that pipeline topology can be changed
//! without recompiling.
//!
//! Each processor and sink declares a `from` list naming the components it
//! should receive messages from.  If multiple components reference the same
//! predecessor, a **broadcast** channel is used automatically; otherwise a
//! standard **mpsc** channel is created.
//!
//! ## TOML schema
//!
//! ```toml
//! [[service]]
//! name = "my-pipeline"
//! channel_capacity = 2048          # optional, defaults to 1024
//!
//!   [[service.source]]
//!   type = "interval"              # looked up in the ComponentRegistry
//!   name = "tick"                  # optional override; defaults to `type`
//!     [service.source.settings]    # arbitrary key-value pairs passed to factory
//!     interval_ms = 1000
//!
//!   [[service.processor]]
//!   type = "json_insert"
//!   name = "tag"
//!   from = ["tick"]               # receive from source named "tick"
//!     [service.processor.settings]
//!     key = "env"
//!     value = "production"
//!
//!   [[service.sink]]
//!   type = "log"
//!   name = "console"
//!   from = ["tag"]                # receive from processor named "tag"
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use genpipe::config::PipelineConfig;
//! use genpipe::registry::ComponentRegistry;
//!
//! let cfg = PipelineConfig::from_file("pipeline.toml")?;
//! let registry = ComponentRegistry::new();
//! // … register factories …
//! let services = cfg.build_services(&registry)?;
//! ```

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Error;
use crate::Result;

// ── Error strategy ──────────────────────────────────────────────────────

/// How the runtime handles processor / sink errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    /// Log the error and drop the message (default).
    #[default]
    Drop,
    /// Retry processing up to `max_retry_attempts` times before dropping.
    Retry,
}

impl std::str::FromStr for PipelineConfig {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Config(e.to_string()))
    }
}

// ── Top-level config ────────────────────────────────────────────────────

/// Root of the pipeline configuration file.
///
/// Holds one or more [`ServiceConfig`] entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// One or more services to run concurrently.
    #[serde(rename = "service")]
    pub services: Vec<ServiceConfig>,
}

impl PipelineConfig {
    /// Read and parse a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        content.parse()
    }

    /// Build all [`Service`](crate::Service) instances described by this
    /// config, resolving components through the given
    /// [`ComponentRegistry`](crate::registry::ComponentRegistry).
    pub fn build_services(
        &self,
        registry: &crate::registry::ComponentRegistry,
    ) -> Result<Vec<crate::Service>> {
        self.services
            .iter()
            .map(|svc_cfg| crate::Service::from_config(svc_cfg, registry))
            .collect()
    }

    /// Convenience: build services and wrap them in a
    /// [`ServiceRunner`](crate::ServiceRunner) ready to be `.run()`.
    pub fn build_runner(
        &self,
        registry: &crate::registry::ComponentRegistry,
    ) -> Result<crate::ServiceRunner> {
        let services = self.build_services(registry)?;
        let mut runner = crate::ServiceRunner::new();
        for svc in services {
            runner = runner.with_service(svc);
        }
        Ok(runner)
    }
}

// ── Service config ──────────────────────────────────────────────────────

/// Describes a single pipeline service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    /// The service name (used in logs and diagnostics).
    pub name: String,

    /// Bounded channel capacity between stages (defaults to **1024**).
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// How long (in milliseconds) to wait for in-flight messages to drain
    /// before force-shutting down processors and sinks (defaults to **30 000**).
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,

    /// Error handling strategy for processor and sink failures.
    #[serde(default)]
    pub error_strategy: ErrorStrategy,

    /// Maximum number of retry attempts when `error_strategy` is `retry`
    /// (defaults to **3**).
    #[serde(default = "default_max_retry_attempts")]
    pub max_retry_attempts: usize,

    /// One or more sources.
    #[serde(default, rename = "source")]
    pub sources: Vec<ComponentConfig>,

    /// Zero or more processors (applied in order).
    #[serde(default, rename = "processor")]
    pub processors: Vec<ComponentConfig>,

    /// One or more sinks.
    #[serde(default, rename = "sink")]
    pub sinks: Vec<ComponentConfig>,
}

fn default_channel_capacity() -> usize {
    1024
}

fn default_drain_timeout_ms() -> u64 {
    30_000
}

fn default_max_retry_attempts() -> usize {
    3
}

// ── Component config ────────────────────────────────────────────────────

/// Describes a single source, processor, or sink.
///
/// The `type` field selects the factory in the [`ComponentRegistry`], and
/// the `settings` table is forwarded as a `serde_json::Value` so each
/// factory can deserialise whatever it needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentConfig {
    /// The component type key used to look up the factory in the registry.
    #[serde(rename = "type")]
    pub component_type: String,

    /// An optional human-readable name. Defaults to `component_type` when
    /// omitted.
    #[serde(default)]
    pub name: Option<String>,

    /// Predecessor component names this component receives messages from.
    ///
    /// Sources do not use this field. Processors and sinks must list at
    /// least one predecessor.  When multiple downstream components name
    /// the same predecessor, a broadcast channel is created automatically.
    #[serde(default)]
    pub from: Vec<String>,

    /// Arbitrary settings forwarded to the factory function.
    ///
    /// **Note:** Settings are parsed from TOML but stored as
    /// [`serde_json::Value`].  TOML-specific types such as datetimes are
    /// converted to strings during this process.  If you need faithful
    /// TOML value semantics, deserialise from
    /// [`ComponentConfig::settings_value`] instead.
    #[serde(default)]
    pub settings: HashMap<String, Value>,
}

impl ComponentConfig {
    /// Returns `name` if set, otherwise falls back to `component_type`.
    pub fn resolved_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.component_type)
    }

    /// Convenience: get a setting as a string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.settings.get(key).and_then(|v| v.as_str())
    }

    /// Convenience: get a setting as a `u64`.
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.settings.get(key).and_then(|v| v.as_u64())
    }

    /// Convenience: get a setting as an `f64`.
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.settings.get(key).and_then(|v| v.as_f64())
    }

    /// Convenience: get a setting as a `bool`.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.settings.get(key).and_then(|v| v.as_bool())
    }

    /// Convenience: get the full settings map as a [`serde_json::Value`]
    /// object, suitable for deserialising into a typed struct.
    pub fn settings_value(&self) -> Value {
        serde_json::to_value(&self.settings).unwrap_or(Value::Object(Default::default()))
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[service]]
name = "ingestion"
channel_capacity = 2048

  [[service.source]]
  type = "interval"
  name = "heartbeat"
    [service.source.settings]
    interval_ms = 1000

  [[service.processor]]
  type = "filter"
  from = ["heartbeat"]
    [service.processor.settings]
    kind = "important"

  [[service.processor]]
  type = "uppercase"
  from = ["filter"]

  [[service.sink]]
  type = "log"
  from = ["uppercase"]

  [[service.sink]]
  type = "file"
  name = "audit-log"
  from = ["uppercase"]
    [service.sink.settings]
    path = "/var/log/audit.jsonl"

[[service]]
name = "metrics"

  [[service.source]]
  type = "interval"
    [service.source.settings]
    interval_ms = 5000

  [[service.sink]]
  type = "log"
  from = ["interval"]
"#;

    #[test]
    fn parse_full_config() {
        let cfg: PipelineConfig = SAMPLE.parse().expect("should parse");
        assert_eq!(cfg.services.len(), 2);

        let svc = &cfg.services[0];
        assert_eq!(svc.name, "ingestion");
        assert_eq!(svc.channel_capacity, 2048);
        assert_eq!(svc.sources.len(), 1);
        assert_eq!(svc.processors.len(), 2);
        assert_eq!(svc.sinks.len(), 2);

        // Source
        let src = &svc.sources[0];
        assert_eq!(src.component_type, "interval");
        assert_eq!(src.resolved_name(), "heartbeat");
        assert_eq!(src.get_u64("interval_ms"), Some(1000));

        // Processors
        assert_eq!(svc.processors[0].component_type, "filter");
        assert_eq!(svc.processors[0].from, vec!["heartbeat"]);
        assert_eq!(svc.processors[0].get_str("kind"), Some("important"));
        assert_eq!(svc.processors[1].component_type, "uppercase");
        assert_eq!(svc.processors[1].from, vec!["filter"]);
        assert!(svc.processors[1].settings.is_empty());

        // Sinks
        assert_eq!(svc.sinks[0].component_type, "log");
        assert_eq!(svc.sinks[0].from, vec!["uppercase"]);
        assert_eq!(svc.sinks[1].resolved_name(), "audit-log");
        assert_eq!(svc.sinks[1].from, vec!["uppercase"]);
        assert_eq!(svc.sinks[1].get_str("path"), Some("/var/log/audit.jsonl"));

        // Second service inherits default capacity.
        let svc2 = &cfg.services[1];
        assert_eq!(svc2.name, "metrics");
        assert_eq!(svc2.channel_capacity, 1024);
    }

    #[test]
    fn empty_config_parses() {
        // No services at all is _syntactically_ valid.
        let cfg: PipelineConfig = toml::from_str("service = []").unwrap();
        assert!(cfg.services.is_empty());
    }

    #[test]
    fn invalid_toml_returns_error() {
        let result: Result<PipelineConfig> = "not valid { toml".parse();
        assert!(result.is_err());
    }
}
