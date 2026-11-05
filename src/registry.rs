//! A registry of component factories that turns [`ComponentConfig`] entries
//! into live [`Source`], [`Processor`], and [`Sink`] trait objects.
//!
//! Users register closures (factories) keyed by a **type** string.  When a
//! TOML config is loaded, each component's `type` field is looked up in the
//! registry to instantiate the corresponding component.
//!
//! ```rust,ignore
//! use genpipe::registry::ComponentRegistry;
//! use genpipe::components::sources::IntervalSource;
//! use genpipe::components::sinks::LogSink;
//! use std::time::Duration;
//!
//! let mut registry = ComponentRegistry::new();
//!
//! registry.register_source("interval", |cfg| {
//!     let ms = cfg.get_u64("interval_ms").unwrap_or(1000);
//!     let name = cfg.resolved_name().to_owned();
//!     Ok(Box::new(IntervalSource::new(name, Duration::from_millis(ms))))
//! });
//!
//! registry.register_sink("log", |cfg| {
//!     Ok(Box::new(LogSink::new(cfg.resolved_name())))
//! });
//! ```

use std::collections::HashMap;

use crate::components::{Processor, Sink, Source};
use crate::config::ComponentConfig;
use crate::error::Error;
use crate::Result;

// ── Factory function types ──────────────────────────────────────────────

/// A factory that produces a boxed [`Source`] from a [`ComponentConfig`].
pub type SourceFactory = Box<dyn Fn(&ComponentConfig) -> Result<Box<dyn Source>> + Send + Sync>;

/// A factory that produces a boxed [`Processor`] from a [`ComponentConfig`].
pub type ProcessorFactory =
    Box<dyn Fn(&ComponentConfig) -> Result<Box<dyn Processor>> + Send + Sync>;

/// A factory that produces a boxed [`Sink`] from a [`ComponentConfig`].
pub type SinkFactory = Box<dyn Fn(&ComponentConfig) -> Result<Box<dyn Sink>> + Send + Sync>;

// ── Registry ────────────────────────────────────────────────────────────

/// Stores factories keyed by their component **type** string.
///
/// Use [`ComponentRegistry::with_builtins`] to get a registry pre-loaded
/// with the built-in components from [`crate::components`].
pub struct ComponentRegistry {
    sources: HashMap<String, SourceFactory>,
    processors: HashMap<String, ProcessorFactory>,
    sinks: HashMap<String, SinkFactory>,
}

impl ComponentRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            processors: HashMap::new(),
            sinks: HashMap::new(),
        }
    }

    /// Create a registry pre-loaded with all built-in components.
    ///
    /// Currently registered built-ins:
    ///
    /// | Kind      | Type key       | Settings |
    /// |-----------|----------------|----------|
    /// | source    | `interval`     | `interval_ms` (u64, default 1000) |
    /// | source    | `nats`         | `url` (default `nats://localhost:4222`), `subject` (required), auth settings (see [`NatsAuth::from_settings`](crate::components::nats_auth::NatsAuth::from_settings)) |
    /// | processor | `passthrough`  | — |
    /// | sink      | `log`          | — |
    /// | sink      | `nats`         | `url` (default `nats://localhost:4222`), `subject` (required), auth settings (see [`NatsAuth::from_settings`](crate::components::nats_auth::NatsAuth::from_settings)) |
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        register_builtins(&mut reg);
        reg
    }

    // ── Source ───────────────────────────────────────────────────────

    /// Register a source factory under the given type key.
    pub fn register_source<F>(&mut self, type_key: impl Into<String>, factory: F)
    where
        F: Fn(&ComponentConfig) -> Result<Box<dyn Source>> + Send + Sync + 'static,
    {
        self.sources.insert(type_key.into(), Box::new(factory));
    }

    /// Look up and invoke a source factory.
    pub fn create_source(&self, config: &ComponentConfig) -> Result<Box<dyn Source>> {
        let factory = self.sources.get(&config.component_type).ok_or_else(|| {
            Error::Config(format!(
                "unknown source type `{}`; registered types: [{}]",
                config.component_type,
                self.sources.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })?;
        factory(config)
    }

    // ── Processor ───────────────────────────────────────────────────

    /// Register a processor factory under the given type key.
    pub fn register_processor<F>(&mut self, type_key: impl Into<String>, factory: F)
    where
        F: Fn(&ComponentConfig) -> Result<Box<dyn Processor>> + Send + Sync + 'static,
    {
        self.processors.insert(type_key.into(), Box::new(factory));
    }

    /// Look up and invoke a processor factory.
    pub fn create_processor(&self, config: &ComponentConfig) -> Result<Box<dyn Processor>> {
        let factory = self.processors.get(&config.component_type).ok_or_else(|| {
            Error::Config(format!(
                "unknown processor type `{}`; registered types: [{}]",
                config.component_type,
                self.processors
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        factory(config)
    }

    // ── Sink ────────────────────────────────────────────────────────

    /// Register a sink factory under the given type key.
    pub fn register_sink<F>(&mut self, type_key: impl Into<String>, factory: F)
    where
        F: Fn(&ComponentConfig) -> Result<Box<dyn Sink>> + Send + Sync + 'static,
    {
        self.sinks.insert(type_key.into(), Box::new(factory));
    }

    /// Look up and invoke a sink factory.
    pub fn create_sink(&self, config: &ComponentConfig) -> Result<Box<dyn Sink>> {
        let factory = self.sinks.get(&config.component_type).ok_or_else(|| {
            Error::Config(format!(
                "unknown sink type `{}`; registered types: [{}]",
                config.component_type,
                self.sinks.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })?;
        factory(config)
    }

    // ── Introspection ───────────────────────────────────────────────

    /// All registered source type keys.
    pub fn source_types(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }

    /// All registered processor type keys.
    pub fn processor_types(&self) -> Vec<&str> {
        self.processors.keys().map(|s| s.as_str()).collect()
    }

    /// All registered sink type keys.
    pub fn sink_types(&self) -> Vec<&str> {
        self.sinks.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ComponentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Built-in registrations ──────────────────────────────────────────────

/// Resolve NATS auth for a config-built component and warn (without
/// revealing the secret) when credentials are embedded in the URL.
fn nats_auth_from_config(
    cfg: &ComponentConfig,
    url: &str,
) -> Result<crate::components::nats_auth::NatsAuth> {
    use crate::components::nats_auth::{NatsAuth, redact_url, url_has_credentials};

    if url_has_credentials(url) {
        tracing::warn!(
            component = %cfg.resolved_name(),
            url = %redact_url(url),
            "nats url contains credentials; prefer `token_env`, `user`/`password_env`, \
             `nkey_seed_env` or `credentials_file` so secrets stay out of config files"
        );
    }
    NatsAuth::from_settings(cfg)
}

fn register_builtins(reg: &mut ComponentRegistry) {
    use crate::components::processors::MapProcessor;
    use crate::components::sinks::{LogSink, NatsSink};
    use crate::components::sources::{IntervalSource, NatsSource};
    use std::time::Duration;

    // ── Sources ─────────────────────────────────────────────────────

    reg.register_source("interval", |cfg| {
        let ms = cfg.get_u64("interval_ms").unwrap_or(1000);
        let name = cfg.resolved_name().to_owned();
        Ok(Box::new(IntervalSource::new(name, Duration::from_millis(ms))))
    });

    reg.register_source("nats", |cfg| {
        let name = cfg.resolved_name().to_owned();
        let url = cfg
            .get_str("url")
            .unwrap_or("nats://localhost:4222")
            .to_owned();
        let subject = cfg
            .get_str("subject")
            .ok_or_else(|| {
                crate::error::Error::Config("nats source requires a `subject` setting".into())
            })?
            .to_owned();
        let auth = nats_auth_from_config(cfg, &url)?;
        Ok(Box::new(NatsSource::new(name, url, subject).with_auth(auth)))
    });

    // ── Processors ──────────────────────────────────────────────────

    reg.register_processor("passthrough", |cfg| {
        let name = cfg.resolved_name().to_owned();
        Ok(Box::new(MapProcessor::new(name, |msg| msg)))
    });

    // ── Sinks ───────────────────────────────────────────────────────

    reg.register_sink("log", |cfg| {
        Ok(Box::new(LogSink::new(cfg.resolved_name())))
    });

    reg.register_sink("nats", |cfg| {
        let name = cfg.resolved_name().to_owned();
        let url = cfg
            .get_str("url")
            .unwrap_or("nats://localhost:4222")
            .to_owned();
        let subject = cfg
            .get_str("subject")
            .ok_or_else(|| {
                crate::error::Error::Config("nats sink requires a `subject` setting".into())
            })?
            .to_owned();
        let auth = nats_auth_from_config(cfg, &url)?;
        Ok(Box::new(NatsSink::new(name, url, subject).with_auth(auth)))
    });
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_registered() {
        let reg = ComponentRegistry::with_builtins();
        assert!(reg.source_types().contains(&"interval"));
        assert!(reg.sink_types().contains(&"log"));
    }

    #[test]
    fn unknown_type_returns_error() {
        let reg = ComponentRegistry::new();
        let cfg = ComponentConfig {
            component_type: "nope".into(),
            name: None,
            from: Vec::new(),
            settings: Default::default(),
        };
        assert!(reg.create_source(&cfg).is_err());
        assert!(reg.create_processor(&cfg).is_err());
        assert!(reg.create_sink(&cfg).is_err());
    }

    #[test]
    fn create_builtin_source() {
        let reg = ComponentRegistry::with_builtins();
        let cfg = ComponentConfig {
            component_type: "interval".into(),
            name: Some("tick".into()),
            from: Vec::new(),
            settings: [("interval_ms".into(), serde_json::json!(500))]
                .into_iter()
                .collect(),
        };
        let source = reg.create_source(&cfg).expect("should create interval source");
        assert_eq!(source.name(), "tick");
    }

    #[test]
    fn nats_rejects_inline_password() {
        let reg = ComponentRegistry::with_builtins();
        let cfg = ComponentConfig {
            component_type: "nats".into(),
            name: Some("pub".into()),
            from: Vec::new(),
            settings: [
                ("subject".into(), serde_json::json!("out")),
                ("password".into(), serde_json::json!("hunter2")),
            ]
            .into_iter()
            .collect(),
        };
        let err = reg.create_sink(&cfg).err().expect("inline password must be rejected");
        assert!(!err.to_string().contains("hunter2"));
        assert!(reg.create_source(&cfg).is_err());
    }

    #[test]
    fn create_builtin_sink() {
        let reg = ComponentRegistry::with_builtins();
        let cfg = ComponentConfig {
            component_type: "log".into(),
            name: Some("console".into()),
            from: Vec::new(),
            settings: Default::default(),
        };
        let sink = reg.create_sink(&cfg).expect("should create log sink");
        assert_eq!(sink.name(), "console");
    }
}
