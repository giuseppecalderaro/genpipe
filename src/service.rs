use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::components::{Processor, Sink, Source};
use crate::config::{ErrorStrategy, ServiceConfig};
use crate::error::Error;
use crate::registry::ComponentRegistry;
use crate::runtime::{self, RuntimeConfig};
use crate::wiring::Wiring;
use crate::{Message, Result};

// ── Named component wrappers ────────────────────────────────────────────

struct NamedSource {
    name: String,
    source: Box<dyn Source>,
}

struct NamedProcessor {
    name: String,
    from: Vec<String>,
    processor: Box<dyn Processor>,
}

struct NamedSink {
    name: String,
    from: Vec<String>,
    sink: Box<dyn Sink>,
}

// ── Service ─────────────────────────────────────────────────────────────

/// A **Service** is a fully wired pipeline described as a directed graph
/// of sources, processors, and sinks.
///
/// Build one with [`Service::builder`]:
///
/// ```rust,ignore
/// let svc = Service::builder("my-service")
///     .source(my_source)
///     .processor(transform)
///     .processor(filter)
///     .sink(output_sink)
///     .channel_capacity(2048)
///     .build()?;
///
/// svc.run(shutdown_token).await?;
/// ```
pub struct Service {
    name: String,
    sources: Vec<NamedSource>,
    processors: Vec<NamedProcessor>,
    sinks: Vec<NamedSink>,
    channel_capacity: usize,
    drain_timeout: Duration,
    error_strategy: ErrorStrategy,
    max_retry_attempts: usize,
}

// ── Builder ─────────────────────────────────────────────────────────────

/// Builder for constructing a [`Service`].
///
/// When adding components via the builder (without explicit `from`), the
/// pipeline is wired **linearly**:
///
/// - First processor receives from **all sources**.
/// - Each subsequent processor receives from the **previous processor**.
/// - All sinks receive from the **last processor** (or all sources if
///   there are no processors).
#[must_use]
pub struct ServiceBuilder {
    name: String,
    sources: Vec<NamedSource>,
    processors: Vec<NamedProcessor>,
    sinks: Vec<NamedSink>,
    channel_capacity: usize,
    drain_timeout: Duration,
    error_strategy: ErrorStrategy,
    max_retry_attempts: usize,
}

impl ServiceBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sources: Vec::new(),
            processors: Vec::new(),
            sinks: Vec::new(),
            channel_capacity: 1024,
            drain_timeout: Duration::from_secs(30),
            error_strategy: ErrorStrategy::default(),
            max_retry_attempts: 3,
        }
    }

    /// Register a [`Source`]. At least one source is required.
    pub fn source(mut self, source: impl Source + 'static) -> Self {
        let name = source.name().to_owned();
        self.sources.push(NamedSource {
            name,
            source: Box::new(source),
        });
        self
    }

    /// Append a [`Processor`] to the processing chain.
    ///
    /// `from` is auto-wired: the first processor receives from all sources;
    /// subsequent processors receive from the previous processor.
    pub fn processor(mut self, processor: impl Processor + 'static) -> Self {
        let name = processor.name().to_owned();
        let from = if self.processors.is_empty() {
            self.sources.iter().map(|s| s.name.clone()).collect()
        } else {
            vec![self.processors.last().unwrap().name.clone()]
        };
        self.processors.push(NamedProcessor {
            name,
            from,
            processor: Box::new(processor),
        });
        self
    }

    /// Register a [`Sink`]. At least one sink is required.
    ///
    /// `from` is auto-wired: sinks receive from the last processor, or
    /// from all sources when there are no processors.
    pub fn sink(mut self, sink: impl Sink + 'static) -> Self {
        let name = sink.name().to_owned();
        let from = if let Some(last_proc) = self.processors.last() {
            vec![last_proc.name.clone()]
        } else {
            self.sources.iter().map(|s| s.name.clone()).collect()
        };
        self.sinks.push(NamedSink {
            name,
            from,
            sink: Box::new(sink),
        });
        self
    }

    /// Set the bounded channel capacity between stages (default: **1024**).
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set how long to wait for in-flight messages to drain on shutdown
    /// (default: **30 s**).
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Set the error handling strategy (default: [`ErrorStrategy::Drop`]).
    pub fn error_strategy(mut self, strategy: ErrorStrategy) -> Self {
        self.error_strategy = strategy;
        self
    }

    /// Set the maximum number of retry attempts when using
    /// [`ErrorStrategy::Retry`] (default: **3**).
    pub fn max_retry_attempts(mut self, attempts: usize) -> Self {
        self.max_retry_attempts = attempts;
        self
    }

    /// Validate configuration and produce a [`Service`].
    pub fn build(self) -> Result<Service> {
        if self.sources.is_empty() {
            return Err(Error::Config("at least one source is required".into()));
        }
        if self.sinks.is_empty() {
            return Err(Error::Config("at least one sink is required".into()));
        }

        Ok(Service {
            name: self.name,
            sources: self.sources,
            processors: self.processors,
            sinks: self.sinks,
            channel_capacity: self.channel_capacity,
            drain_timeout: self.drain_timeout,
            error_strategy: self.error_strategy,
            max_retry_attempts: self.max_retry_attempts,
        })
    }
}

impl Service {
    /// Start building a new service with the given name.
    pub fn builder(name: impl Into<String>) -> ServiceBuilder {
        ServiceBuilder::new(name)
    }

    /// Build a [`Service`] from a [`ServiceConfig`] and a
    /// [`ComponentRegistry`].
    pub fn from_config(config: &ServiceConfig, registry: &ComponentRegistry) -> Result<Self> {
        let mut sources = Vec::new();
        let mut processors = Vec::new();
        let mut sinks = Vec::new();

        for src_cfg in &config.sources {
            let source = registry.create_source(src_cfg)?;
            let name = src_cfg.resolved_name().to_owned();
            sources.push(NamedSource { name, source });
        }

        for proc_cfg in &config.processors {
            let processor = registry.create_processor(proc_cfg)?;
            let name = proc_cfg.resolved_name().to_owned();
            let from = proc_cfg.from.clone();
            processors.push(NamedProcessor { name, from, processor });
        }

        for sink_cfg in &config.sinks {
            let sink = registry.create_sink(sink_cfg)?;
            let name = sink_cfg.resolved_name().to_owned();
            let from = sink_cfg.from.clone();
            sinks.push(NamedSink { name, from, sink });
        }

        // Validate: build set of known node names (HashSet for O(1) lookup)
        let known_names: HashSet<&str> = sources
            .iter()
            .map(|s| s.name.as_str())
            .chain(processors.iter().map(|p| p.name.as_str()))
            .collect();

        // Validate: every from reference resolves
        for proc in &processors {
            for dep in &proc.from {
                if !known_names.contains(dep.as_str()) {
                    return Err(Error::Config(format!(
                        "processor '{}': unknown predecessor '{dep}'; known nodes: [{}]",
                        proc.name,
                        known_names.iter().copied().collect::<Vec<_>>().join(", ")
                    )));
                }
            }
        }
        for sink in &sinks {
            for dep in &sink.from {
                if !known_names.contains(dep.as_str()) {
                    return Err(Error::Config(format!(
                        "sink '{}': unknown predecessor '{dep}'; known nodes: [{}]",
                        sink.name,
                        known_names.iter().copied().collect::<Vec<_>>().join(", ")
                    )));
                }
            }
        }

        // Validate: no duplicate from entries
        for proc in &processors {
            let mut seen = HashSet::new();
            for dep in &proc.from {
                if !seen.insert(dep.as_str()) {
                    return Err(Error::Config(format!(
                        "processor '{}': duplicate 'from' entry '{dep}'",
                        proc.name
                    )));
                }
            }
        }
        for sink in &sinks {
            let mut seen = HashSet::new();
            for dep in &sink.from {
                if !seen.insert(dep.as_str()) {
                    return Err(Error::Config(format!(
                        "sink '{}': duplicate 'from' entry '{dep}'",
                        sink.name
                    )));
                }
            }
        }

        // Validate: no cycles among processors
        if let Some(cycle_node) = detect_processor_cycle(&processors) {
            return Err(Error::Config(format!(
                "cycle detected in processor graph involving '{}'",
                cycle_node
            )));
        }

        Ok(Service {
            name: config.name.clone(),
            sources,
            processors,
            sinks,
            channel_capacity: config.channel_capacity,
            drain_timeout: Duration::from_millis(config.drain_timeout_ms),
            error_strategy: config.error_strategy,
            max_retry_attempts: config.max_retry_attempts,
        })
    }

    /// The service name (used in logs and diagnostics).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the service until all sources finish or the shutdown token is
    /// cancelled.
    ///
    /// ## Graceful drain
    ///
    /// When the shutdown token is cancelled:
    ///
    /// 1. **Sources** stop producing immediately.
    /// 2. **Processors and sinks** continue draining buffered messages
    ///    until their input channels close naturally.
    /// 3. A safety timeout (drain_timeout) force-stops any component
    ///    that has not finished draining.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let name = self.name;
        let capacity = self.channel_capacity;

        let config = RuntimeConfig {
            drain_timeout: self.drain_timeout,
            error_strategy: self.error_strategy,
            max_retry_attempts: self.max_retry_attempts,
        };

        info!(service = %name, "starting service");

        // Build channel graph
        let source_names: Vec<String> = self.sources.iter().map(|s| s.name.clone()).collect();
        let proc_info: Vec<(String, Vec<String>)> = self
            .processors.iter().map(|p| (p.name.clone(), p.from.clone())).collect();
        let sink_info: Vec<(String, Vec<String>)> = self
            .sinks.iter().map(|s| (s.name.clone(), s.from.clone())).collect();

        let mut wiring = Wiring::build(&source_names, &proc_info, &sink_info, capacity);
        let mut tasks = JoinSet::new();

        // Graceful drain: sources use the external shutdown token;
        // processors and sinks use drain_shutdown (cancelled after timeout).
        let drain_shutdown = CancellationToken::new();

        // Spawn the drain watchdog *outside* the JoinSet so it doesn't
        // prevent `tasks` from completing when sources finish naturally.
        let watchdog = {
            let ext = shutdown.clone();
            let drain = drain_shutdown.clone();
            let timeout = config.drain_timeout;
            let svc = name.clone();

            tokio::spawn(async move {
                ext.cancelled().await;
                info!(service = %svc, ?timeout, "shutdown requested, draining pipeline");
                tokio::time::sleep(timeout).await;
                warn!(service = %svc, "drain timeout reached, forcing shutdown");
                drain.cancel();
            })
        };

        // Spawn broadcast relay tasks
        let relays = wiring.take_broadcast_relays();
        runtime::spawn_broadcast_relays(relays, &name, &shutdown, &mut tasks);

        // Spawn sinks (consumers first, so they are ready)
        for sink in self.sinks {
            let rx = wiring.take_consumer_rx(&sink.name);
            let token = drain_shutdown.clone();
            let svc = name.clone();
            let cfg = config.clone();

            tasks.spawn(async move {
                runtime::run_sink(rx, sink.sink.as_ref(), &sink.name, &svc, token, &cfg).await;
            });
        }

        // Spawn processors
        for proc in self.processors {
            let rx = wiring.take_consumer_rx(&proc.name);
            let output = wiring.producer_output(&proc.name);
            let token = drain_shutdown.clone();
            let svc = name.clone();
            let cfg = config.clone();

            tasks.spawn(async move {
                runtime::run_processor(
                    rx, output, proc.processor.as_ref(), &proc.name, &svc, token, &cfg,
                ).await;
            });
        }

        // Take source senders before dropping consumer_tx
        let mut source_senders: HashMap<String, mpsc::Sender<Message>> = self
            .sources.iter()
            .map(|src| (src.name.clone(), wiring.take_source_tx(&src.name)))
            .collect();

        wiring.finish();

        // Spawn sources (producers last, consumers are ready)
        for src in self.sources {
            let tx = source_senders.remove(&src.name).unwrap();
            let token = shutdown.clone();
            let svc = name.clone();

            tasks.spawn(async move {
                info!(service = %svc, source = %src.name, "source starting");
                if let Err(e) = src.source.run(tx, token).await {
                    error!(service = %svc, source = %src.name, error = %e, "source error");
                }
                info!(service = %svc, source = %src.name, "source stopped");
            });
        }

        // Await every task
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                error!(service = %name, error = %e, "task panicked");
            }
        }

        // All pipeline tasks finished — cancel the drain watchdog if it
        // is still waiting (happens when sources finish naturally without
        // an external shutdown signal).
        watchdog.abort();

        info!(service = %name, "service stopped");
        Ok(())
    }
}

// ── DAG validation ──────────────────────────────────────────────────────

/// Detect cycles among processor nodes using iterative DFS.
fn detect_processor_cycle(processors: &[NamedProcessor]) -> Option<String> {
    let proc_names: HashSet<&str> = processors.iter().map(|p| p.name.as_str()).collect();

    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for proc in processors {
        let deps: Vec<&str> = proc.from.iter()
            .map(|s| s.as_str())
            .filter(|s| proc_names.contains(s))
            .collect();
        adj.insert(proc.name.as_str(), deps);
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Color { White, Gray, Black }

    let mut color: HashMap<&str, Color> = proc_names.iter().map(|&n| (n, Color::White)).collect();

    for &start in &proc_names {
        if color[start] != Color::White { continue; }

        let mut stack: Vec<(&str, usize)> = vec![(start, 0)];
        *color.get_mut(start).unwrap() = Color::Gray;

        while let Some((node, idx)) = stack.last_mut() {
            let neighbours = adj.get(*node).map(|v| v.as_slice()).unwrap_or_default();

            if *idx >= neighbours.len() {
                *color.get_mut(*node).unwrap() = Color::Black;
                stack.pop();
                continue;
            }

            let next = neighbours[*idx];
            *idx += 1;

            match color[next] {
                Color::White => {
                    *color.get_mut(next).unwrap() = Color::Gray;
                    stack.push((next, 0));
                }
                Color::Gray => return Some(next.to_owned()),
                Color::Black => {}
            }
        }
    }

    None
}
