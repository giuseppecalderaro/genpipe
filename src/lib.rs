//! # Helix
//!
//! A framework for building **pipelined services** on top of [Tokio](https://tokio.rs).
//!
//! Each service is assembled from three kinds of components connected by
//! bounded in-memory channels carrying normalised [`Message`] objects:
//!
//! | Component       | Role |
//! |-----------------|------|
//! | [`Source`]       | Produces messages (e.g. from a network socket, timer, or file). |
//! | [`Processor`]   | Transforms, filters, or splits messages as they flow through. |
//! | [`Sink`]        | Consumes messages (e.g. writes to a database, forwards to an API). |
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use helix::prelude::*;
//! use helix::components::{sources::IntervalSource, sinks::LogSink};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> helix::Result<()> {
//!     let shutdown = CancellationToken::new();
//!
//!     let service = Service::builder("heartbeat-logger")
//!         .source(IntervalSource::new("tick", Duration::from_secs(1)))
//!         .sink(LogSink::default())
//!         .build()?;
//!
//!     service.run(shutdown).await
//! }
//! ```
//!
//! ## Pipeline topology
//!
//! ```text
//! [Source 1] ──┐
//! [Source 2] ──┼──► [channel] ──► Processor chain ──► [channel] ──┬──► [Sink 1]
//! [Source N] ──┘                                                  ├──► [Sink 2]
//!                                                                 └──► [Sink N]
//! ```

// ── Modules ─────────────────────────────────────────────────────────────

pub mod components;
pub mod config;
pub mod error;
pub mod message;
pub mod registry;
pub mod runner;
pub(crate) mod runtime;
pub mod service;
pub(crate) mod wiring;

// ── Re-exports ──────────────────────────────────────────────────────────

pub use components::{Processor, ProcessorOutput, Sink, Source};
pub use config::{ComponentConfig, ErrorStrategy, PipelineConfig, ServiceConfig};
pub use error::{Error, Result};
pub use message::{Encoding, Message, Payload};
pub use registry::ComponentRegistry;
pub use runner::ServiceRunner;
pub use service::{Service, ServiceBuilder};

/// A boxed, `Send`-able future — used by closure-based components.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Convenience prelude — `use helix::prelude::*` to get started quickly.
pub mod prelude {
    pub use crate::components;
    pub use crate::config::{ComponentConfig, ErrorStrategy, PipelineConfig, ServiceConfig};
    pub use crate::error::{Error, Result};
    pub use crate::components::{Processor, ProcessorOutput, Sink, Source};
    pub use crate::message::{Encoding, Message, Payload};
    pub use crate::registry::ComponentRegistry;
    pub use crate::runner::ServiceRunner;
    pub use crate::service::{Service, ServiceBuilder};
    pub use crate::BoxFuture;

    pub use async_trait::async_trait;
    pub use tokio::sync::mpsc;
    pub use tokio_util::sync::CancellationToken;
}

#[cfg(test)]
mod tests {
    use crate::components::processors::{FilterProcessor, MapProcessor};
    use crate::components::sinks::CollectorSink;
    use crate::components::sources::IterSource;
    use crate::prelude::*;

    #[tokio::test]
    async fn end_to_end_pipeline() {
        let input = vec![
            Message::text("test", "hello"),
            Message::text("test", "world"),
            Message::text("test", "ignore me"),
        ];

        let collector = CollectorSink::new("collector");
        let handle = collector.handle();

        let svc = Service::builder("test-svc")
            .source(IterSource::new("input", input))
            .processor(FilterProcessor::new("only-short", |msg| {
                msg.payload().as_text().is_some_and(|t| t.len() <= 5)
            }))
            .processor(MapProcessor::new("uppercase", |mut msg| {
                if let Some(text) = msg.payload().as_text() {
                    msg.set_payload(Payload::Text(text.to_uppercase()));
                }
                msg
            }))
            .sink(collector)
            .build()
            .expect("build should succeed");

        let shutdown = CancellationToken::new();
        svc.run(shutdown).await.expect("service should run");

        let collected = handle.lock().await;
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].payload().as_text().unwrap(), "HELLO");
        assert_eq!(collected[1].payload().as_text().unwrap(), "WORLD");
    }

    #[test]
    fn builder_requires_source_and_sink() {
        let result = Service::builder("empty").build();
        assert!(result.is_err());

        let result = Service::builder("no-sink")
            .source(IterSource::new("s", vec![]))
            .build();
        assert!(result.is_err());
    }
}

