//! Component traits and ready-made implementations for building pipelines.
//!
//! This module defines the three core component traits — [`Source`],
//! [`Processor`] (with [`ProcessorOutput`]), and [`Sink`] — as well as
//! ready-made implementations in the [`sources`], [`processors`], and
//! [`sinks`] sub-modules.

pub mod processors;
pub mod sinks;
pub mod sources;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{Message, Result};

// ── Source ───────────────────────────────────────────────────────────────

/// A **Source** produces messages and pushes them into the pipeline.
///
/// Implementations should respect the [`CancellationToken`] for graceful
/// shutdown: when the token is cancelled the source must stop producing
/// and return.
///
/// # Example
///
/// ```rust,ignore
/// use async_trait::async_trait;
/// use helix::prelude::*;
///
/// struct TickSource;
///
/// #[async_trait]
/// impl Source for TickSource {
///     fn name(&self) -> &str { "tick" }
///
///     async fn run(
///         &self,
///         tx: mpsc::Sender<Message>,
///         shutdown: CancellationToken,
///     ) -> Result<()> {
///         let mut i = 0u64;
///         loop {
///             tokio::select! {
///                 _ = shutdown.cancelled() => break,
///                 _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
///                     let msg = Message::text("tick", i.to_string());
///                     tx.send(msg).await.map_err(|e| helix::Error::ChannelSend(e.to_string()))?;
///                     i += 1;
///                 }
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Source: Send + Sync {
    /// A human-readable name used in logs and diagnostics.
    fn name(&self) -> &str;

    /// Start producing messages.
    ///
    /// * `tx`       – channel sender to push messages into.
    /// * `shutdown` – token that signals graceful shutdown.
    async fn run(&self, tx: mpsc::Sender<Message>, shutdown: CancellationToken) -> Result<()>;
}

// ── Processor ────────────────────────────────────────────────────────────

/// Describes what a [`Processor`] wants to do with a message.
pub enum ProcessorOutput {
    /// Emit a single (possibly transformed) message.
    Single(Message),
    /// Emit multiple messages from one input (fan-out / split).
    Many(Vec<Message>),
    /// Drop the message entirely (filter).
    Drop,
}

/// A **Processor** transforms, filters, or splits messages flowing through
/// the pipeline.
///
/// Processors are applied **in order**: the output of one processor is fed
/// into the next. A processor may:
///
/// - Transform a message and pass it along ([`ProcessorOutput::Single`]).
/// - Split a message into many ([`ProcessorOutput::Many`]).
/// - Drop a message ([`ProcessorOutput::Drop`]).
///
/// # Example
///
/// ```rust,ignore
/// use async_trait::async_trait;
/// use helix::prelude::*;
///
/// struct UpperCase;
///
/// #[async_trait]
/// impl Processor for UpperCase {
///     fn name(&self) -> &str { "uppercase" }
///
///     async fn process(&self, mut msg: Message) -> Result<ProcessorOutput> {
///         if let Some(text) = msg.payload().as_text() {
///             let upper = text.to_uppercase();
///             msg.set_payload(Payload::Text(upper));
///         }
///         Ok(ProcessorOutput::Single(msg))
///     }
/// }
/// ```
#[async_trait]
pub trait Processor: Send + Sync {
    /// A human-readable name used in logs and diagnostics.
    fn name(&self) -> &str;

    /// Process a single message and return the desired output.
    async fn process(&self, message: Message) -> Result<ProcessorOutput>;
}

// ── Sink ─────────────────────────────────────────────────────────────────

/// A **Sink** consumes messages at the end of a pipeline.
///
/// When multiple sinks are registered on a service every message is
/// **cloned** and delivered to each sink (fan-out).
///
/// # Example
///
/// ```rust,ignore
/// use async_trait::async_trait;
/// use helix::prelude::*;
///
/// struct PrintSink;
///
/// #[async_trait]
/// impl Sink for PrintSink {
///     fn name(&self) -> &str { "print" }
///
///     async fn consume(&self, msg: Message) -> Result<()> {
///         println!("{}: {}", msg.origin(), serde_json::to_string(msg.payload())?);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink: Send + Sync {
    /// A human-readable name used in logs and diagnostics.
    fn name(&self) -> &str;

    /// Consume a single message.
    async fn consume(&self, message: Message) -> Result<()>;

    /// Called once when the sink is shutting down.
    ///
    /// Use this for cleanup such as flushing buffers or closing
    /// connections.  The default implementation is a no-op.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

