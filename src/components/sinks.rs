use async_trait::async_trait;
use tracing::info;

use super::Sink;
use crate::{Message, Result};

// ── re-export for convenience ───────────────────────────────────────────
pub use nats_sink::NatsSink;

/// Logs every message via `tracing::info`.
///
/// Handy for debugging and development.
pub struct LogSink {
    name: String,
}

impl LogSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Default for LogSink {
    fn default() -> Self {
        Self::new("log")
    }
}

#[async_trait]
impl Sink for LogSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn consume(&self, message: Message) -> Result<()> {
        info!(
            sink = %self.name,
            uuid = %message.uuid(),
            origin = %message.origin(),
            payload = ?message.payload(),
            "message received"
        );
        Ok(())
    }
}

/// Consumes messages with a synchronous closure.
///
/// ```rust,ignore
/// use genpipe::components::sinks::FnSink;
///
/// let printer = FnSink::new("print", |msg| {
///     println!("{}: {:?}", msg.origin(), msg.payload());
///     Ok(())
/// });
/// ```
pub struct FnSink<F>
where
    F: Fn(Message) -> Result<()> + Send + Sync,
{
    name: String,
    func: F,
}

impl<F> FnSink<F>
where
    F: Fn(Message) -> Result<()> + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> Sink for FnSink<F>
where
    F: Fn(Message) -> Result<()> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn consume(&self, message: Message) -> Result<()> {
        (self.func)(message)
    }
}

/// Consumes messages with an async closure.
///
/// ```rust,ignore
/// use genpipe::components::sinks::AsyncFnSink;
///
/// let writer = AsyncFnSink::new("writer", |msg| Box::pin(async move {
///     // async I/O here …
///     Ok(())
/// }));
/// ```
pub struct AsyncFnSink<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<()>> + Send + Sync,
{
    name: String,
    func: F,
}

impl<F> AsyncFnSink<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<()>> + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> Sink for AsyncFnSink<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<()>> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn consume(&self, message: Message) -> Result<()> {
        (self.func)(message).await
    }
}

/// Collects all received messages into an internal buffer.
///
/// Primarily useful for testing. Retrieve messages after the pipeline
/// finishes via [`CollectorSink::messages`].
pub struct CollectorSink {
    name: String,
    messages: std::sync::Arc<tokio::sync::Mutex<Vec<Message>>>,
}

impl CollectorSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            messages: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Get a clone of the shared handle so you can read collected messages
    /// after the pipeline finishes.
    pub fn handle(&self) -> std::sync::Arc<tokio::sync::Mutex<Vec<Message>>> {
        self.messages.clone()
    }
}

#[async_trait]
impl Sink for CollectorSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn consume(&self, message: Message) -> Result<()> {
        self.messages.lock().await.push(message);
        Ok(())
    }
}

// ── NATS sink ───────────────────────────────────────────────────────────

mod nats_sink {
    use async_trait::async_trait;
    use tokio::sync::OnceCell;
    use tracing::info;

    use crate::components::Sink;
    use crate::error::Error;
    use crate::message::Payload;
    use crate::{Message, Result};

    /// Publishes each pipeline message to a NATS subject.
    ///
    /// The payload is serialised as follows:
    ///
    /// | Payload variant | Published bytes |
    /// |-|-|
    /// | `Bytes(b)` | raw `b` |
    /// | `Text(s)` | UTF-8 bytes of `s` |
    /// | `Json(v)` | compact JSON bytes |
    /// | `Empty` | zero-length payload |
    ///
    /// # Configuration (TOML)
    ///
    /// ```toml
    /// [[service.sink]]
    /// type = "nats"
    /// name = "publisher"
    ///   [service.sink.settings]
    ///   url = "nats://localhost:4222"
    ///   subject = "output.processed"
    /// ```
    pub struct NatsSink {
        name: String,
        url: String,
        subject: String,
        client: OnceCell<async_nats::Client>,
    }

    impl NatsSink {
        pub fn new(
            name: impl Into<String>,
            url: impl Into<String>,
            subject: impl Into<String>,
        ) -> Self {
            Self {
                name: name.into(),
                url: url.into(),
                subject: subject.into(),
                client: OnceCell::new(),
            }
        }

        /// Lazy-connect on first message so that construction is infallible.
        async fn client(&self) -> Result<&async_nats::Client> {
            self.client
                .get_or_try_init(|| async {
                    let c = async_nats::connect(&self.url)
                        .await
                        .map_err(|e| Error::Nats(format!("connect to {}: {e}", self.url)))?;

                    info!(
                        sink = %self.name,
                        url = %self.url,
                        subject = %self.subject,
                        "nats sink connected"
                    );

                    Ok(c)
                })
                .await
        }
    }

    #[async_trait]
    impl Sink for NatsSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn consume(&self, message: Message) -> Result<()> {
            let client = self.client().await?;

            let payload: bytes::Bytes = match message.into_payload() {
                Payload::Bytes(b) => b,
                Payload::Text(s) => bytes::Bytes::from(s),
                Payload::Json(v) => bytes::Bytes::from(
                    serde_json::to_vec(&v).map_err(|e| Error::Nats(e.to_string()))?,
                ),
                Payload::Empty => bytes::Bytes::new(),
            };

            client
                .publish(self.subject.clone(), payload)
                .await
                .map_err(|e| Error::Nats(format!("publish to {}: {e}", self.subject)))?;

            Ok(())
        }

        async fn shutdown(&self) -> Result<()> {
            if let Some(client) = self.client.get() {
                client
                    .flush()
                    .await
                    .map_err(|e| Error::Nats(format!("flush: {e}")))?;
            }
            Ok(())
        }
    }
}
