use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::Source;
use crate::error::Error;
use crate::{Message, Result};

// ── re-export for convenience ───────────────────────────────────────────
pub use nats_source::NatsSource;

/// Emits a sequentially numbered message at a fixed interval.
///
/// Useful for heartbeats, testing, and driving time-based pipelines.
///
/// ```rust,ignore
/// use genpipe::components::sources::IntervalSource;
/// use std::time::Duration;
///
/// let ticker = IntervalSource::new("heartbeat", Duration::from_secs(5));
/// ```
pub struct IntervalSource {
    name: String,
    interval: Duration,
}

impl IntervalSource {
    pub fn new(name: impl Into<String>, interval: Duration) -> Self {
        Self {
            name: name.into(),
            interval,
        }
    }
}

#[async_trait]
impl Source for IntervalSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, tx: mpsc::Sender<Message>, shutdown: CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(self.interval);
        let mut seq: u64 = 0;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let msg = Message::json(
                        &self.name,
                        json!({ "seq": seq }),
                    );
                    tx.send(msg)
                        .await
                        .map_err(|e| Error::ChannelSend(e.to_string()))?;
                    seq += 1;
                }
            }
        }

        Ok(())
    }
}

/// Wraps an async closure as a [`Source`].
///
/// ```rust,ignore
/// use genpipe::components::sources::FnSource;
///
/// let src = FnSource::new("custom", |tx, shutdown| Box::pin(async move {
///     tx.send(Message::text("custom", "hello")).await.ok();
///     Ok(())
/// }));
/// ```
pub struct FnSource<F>
where
    F: Fn(mpsc::Sender<Message>, CancellationToken) -> crate::BoxFuture<'static, Result<()>>
        + Send
        + Sync,
{
    name: String,
    func: F,
}

impl<F> FnSource<F>
where
    F: Fn(mpsc::Sender<Message>, CancellationToken) -> crate::BoxFuture<'static, Result<()>>
        + Send
        + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> Source for FnSource<F>
where
    F: Fn(mpsc::Sender<Message>, CancellationToken) -> crate::BoxFuture<'static, Result<()>>
        + Send
        + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, tx: mpsc::Sender<Message>, shutdown: CancellationToken) -> Result<()> {
        (self.func)(tx, shutdown).await
    }
}

/// A source that emits a fixed list of messages and then exits.
///
/// Great for testing and one-shot pipelines.
pub struct IterSource {
    name: String,
    messages: Vec<Message>,
}

impl IterSource {
    pub fn new(name: impl Into<String>, messages: Vec<Message>) -> Self {
        Self {
            name: name.into(),
            messages,
        }
    }
}

#[async_trait]
impl Source for IterSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, tx: mpsc::Sender<Message>, _shutdown: CancellationToken) -> Result<()> {
        for msg in &self.messages {
            tx.send(msg.clone())
                .await
                .map_err(|e| Error::ChannelSend(e.to_string()))?;
        }
        Ok(())
    }
}

// ── NATS source ─────────────────────────────────────────────────────────

mod nats_source {
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing::{error, info, warn};

    use crate::components::Source;
    use crate::components::nats_auth::{self, NatsAuth, redact_url};
    use crate::error::Error;
    use crate::message::Payload;
    use crate::{Message, Result};

    /// Subscribes to a NATS subject and emits each received message into
    /// the pipeline.
    ///
    /// Incoming payloads are stored as [`Payload::Bytes`].  If the bytes
    /// are valid UTF-8 they can be promoted to text downstream; if they
    /// are valid JSON a processor can deserialise them.
    ///
    /// The source will retry connecting to the NATS server with
    /// exponential backoff (1 s → 2 s → 4 s … capped at 60 s).
    ///
    /// # Configuration (TOML)
    ///
    /// ```toml
    /// [[service.source]]
    /// type = "nats"
    /// name = "events"
    ///   [service.source.settings]
    ///   url = "nats://localhost:4222"
    ///   subject = "events.>"
    ///   # Optional auth — secrets are read from env vars, never inline:
    ///   # token_env = "NATS_TOKEN"
    /// ```
    ///
    /// See [`NatsAuth::from_settings`] for all authentication settings.
    pub struct NatsSource {
        name: String,
        url: String,
        subject: String,
        auth: NatsAuth,
    }

    impl NatsSource {
        pub fn new(
            name: impl Into<String>,
            url: impl Into<String>,
            subject: impl Into<String>,
        ) -> Self {
            Self {
                name: name.into(),
                url: url.into(),
                subject: subject.into(),
                auth: NatsAuth::None,
            }
        }

        /// Set the authentication method used when connecting.
        pub fn with_auth(mut self, auth: NatsAuth) -> Self {
            self.auth = auth;
            self
        }
    }

    #[async_trait]
    impl Source for NatsSource {
        fn name(&self) -> &str {
            &self.name
        }

        async fn run(
            &self,
            tx: mpsc::Sender<Message>,
            shutdown: CancellationToken,
        ) -> Result<()> {
            // Retry connection with exponential backoff.
            let client = {
                let mut delay = Duration::from_secs(1);
                let max_delay = Duration::from_secs(60);

                loop {
                    match nats_auth::connect(&self.url, &self.auth).await {
                        Ok(c) => break c,
                        Err(e) => {
                            warn!(
                                source = %self.name,
                                url = %redact_url(&self.url),
                                error = %e,
                                retry_in = ?delay,
                                "nats connect failed, retrying"
                            );
                            tokio::select! {
                                biased;
                                _ = shutdown.cancelled() => return Ok(()),
                                _ = tokio::time::sleep(delay) => {}
                            }
                            delay = (delay * 2).min(max_delay);
                        }
                    }
                }
            };

            let mut subscriber = client
                .subscribe(self.subject.clone())
                .await
                .map_err(|e| Error::Nats(format!("subscribe to {}: {e}", self.subject)))?;

            info!(
                source = %self.name,
                url = %redact_url(&self.url),
                subject = %self.subject,
                "nats source connected"
            );

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    maybe_msg = subscriber.next() => {
                        match maybe_msg {
                            Some(nats_msg) => {
                                let mut msg = Message::new(
                                    &self.name,
                                    Payload::Bytes(nats_msg.payload),
                                );
                                msg.set_metadata(
                                    "nats.subject",
                                    serde_json::Value::String(nats_msg.subject.into_string()),
                                );
                                if let Some(reply) = nats_msg.reply {
                                    msg.set_metadata(
                                        "nats.reply",
                                        serde_json::Value::String(reply.into_string()),
                                    );
                                }
                                if tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            None => {
                                error!(source = %self.name, "nats subscription closed");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
    }
}
