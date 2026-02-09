//! A complete example demonstrating a multi-stage pipeline.
//!
//! Run with:
//!
//!     cargo run --example basic_pipeline

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use helix::prelude::*;
use helix::components::processors::{FilterProcessor, MapProcessor};
use helix::components::sinks::LogSink;
use helix::components::sources::IntervalSource;

// ── Custom source ───────────────────────────────────────────────────────

/// A source that produces numbered text messages.
struct CounterSource {
    name: String,
    count: u64,
}

impl CounterSource {
    fn new(name: &str, count: u64) -> Self {
        Self {
            name: name.to_owned(),
            count,
        }
    }
}

#[async_trait]
impl Source for CounterSource {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, tx: mpsc::Sender<Message>, _shutdown: CancellationToken) -> Result<()> {
        for i in 0..self.count {
            let mut msg = Message::text(&self.name, format!("event-{i}"));
            if i % 3 == 0 {
                msg.set_metadata("priority", serde_json::json!("important"));
            }
            tx.send(msg)
                .await
                .map_err(|e| helix::Error::ChannelSend(e.to_string()))?;
        }
        Ok(())
    }
}

// ── Custom sink ─────────────────────────────────────────────────────────

/// A sink that counts received messages.
struct CountingSink {
    name: String,
    counter: Arc<AtomicU64>,
}

impl CountingSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn count(&self) -> Arc<AtomicU64> {
        self.counter.clone()
    }
}

#[async_trait]
impl Sink for CountingSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn consume(&self, message: Message) -> Result<()> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "[{}] #{n}: origin={}, payload={:?}",
            self.name,
            message.origin(),
            message.payload()
        );
        Ok(())
    }
}

// ── main ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> helix::Result<()> {
    // Initialise tracing (optional but recommended).
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let shutdown = CancellationToken::new();

    // ── Service 1: one-shot pipeline ────────────────────────────────
    let counting_sink = CountingSink::new("counter");
    let total = counting_sink.count();

    let oneshot = Service::builder("oneshot-pipeline")
        .source(CounterSource::new("events", 20))
        .processor(FilterProcessor::new("important-only", |msg| {
            msg.get_metadata("priority").and_then(|v| v.as_str()) == Some("important")
        }))
        .processor(MapProcessor::new("tag", |mut msg| {
            msg.set_metadata("processed", serde_json::json!(true));
            msg
        }))
        .sink(counting_sink)
        .build()?;

    // ── Service 2: interval-based (runs for 3 seconds) ──────────────
    let shutdown2 = shutdown.clone();
    let timer_service = Service::builder("ticker")
        .source(IntervalSource::new("heartbeat", Duration::from_secs(1)))
        .sink(LogSink::default())
        .build()?;

    // Cancel the ticker after 3 seconds.
    tokio::spawn({
        let token = shutdown.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            token.cancel();
        }
    });

    // ── Run both services concurrently ──────────────────────────────
    let runner = ServiceRunner::new()
        .with_service(oneshot)
        .with_service(timer_service);

    runner.run(shutdown2).await?;

    println!(
        "\nTotal messages processed by counting sink: {}",
        total.load(Ordering::Relaxed)
    );

    Ok(())
}
