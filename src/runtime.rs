//! Per-component task functions and broadcast relay logic.
//!
//! This module is crate-private.  It contains the async task bodies that
//! run each source, processor, and sink as independent tokio tasks.

use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::components::{Processor, ProcessorOutput, Sink};
use crate::config::ErrorStrategy;
use crate::wiring::{BroadcastRelay, ProducerOutput};
use crate::Message;

// ═══════════════════════════════════════════════════════════════════════
// RuntimeConfig
// ═══════════════════════════════════════════════════════════════════════

/// Runtime configuration forwarded to each component task.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub drain_timeout: Duration,
    pub error_strategy: ErrorStrategy,
    pub max_retry_attempts: usize,
}

// ═══════════════════════════════════════════════════════════════════════
// Broadcast relay spawning
// ═══════════════════════════════════════════════════════════════════════

/// Spawn all broadcast relay tasks.
///
/// For each [`BroadcastRelay`]:
/// - If the producer is a source, a `mpsc → broadcast` relay is spawned.
/// - For every downstream consumer a `broadcast → mpsc` relay is spawned.
pub(crate) fn spawn_broadcast_relays(
    relays: Vec<BroadcastRelay>,
    service: &str,
    shutdown: &CancellationToken,
    tasks: &mut JoinSet<()>,
) {
    for relay in relays {
        info!(
            service = %service,
            producer = %relay.producer,
            subscribers = relay.consumers.len(),
            "using broadcast channel"
        );

        // Source broadcast: mpsc → broadcast relay.
        if let Some(rx) = relay.source_rx {
            let btx = relay.broadcast_tx.clone();
            let svc = service.to_owned();
            let pname = relay.producer.clone();
            let token = shutdown.clone();
            tasks.spawn(async move {
                relay_mpsc_to_broadcast(rx, btx, &svc, &pname, token).await;
            });
        }

        // Per-consumer: broadcast → consumer mpsc relay.
        for (consumer_name, tx) in relay.consumers {
            let mut brx = relay.broadcast_tx.subscribe();
            let token = shutdown.clone();
            let svc = service.to_owned();
            let cname = consumer_name;

            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break,
                        result = brx.recv() => {
                            match result {
                                Ok(msg) => {
                                    if tx.send(msg).await.is_err() {
                                        debug!(service = %svc, consumer = %cname,
                                               "consumer channel closed");
                                        break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                    warn!(service = %svc, consumer = %cname,
                                          lagged = n, "broadcast receiver lagged, {n} message(s) lost");
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }
                }
            });
        }
    }
}

/// Relay messages from an mpsc receiver into a broadcast sender.
async fn relay_mpsc_to_broadcast(
    mut rx: mpsc::Receiver<Message>,
    tx: broadcast::Sender<Message>,
    service: &str,
    producer: &str,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            msg = rx.recv() => {
                match msg {
                    Some(m) => {
                        if tx.send(m).is_err() {
                            debug!(service = %service, producer = %producer,
                                   "broadcast: no active receivers");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Processor task
// ═══════════════════════════════════════════════════════════════════════

/// Run a single processor: read from input, process, send to output.
///
/// The processor drains its input channel naturally when upstream
/// producers finish.  The `drain_shutdown` token is a safety net that
/// force-stops the loop if the drain takes longer than the configured
/// timeout.
pub(crate) async fn run_processor(
    mut rx: mpsc::Receiver<Message>,
    output: ProducerOutput,
    processor: &dyn Processor,
    proc_name: &str,
    service: &str,
    drain_shutdown: CancellationToken,
    config: &RuntimeConfig,
) {
    info!(service = %service, processor = %proc_name, "processor running");

    loop {
        tokio::select! {
            biased;
            _ = drain_shutdown.cancelled() => {
                warn!(service = %service, processor = %proc_name,
                      "drain timeout reached, forcing shutdown");
                break;
            }
            msg = rx.recv() => {
                match msg {
                    Some(message) => {
                        process_message(
                            message,
                            processor,
                            &output,
                            proc_name,
                            service,
                            config,
                        )
                        .await;
                    }
                    None => {
                        debug!(service = %service, processor = %proc_name,
                               "input channel closed");
                        break;
                    }
                }
            }
        }
    }

    debug!(service = %service, processor = %proc_name, "processor stopped");
}

/// Process a single message with retry support.
async fn process_message(
    message: Message,
    processor: &dyn Processor,
    output: &ProducerOutput,
    proc_name: &str,
    service: &str,
    config: &RuntimeConfig,
) {
    let max_attempts = match config.error_strategy {
        ErrorStrategy::Drop => 1,
        ErrorStrategy::Retry => config.max_retry_attempts.max(1),
    };

    let mut last_error = None;

    for attempt in 1..=max_attempts {
        let msg = message.clone();
        match processor.process(msg).await {
            Ok(ProcessorOutput::Single(m)) => {
                if !output.send(m).await {
                    warn!(service = %service, processor = %proc_name,
                          "output channel closed");
                }
                return;
            }
            Ok(ProcessorOutput::Many(ms)) => {
                for m in ms {
                    if !output.send(m).await {
                        warn!(service = %service, processor = %proc_name,
                              "output channel closed");
                        return;
                    }
                }
                return;
            }
            Ok(ProcessorOutput::Drop) => {
                debug!(service = %service, processor = %proc_name, "message dropped");
                return;
            }
            Err(e) => {
                if attempt < max_attempts {
                    warn!(
                        service = %service,
                        processor = %proc_name,
                        attempt,
                        max_attempts,
                        error = %e,
                        "processor error, retrying"
                    );
                }
                last_error = Some(e);
            }
        }
    }

    if let Some(e) = last_error {
        error!(
            service = %service,
            processor = %proc_name,
            error = %e,
            attempts = max_attempts,
            "processor error, dropping message"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Sink task
// ═══════════════════════════════════════════════════════════════════════

/// Run a single sink: read from input, deliver to sink.
///
/// Like the processor task, the sink drains its input channel naturally
/// and uses `drain_shutdown` as a safety-net timeout.  After the loop
/// exits, the sink's [`Sink::shutdown`] hook is called (e.g. to flush
/// buffers).
pub(crate) async fn run_sink(
    mut rx: mpsc::Receiver<Message>,
    sink: &dyn Sink,
    sink_name: &str,
    service: &str,
    drain_shutdown: CancellationToken,
    config: &RuntimeConfig,
) {
    info!(service = %service, sink = %sink_name, "sink running");

    loop {
        tokio::select! {
            biased;
            _ = drain_shutdown.cancelled() => {
                warn!(service = %service, sink = %sink_name,
                      "drain timeout reached, forcing shutdown");
                break;
            }
            msg = rx.recv() => {
                match msg {
                    Some(message) => {
                        consume_message(message, sink, sink_name, service, config).await;
                    }
                    None => {
                        debug!(service = %service, sink = %sink_name,
                               "input channel closed");
                        break;
                    }
                }
            }
        }
    }

    // Call the sink's shutdown hook (e.g. flush buffers).
    if let Err(e) = sink.shutdown().await {
        error!(service = %service, sink = %sink_name,
               error = %e, "sink shutdown error");
    }

    debug!(service = %service, sink = %sink_name, "sink stopped");
}

/// Consume a single message with retry support.
async fn consume_message(
    message: Message,
    sink: &dyn Sink,
    sink_name: &str,
    service: &str,
    config: &RuntimeConfig,
) {
    let max_attempts = match config.error_strategy {
        ErrorStrategy::Drop => 1,
        ErrorStrategy::Retry => config.max_retry_attempts.max(1),
    };

    let mut last_error = None;

    for attempt in 1..=max_attempts {
        let msg = message.clone();
        match sink.consume(msg).await {
            Ok(()) => return,
            Err(e) => {
                if attempt < max_attempts {
                    warn!(
                        service = %service,
                        sink = %sink_name,
                        attempt,
                        max_attempts,
                        error = %e,
                        "sink error, retrying"
                    );
                }
                last_error = Some(e);
            }
        }
    }

    if let Some(e) = last_error {
        error!(
            service = %service,
            sink = %sink_name,
            error = %e,
            attempts = max_attempts,
            "sink error, dropping message"
        );
    }
}
