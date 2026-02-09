//! Channel wiring for DAG-based service topologies.
//!
//! This module is crate-private.  It analyses the component graph built by
//! [`Service`](crate::Service) and pre-creates all the `mpsc` and
//! `broadcast` channels that connect sources → processors → sinks.

use std::collections::HashMap;

use tokio::sync::{broadcast, mpsc};
use tracing::warn;

use crate::Message;

// ═══════════════════════════════════════════════════════════════════════
// ProducerOutput
// ═══════════════════════════════════════════════════════════════════════

/// How a producer (processor) sends its output downstream.
pub(crate) enum ProducerOutput {
    /// Single subscriber — direct mpsc sender.
    Direct(mpsc::Sender<Message>),
    /// Multiple subscribers — broadcast sender.
    Broadcast(broadcast::Sender<Message>),
    /// No subscribers (output is discarded).
    None,
}

impl ProducerOutput {
    pub(crate) async fn send(&self, msg: Message) -> bool {
        match self {
            Self::Direct(tx) => tx.send(msg).await.is_ok(),
            Self::Broadcast(tx) => tx.send(msg).is_ok(),
            Self::None => true,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// BroadcastRelay — data needed to spawn relay tasks
// ═══════════════════════════════════════════════════════════════════════

/// All the data needed to spawn broadcast relay tasks for one producer.
pub(crate) struct BroadcastRelay {
    pub producer: String,
    pub broadcast_tx: broadcast::Sender<Message>,
    /// Present only when the producer is a source (needs mpsc → broadcast
    /// relay).
    pub source_rx: Option<mpsc::Receiver<Message>>,
    /// `(consumer_name, consumer_tx)` pairs.
    pub consumers: Vec<(String, mpsc::Sender<Message>)>,
}

// ═══════════════════════════════════════════════════════════════════════
// Wiring
// ═══════════════════════════════════════════════════════════════════════

/// Pre-computed channel wiring for every component in the service.
///
/// All maps support one-time extraction via `remove` during the spawn
/// phase.  The struct is consumed via [`Wiring::finish`] once all
/// channels have been handed out.
pub(crate) struct Wiring {
    /// Input receivers for every consumer (processor or sink).
    consumer_rx: HashMap<String, mpsc::Receiver<Message>>,
    /// Input senders for every consumer — cloned to upstream producers.
    consumer_tx: HashMap<String, mpsc::Sender<Message>>,
    /// Per-source mpsc senders handed to `Source::run`.
    source_tx: HashMap<String, mpsc::Sender<Message>>,
    /// Per-producer broadcast senders (only present when subs > 1).
    broadcast_tx: HashMap<String, broadcast::Sender<Message>>,
    /// Intermediate mpsc receivers for sources with broadcast fan-out.
    broadcast_source_rx: HashMap<String, mpsc::Receiver<Message>>,
    /// Subscriber count per producer name.
    subscriber_count: HashMap<String, usize>,
    /// `downstream_of[producer]` → list of consumer names.
    downstream_of: HashMap<String, Vec<String>>,
}

impl Wiring {
    /// Analyse the component graph and create all channels.
    ///
    /// `source_names` – ordered list of source names.
    /// `processors`   – `(name, from_list)` for each processor.
    /// `sinks`        – `(name, from_list)` for each sink.
    pub(crate) fn build(
        source_names: &[String],
        processors: &[(String, Vec<String>)],
        sinks: &[(String, Vec<String>)],
        capacity: usize,
    ) -> Self {
        // ── Subscriber counts & downstream map ──────────────────────

        let mut subscriber_count: HashMap<String, usize> = HashMap::new();
        let mut downstream_of: HashMap<String, Vec<String>> = HashMap::new();

        for (proc_name, from) in processors {
            for dep in from {
                *subscriber_count.entry(dep.clone()).or_default() += 1;
                downstream_of
                    .entry(dep.clone())
                    .or_default()
                    .push(proc_name.clone());
            }
        }
        for (sink_name, from) in sinks {
            for dep in from {
                *subscriber_count.entry(dep.clone()).or_default() += 1;
                downstream_of
                    .entry(dep.clone())
                    .or_default()
                    .push(sink_name.clone());
            }
        }

        // ── Consumer input channels ─────────────────────────────────

        let mut consumer_tx = HashMap::new();
        let mut consumer_rx = HashMap::new();

        for (name, _) in processors {
            let (tx, rx) = mpsc::channel(capacity);
            consumer_tx.insert(name.clone(), tx);
            consumer_rx.insert(name.clone(), rx);
        }
        for (name, _) in sinks {
            let (tx, rx) = mpsc::channel(capacity);
            consumer_tx.insert(name.clone(), tx);
            consumer_rx.insert(name.clone(), rx);
        }

        // ── Source output channels ──────────────────────────────────

        let mut source_tx = HashMap::new();
        let mut broadcast_tx_map = HashMap::new();
        let mut broadcast_source_rx = HashMap::new();

        for src_name in source_names {
            let subs = subscriber_count.get(src_name).copied().unwrap_or(0);

            if subs == 0 {
                warn!(source = %src_name, "source has no subscribers");
                let (tx, _rx) = mpsc::channel(capacity);
                source_tx.insert(src_name.clone(), tx);
            } else if subs == 1 {
                let consumer_name = &downstream_of[src_name][0];
                let tx = consumer_tx[consumer_name].clone();
                source_tx.insert(src_name.clone(), tx);
            } else {
                // Broadcast: source → mpsc → relay → broadcast → consumers.
                let (tx, rx) = mpsc::channel(capacity);
                source_tx.insert(src_name.clone(), tx);
                broadcast_source_rx.insert(src_name.clone(), rx);

                let (btx, _) = broadcast::channel(capacity);
                broadcast_tx_map.insert(src_name.clone(), btx);
            }
        }

        // ── Processor broadcast channels (subs > 1) ─────────────────

        for (proc_name, _) in processors {
            let subs = subscriber_count.get(proc_name).copied().unwrap_or(0);
            if subs > 1 {
                let (btx, _) = broadcast::channel(capacity);
                broadcast_tx_map.insert(proc_name.clone(), btx);
            }
        }

        Self {
            consumer_rx,
            consumer_tx,
            source_tx,
            broadcast_tx: broadcast_tx_map,
            broadcast_source_rx,
            subscriber_count,
            downstream_of,
        }
    }

    /// Take the mpsc receiver for a consumer (processor or sink).
    pub(crate) fn take_consumer_rx(&mut self, name: &str) -> mpsc::Receiver<Message> {
        self.consumer_rx
            .remove(name)
            .unwrap_or_else(|| panic!("consumer rx for `{name}` must exist"))
    }

    /// Take the mpsc sender for a source.
    pub(crate) fn take_source_tx(&mut self, name: &str) -> mpsc::Sender<Message> {
        self.source_tx
            .remove(name)
            .unwrap_or_else(|| panic!("source tx for `{name}` must exist"))
    }

    /// Build the [`ProducerOutput`] for a processor.
    pub(crate) fn producer_output(&self, proc_name: &str) -> ProducerOutput {
        let subs = self.subscriber_count.get(proc_name).copied().unwrap_or(0);

        if subs == 0 {
            warn!(processor = %proc_name, "processor has no subscribers");
            ProducerOutput::None
        } else if subs == 1 {
            let consumer_name = &self.downstream_of[proc_name][0];
            let tx = self.consumer_tx[consumer_name].clone();
            ProducerOutput::Direct(tx)
        } else {
            let btx = self.broadcast_tx[proc_name].clone();
            ProducerOutput::Broadcast(btx)
        }
    }

    /// Extract all broadcast relay metadata.
    ///
    /// Returns a `Vec<BroadcastRelay>` — one per producer that needs
    /// broadcast fan-out.  After this call the `broadcast_source_rx` map
    /// is empty.
    pub(crate) fn take_broadcast_relays(&mut self) -> Vec<BroadcastRelay> {
        // Move source_rxs out so we can iterate broadcast_tx without
        // borrow conflicts.
        let mut source_rxs = std::mem::take(&mut self.broadcast_source_rx);
        let mut relays = Vec::new();

        for (producer, btx) in &self.broadcast_tx {
            let consumers = self
                .downstream_of
                .get(producer)
                .cloned()
                .unwrap_or_default();

            let source_rx = source_rxs.remove(producer);

            let consumer_txs: Vec<(String, mpsc::Sender<Message>)> = consumers
                .iter()
                .map(|name| (name.clone(), self.consumer_tx[name].clone()))
                .collect();

            relays.push(BroadcastRelay {
                producer: producer.clone(),
                broadcast_tx: btx.clone(),
                source_rx,
                consumers: consumer_txs,
            });
        }

        relays
    }

    /// Drop all consumer sender clones so that channels close naturally
    /// once upstream producers finish.
    pub(crate) fn finish(self) {
        drop(self.consumer_tx);
    }
}
