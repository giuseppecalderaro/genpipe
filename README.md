# Genpipe

A Rust framework for building **pipelined services** on top of [Tokio](https://tokio.rs).

Assemble directed graphs of **sources**, **processors**, and **sinks** connected by bounded in-memory channels carrying normalised [`Message`](src/message.rs) objects. Define topologies in code with a builder API, or declaratively via TOML configuration files.

```text
[Source 1] ──┐
[Source 2] ──┼──► [Processor A] ──► [Processor B] ──┬──► [Sink 1]
[Source N] ──┘                                      ├──► [Sink 2]
                                                    └──► [Sink N]
```

When multiple downstream components reference the same upstream node, Genpipe automatically creates a **broadcast channel** so every consumer receives a copy.

### Key features

- **DAG-based routing** with automatic `mpsc` / `broadcast` channel selection
- **Cycle detection** — the processor graph is validated at startup (three-colour DFS)
- **Graceful drain** — on shutdown, sources stop immediately while processors and sinks drain buffered messages; a safety timeout force-stops stragglers
- **Error strategy** — configure `Drop` (default) or `Retry` with a configurable max-attempts for processor/sink failures
- **NATS integration** — built-in source (with exponential-backoff reconnect) and sink (with flush-on-shutdown)
- **Code-first _or_ config-driven** topologies via builder API or TOML files

---

## Quick start

### Code-first (builder API)

```rust
use genpipe::prelude::*;
use genpipe::components::{sources::IntervalSource, sinks::LogSink};
use std::time::Duration;

#[tokio::main]
async fn main() -> genpipe::Result<()> {
    let shutdown = CancellationToken::new();

    let service = Service::builder("heartbeat")
        .source(IntervalSource::new("tick", Duration::from_secs(1)))
        .sink(LogSink::default())
        .build()?;

    service.run(shutdown).await
}
```

### Config-driven (TOML)

```toml
# config/pipeline.toml
[[service]]
name = "heartbeat"

  [[service.source]]
  type = "interval"
  name = "tick"
    [service.source.settings]
    interval_ms = 1000

  [[service.sink]]
  type = "log"
  name = "console"
  from = ["tick"]
```

```sh
cargo run -- -c config/pipeline.toml
```

---

## Core concepts

| Concept | Description |
|---------|-------------|
| **Message** | The normalised envelope flowing through every pipeline. Carries a UUID, timestamp, origin tag, metadata map, and a typed `Payload` (JSON, Text, Bytes, or Empty). |
| **Source** | Produces messages — e.g. a timer, NATS subscription, or file watcher. |
| **Processor** | Transforms, filters, or splits messages. Returns `Single`, `Many`, or `Drop`. |
| **Sink** | Consumes messages — e.g. logs, writes to a database, publishes to NATS. Implement `consume()` and optionally `shutdown()` for cleanup. |
| **Service** | A fully wired DAG of sources → processors → sinks. |
| **ServiceRunner** | Runs multiple services concurrently and coordinates shutdown. |
| **ComponentRegistry** | Maps type-key strings to factory closures so TOML configs can instantiate components at runtime. |

---

## Message

```rust
let msg = Message::text("my-source", "hello world")
    .with_metadata("env", "production");

// Serialization
let json = msg.to_json()?;
let msgpack = msg.to_msgpack()?;
let decoded = Message::from_json(&json)?;

// Encoding-agnostic
let bytes = msg.encode(Encoding::MsgPackNamed)?;
let decoded = Message::decode(&bytes, Encoding::MsgPackNamed)?;
```

Payload variants: `Json(Value)`, `Text(String)`, `Bytes(bytes::Bytes)`, `Empty`.

---

## Built-in components

### Sources

| Type key | Struct | Description |
|----------|--------|-------------|
| `interval` | `IntervalSource` | Emits numbered JSON messages at a fixed interval. Settings: `interval_ms`. |
| `nats` | `NatsSource` | Subscribes to a NATS subject with exponential-backoff reconnect. Settings: `url`, `subject`, plus [auth](#nats-authentication). |
| — | `IterSource` | Emits a fixed list of messages (useful for tests). |
| — | `FnSource` | Wraps an async closure as a source. |

### Processors

| Type key | Struct | Description |
|----------|--------|-------------|
| `passthrough` | `MapProcessor` | Forwards messages unchanged. |
| — | `MapProcessor` | Applies a synchronous `Fn(Message) -> Message`. |
| — | `FilterProcessor` | Drops messages that fail a predicate. |
| — | `FnProcessor` | Async closure returning `ProcessorOutput`. |

### Sinks

| Type key | Struct | Description |
|----------|--------|-------------|
| `log` | `LogSink` | Logs via `tracing::info`. |
| `nats` | `NatsSink` | Publishes to a NATS subject; flushes on shutdown. Settings: `url`, `subject`, plus [auth](#nats-authentication). |
| — | `FnSink` | Synchronous closure sink. |
| — | `AsyncFnSink` | Async closure sink. |
| — | `CollectorSink` | Collects messages into a `Vec` (for testing). |

### NATS authentication

Secrets are never written in the config file. The `nats` source and sink read them from environment variables, or from a `.creds` file path. Configure at most one method:

| Settings | Method |
|----------|--------|
| `user` + `password_env` | Username (inline) and the name of the env var holding the password |
| `token_env` | Name of the env var holding the token |
| `nkey_seed_env` | Name of the env var holding the NKey seed |
| `credentials_file` | Path to a NATS `.creds` file (JWT + NKey) |

```toml
[[service.sink]]
type = "nats"
name = "publisher"
  [service.sink.settings]
  url = "nats://nats.internal:4222"
  subject = "output.processed"
  token_env = "NATS_TOKEN"
```

Inline secret settings (`password`, `token`, `nkey`, `nkey_seed`, `seed`, `credentials`) are rejected at startup. Credentials embedded in the URL (`nats://user:pass@host`) still work, but they trigger a warning. URLs are always redacted (`nats://user:***@host`) in logs and error messages. In code, use `NatsSource::new(..).with_auth(NatsAuth::Token(..))`.

---

## DAG routing & broadcast

Processors and sinks declare their predecessors via a `from` list. The runtime analyses subscriber counts at startup:

- **1 subscriber** → direct `mpsc` channel (zero-copy).
- **N subscribers** → `broadcast` channel with per-consumer relay tasks.

```toml
[[service.sink]]
type = "log"
name = "console"
from = ["transform"]

[[service.sink]]
type = "log"
name = "backup"
from = ["transform"]    # same predecessor → broadcast
```

The builder API auto-wires linearly when `from` is not specified.

---

## TOML configuration schema

```toml
[[service]]
name = "my-pipeline"
channel_capacity = 2048          # optional, default 1024
drain_timeout_ms = 30000         # optional, default 30000 (ms)
error_strategy = "drop"          # optional, "drop" (default) or "retry"
max_retry_attempts = 3           # optional, default 3 (used when error_strategy = "retry")

  [[service.source]]
  type = "interval"              # registry type key
  name = "tick"                  # optional, defaults to type
    [service.source.settings]
    interval_ms = 1000

  [[service.processor]]
  type = "passthrough"
  name = "transform"
  from = ["tick"]                # required for processors
    [service.processor.settings]
    # arbitrary key-value pairs forwarded to the factory

  [[service.sink]]
  type = "log"
  name = "output"
  from = ["transform"]          # required for sinks
```

---

## Running

```sh
# Default config (config/pipeline.toml)
cargo run

# Custom config
cargo run -- --config config/broadcast_demo.toml

# With debug logging
RUST_LOG=debug cargo run

# Run examples
cargo run --example basic_pipeline
cargo run --example config_pipeline
```

Shutdown: press **Ctrl-C** — the runner cancels a `CancellationToken` that propagates to every component. Sources stop immediately, while processors and sinks enter a **drain phase** (configurable via `drain_timeout_ms`) to process remaining buffered messages before the pipeline is torn down.

---

## Extending

### Custom source

```rust
use genpipe::prelude::*;

struct MySource { name: String }

#[async_trait]
impl Source for MySource {
    fn name(&self) -> &str { &self.name }

    async fn run(&self, tx: mpsc::Sender<Message>, shutdown: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    tx.send(Message::text(&self.name, "ping")).await
                        .map_err(|e| Error::ChannelSend(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}
```

### Registering in the registry

```rust
let mut registry = ComponentRegistry::with_builtins();

registry.register_source("my_source", |cfg| {
    let name = cfg.resolved_name().to_owned();
    Ok(Box::new(MySource { name }))
});
```

---

## Project structure

```
genpipe/
├── Cargo.toml
├── config/
│   ├── pipeline.toml            # Default pipeline config
│   └── broadcast_demo.toml      # Multi-source broadcast example
├── examples/
│   ├── basic_pipeline.rs        # Code-first multi-service example
│   └── config_pipeline.rs       # Config-driven example
└── src/
    ├── bin/
    │   └── runner.rs            # CLI binary (clap)
    ├── lib.rs                   # Library root + prelude
    ├── message.rs               # Message, Payload, Encoding
    ├── error.rs                 # Error enum
    ├── config.rs                # TOML config parsing
    ├── registry.rs              # Component factory registry
    ├── runner.rs                # ServiceRunner
    ├── service.rs               # Service, ServiceBuilder, DAG validation
    ├── wiring.rs                # Channel graph construction (internal)
    ├── runtime.rs               # Per-component task runners with drain & retry (internal)
    └── components/
        ├── mod.rs               # Source, Processor, Sink traits
        ├── nats_auth.rs         # NatsAuth, URL redaction
        ├── sources.rs           # IntervalSource, FnSource, IterSource, NatsSource
        ├── processors.rs        # MapProcessor, FilterProcessor, FnProcessor
        └── sinks.rs             # LogSink, FnSink, AsyncFnSink, CollectorSink, NatsSink
├── tests/
│   └── dag_wiring.rs            # Integration tests for DAG topology & validation
```

---

## License

Unlicensed — add your preferred license here.
