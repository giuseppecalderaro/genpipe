//! Run a pipeline defined entirely by a TOML config file.
//!
//! Usage:
//!
//!     cargo run --example config_pipeline
//!     cargo run --example config_pipeline -- path/to/your.toml
//!
//! Without arguments it loads the bundled `examples/pipeline.toml`.

use std::time::Duration;

use helix::prelude::*;

#[tokio::main]
async fn main() -> helix::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    // ── Load config ─────────────────────────────────────────────────
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/pipeline.toml".into());

    let config = PipelineConfig::from_file(&path)?;
    println!("Loaded {} service(s) from {path}", config.services.len());

    // ── Build a registry (built-ins + any custom components) ────────
    let registry = ComponentRegistry::with_builtins();

    // You can extend the registry before building:
    //
    //   registry.register_processor("my_proc", |cfg| { … });
    //   registry.register_sink("my_sink", |cfg| { … });

    // ── Build & run ─────────────────────────────────────────────────
    let runner = config.build_runner(&registry)?;

    let shutdown = CancellationToken::new();

    // Auto-shutdown after 5 seconds for this demo.
    tokio::spawn({
        let token = shutdown.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            token.cancel();
        }
    });

    println!("Running pipeline (will auto-stop in 5s)…\n");
    runner.run(shutdown).await?;

    println!("\nDone.");
    Ok(())
}
