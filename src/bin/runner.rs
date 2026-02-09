use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use helix::config::PipelineConfig;
use helix::registry::ComponentRegistry;

/// Helix — run any pipeline from a TOML config file.
#[derive(Parser)]
#[command(name = "helix", version, about)]
struct Cli {
    /// Path to the pipeline TOML config file.
    #[arg(short, long, default_value = "config/pipeline.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Initialise tracing from the RUST_LOG env var (defaults to info).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    // ── Load config ─────────────────────────────────────────────────
    info!(config = %cli.config.display(), "loading pipeline config");

    let config = match PipelineConfig::from_file(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!(error = %e, path = %cli.config.display(), "failed to load config");
            return ExitCode::from(1);
        }
    };

    let service_count = config.services.len();
    info!(service_count, "config loaded");

    // ── Build the pipeline ──────────────────────────────────────────
    let registry = ComponentRegistry::with_builtins();

    let runner = match config.build_runner(&registry) {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "failed to build pipeline from config");
            return ExitCode::from(1);
        }
    };

    // ── Graceful shutdown on Ctrl-C ─────────────────────────────────
    let shutdown = CancellationToken::new();

    tokio::spawn({
        let token = shutdown.clone();
        async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!(error = %e, "failed to listen for ctrl-c");
                return;
            }
            info!("received ctrl-c, shutting down…");
            token.cancel();
        }
    });

    // ── Run ─────────────────────────────────────────────────────────
    info!("starting pipeline");

    match runner.run(shutdown).await {
        Ok(()) => {
            info!("pipeline finished");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = %e, "pipeline finished with error");
            ExitCode::from(1)
        }
    }
}
