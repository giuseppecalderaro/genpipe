use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{Result, Service};

/// Runs one or more [`Service`]s concurrently and coordinates shutdown.
///
/// ```rust,ignore
/// let runner = ServiceRunner::new()
///     .with_service(service_a)
///     .with_service(service_b);
///
/// runner.run(shutdown_token).await?;
/// ```
pub struct ServiceRunner {
    services: Vec<Service>,
}

impl ServiceRunner {
    /// Create an empty runner.
    pub fn new() -> Self {
        Self {
            services: Vec::new(),
        }
    }

    /// Register a [`Service`] to be run.
    #[must_use]
    pub fn with_service(mut self, service: Service) -> Self {
        self.services.push(service);
        self
    }

    /// Run **all** registered services concurrently.
    ///
    /// Returns when every service has finished or all have respected the
    /// shutdown token.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let count = self.services.len();
        info!(count, "service runner starting");

        let mut handles = Vec::with_capacity(count);

        for service in self.services {
            let token = shutdown.clone();
            let name = service.name().to_owned();
            handles.push(tokio::spawn(async move {
                let result = service.run(token).await;
                (name, result)
            }));
        }

        let mut errors = Vec::new();
        for handle in handles {
            match handle.await {
                Ok((name, Ok(()))) => {
                    info!(service = %name, "service finished successfully");
                }
                Ok((name, Err(e))) => {
                    error!(service = %name, error = %e, "service finished with error");
                    errors.push(e);
                }
                Err(e) => {
                    error!(error = %e, "service task panicked");
                }
            }
        }

        if let Some(first) = errors.into_iter().next() {
            return Err(first);
        }

        info!("service runner stopped");
        Ok(())
    }
}

impl Default for ServiceRunner {
    fn default() -> Self {
        Self::new()
    }
}
