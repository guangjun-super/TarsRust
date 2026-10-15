//! Per-proxy discovery snapshot; never consults mutable communicator configuration.
use std::{sync::Arc, time::Duration};
use tars_core::{Result, TarsError};
use tars_protocol::Endpoint;
use tars_registry::registry::{EndpointManager, Registrar};

pub(crate) struct Discovery {
    manager: EndpointManager,
    cache: Option<crate::communicator::cache::DiscoveryCache>,
    pub interval: Duration,
    timeout: Duration,
    pub initialized: tokio::sync::Mutex<bool>,
}
impl Discovery {
    pub fn new(
        name: &str,
        registrar: Arc<dyn Registrar>,
        set: Option<String>,
        interval: u64,
        timeout: u64,
        cache: Option<crate::communicator::cache::DiscoveryCache>,
    ) -> Self {
        Self {
            cache,
            manager: EndpointManager::new(name, registrar).with_set_division(set),
            interval: Duration::from_millis(interval.max(1)),
            timeout: Duration::from_millis(timeout.max(1)),
            initialized: tokio::sync::Mutex::new(false),
        }
    }
    pub async fn initial(&self) -> Result<Vec<Endpoint>> {
        match self.refresh().await {
            Ok(endpoints) => Ok(endpoints),
            Err(error) => {
                if let Some(cache) = &self.cache {
                    match cache.load().await {
                        Ok(Some(endpoints)) if !endpoints.is_empty() => return Ok(endpoints),
                        Err(cache_error) => {
                            tracing::warn!("cannot load endpoint cache: {cache_error}")
                        }
                        _ => {}
                    }
                }
                Err(error)
            }
        }
    }
    pub async fn refresh(&self) -> Result<Vec<Endpoint>> {
        tokio::time::timeout(self.timeout, self.manager.refresh())
            .await
            .map_err(|_| TarsError::Timeout(self.timeout.as_millis() as u64))??;
        let endpoints = self.manager.get_active().await;
        if let Some(cache) = &self.cache {
            if let Err(error) = cache.store(&endpoints).await {
                tracing::warn!("cannot save endpoint cache: {error}");
            }
        }
        Ok(endpoints)
    }
}
