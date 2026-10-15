//! # Communicator Module
//!
//! Communicator is the client-side communication manager.

pub(crate) mod cache;

use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::servant::{discovery::Discovery, ServantProxy};
use tars_core::{Result, TarsError};
use tars_protocol::endpoint::parse_obj_name;
use tars_protocol::Endpoint;
use tars_registry::registry::{Registrar, TarsRegistry};
use tars_transport::transport::TarsClientConfig;
use tars_util::ClientConfig;

/// Global communicator instance
static GLOBAL_COMMUNICATOR: OnceCell<Arc<Communicator>> = OnceCell::new();

/// Communicator manages client-side communication
pub struct Communicator {
    /// Client configuration
    config: RwLock<ClientConfig>,
    /// Servant proxies cache
    proxies: RwLock<HashMap<String, Arc<ServantProxy>>>,
    registrar: RwLock<Option<Arc<dyn Registrar>>>,
    /// Properties
    properties: RwLock<HashMap<String, String>>,
}

impl Default for Communicator {
    fn default() -> Self {
        Self::new()
    }
}

impl Communicator {
    /// Create a new Communicator
    pub fn new() -> Self {
        Self {
            config: RwLock::new(ClientConfig::default()),
            proxies: RwLock::new(HashMap::new()),
            properties: RwLock::new(HashMap::new()),
            registrar: RwLock::new(None),
        }
    }

    /// Create with configuration
    pub fn with_config(config: ClientConfig) -> Self {
        let comm = Self::new();
        comm.set_config(config);
        comm
    }

    pub fn set_config(&self, config: ClientConfig) {
        self.set_property("locator", &config.locator);
        *self.config.write() = config;
    }

    pub fn set_registrar(&self, registrar: Arc<dyn Registrar>) {
        *self.registrar.write() = Some(registrar);
        // Existing proxies retain their captured registrar; new ones must not reuse them.
        self.proxies.write().clear();
    }

    /// Get the global communicator instance
    pub fn global() -> Arc<Communicator> {
        GLOBAL_COMMUNICATOR
            .get_or_init(|| Arc::new(Communicator::new()))
            .clone()
    }

    /// Get client configuration
    pub fn config(&self) -> ClientConfig {
        self.config.read().clone()
    }

    /// Set locator
    pub fn set_locator(&self, locator: &str) {
        self.config.write().locator = locator.to_string();
        self.set_property("locator", locator);
    }

    /// Get locator
    pub fn locator(&self) -> String {
        self.config.read().locator.clone()
    }

    /// Set a property
    pub fn set_property(&self, key: &str, value: &str) {
        self.properties
            .write()
            .insert(key.to_string(), value.to_string());
    }

    /// Get a property
    pub fn get_property(&self, key: &str) -> Option<String> {
        self.properties.read().get(key).cloned()
    }

    /// Create servant proxy from object name string
    ///
    /// # Arguments
    ///
    /// * `obj_name` - Object name, can be:
    ///   - "App.Server.Obj" - Query from registry
    ///   - "App.Server.Obj@tcp -h 127.0.0.1 -p 10000" - Direct connection
    ///
    /// # Returns
    ///
    /// Arc<ServantProxy> that can be used for RPC calls
    pub fn string_to_proxy(&self, obj_name: &str) -> Result<Arc<ServantProxy>> {
        if obj_name.is_empty() {
            return Err(TarsError::InvalidArgument("empty object name".into()));
        }

        let config = self.config.read().clone();
        let props = self.properties.read();
        let enabled = props
            .get("enableset")
            .is_some_and(|v| v.eq_ignore_ascii_case("y") || v.eq_ignore_ascii_case("true"));
        let set = if enabled {
            Some(props.get("setdivision").cloned().unwrap_or_default())
        } else {
            None
        };
        drop(props);
        let key = format!("{:?}", (obj_name, &config.locator, &set));
        let mut proxies = self.proxies.write();
        if let Some(proxy) = proxies.get(&key) {
            return Ok(proxy.clone());
        }
        let (name, endpoints) = parse_obj_name(obj_name);
        if name.is_empty() {
            return Err(TarsError::InvalidArgument("empty object name".into()));
        }
        if obj_name.contains('@') && endpoints.is_empty() {
            return Err(TarsError::Config("invalid direct endpoint".into()));
        }
        let discovery = if endpoints.is_empty() {
            let registrar = match self.registrar.read().clone() {
                Some(registrar) => registrar,
                None if !config.locator.is_empty() => Arc::new(
                    TarsRegistry::new(&config.locator)
                        .with_timeout(config.async_invoke_timeout.min(i32::MAX as u64) as i32),
                ),
                None => {
                    return Err(TarsError::Config(
                        "object-name discovery requires a locator or registrar".into(),
                    ))
                }
            };
            Some(Arc::new(Discovery::new(
                &name,
                registrar,
                set,
                config.refresh_endpoint_interval,
                config.async_invoke_timeout,
                config
                    .endpoint_cache_path
                    .clone()
                    .map(|path| cache::DiscoveryCache::new(path, key.clone())),
            )))
        } else {
            None
        };

        // Create client config
        let client_config = TarsClientConfig::tcp()
            .with_queue_len(config.queue_len)
            .with_idle_timeout(config.idle_timeout_duration())
            .with_read_timeout(config.read_timeout_duration())
            .with_write_timeout(config.write_timeout_duration())
            .with_dial_timeout(config.dial_timeout_duration());

        // Create proxy
        let proxy = ServantProxy::managed(
            &name,
            endpoints,
            client_config,
            discovery,
            config.check_status_interval,
        );
        proxy.set_timeout(config.async_invoke_timeout);

        // Cache proxy
        proxies.insert(key, Arc::clone(&proxy));

        Ok(proxy)
    }

    /// Get or create servant proxy
    pub fn get_servant_proxy(&self, obj_name: &str) -> Result<Arc<ServantProxy>> {
        self.string_to_proxy(obj_name)
    }

    /// Refresh servant endpoints
    pub fn refresh_servant(&self, obj_name: &str, endpoints: Vec<Endpoint>) -> Result<()> {
        let proxies = self.proxies.read();
        for proxy in proxies
            .values()
            .filter(|proxy| proxy.name() == parse_obj_name(obj_name).0)
        {
            proxy.refresh_endpoints(endpoints.clone());
        }
        Ok(())
    }

    /// Calculate hash key for this communicator (for endpoint manager)
    pub fn hash_key(&self) -> String {
        use md5::{Digest, Md5};

        let mut hasher = Md5::new();
        let props = self.properties.read();

        for key in &["locator", "enableset", "setdivision"] {
            if let Some(value) = props.get(*key) {
                hasher.update(format!("{}:{}", key, value).as_bytes());
            }
        }

        hex::encode(hasher.finalize())
    }
}

/// Get the global communicator
pub fn get_communicator() -> Arc<Communicator> {
    Communicator::global()
}

/// Create a new communicator
pub fn new_communicator() -> Communicator {
    Communicator::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_communicator_creation() {
        let comm = Communicator::new();
        assert!(comm.locator().is_empty());
    }

    #[test]
    fn test_communicator_properties() {
        let comm = Communicator::new();
        comm.set_property("key", "value");
        assert_eq!(comm.get_property("key"), Some("value".to_string()));
        assert_eq!(comm.get_property("nonexistent"), None);
    }

    #[test]
    fn test_communicator_locator() {
        let comm = Communicator::new();
        comm.set_locator("tars.tarsregistry.QueryObj@tcp -h 127.0.0.1 -p 17890");
        assert!(comm.locator().contains("QueryObj"));
    }

    #[tokio::test]
    async fn test_string_to_proxy_direct() {
        let comm = Communicator::new();
        let result = comm.string_to_proxy("Test.HelloServer.HelloObj@tcp -h 127.0.0.1 -p 10000");
        assert!(result.is_ok());

        let proxy = result.unwrap();
        assert_eq!(proxy.name(), "Test.HelloServer.HelloObj");
    }

    #[test]
    fn test_string_to_proxy_empty() {
        let comm = Communicator::new();
        let result = comm.string_to_proxy("");
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_key() {
        let comm = Communicator::new();
        comm.set_property("locator", "test");
        let key1 = comm.hash_key();

        comm.set_property("locator", "test2");
        let key2 = comm.hash_key();

        assert_ne!(key1, key2);
    }
}
