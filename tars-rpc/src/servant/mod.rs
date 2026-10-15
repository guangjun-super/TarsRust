//! # Servant Module
//!
//! ServantProxy is the client-side RPC proxy for calling remote services.

pub(crate) mod discovery;

use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use tars_core::{consts, Context, Result, TarsError};
use tars_protocol::protocol::{RequestPacket, ResponsePacket, TarsProtocol};
use tars_protocol::Endpoint;
use tars_registry::adapter::AdapterProxy;
use tars_transport::filter::Message;
use tars_transport::transport::TarsClientConfig;
use tars_util::selector::{create_selector, HashType, Selector};

/// Global request ID counter
static REQUEST_ID: AtomicI32 = AtomicI32::new(0);

/// Generate unique request ID
fn gen_request_id() -> i32 {
    loop {
        let current = REQUEST_ID.load(Ordering::SeqCst);
        let next = if current >= i32::MAX - 1 {
            1
        } else {
            current + 1
        };
        if REQUEST_ID
            .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if next != 0 {
                return next;
            }
        }
    }
}

/// ServantProxy for client-side RPC calls
pub struct ServantProxy {
    /// Service name (e.g., "Test.HelloServer.HelloObj")
    name: String,
    /// Protocol handler
    #[allow(dead_code)]
    protocol: Arc<TarsProtocol>,
    /// Endpoint selector
    selector: Arc<dyn Selector>,
    mod_selector: Arc<dyn Selector>,
    consistent_selector: Arc<dyn Selector>,
    routing: parking_lot::Mutex<String>,
    probes: parking_lot::Mutex<Vec<Endpoint>>,
    /// Active adapters
    adapters: RwLock<HashMap<Endpoint, Arc<AdapterProxy>>>,
    /// Active endpoints
    active_endpoints: RwLock<Vec<Endpoint>>,
    /// Timeout in milliseconds
    timeout: AtomicI64,
    /// Protocol version
    version: i16,
    /// Queue length
    queue_len: AtomicI32,
    discovery: Option<Arc<discovery::Discovery>>,
    owner: OnceCell<Weak<ServantProxy>>,
    maintenance: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    health_interval: Duration,
    /// Client config
    client_config: TarsClientConfig,
}

impl ServantProxy {
    /// Create a new ServantProxy
    pub fn new(name: &str, endpoints: Vec<Endpoint>, config: TarsClientConfig) -> Self {
        let selector = create_selector("roundrobin");
        selector.refresh(endpoints.clone());

        Self {
            name: name.to_string(),
            protocol: Arc::new(TarsProtocol::new()),
            selector,
            mod_selector: create_selector("modhash"),
            consistent_selector: create_selector("consistenthash"),
            routing: parking_lot::Mutex::new(String::new()),
            probes: parking_lot::Mutex::new(Vec::new()),
            adapters: RwLock::new(HashMap::new()),
            active_endpoints: RwLock::new(endpoints.clone()),
            timeout: AtomicI64::new(consts::DEFAULT_ASYNC_TIMEOUT as i64),
            version: consts::TARS_VERSION,
            queue_len: AtomicI32::new(0),
            client_config: config,
            discovery: None,
            owner: OnceCell::new(),
            maintenance: parking_lot::Mutex::new(None),
            health_interval: Duration::from_secs(10),
        }
    }

    pub(crate) fn managed(
        name: &str,
        endpoints: Vec<Endpoint>,
        config: TarsClientConfig,
        discovery: Option<Arc<discovery::Discovery>>,
        health_interval: u64,
    ) -> Arc<Self> {
        let mut proxy = Self::new(name, endpoints, config);
        proxy.discovery = discovery;
        proxy.health_interval = Duration::from_millis(health_interval.max(1));
        let proxy = Arc::new(proxy);
        let _ = proxy.owner.set(Arc::downgrade(&proxy));
        proxy
    }

    async fn ensure_endpoints(&self) -> Result<()> {
        if let Some(discovery) = &self.discovery {
            let mut initialized = discovery.initialized.lock().await;
            if !*initialized {
                self.refresh_endpoints(discovery.initial().await?);
                *initialized = true;
            }
        }
        self.start_maintenance();
        Ok(())
    }

    fn start_maintenance(&self) {
        let Some(owner) = self.owner.get().cloned() else {
            return;
        };
        let mut task = self.maintenance.lock();
        if task.is_some() {
            return;
        }
        let discovery = self.discovery.clone();
        let health_interval = self.health_interval;
        *task = Some(tokio::spawn(async move {
            let mut health = tokio::time::interval(health_interval);
            let mut refresh = tokio::time::interval(
                discovery
                    .as_ref()
                    .map_or(Duration::from_secs(60), |d| d.interval),
            );
            health.tick().await;
            refresh.tick().await;
            loop {
                tokio::select! {
                    _ = health.tick() => {
                        let Some(proxy) = owner.upgrade() else { break };
                        proxy.check_health();
                    }
                    _ = refresh.tick(), if discovery.is_some() => {
                        let discovery = discovery.as_ref().unwrap();
                        // No strong proxy reference is held over registry I/O.
                        match discovery.refresh().await {
                            Ok(endpoints) => {
                                let Some(proxy) = owner.upgrade() else { break };
                                proxy.refresh_endpoints(endpoints);
                            }
                            Err(error) => tracing::warn!("endpoint refresh failed: {error}"),
                        }
                    }
                }
            }
        }));
    }

    fn check_health(&self) {
        let mut routing = self.routing.lock();
        let mut probes = self.probes.lock();
        for adapter in self.adapters.read().values() {
            if adapter.check_active().1 && !probes.contains(adapter.endpoint()) {
                probes.push(adapter.endpoint().clone());
            }
        }
        self.rebuild_routing(&mut routing);
    }

    fn rebuild_routing(&self, fingerprint: &mut String) {
        let endpoints = self.active_endpoints.read();
        let adapters = self.adapters.read();
        let mut healthy: Vec<_> = endpoints
            .iter()
            .filter(|ep| adapters.get(*ep).is_none_or(|a| a.is_active()))
            .cloned()
            .collect();
        // Registry-active nodes remain a last resort when all local checks failed.
        if healthy.is_empty() {
            healthy = endpoints.clone();
        }
        let next = format!("{healthy:?}");
        if *fingerprint == next {
            return;
        }
        self.mod_selector.refresh(healthy.clone());
        self.consistent_selector.refresh(healthy.clone());
        let weighted = tars_util::selector::build_static_weight_list(&healthy)
            .map(|indices| indices.into_iter().map(|i| healthy[i].clone()).collect())
            .unwrap_or(healthy);
        self.selector.refresh(weighted);
        *fingerprint = next;
    }

    /// Get service name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set timeout in milliseconds
    pub fn set_timeout(&self, timeout_ms: u64) {
        self.timeout.store(timeout_ms as i64, Ordering::SeqCst);
    }

    /// Get timeout
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout.load(Ordering::SeqCst) as u64)
    }

    /// Refresh endpoints
    pub fn refresh_endpoints(&self, endpoints: Vec<Endpoint>) {
        let mut routing = self.routing.lock();
        let mut adapters = self.adapters.write();
        let mut active = self.active_endpoints.write();

        // Remove old adapters
        let new_set: std::collections::HashSet<_> = endpoints.iter().collect();
        adapters.retain(|ep, adapter| {
            if new_set.contains(ep)
                && endpoints.iter().any(|new| {
                    new == ep && format!("{new:?}") == format!("{:?}", adapter.endpoint())
                })
            {
                true
            } else {
                adapter.close();
                false
            }
        });

        // Add new adapters
        for ep in &endpoints {
            if ep.is_tcp() && !adapters.contains_key(ep) {
                let adapter = AdapterProxy::new(ep.clone(), self.client_config.clone());
                adapters.insert(ep.clone(), adapter);
            }
        }

        self.probes.lock().retain(|ep| endpoints.contains(ep));
        *active = endpoints;
        drop(active);
        drop(adapters);
        self.rebuild_routing(&mut routing);
    }

    /// Get or create adapter for endpoint
    fn get_or_create_adapter(&self, endpoint: &Endpoint) -> Arc<AdapterProxy> {
        let adapters = self.adapters.read();
        if let Some(adapter) = adapters.get(endpoint) {
            return Arc::clone(adapter);
        }
        drop(adapters);

        let mut adapters = self.adapters.write();
        if let Some(adapter) = adapters.get(endpoint) {
            return Arc::clone(adapter);
        }

        let adapter = AdapterProxy::new(endpoint.clone(), self.client_config.clone());
        adapters.insert(endpoint.clone(), Arc::clone(&adapter));
        adapter
    }

    /// Select an adapter for the request
    fn select_adapter(&self, msg: &Message) -> Result<Arc<AdapterProxy>> {
        self.check_health();
        let _routing = self.routing.lock();
        if let Some(endpoint) = self.probes.lock().pop() {
            return Ok(self.get_or_create_adapter(&endpoint));
        }
        let endpoint = if msg.is_hash {
            match msg.hash_type {
                HashType::ModHash => self.mod_selector.select(msg)?,
                HashType::ConsistentHash => self.consistent_selector.select(msg)?,
            }
        } else {
            self.selector.select(msg)?
        };
        if !endpoint.is_tcp() {
            return Err(TarsError::Config(format!(
                "RPC transport currently requires TCP: {}",
                endpoint.to_endpoint_string()
            )));
        }
        Ok(self.get_or_create_adapter(&endpoint))
    }

    /// Invoke a remote method
    pub async fn invoke(
        &self,
        ctx: Context,
        func_name: &str,
        buffer: Vec<u8>,
        status: HashMap<String, String>,
        context: HashMap<String, String>,
    ) -> Result<ResponsePacket> {
        let mut msg = Message::new();

        // Build request
        msg.req.i_version = self.version;
        msg.req.c_packet_type = consts::TARS_NORMAL;
        msg.req.i_request_id = gen_request_id();
        msg.req.s_servant_name = self.name.clone();
        msg.req.s_func_name = func_name.to_string();
        msg.req.s_buffer = buffer;
        msg.req.i_timeout = self.timeout.load(Ordering::SeqCst) as i32;
        msg.req.status = status;
        msg.req.context = context;

        // Handle dyeing
        if let Some(dye_key) = ctx.dyeing_key() {
            msg.req
                .status
                .insert(consts::STATUS_DYED_KEY.to_string(), dye_key.to_string());
            msg.req.add_message_type(consts::TARS_MESSAGE_TYPE_DYED);
        }

        // Handle tracing
        if let Some(trace_key) = ctx.trace_key() {
            msg.req
                .status
                .insert(consts::STATUS_TRACE_KEY.to_string(), trace_key.to_string());
            msg.req.add_message_type(consts::TARS_MESSAGE_TYPE_TRACE);
        }

        self.do_invoke(ctx, msg).await
    }

    /// Invoke with oneway (no response)
    pub async fn invoke_oneway(
        &self,
        _ctx: Context,
        func_name: &str,
        buffer: Vec<u8>,
        status: HashMap<String, String>,
        context: HashMap<String, String>,
    ) -> Result<()> {
        self.ensure_endpoints().await?;
        let mut req = RequestPacket::new();
        req.i_version = self.version;
        req.c_packet_type = consts::TARS_ONEWAY;
        req.i_request_id = gen_request_id();
        req.s_servant_name = self.name.clone();
        req.s_func_name = func_name.to_string();
        req.s_buffer = buffer;
        req.i_timeout = self.timeout.load(Ordering::SeqCst) as i32;
        req.status = status;
        req.context = context;

        let msg = Message::with_request(req);
        let adapter = self.select_adapter(&msg)?;
        match tokio::time::timeout(self.timeout(), adapter.send(&msg.req)).await {
            Ok(Ok(())) => adapter.success_add(),
            Ok(Err(error)) => {
                adapter.fail_add();
                return Err(error);
            }
            Err(_) => {
                adapter.fail_add();
                return Err(TarsError::Timeout(self.timeout().as_millis() as u64));
            }
        }

        Ok(())
    }

    /// Invoke with hash routing
    pub async fn invoke_hash(
        &self,
        ctx: Context,
        func_name: &str,
        buffer: Vec<u8>,
        hash_code: u32,
        hash_type: HashType,
    ) -> Result<ResponsePacket> {
        let mut msg = Message::new();

        msg.req.i_version = self.version;
        msg.req.c_packet_type = consts::TARS_NORMAL;
        msg.req.i_request_id = gen_request_id();
        msg.req.s_servant_name = self.name.clone();
        msg.req.s_func_name = func_name.to_string();
        msg.req.s_buffer = buffer;
        msg.req.i_timeout = self.timeout.load(Ordering::SeqCst) as i32;

        msg.is_hash = true;
        msg.hash_code = hash_code;
        msg.hash_type = hash_type;

        self.do_invoke(ctx, msg).await
    }

    /// Internal invoke implementation
    async fn do_invoke(&self, mut ctx: Context, msg: Message) -> Result<ResponsePacket> {
        self.ensure_endpoints().await?;
        // Check queue limit
        let queue_len = self.queue_len.fetch_add(1, Ordering::SeqCst);
        if queue_len >= DEFAULT_OBJ_QUEUE_MAX {
            self.queue_len.fetch_sub(1, Ordering::SeqCst);
            return Err(TarsError::QueueFull);
        }

        let _queue = QueueGuard(&self.queue_len);
        // Set timeout context
        let timeout = self.timeout();
        ctx.set_timeout(timeout);

        // Select adapter
        let adapter = match self.select_adapter(&msg) {
            Ok(adp) => adp,
            Err(e) => {
                return Err(e);
            }
        };

        // Update context with server info
        ctx.set_server_ip(adapter.endpoint().host.clone());
        ctx.set_server_port(adapter.endpoint().port);

        // Register response channel
        let request_id = msg.req.i_request_id;
        let rx = adapter.register_response(request_id);
        let _response = ResponseGuard {
            adapter: adapter.clone(),
            request_id,
        };

        // One deadline covers queueing the send and waiting for the response.
        let result = tokio::time::timeout(timeout, async {
            adapter.send(&msg.req).await?;
            rx.await.map_err(|_| TarsError::ConnectionClosed)
        })
        .await;

        match result {
            Ok(Ok(resp)) => {
                adapter.success_add();
                if resp.is_success() {
                    Ok(resp)
                } else {
                    Err(TarsError::ServerError {
                        code: resp.i_ret,
                        message: resp.s_result_desc,
                    })
                }
            }
            Ok(Err(error)) => {
                adapter.fail_add();
                Err(error)
            }
            Err(_) => {
                adapter.fail_add();
                Err(TarsError::Timeout(timeout.as_millis() as u64))
            }
        }
    }
}

struct QueueGuard<'a>(&'a AtomicI32);
impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ResponseGuard {
    adapter: Arc<AdapterProxy>,
    request_id: i32,
}
impl Drop for ResponseGuard {
    fn drop(&mut self) {
        self.adapter.unregister_response(self.request_id);
    }
}

impl Drop for ServantProxy {
    fn drop(&mut self) {
        if let Some(task) = self.maintenance.get_mut().take() {
            task.abort();
        }
        for adapter in self.adapters.get_mut().values() {
            adapter.close();
        }
    }
}

/// Default max queue size per object
const DEFAULT_OBJ_QUEUE_MAX: i32 = 10000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_request_id() {
        let id1 = gen_request_id();
        let id2 = gen_request_id();
        assert_ne!(id1, id2);
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
    }

    #[tokio::test]
    async fn test_servant_proxy_creation() {
        let endpoints = vec![Endpoint::tcp("127.0.0.1", 10000)];
        let config = TarsClientConfig::tcp();
        let proxy = ServantProxy::new("Test.HelloServer.HelloObj", endpoints, config);

        assert_eq!(proxy.name(), "Test.HelloServer.HelloObj");
    }

    #[tokio::test]
    async fn test_servant_proxy_timeout() {
        let endpoints = vec![Endpoint::tcp("127.0.0.1", 10000)];
        let config = TarsClientConfig::tcp();
        let proxy = ServantProxy::new("Test.HelloServer.HelloObj", endpoints, config);

        proxy.set_timeout(5000);
        assert_eq!(proxy.timeout(), Duration::from_millis(5000));
    }
}

#[cfg(test)]
mod routing_tests;
