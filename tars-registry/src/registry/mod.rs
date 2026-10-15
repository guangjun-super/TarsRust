//! # Registry Module
//!
//! Service registration and discovery interface with full QueryF protocol support.
//!
//! ## Circuit Breaker
//!
//! This module implements a circuit breaker pattern for registry nodes:
//! - Each registry node (IP:Port) has its own circuit breaker state
//! - Nodes are marked as unavailable after timeout or error
//! - Automatic recovery after a configurable interval

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use tars_core::codec::{Buffer, Reader};
use tars_core::{Result, TarsError};
use tars_protocol::endpoint::ServantInstance;
use tars_protocol::protocol::queryf::{
    decode_endpoint_list, EndpointF, QUERY_FIND_OBJECT_BY_ID_IN_SAME_GROUP,
    QUERY_FIND_OBJECT_BY_ID_IN_SAME_SET,
};
use tars_protocol::protocol::RequestPacket;
use tars_protocol::Endpoint;
use tars_protocol::ResponsePacket;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Circuit breaker state for a single registry node
#[derive(Debug)]
pub struct NodeCircuitBreaker {
    /// Node address (ip:port)
    address: String,
    /// Whether the node is available
    available: AtomicBool,
    /// Consecutive failure count
    fail_count: AtomicI32,
    /// Last failure timestamp (unix seconds)
    last_fail_time: AtomicI64,
    /// Last success timestamp (unix seconds)
    last_success_time: AtomicI64,
    /// Circuit open time (when it was marked unavailable)
    circuit_open_time: AtomicI64,
}

impl NodeCircuitBreaker {
    /// Create a new circuit breaker for a node
    pub fn new(address: &str) -> Self {
        let now = now_secs();
        Self {
            address: address.to_string(),
            available: AtomicBool::new(true),
            fail_count: AtomicI32::new(0),
            last_fail_time: AtomicI64::new(0),
            last_success_time: AtomicI64::new(now),
            circuit_open_time: AtomicI64::new(0),
        }
    }

    /// Check if the node is available
    pub fn is_available(&self) -> bool {
        if self.available.load(Ordering::SeqCst) {
            return true;
        }

        // Check if we should try to recover (half-open state)
        let now = now_secs();
        let open_time = self.circuit_open_time.load(Ordering::SeqCst);

        // Try to recover after REGISTRY_RECOVER_INTERVAL seconds
        if now - open_time >= REGISTRY_RECOVER_INTERVAL {
            debug!(
                "Node {} entering half-open state for recovery attempt",
                self.address
            );
            return true;
        }

        false
    }

    /// Record a successful request
    pub fn record_success(&self) {
        self.available.store(true, Ordering::SeqCst);
        self.fail_count.store(0, Ordering::SeqCst);
        self.last_success_time.store(now_secs(), Ordering::SeqCst);
        debug!("Node {} marked as available after success", self.address);
    }

    /// Record a failed request (timeout or error)
    /// Returns true if the circuit was just opened (node became unavailable)
    pub fn record_failure(&self) -> bool {
        let count = self.fail_count.fetch_add(1, Ordering::SeqCst) + 1;
        self.last_fail_time.store(now_secs(), Ordering::SeqCst);

        // Open circuit after REGISTRY_FAIL_THRESHOLD consecutive failures
        if count >= REGISTRY_FAIL_THRESHOLD {
            let was_available = self.available.swap(false, Ordering::SeqCst);
            if was_available {
                self.circuit_open_time.store(now_secs(), Ordering::SeqCst);
                warn!(
                    "Node {} circuit opened after {} consecutive failures",
                    self.address, count
                );
                return true;
            }
        }

        false
    }

    /// Get the node address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Reset the circuit breaker state
    pub fn reset(&self) {
        self.available.store(true, Ordering::SeqCst);
        self.fail_count.store(0, Ordering::SeqCst);
        self.circuit_open_time.store(0, Ordering::SeqCst);
    }
}

/// Registry circuit breaker manager
/// Manages circuit breakers for multiple registry nodes
pub struct RegistryCircuitBreaker {
    /// Circuit breakers by node address
    breakers: Mutex<HashMap<String, Arc<NodeCircuitBreaker>>>,
}

impl RegistryCircuitBreaker {
    /// Create a new registry circuit breaker manager
    pub fn new() -> Self {
        Self {
            breakers: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a circuit breaker for a node
    pub fn get_breaker(&self, address: &str) -> Arc<NodeCircuitBreaker> {
        let mut breakers = self.breakers.lock();
        breakers
            .entry(address.to_string())
            .or_insert_with(|| Arc::new(NodeCircuitBreaker::new(address)))
            .clone()
    }

    /// Get all available nodes from a list
    pub fn filter_available(&self, addresses: &[String]) -> Vec<String> {
        let breakers = self.breakers.lock();
        addresses
            .iter()
            .filter(|addr| {
                breakers
                    .get(*addr)
                    .map(|b| b.is_available())
                    .unwrap_or(true) // New nodes are considered available
            })
            .cloned()
            .collect()
    }

    /// Get the number of available nodes
    pub fn available_count(&self, addresses: &[String]) -> usize {
        self.filter_available(addresses).len()
    }

    /// Reset all circuit breakers
    pub fn reset_all(&self) {
        let breakers = self.breakers.lock();
        for breaker in breakers.values() {
            breaker.reset();
        }
    }
}

impl Default for RegistryCircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current time in seconds
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Registry circuit breaker constants
/// Number of consecutive failures before opening circuit
const REGISTRY_FAIL_THRESHOLD: i32 = 1; // Open immediately on first failure
/// Interval to try recovery (seconds)
const REGISTRY_RECOVER_INTERVAL: i64 = 30;

/// Registrar trait for service registration and discovery
#[async_trait]
pub trait Registrar: Send + Sync {
    /// Register a servant instance
    async fn register(&self, servant: &ServantInstance) -> Result<()>;

    /// Deregister a servant instance
    async fn deregister(&self, servant: &ServantInstance) -> Result<()>;

    /// Query servant endpoints by object ID
    /// Returns (active_endpoints, inactive_endpoints)
    async fn query_servant(&self, id: &str) -> Result<(Vec<Endpoint>, Vec<Endpoint>)>;

    /// Query servant endpoints by object ID and SET division
    async fn query_servant_by_set(
        &self,
        id: &str,
        set: &str,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)>;
}

/// Direct registrar implementation (no service discovery)
pub struct DirectRegistrar {
    endpoints: Vec<Endpoint>,
}

impl DirectRegistrar {
    pub fn new(endpoints: Vec<Endpoint>) -> Self {
        Self { endpoints }
    }
}

#[async_trait]
impl Registrar for DirectRegistrar {
    async fn register(&self, _servant: &ServantInstance) -> Result<()> {
        // Direct mode doesn't support registration
        Ok(())
    }

    async fn deregister(&self, _servant: &ServantInstance) -> Result<()> {
        // Direct mode doesn't support deregistration
        Ok(())
    }

    async fn query_servant(&self, _id: &str) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        Ok((self.endpoints.clone(), vec![]))
    }

    async fn query_servant_by_set(
        &self,
        _id: &str,
        _set: &str,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        Ok((self.endpoints.clone(), vec![]))
    }
}

/// Tars registry client (communicates with tars.tarsregistry.QueryObj)
///
/// Supports multiple registry nodes with circuit breaker for each node.
/// When a node fails (timeout or error), it will be marked as unavailable
/// and requests will be routed to other available nodes.
pub struct TarsRegistry {
    /// Locator string (e.g., "tars.tarsregistry.QueryObj@tcp -h 127.0.0.1 -p 17890")
    locator: String,
    /// All registry node addresses parsed from locator
    nodes: Vec<String>,
    /// Circuit breaker for managing node availability
    circuit_breaker: RegistryCircuitBreaker,
    /// Current node index for round-robin selection
    current_index: std::sync::atomic::AtomicUsize,
    /// Query timeout in milliseconds
    timeout: i32,
}

impl TarsRegistry {
    pub fn new(locator: &str) -> Self {
        let nodes = Self::parse_all_nodes(locator);
        info!(
            "TarsRegistry initialized with {} nodes: {:?}",
            nodes.len(),
            nodes
        );

        Self {
            locator: locator.to_string(),
            nodes,
            circuit_breaker: RegistryCircuitBreaker::new(),
            current_index: std::sync::atomic::AtomicUsize::new(0),
            timeout: 5000,
        }
    }

    pub fn with_timeout(mut self, timeout_ms: i32) -> Self {
        self.timeout = timeout_ms;
        self
    }

    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Get the circuit breaker for external access
    pub fn circuit_breaker(&self) -> &RegistryCircuitBreaker {
        &self.circuit_breaker
    }

    /// Get all configured nodes
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    /// Get available nodes count
    pub fn available_nodes_count(&self) -> usize {
        self.circuit_breaker.available_count(&self.nodes)
    }

    /// Parse all nodes from locator string
    /// Supports formats:
    /// - Single node: "ServiceName@tcp -h host -p port"
    /// - Multiple nodes: "ServiceName@tcp -h host1 -p port1:tcp -h host2 -p port2"
    fn parse_all_nodes(locator: &str) -> Vec<String> {
        let mut nodes = Vec::new();

        // Split by '@' to get service name and endpoints
        let parts: Vec<&str> = locator.split('@').collect();
        if parts.len() < 2 {
            return nodes;
        }

        // The rest after '@' contains endpoint definitions
        let endpoints_str = parts[1..].join("@");

        // Split by protocol indicators (:tcp, :udp, :ssl) to get individual endpoints
        // Format: "tcp -h host1 -p port1:tcp -h host2 -p port2"
        let endpoint_parts: Vec<&str> = endpoints_str
            .split(":tcp")
            .flat_map(|s| s.split(":udp"))
            .flat_map(|s| s.split(":ssl"))
            .collect();

        for part in endpoint_parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Parse host and port from this endpoint part
            // Format: "tcp -h host -p port" or "-h host -p port" or just host/port tokens
            let tokens: Vec<&str> = part.split_whitespace().collect();
            let mut host = String::new();
            let mut port = String::new();

            let mut i = 0;
            while i < tokens.len() {
                match tokens[i] {
                    "-h" if i + 1 < tokens.len() => {
                        host = tokens[i + 1].to_string();
                        i += 2;
                    }
                    "-p" if i + 1 < tokens.len() => {
                        // Port might have trailing colon from split
                        let port_str = tokens[i + 1].trim_end_matches(':');
                        port = port_str.to_string();
                        i += 2;
                    }
                    _ => i += 1,
                }
            }

            if !host.is_empty() && !port.is_empty() {
                nodes.push(format!("{}:{}", host, port));
            }
        }

        // If no nodes found, try simple parsing
        if nodes.is_empty() {
            if let Some(addr) = Self::parse_single_locator(locator) {
                nodes.push(addr);
            }
        }

        nodes
    }

    /// Parse a single locator string to get one endpoint
    fn parse_single_locator(locator: &str) -> Option<String> {
        let parts: Vec<&str> = locator.split('@').collect();
        if parts.len() < 2 {
            return None;
        }

        let endpoint_str = parts[1];
        let mut host = "127.0.0.1";
        let mut port = "17890";

        let tokens: Vec<&str> = endpoint_str.split_whitespace().collect();
        let mut i = 0;
        while i < tokens.len() {
            match tokens[i] {
                "-h" if i + 1 < tokens.len() => {
                    host = tokens[i + 1];
                    i += 2;
                }
                "-p" if i + 1 < tokens.len() => {
                    port = tokens[i + 1];
                    i += 2;
                }
                _ => i += 1,
            }
        }

        Some(format!("{}:{}", host, port))
    }

    /// Convert EndpointF to Endpoint
    fn convert_endpoint(epf: &EndpointF) -> Endpoint {
        let mut ep = if epf.istcp == 1 {
            Endpoint::tcp(&epf.host, epf.port as u16)
        } else if epf.istcp == 2 {
            Endpoint::ssl(&epf.host, epf.port as u16)
        } else {
            Endpoint::udp(&epf.host, epf.port as u16)
        };

        ep.timeout = epf.timeout as u64;
        ep.weight = epf.weight as u32;
        ep.set_id = epf.set_id.clone();
        ep.grid = epf.grid;
        ep.qos = epf.qos;
        ep.weight_type = epf.weight_type as i16;
        ep.auth_type = epf.auth_type;
        ep
    }

    /// Execute query on a specific node with circuit breaker
    async fn query_on_node(
        &self,
        addr: &str,
        id: &str,
        func: &str,
        set: Option<&str>,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        let breaker = self.circuit_breaker.get_breaker(addr);

        // Try to connect and query
        let result = self.do_query_internal(addr, id, func, set).await;

        match &result {
            Ok(_) => {
                breaker.record_success();
            }
            Err(e) => {
                warn!("Query failed on node {}: {}", addr, e);
                breaker.record_failure();
            }
        }

        result
    }

    /// Internal query implementation on a specific node
    async fn do_query_internal(
        &self,
        addr: &str,
        id: &str,
        func: &str,
        set: Option<&str>,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        let mut stream = TcpStream::connect(addr).await?;

        // Build request body
        let mut body_buf = Buffer::new();
        body_buf.write_string(id, 1)?; // id at tag 1

        if let Some(set_id) = set {
            body_buf.write_string(set_id, 2)?; // setId at tag 2
        }

        // Build request packet
        let mut req = RequestPacket::new();
        req.s_servant_name = self
            .locator
            .split('@')
            .next()
            .unwrap_or_default()
            .to_string();
        req.s_func_name = func.to_string();
        req.s_buffer = body_buf.to_bytes();
        req.i_timeout = self.timeout;

        stream.write_all(&req.encode()?).await?;
        let size = stream.read_u32().await? as usize;
        if !(4..=100 * 1024 * 1024).contains(&size) {
            return Err(TarsError::Protocol("invalid registry frame size".into()));
        }
        let mut packet = vec![0; size];
        packet[..4].copy_from_slice(&(size as u32).to_be_bytes());
        stream.read_exact(&mut packet[4..]).await?;
        let rsp = ResponsePacket::decode(&packet)?;

        if rsp.i_ret != 0 {
            return Err(TarsError::ServerError {
                code: rsp.i_ret,
                message: rsp.s_result_desc.clone(),
            });
        }

        // Parse response
        let mut reader = Reader::new(&rsp.s_buffer);

        // Return value at tag 0
        let ret = reader.read_int32(0, true)?;
        if ret != 0 {
            return Err(TarsError::ServerError {
                code: ret,
                message: format!("{func}: {id}"),
            });
        }
        let active_tag = if set.is_some() { 3 } else { 2 };

        // Active endpoints at tag 2
        let active_epf = decode_endpoint_list(&mut reader, active_tag, true)?;

        // Inactive endpoints at tag 3
        let inactive_epf = decode_endpoint_list(&mut reader, active_tag + 1, false)?;

        let active: Vec<Endpoint> = active_epf.iter().map(Self::convert_endpoint).collect();
        let inactive: Vec<Endpoint> = inactive_epf.iter().map(Self::convert_endpoint).collect();

        debug!(
            "Query {} on node {} returned {} active, {} inactive endpoints",
            id,
            addr,
            active.len(),
            inactive.len()
        );

        Ok((active, inactive))
    }

    /// Query endpoints with automatic failover
    async fn do_query(
        &self,
        id: &str,
        func: &str,
        set: Option<&str>,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        let mut candidates = self.circuit_breaker.filter_available(&self.nodes);
        if candidates.is_empty() {
            candidates = self.nodes.clone();
        }
        if candidates.is_empty() {
            return Err(TarsError::NoEndpoint);
        }
        let start = self.current_index.fetch_add(1, Ordering::SeqCst) % candidates.len();
        candidates.rotate_left(start);
        let timeout_ms = self.timeout.max(1) as u64;
        let query = async {
            let mut last_error = TarsError::NoEndpoint;
            for node in candidates {
                // Share the total deadline fairly so one stalled registry cannot starve failover.
                let per_node = std::time::Duration::from_millis(
                    (timeout_ms / self.nodes.len().max(1) as u64).max(1),
                );
                match tokio::time::timeout(per_node, self.query_on_node(&node, id, func, set)).await
                {
                    Ok(Ok(result)) => return Ok(result),
                    Ok(Err(error)) => last_error = error,
                    Err(_) => {
                        self.circuit_breaker.get_breaker(&node).record_failure();
                        last_error = TarsError::Timeout(timeout_ms);
                    }
                }
            }
            Err(last_error)
        };
        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), query)
            .await
            .map_err(|_| TarsError::Timeout(timeout_ms))?
    }
}

#[async_trait]
impl Registrar for TarsRegistry {
    async fn register(&self, _servant: &ServantInstance) -> Result<()> {
        // Native Tars registration is configured by the platform and maintained by tarsnode.
        Ok(())
    }

    async fn deregister(&self, _servant: &ServantInstance) -> Result<()> {
        // As in TarsGo, native deregistration is handled by the platform.
        Ok(())
    }

    async fn query_servant(&self, id: &str) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        self.do_query(id, QUERY_FIND_OBJECT_BY_ID_IN_SAME_GROUP, None)
            .await
    }

    async fn query_servant_by_set(
        &self,
        id: &str,
        set: &str,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        self.do_query(id, QUERY_FIND_OBJECT_BY_ID_IN_SAME_SET, Some(set))
            .await
    }
}

/// Endpoint manager for caching and load balancing
pub struct EndpointManager {
    obj_name: String,
    registrar: Arc<dyn Registrar>,
    active_endpoints: RwLock<Vec<Endpoint>>,
    inactive_endpoints: RwLock<Vec<Endpoint>>,
    refresh_interval_ms: u64,
    set_division: Option<String>,
}

impl EndpointManager {
    pub fn new(obj_name: &str, registrar: Arc<dyn Registrar>) -> Self {
        Self {
            obj_name: obj_name.to_string(),
            registrar,
            active_endpoints: RwLock::new(vec![]),
            inactive_endpoints: RwLock::new(vec![]),
            refresh_interval_ms: 60_000, // 60 seconds default
            set_division: None,
        }
    }

    pub fn with_refresh_interval(mut self, interval_ms: u64) -> Self {
        self.refresh_interval_ms = interval_ms;
        self
    }

    pub fn with_set_division(mut self, set_division: Option<String>) -> Self {
        self.set_division = set_division;
        self
    }

    /// Refresh endpoints from registry
    pub async fn refresh(&self) -> Result<()> {
        let (active, inactive) = match &self.set_division {
            Some(set) => {
                self.registrar
                    .query_servant_by_set(&self.obj_name, set)
                    .await?
            }
            None => self.registrar.query_servant(&self.obj_name).await?,
        };
        if active.is_empty() {
            return Err(TarsError::NoEndpoint);
        }

        {
            let mut guard = self.active_endpoints.write().await;
            *guard = active;
        }
        {
            let mut guard = self.inactive_endpoints.write().await;
            *guard = inactive;
        }

        Ok(())
    }

    /// Get active endpoints
    pub async fn get_active(&self) -> Vec<Endpoint> {
        self.active_endpoints.read().await.clone()
    }

    /// Get inactive endpoints
    pub async fn get_inactive(&self) -> Vec<Endpoint> {
        self.inactive_endpoints.read().await.clone()
    }

    /// Start background refresh task
    pub fn start_refresh_task(self: Arc<Self>) {
        let interval = std::time::Duration::from_millis(self.refresh_interval_ms);
        let manager = Arc::downgrade(&self);

        tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval(interval.max(std::time::Duration::from_millis(1)));
            loop {
                interval_timer.tick().await;
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                if let Err(e) = manager.refresh().await {
                    error!(
                        "Failed to refresh endpoints for {}: {}",
                        manager.obj_name, e
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_direct_registrar() {
        let endpoints = vec![
            Endpoint::tcp("127.0.0.1", 10000),
            Endpoint::tcp("127.0.0.1", 10001),
        ];
        let registrar = DirectRegistrar::new(endpoints);

        let (active, inactive) = registrar.query_servant("Test.HelloObj").await.unwrap();
        assert_eq!(active.len(), 2);
        assert!(inactive.is_empty());
    }

    #[test]
    fn test_parse_single_node() {
        let registry = TarsRegistry::new("tars.tarsregistry.QueryObj@tcp -h 192.168.1.1 -p 17890");
        assert_eq!(registry.nodes().len(), 1);
        assert_eq!(registry.nodes()[0], "192.168.1.1:17890");
    }

    #[test]
    fn test_parse_multiple_nodes() {
        let registry = TarsRegistry::new(
            "tars.tarsregistry.QueryObj@tcp -h 192.168.1.1 -p 17890:tcp -h 192.168.1.2 -p 17891",
        );
        assert_eq!(registry.nodes().len(), 2);
        assert!(registry.nodes().contains(&"192.168.1.1:17890".to_string()));
        assert!(registry.nodes().contains(&"192.168.1.2:17891".to_string()));
    }

    #[test]
    fn test_circuit_breaker_initial_state() {
        let breaker = NodeCircuitBreaker::new("127.0.0.1:17890");
        assert!(breaker.is_available());
    }

    #[test]
    fn test_circuit_breaker_opens_on_failure() {
        let breaker = NodeCircuitBreaker::new("127.0.0.1:17890");

        // First failure should open circuit (REGISTRY_FAIL_THRESHOLD = 1)
        let opened = breaker.record_failure();
        assert!(opened);
        assert!(!breaker.available.load(Ordering::SeqCst));
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let breaker = NodeCircuitBreaker::new("127.0.0.1:17890");

        // Open the circuit
        breaker.record_failure();
        assert!(!breaker.available.load(Ordering::SeqCst));

        // Success should reset
        breaker.record_success();
        assert!(breaker.available.load(Ordering::SeqCst));
        assert_eq!(breaker.fail_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_registry_circuit_breaker_manager() {
        let manager = RegistryCircuitBreaker::new();
        let addresses = vec![
            "192.168.1.1:17890".to_string(),
            "192.168.1.2:17890".to_string(),
        ];

        // Initially all should be available
        let available = manager.filter_available(&addresses);
        assert_eq!(available.len(), 2);

        // Mark one as failed
        let breaker = manager.get_breaker("192.168.1.1:17890");
        breaker.record_failure();

        // Now only one should be available
        let available = manager.filter_available(&addresses);
        assert_eq!(available.len(), 1);
        assert_eq!(available[0], "192.168.1.2:17890");
    }

    #[test]
    fn test_convert_endpoint() {
        let epf = EndpointF {
            host: "10.0.0.1".to_string(),
            port: 8080,
            timeout: 3000,
            istcp: 1,
            grid: 0,
            groupworkid: 0,
            grouprealid: 0,
            set_id: "test.1.1".to_string(),
            qos: 0,
            bak_flag: 0,
            weight: 100,
            weight_type: 0,
            auth_type: 0,
        };

        let ep = TarsRegistry::convert_endpoint(&epf);
        assert_eq!(ep.host, "10.0.0.1");
        assert_eq!(ep.port, 8080);
        assert_eq!(ep.timeout, 3000);
        assert_eq!(ep.weight, 100);
        assert_eq!(ep.set_id, "test.1.1");
    }
}
