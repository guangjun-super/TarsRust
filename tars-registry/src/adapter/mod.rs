//! # Adapter Module
//!
//! AdapterProxy manages a connection to a single service endpoint.

use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::debug;

use tars_core::codec::PackageStatus;
use tars_core::{consts, Result};
use tars_protocol::protocol::{Protocol, RequestPacket, ResponsePacket, TarsProtocol};
use tars_protocol::Endpoint;
use tars_transport::transport::{ClientProtocol, TarsClient, TarsClientConfig};

/// AdapterProxy manages connection to a single endpoint
pub struct AdapterProxy {
    clock: tokio::time::Instant,
    /// Endpoint information
    endpoint: Endpoint,
    /// Transport client
    client: parking_lot::RwLock<Arc<TarsClient>>,
    client_config: TarsClientConfig,
    /// Protocol handler
    protocol: Arc<TarsProtocol>,
    /// Response channels: request_id -> response sender (shared with AdapterProtocolHandler)
    responses: Arc<DashMap<i32, oneshot::Sender<ResponsePacket>>>,
    /// Fail count
    fail_count: AtomicI32,
    /// Last fail count (consecutive)
    last_fail_count: AtomicI32,
    /// Send count
    send_count: AtomicI32,
    /// Success count
    success_count: AtomicI32,
    /// Last success time (unix seconds)
    last_success_time: AtomicI64,
    /// Last block time
    last_block_time: AtomicI64,
    /// Last check time
    last_check_time: AtomicI64,
    /// Status: true = active
    status: AtomicBool,
    /// Closed flag
    closed: AtomicBool,
    /// Push callback
    push_callback: Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>,
}

impl AdapterProxy {
    /// Create a new AdapterProxy
    pub fn new(endpoint: Endpoint, config: TarsClientConfig) -> Arc<Self> {
        let protocol = Arc::new(TarsProtocol::new());
        let address = endpoint.address();
        let responses = Arc::new(DashMap::new());

        let adapter = Arc::new(Self {
            clock: tokio::time::Instant::now(),
            endpoint,
            client_config: config.clone(),
            client: parking_lot::RwLock::new(TarsClient::new(
                &address,
                Arc::new(AdapterProtocolHandler::new(Arc::clone(&responses))),
                config,
            )),
            protocol,
            responses,
            fail_count: AtomicI32::new(0),
            last_fail_count: AtomicI32::new(0),
            send_count: AtomicI32::new(0),
            success_count: AtomicI32::new(0),
            last_success_time: AtomicI64::new(0),
            last_block_time: AtomicI64::new(0),
            last_check_time: AtomicI64::new(0),
            status: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            push_callback: None,
        });

        adapter
    }

    /// Get endpoint
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Check if adapter is active
    pub fn is_active(&self) -> bool {
        self.status.load(Ordering::SeqCst)
    }

    /// Check if adapter is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Send a request
    pub async fn send(&self, req: &RequestPacket) -> Result<()> {
        self.send_count.fetch_add(1, Ordering::SeqCst);

        let data = self.protocol.request_pack(req)?;
        if self.is_closed() {
            return Err(tars_core::TarsError::ConnectionClosed);
        }
        let client = {
            let mut client = self.client.write();
            if client.is_closed() {
                *client = TarsClient::new(
                    &self.endpoint.address(),
                    Arc::new(AdapterProtocolHandler::new(self.responses.clone())),
                    self.client_config.clone(),
                );
            }
            client.clone()
        };
        client.send(data).await?;

        Ok(())
    }

    /// Register response channel
    pub fn register_response(&self, request_id: i32) -> oneshot::Receiver<ResponsePacket> {
        let (tx, rx) = oneshot::channel();
        self.responses.insert(request_id, tx);
        rx
    }

    /// Unregister response channel
    pub fn unregister_response(&self, request_id: i32) {
        self.responses.remove(&request_id);
    }

    /// Handle received response
    pub fn handle_response(&self, response: ResponsePacket) {
        if response.i_request_id == 0 {
            // Server push
            self.handle_push(&response);
            return;
        }

        if let Some((_, tx)) = self.responses.remove(&response.i_request_id) {
            let _ = tx.send(response);
        } else {
            debug!("No handler for request {}", response.i_request_id);
        }
    }

    /// Handle server push
    fn handle_push(&self, response: &ResponsePacket) {
        if response.s_result_desc == consts::RECONNECT_MSG {
            debug!("Received reconnect message");
            // TODO: Handle reconnect
            return;
        }

        if let Some(ref callback) = self.push_callback {
            callback(response.s_buffer.clone());
        }
    }

    /// Record success
    pub fn success_add(&self) {
        if !self.is_active() {
            self.reset();
        }
        self.last_success_time
            .store(self.clock.elapsed().as_secs() as i64, Ordering::SeqCst);
        self.success_count.fetch_add(1, Ordering::SeqCst);
        self.last_fail_count.store(0, Ordering::SeqCst);
    }

    /// Record failure
    pub fn fail_add(&self) {
        self.last_fail_count.fetch_add(1, Ordering::SeqCst);
        self.fail_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Check and update active status
    /// Returns (first_time_inactive, need_check)
    pub fn check_active(&self) -> (bool, bool) {
        if self.closed.load(Ordering::SeqCst) {
            return (false, false);
        }

        let now = self.clock.elapsed().as_secs() as i64;

        if self.status.load(Ordering::SeqCst) {
            // Active status
            let last_success = self.last_success_time.load(Ordering::SeqCst);
            let last_fail_count = self.last_fail_count.load(Ordering::SeqCst);

            // Check consecutive failures within interval
            if (now - last_success) >= consts::FAIL_INTERVAL as i64
                && last_fail_count >= consts::FAIL_N
            {
                self.status.store(false, Ordering::SeqCst);
                self.last_block_time.store(now, Ordering::SeqCst);
                return (true, false);
            }

            // Periodic check
            let last_check = self.last_check_time.load(Ordering::SeqCst);
            if (now - last_check) >= consts::CHECK_TIME as i64 {
                self.last_check_time.store(now, Ordering::SeqCst);

                let fail_count = self.fail_count.load(Ordering::SeqCst);
                let send_count = self.send_count.load(Ordering::SeqCst);

                // Check failure ratio
                if fail_count >= consts::OVER_N
                    && send_count > 0
                    && (fail_count as f32 / send_count as f32) >= consts::FAIL_RATIO
                {
                    self.status.store(false, Ordering::SeqCst);
                    self.last_block_time.store(now, Ordering::SeqCst);
                    return (true, false);
                }
            }

            return (false, false);
        }

        // Inactive status - check if we should try to reactivate
        let last_block = self.last_block_time.load(Ordering::SeqCst);
        if (now - last_block) >= consts::TRY_TIME_INTERVAL as i64 {
            self.last_block_time.store(now, Ordering::SeqCst);
            return (false, true);
        }

        (false, false)
    }

    /// Reset statistics
    pub fn reset(&self) {
        let now = self.clock.elapsed().as_secs() as i64;
        self.send_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        self.fail_count.store(0, Ordering::SeqCst);
        self.last_fail_count.store(0, Ordering::SeqCst);
        self.last_block_time.store(now, Ordering::SeqCst);
        self.last_check_time.store(now, Ordering::SeqCst);
        self.status.store(true, Ordering::SeqCst);
    }

    /// Close the adapter
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.client.read().close();
        self.responses.clear();
    }
}

/// Protocol handler for adapter
struct AdapterProtocolHandler {
    responses: Arc<DashMap<i32, oneshot::Sender<ResponsePacket>>>,
}

impl AdapterProtocolHandler {
    fn new(responses: Arc<DashMap<i32, oneshot::Sender<ResponsePacket>>>) -> Self {
        Self { responses }
    }
}

impl ClientProtocol for AdapterProtocolHandler {
    fn parse_package(&self, buff: &[u8]) -> (usize, PackageStatus) {
        tars_core::codec::parse_package(buff)
    }

    fn recv(&self, pkg: Vec<u8>) {
        let resp = match ResponsePacket::decode(&pkg) {
            Ok(r) => r,
            Err(e) => {
                debug!("Failed to decode response: {}", e);
                return;
            }
        };

        if let Some((_, tx)) = self.responses.remove(&resp.i_request_id) {
            let _ = tx.send(resp);
        } else {
            debug!("No handler for request {}", resp.i_request_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adapter_proxy_creation() {
        let endpoint = Endpoint::tcp("127.0.0.1", 10000);
        let config = TarsClientConfig::tcp();
        let adapter = AdapterProxy::new(endpoint, config);

        assert!(adapter.is_active());
        assert!(!adapter.is_closed());
    }

    #[tokio::test]
    async fn test_adapter_statistics() {
        let endpoint = Endpoint::tcp("127.0.0.1", 10000);
        let config = TarsClientConfig::tcp();
        let adapter = AdapterProxy::new(endpoint, config);

        adapter.success_add();
        adapter.success_add();
        adapter.fail_add();

        assert_eq!(adapter.success_count.load(Ordering::SeqCst), 2);
        assert_eq!(adapter.fail_count.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpListener};
    #[tokio::test]
    async fn exhausted_transport_can_send_again_on_recovery() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let adapter = AdapterProxy::new(
            Endpoint::tcp("127.0.0.1", listener.local_addr().unwrap().port()),
            TarsClientConfig::tcp(),
        );
        adapter.client.read().close();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let size = stream.read_u32().await.unwrap();
            let mut packet = vec![0; size as usize];
            packet[..4].copy_from_slice(&size.to_be_bytes());
            stream.read_exact(&mut packet[4..]).await.unwrap();
            assert_eq!(RequestPacket::decode(&packet).unwrap().s_func_name, "probe");
        });
        let mut request = RequestPacket::new();
        request.s_func_name = "probe".into();
        adapter.send(&request).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        adapter.close();
    }
}
