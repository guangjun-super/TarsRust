//! Tars client transport implementation

use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, info, warn};

use super::tls::parse_server_name;
use super::{ClientProtocol, TarsClientConfig};
use tars_core::codec::PackageStatus;
use tars_core::{Result, TarsError};

/// Message to be sent
struct SendMessage {
    data: Vec<u8>,
}

/// Tars client for managing connection to a remote endpoint
pub struct TarsClient {
    /// Remote address
    address: String,
    /// Client configuration
    config: TarsClientConfig,
    /// Protocol handler
    protocol: Arc<dyn ClientProtocol>,
    /// Send channel
    send_tx: mpsc::Sender<SendMessage>,
    connection_task: Mutex<Option<tokio::task::AbortHandle>>,
    /// Close flag
    closed: AtomicBool,
    /// Number of pending requests
    invoke_num: AtomicI32,
    /// Last activity time
    last_activity: Mutex<Instant>,
}

impl TarsClient {
    /// Create a new TarsClient
    pub fn new(
        address: &str,
        protocol: Arc<dyn ClientProtocol>,
        config: TarsClientConfig,
    ) -> Arc<Self> {
        let (send_tx, send_rx) = mpsc::channel(config.queue_len);

        let client = Arc::new(Self {
            address: address.to_string(),
            config,
            protocol,
            send_tx,
            closed: AtomicBool::new(false),
            connection_task: Mutex::new(None),
            invoke_num: AtomicI32::new(0),
            last_activity: Mutex::new(Instant::now()),
        });

        // Start background connection task
        let client_clone = Arc::clone(&client);
        let task = tokio::spawn(async move {
            client_clone.connection_loop(send_rx).await;
        });
        *client.connection_task.lock() = Some(task.abort_handle());

        client
    }

    /// Send data to the remote endpoint
    pub async fn send(&self, data: Vec<u8>) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TarsError::ConnectionClosed);
        }

        self.invoke_num.fetch_add(1, Ordering::SeqCst);
        *self.last_activity.lock() = Instant::now();

        self.send_tx
            .send(SendMessage { data })
            .await
            .map_err(|_| TarsError::ConnectionClosed)?;

        Ok(())
    }

    /// Check if client is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Get pending invoke count
    pub fn invoke_count(&self) -> i32 {
        self.invoke_num.load(Ordering::SeqCst)
    }

    /// Close the client
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(task) = self.connection_task.lock().take() {
            task.abort();
        }
    }

    /// Reconnect to the server
    pub async fn reconnect(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(TarsError::ConnectionClosed);
        }
        // Reconnection is handled by the connection loop
        Ok(())
    }

    /// Main connection loop
    async fn connection_loop(self: Arc<Self>, mut send_rx: mpsc::Receiver<SendMessage>) {
        let mut retry_count = 0;
        let max_retries = 3;

        loop {
            if self.closed.load(Ordering::SeqCst) {
                break;
            }

            match self.connect_and_handle(&mut send_rx).await {
                Ok(_) => {
                    retry_count = 0;
                }
                Err(e) => {
                    warn!("Connection error: {}, retrying...", e);
                    retry_count += 1;
                    if retry_count > max_retries {
                        error!("Max retries exceeded, closing connection");
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100 * retry_count as u64))
                        .await;
                }
            }
        }

        self.closed.store(true, Ordering::SeqCst);
    }

    /// Connect and handle communication
    async fn connect_and_handle(&self, send_rx: &mut mpsc::Receiver<SendMessage>) -> Result<()> {
        // Connect with timeout
        let tcp_stream =
            tokio::time::timeout(self.config.dial_timeout, TcpStream::connect(&self.address))
                .await
                .map_err(|_| TarsError::Timeout(self.config.dial_timeout.as_millis() as u64))?
                .map_err(TarsError::Transport)?;

        // Set TCP options
        tcp_stream.set_nodelay(true)?;

        // Check if TLS is enabled
        if self.config.is_ssl() {
            // TLS connection
            let tls_config =
                self.config.tls_config.as_ref().ok_or_else(|| {
                    TarsError::Config("TLS config not set for SSL connection".into())
                })?;

            let server_name = parse_server_name(&self.address)?;
            let connector = TlsConnector::from(tls_config.clone());

            info!("Establishing TLS connection to {}", self.address);

            let tls_stream = tokio::time::timeout(
                self.config.dial_timeout,
                connector.connect(server_name, tcp_stream),
            )
            .await
            .map_err(|_| TarsError::Timeout(self.config.dial_timeout.as_millis() as u64))?
            .map_err(|e| TarsError::Config(format!("TLS handshake failed: {}", e)))?;

            info!("TLS connection established to {}", self.address);

            let (read_half, write_half) = tokio::io::split(tls_stream);
            self.handle_connection(read_half, write_half, send_rx).await
        } else {
            // Plain TCP connection
            let (read_half, write_half) = tcp_stream.into_split();
            self.handle_connection(read_half, write_half, send_rx).await
        }
    }

    /// Handle read/write on established connection
    async fn handle_connection<R, W>(
        &self,
        mut read_half: R,
        mut write_half: W,
        send_rx: &mut mpsc::Receiver<SendMessage>,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send,
    {
        // Spawn read task
        let protocol = Arc::clone(&self.protocol);
        let read_timeout = self.config.read_timeout;

        let mut read_handle = tokio::spawn(async move {
            let mut buffer = vec![0u8; 4096];
            let mut accumulated = Vec::new();

            loop {
                match tokio::time::timeout(read_timeout, read_half.read(&mut buffer)).await {
                    Ok(Ok(0)) => {
                        debug!("Connection closed by peer");
                        break;
                    }
                    Ok(Ok(n)) => {
                        accumulated.extend_from_slice(&buffer[..n]);

                        // Parse complete packages
                        loop {
                            let (pkg_len, status) = protocol.parse_package(&accumulated);
                            match status {
                                PackageStatus::Full => {
                                    let pkg = accumulated.drain(..pkg_len).collect();
                                    protocol.recv(pkg);
                                }
                                PackageStatus::Less => break,
                                PackageStatus::Error => {
                                    error!("Package parse error");
                                    return Err(TarsError::Protocol("package parse error".into()));
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Read error: {}", e);
                        return Err(TarsError::Transport(e));
                    }
                    Err(_) => {
                        // Timeout - check idle
                        continue;
                    }
                }
            }
            Ok(())
        });

        let _read_guard = AbortOnDrop(read_handle.abort_handle());
        // Write loop
        let write_timeout = self.config.write_timeout;
        loop {
            if self.closed.load(Ordering::SeqCst) {
                break;
            }

            tokio::select! {
                stopped = &mut read_handle => {
                    return stopped.map_err(|error| TarsError::Protocol(format!("reader stopped: {error}")))?;
                }
                Some(msg) = send_rx.recv() => {
                    match tokio::time::timeout(write_timeout, write_half.write_all(&msg.data)).await {
                        Ok(Ok(_)) => {
                            self.invoke_num.fetch_sub(1, Ordering::SeqCst);
                        }
                        Ok(Err(e)) => {
                            error!("Write error: {}", e);
                            self.invoke_num.fetch_sub(1, Ordering::SeqCst);
                            return Err(TarsError::Transport(e));
                        }
                        Err(_) => {
                            error!("Write timeout");
                            self.invoke_num.fetch_sub(1, Ordering::SeqCst);
                            return Err(TarsError::Timeout(write_timeout.as_millis() as u64));
                        }
                    }
                }
                _ = tokio::time::sleep(self.config.idle_timeout) => {
                    if self.invoke_num.load(Ordering::SeqCst) == 0 {
                        debug!("Connection idle, closing");
                        break;
                    }
                }
            }
        }

        // Clean up
        read_handle.abort();
        Ok(())
    }
}

// A connection cancellation/error must also release the spawned reader half.
struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Drop for TarsClient {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockProtocol;

    impl ClientProtocol for MockProtocol {
        fn parse_package(&self, buff: &[u8]) -> (usize, PackageStatus) {
            if buff.len() < 4 {
                return (0, PackageStatus::Less);
            }
            let len = u32::from_be_bytes([buff[0], buff[1], buff[2], buff[3]]) as usize;
            if buff.len() >= len {
                (len, PackageStatus::Full)
            } else {
                (0, PackageStatus::Less)
            }
        }

        fn recv(&self, _pkg: Vec<u8>) {
            // Mock implementation
        }
    }

    #[tokio::test]
    async fn test_client_creation() {
        let protocol = Arc::new(MockProtocol);
        let config = TarsClientConfig::tcp();
        let client = TarsClient::new("127.0.0.1:9999", protocol, config);

        assert!(!client.is_closed());
        client.close();
        assert!(client.is_closed());
    }
}
