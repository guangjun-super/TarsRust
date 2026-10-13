//! Tars server transport implementation

use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use super::{ServerProtocolHandler, TarsServerConfig};
use tars_core::codec::PackageStatus;
use tars_core::{Context, Result, TarsError};

/// Tars server for handling incoming connections
pub struct TarsServer {
    /// Server configuration
    config: TarsServerConfig,
    /// Protocol handler
    protocol: Arc<dyn ServerProtocolHandler>,
    /// Close flag
    closed: AtomicBool,
    shutdown_tx: watch::Sender<bool>,
    /// Number of active connections
    num_conn: AtomicI32,
    /// Number of active invokes
    num_invoke: AtomicI32,
    /// Last invoke time
    last_invoke: Mutex<Instant>,
}

impl TarsServer {
    /// Create a new TarsServer
    pub fn new(protocol: Arc<dyn ServerProtocolHandler>, config: TarsServerConfig) -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        Arc::new(Self {
            config,
            protocol,
            closed: AtomicBool::new(false),
            shutdown_tx,
            num_conn: AtomicI32::new(0),
            num_invoke: AtomicI32::new(0),
            last_invoke: Mutex::new(Instant::now()),
        })
    }

    /// Start listening and serving
    pub async fn serve(self: Arc<Self>) -> Result<()> {
        let listener = self.bind().await?;
        self.serve_with_listener(listener).await
    }

    /// Bind before starting application tasks, so startup failures can be returned.
    pub async fn bind(&self) -> Result<TcpListener> {
        Ok(TcpListener::bind(&self.config.address).await?)
    }

    /// Serve a previously bound listener.
    pub async fn serve_with_listener(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        // Create TLS acceptor if SSL is enabled
        let tls_acceptor = if self.config.is_ssl() {
            let tls_config = self
                .config
                .tls_config
                .as_ref()
                .ok_or_else(|| TarsError::Config("TLS config not set for SSL server".into()))?;
            info!("Server listening on {} (TLS enabled)", self.config.address);
            Some(TlsAcceptor::from(tls_config.clone()))
        } else {
            info!("Server listening on {}", self.config.address);
            None
        };

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut connections = JoinSet::new();
        loop {
            if self.closed.load(Ordering::SeqCst) {
                break;
            }

            let accepted = tokio::select! {
                _ = shutdown_rx.changed() => break,
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed { warn!("Connection task failed: {error}"); }
                    continue;
                },
                accepted = tokio::time::timeout(self.config.accept_timeout, listener.accept()) => accepted,
            };
            match accepted {
                Ok(Ok((stream, addr))) => {
                    self.num_conn.fetch_add(1, Ordering::SeqCst);
                    let server = Arc::clone(&self);
                    let tls_acceptor = tls_acceptor.clone();

                    connections.spawn(async move {
                        let result = if let Some(acceptor) = tls_acceptor {
                            // TLS connection
                            let handshake = tokio::select! {
                                _ = async {
                                    let mut shutdown = server.shutdown_tx.subscribe();
                                    if !*shutdown.borrow() { let _ = shutdown.changed().await; }
                                } => Err(TarsError::ConnectionClosed),
                                result = tokio::time::timeout(server.config.read_timeout, acceptor.accept(stream)) => {
                                    match result {
                                        Ok(result) => result.map_err(TarsError::from),
                                        Err(_) => Err(TarsError::Timeout(server.config.read_timeout.as_millis() as u64)),
                                    }
                                },
                            };
                            match handshake {
                                Ok(tls_stream) => {
                                    debug!("TLS connection established from {}", addr);
                                    let (read_half, write_half) = tokio::io::split(tls_stream);
                                    server.handle_connection_generic(read_half, write_half, addr).await
                                }
                                Err(e) => {
                                    warn!("TLS handshake failed from {}: {}", addr, e);
                                    Err(e)
                                }
                            }
                        } else {
                            // Plain TCP connection
                            server.handle_connection(stream, addr).await
                        };

                        if let Err(e) = result {
                            debug!("Connection error: {}", e);
                        }
                        server.num_conn.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Ok(Err(e)) => {
                    warn!("Accept error: {}", e);
                }
                Err(_) => {
                    // Timeout, continue
                    continue;
                }
            }
        }
        drop(listener);
        // Idle reads are cancelled immediately; dispatched calls finish and send their replies.
        if tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        Ok(())
    }

    /// Handle a single TCP connection
    async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) -> Result<()> {
        debug!("New connection from {}", addr);

        // Set TCP options
        stream.set_nodelay(self.config.tcp_no_delay)?;

        let (read_half, write_half) = stream.into_split();
        self.handle_connection_generic(read_half, write_half, addr)
            .await
    }

    /// Handle connection with generic read/write halves (works for both TCP and TLS)
    async fn handle_connection_generic<R, W>(
        &self,
        mut read_half: R,
        mut write_half: W,
        addr: SocketAddr,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut buffer = vec![0u8; self.config.tcp_read_buffer];
        let mut accumulated = Vec::new();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            if self.closed.load(Ordering::SeqCst) {
                // Send close message
                let close_msg = self.protocol.get_close_msg();
                let _ = write_half.write_all(&close_msg).await;
                break;
            }

            let read = tokio::select! {
                _ = shutdown_rx.changed() => continue,
                read = tokio::time::timeout(self.config.read_timeout, read_half.read(&mut buffer)) => read,
            };
            match read {
                Ok(Ok(0)) => {
                    debug!("Connection closed by {}", addr);
                    break;
                }
                Ok(Ok(n)) => {
                    accumulated.extend_from_slice(&buffer[..n]);

                    // Parse and handle complete packages
                    loop {
                        let (pkg_len, status) = self.protocol.parse_package(&accumulated);
                        match status {
                            PackageStatus::Full => {
                                if self.closed.load(Ordering::SeqCst) {
                                    break;
                                }
                                let pkg: Vec<u8> = accumulated.drain(..pkg_len).collect();

                                // Check concurrent limit
                                if self.config.max_invoke > 0
                                    && self.num_invoke.load(Ordering::SeqCst)
                                        >= self.config.max_invoke
                                {
                                    warn!("Max invoke limit reached");
                                    continue;
                                }

                                self.num_invoke.fetch_add(1, Ordering::SeqCst);
                                *self.last_invoke.lock() = Instant::now();

                                // Handle request
                                let protocol = Arc::clone(&self.protocol);
                                let handle_timeout = self.config.handle_timeout;
                                let num_invoke = &self.num_invoke;

                                let mut ctx = Context::new();
                                ctx.set_client_ip(addr.ip().to_string());
                                ctx.set_client_port(addr.port());

                                let response = tokio::time::timeout(
                                    handle_timeout,
                                    protocol.invoke(&mut ctx, &pkg),
                                )
                                .await
                                .unwrap_or_else(|_| protocol.invoke_timeout(&pkg));

                                num_invoke.fetch_sub(1, Ordering::SeqCst);

                                // Send response
                                if !response.is_empty() {
                                    match tokio::time::timeout(
                                        self.config.write_timeout,
                                        write_half.write_all(&response),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => {}
                                        result => {
                                            error!("Write error: {:?}", result);
                                            return Err(TarsError::ConnectionClosed);
                                        }
                                    }
                                }
                            }
                            PackageStatus::Less => break,
                            PackageStatus::Error => {
                                error!("Package parse error from {}", addr);
                                return Err(TarsError::Protocol("package parse error".into()));
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    debug!("Read error from {}: {}", addr, e);
                    break;
                }
                Err(_) => {
                    // Read timeout - check idle
                    if self.last_invoke.lock().elapsed() > self.config.idle_timeout {
                        debug!("Connection idle from {}, closing", addr);
                        break;
                    }
                }
            }
        }

        // Call close handler
        let ctx = Context::new();
        self.protocol.do_close(&ctx);

        Ok(())
    }

    /// Check if server is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Get connection count
    pub fn connection_count(&self) -> i32 {
        self.num_conn.load(Ordering::SeqCst)
    }

    /// Get invoke count
    pub fn invoke_count(&self) -> i32 {
        self.num_invoke.load(Ordering::SeqCst)
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(&self) {
        info!("Shutting down server...");
        self.closed.store(true, Ordering::SeqCst);
        self.shutdown_tx.send_replace(true);

        // Wait for pending invokes to complete
        let timeout = std::time::Duration::from_secs(30);
        let start = Instant::now();

        while self.num_invoke.load(Ordering::SeqCst) > 0 {
            if start.elapsed() > timeout {
                warn!("Shutdown timeout, forcing close");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        info!("Server shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockHandler;

    #[async_trait::async_trait]
    impl ServerProtocolHandler for MockHandler {
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

        async fn invoke(&self, _ctx: &mut Context, _pkg: &[u8]) -> Vec<u8> {
            vec![0, 0, 0, 4]
        }

        fn invoke_timeout(&self, _pkg: &[u8]) -> Vec<u8> {
            vec![0, 0, 0, 4]
        }

        fn get_close_msg(&self) -> Vec<u8> {
            vec![0, 0, 0, 4]
        }

        fn do_close(&self, _ctx: &Context) {}
    }

    #[tokio::test]
    async fn test_server_creation() {
        let handler = Arc::new(MockHandler);
        let config = TarsServerConfig::tcp("127.0.0.1:0");
        let server = TarsServer::new(handler, config);

        assert!(!server.is_closed());
        assert_eq!(server.connection_count(), 0);
        assert_eq!(server.invoke_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_sends_protocol_close_message_before_eof() {
        let server = TarsServer::new(Arc::new(MockHandler), TarsServerConfig::tcp("127.0.0.1:0"));
        let listener = server.bind().await.unwrap();
        let address = listener.local_addr().unwrap();
        let runner = server.clone();
        let run = tokio::spawn(runner.serve_with_listener(listener));
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(&[0, 0, 0, 4]).await.unwrap();
        let mut response = [0; 4];
        stream.read_exact(&mut response).await.unwrap();
        server.shutdown().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            stream.read_exact(&mut response),
        )
        .await
        .unwrap()
        .expect("protocol close message was skipped");
        assert_eq!(response, [0, 0, 0, 4]);
        assert_eq!(stream.read(&mut response).await.unwrap(), 0);
        run.await.unwrap().unwrap();
    }
}
