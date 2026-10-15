use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tars_core::{Context, Result, TarsError};
use tars_protocol::{endpoint::ServantInstance, Endpoint, RequestPacket, ResponsePacket};
use tars_registry::registry::Registrar;
use tars_rpc::Communicator;
use tars_util::ClientConfig;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Registry {
    endpoints: RwLock<Option<Vec<Endpoint>>>,
    calls: AtomicUsize,
    set: Option<String>,
}
#[async_trait::async_trait]
impl Registrar for Registry {
    async fn register(&self, _: &ServantInstance) -> Result<()> {
        Ok(())
    }
    async fn deregister(&self, _: &ServantInstance) -> Result<()> {
        Ok(())
    }
    async fn query_servant(&self, id: &str) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        assert_eq!(id, "App.Server.Obj");
        assert!(self.set.is_none());
        self.query().await
    }
    async fn query_servant_by_set(
        &self,
        id: &str,
        set: &str,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        assert_eq!(id, "App.Server.Obj");
        assert_eq!(self.set.as_deref(), Some(set));
        self.query().await
    }
}
impl Registry {
    async fn query(&self) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.endpoints.read().clone() {
            Some(ep) => Ok((ep, vec![])),
            None => Err(TarsError::NoEndpoint),
        }
    }
}
async fn echo(label: u8) -> (Endpoint, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = Endpoint::tcp("127.0.0.1", listener.local_addr().unwrap().port());
    let h = tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                while let Ok(n) = s.read_u32().await {
                    let mut b = vec![0; n as usize];
                    b[..4].copy_from_slice(&n.to_be_bytes());
                    if s.read_exact(&mut b[4..]).await.is_err() {
                        break;
                    }
                    let req = RequestPacket::decode(&b).unwrap();
                    if req.c_packet_type == 1 {
                        continue;
                    }
                    let rsp = ResponsePacket::success(req.i_request_id, vec![label]);
                    if s.write_all(&rsp.encode().unwrap()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    (ep, h)
}
async fn call(p: &tars_rpc::servant::ServantProxy) -> u8 {
    p.invoke(
        Context::new(),
        "echo",
        vec![],
        HashMap::new(),
        HashMap::new(),
    )
    .await
    .unwrap()
    .s_buffer[0]
}
#[tokio::test]
async fn discovers_once_refreshes_and_preserves_on_error() {
    let (a, ha) = echo(1).await;
    let (b, hb) = echo(2).await;
    let registry = Arc::new(Registry {
        endpoints: RwLock::new(Some(vec![a])),
        calls: AtomicUsize::new(0),
        set: Some("sz.area.1".into()),
    });
    let comm = Communicator::with_config(ClientConfig {
        refresh_endpoint_interval: 20,
        ..Default::default()
    });
    comm.set_registrar(registry.clone());
    comm.set_property("enableset", "Y");
    comm.set_property("setdivision", "sz.area.1");
    let proxy = comm.string_to_proxy("App.Server.Obj").unwrap();
    let (x, y) = tokio::join!(call(&proxy), call(&proxy));
    assert_eq!((x, y), (1, 1));
    assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
    *registry.endpoints.write() = Some(vec![b]);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = proxy
                .invoke(
                    Context::new(),
                    "echo",
                    vec![],
                    HashMap::new(),
                    HashMap::new(),
                )
                .await;
            if response.is_ok_and(|response| response.s_buffer == [2]) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for result in [Some(vec![]), None] {
        *registry.endpoints.write() = result;
        let n = registry.calls.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), async {
            while registry.calls.load(Ordering::SeqCst) == n {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(call(&proxy).await, 2);
    }
    comm.set_property("setdivision", "sz.area.2");
    let other = comm.string_to_proxy("App.Server.Obj").unwrap();
    assert!(!Arc::ptr_eq(&proxy, &other));
    drop(proxy);
    drop(other);
    drop(comm);
    // A task retaining its proxy would keep querying after all external owners disappear.
    let n = registry.calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(65)).await;
    assert_eq!(registry.calls.load(Ordering::SeqCst), n);
    ha.abort();
    hb.abort();
}
#[tokio::test]
async fn failed_initial_discovery_retries_and_direct_bypasses_registry() {
    let (ep, h) = echo(7).await;
    let registry = Arc::new(Registry {
        endpoints: RwLock::new(None),
        calls: AtomicUsize::new(0),
        set: None,
    });
    let comm = Communicator::new();
    comm.set_registrar(registry.clone());
    let proxy = comm.string_to_proxy("App.Server.Obj").unwrap();
    assert!(proxy
        .invoke(
            Context::new(),
            "echo",
            vec![],
            HashMap::new(),
            HashMap::new()
        )
        .await
        .is_err());
    *registry.endpoints.write() = Some(vec![ep.clone()]);
    assert_eq!(call(&proxy).await, 7);
    let n = registry.calls.load(Ordering::SeqCst);
    let direct = comm
        .string_to_proxy(&format!("App.Server.Obj@{}", ep.to_endpoint_string()))
        .unwrap();
    assert_eq!(call(&direct).await, 7);
    assert_eq!(registry.calls.load(Ordering::SeqCst), n);
    h.abort();
}
#[test]
fn construction_does_not_require_runtime() {
    let comm = Communicator::with_config(ClientConfig {
        locator: "Query@tcp -h 127.0.0.1 -p 1".into(),
        ..Default::default()
    });
    assert!(comm.string_to_proxy("App.Server.Obj").is_ok());
}

#[tokio::test]
async fn cache_recovers_offline_and_isolates_locator() {
    let (ep, h) = echo(8).await;
    let path = std::env::temp_dir().join(format!(
        "tars-discovery-{}-{}.tarsdat",
        std::process::id(),
        ep.port
    ));
    let registry = Arc::new(Registry {
        endpoints: RwLock::new(Some(vec![ep])),
        calls: AtomicUsize::new(0),
        set: None,
    });
    let config = ClientConfig {
        endpoint_cache_path: Some(path.to_string_lossy().into_owned()),
        locator: "Query@tcp -h 127.0.0.1 -p 1".into(),
        ..Default::default()
    };
    let comm = Communicator::with_config(config.clone());
    comm.set_registrar(registry.clone());
    assert_eq!(
        call(&comm.string_to_proxy("App.Server.Obj").unwrap()).await,
        8
    );
    *registry.endpoints.write() = None;
    drop(comm);
    let offline = Communicator::with_config(config.clone());
    offline.set_registrar(registry.clone());
    assert_eq!(
        call(&offline.string_to_proxy("App.Server.Obj").unwrap()).await,
        8
    );
    let other = Communicator::with_config(ClientConfig {
        locator: "Other@tcp -h 127.0.0.1 -p 2".into(),
        ..config
    });
    other.set_registrar(registry);
    assert!(other
        .string_to_proxy("App.Server.Obj")
        .unwrap()
        .invoke(
            Context::new(),
            "echo",
            vec![],
            HashMap::new(),
            HashMap::new()
        )
        .await
        .is_err());
    std::fs::remove_file(path).unwrap();
    h.abort();
}

struct GatedRegistry {
    endpoint: Endpoint,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl Registrar for GatedRegistry {
    async fn register(&self, _: &ServantInstance) -> Result<()> {
        Ok(())
    }
    async fn deregister(&self, _: &ServantInstance) -> Result<()> {
        Ok(())
    }
    async fn query_servant(&self, _: &str) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok((vec![self.endpoint.clone()], vec![]))
    }
    async fn query_servant_by_set(
        &self,
        id: &str,
        _: &str,
    ) -> Result<(Vec<Endpoint>, Vec<Endpoint>)> {
        self.query_servant(id).await
    }
}
#[tokio::test]
async fn cancelled_initial_query_can_retry() {
    let (endpoint, server) = echo(9).await;
    let registry = Arc::new(GatedRegistry {
        endpoint,
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
    });
    let comm = Communicator::new();
    comm.set_registrar(registry.clone());
    let proxy = comm.string_to_proxy("App.Server.Obj").unwrap();
    let p = proxy.clone();
    let task = tokio::spawn(async move { call(&p).await });
    registry.entered.notified().await;
    task.abort();
    let _ = task.await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), call(&proxy))
            .await
            .unwrap(),
        9
    );
    server.abort();
}
