use super::*;
#[tokio::test]
async fn routing_hash_and_weight_metadata_updates() {
    let mut a = Endpoint::tcp("127.0.0.1", 10001);
    let mut b = Endpoint::tcp("127.0.0.1", 10002);
    let p = ServantProxy::new(
        "App.Server.Obj",
        vec![a.clone(), b.clone()],
        TarsClientConfig::tcp(),
    );
    let mut msg = Message::new();
    msg.is_hash = true;
    msg.hash_code = 1;
    msg.hash_type = HashType::ModHash;
    for _ in 0..4 {
        assert_eq!(p.select_adapter(&msg).unwrap().endpoint().port, 10002);
    }
    msg.hash_type = HashType::ConsistentHash;
    let port = p.select_adapter(&msg).unwrap().endpoint().port;
    for _ in 0..4 {
        assert_eq!(p.select_adapter(&msg).unwrap().endpoint().port, port);
    }
    a.weight_type = 1;
    a.weight = 100;
    b.weight_type = 1;
    b.weight = 10;
    p.refresh_endpoints(vec![a.clone(), b.clone()]);
    msg.is_hash = false;
    let ports: Vec<_> = (0..110)
        .map(|_| p.select_adapter(&msg).unwrap().endpoint().port)
        .collect();
    assert_eq!(ports.iter().filter(|&&p| p == 10001).count(), 100);
    a.weight = 10;
    b.weight = 100;
    p.refresh_endpoints(vec![a, b]);
    let ports: Vec<_> = (0..110)
        .map(|_| p.select_adapter(&msg).unwrap().endpoint().port)
        .collect();
    assert_eq!(ports.iter().filter(|&&p| p == 10002).count(), 100);
    assert_eq!(
        p.adapters
            .read()
            .values()
            .find(|a| a.endpoint().port == 10002)
            .unwrap()
            .endpoint()
            .weight,
        100
    );
}
#[tokio::test(start_paused = true)]
async fn routing_health_recovers_and_all_unhealthy_falls_back() {
    let a = Endpoint::tcp("127.0.0.1", 10001);
    let b = Endpoint::tcp("127.0.0.1", 10002);
    let p = ServantProxy::new(
        "App.Server.Obj",
        vec![a.clone(), b.clone()],
        TarsClientConfig::tcp(),
    );
    let adapter = p.get_or_create_adapter(&a);
    for _ in 0..5 {
        adapter.fail_add();
    }
    tokio::time::advance(Duration::from_secs(6)).await;
    p.check_health();
    assert!(!adapter.is_active());
    for _ in 0..4 {
        assert_eq!(
            p.select_adapter(&Message::new()).unwrap().endpoint().port,
            10002
        );
    }
    tokio::time::advance(Duration::from_secs(31)).await;
    p.check_health();
    assert_eq!(
        p.select_adapter(&Message::new()).unwrap().endpoint().port,
        10001
    );
    adapter.success_add();
    assert!(adapter.is_active());
    let other = p.get_or_create_adapter(&b);
    for _ in 0..5 {
        adapter.fail_add();
        other.fail_add();
    }
    tokio::time::advance(Duration::from_secs(6)).await;
    p.check_health();
    assert!(p.select_adapter(&Message::new()).is_ok());
    let removed = p.get_or_create_adapter(&a);
    p.refresh_endpoints(vec![b]);
    assert!(removed.is_closed());
}

#[tokio::test]
async fn cancelled_invocation_releases_queue_slot() {
    use tokio::{io::AsyncReadExt, net::TcpListener};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Arc::new(ServantProxy::new(
        "App.Server.Obj",
        vec![Endpoint::tcp(
            "127.0.0.1",
            listener.local_addr().unwrap().port(),
        )],
        TarsClientConfig::tcp(),
    ));
    let (entered, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let n = socket.read_u32().await.unwrap();
        let mut b = vec![0; n as usize - 4];
        socket.read_exact(&mut b).await.unwrap();
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let p = proxy.clone();
    let task = tokio::spawn(async move {
        p.invoke(
            Context::new(),
            "wait",
            vec![],
            HashMap::new(),
            HashMap::new(),
        )
        .await
    });
    received.await.unwrap();
    assert_eq!(proxy.queue_len.load(Ordering::SeqCst), 1);
    task.abort();
    let _ = task.await;
    assert_eq!(proxy.queue_len.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn removing_endpoint_closes_connection_promptly() {
    use tokio::{io::AsyncReadExt, net::TcpListener};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = ServantProxy::new(
        "App.Server.Obj",
        vec![Endpoint::tcp(
            "127.0.0.1",
            listener.local_addr().unwrap().port(),
        )],
        TarsClientConfig::tcp(),
    );
    proxy
        .invoke_oneway(
            Context::new(),
            "send",
            vec![],
            HashMap::new(),
            HashMap::new(),
        )
        .await
        .unwrap();
    let (mut stream, _) = listener.accept().await.unwrap();
    let n = stream.read_u32().await.unwrap();
    let mut b = vec![0; n as usize - 4];
    stream.read_exact(&mut b).await.unwrap();
    proxy.refresh_endpoints(vec![]);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(300), stream.read(&mut [0]))
            .await
            .expect("removed endpoint must close promptly")
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn endpoint_removal_releases_pending_responses() {
    use tokio::{io::AsyncReadExt, net::TcpListener};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Arc::new(ServantProxy::new(
        "App.Server.Obj",
        vec![Endpoint::tcp(
            "127.0.0.1",
            listener.local_addr().unwrap().port(),
        )],
        TarsClientConfig::tcp(),
    ));
    let (entered, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let n = socket.read_u32().await.unwrap();
        let mut b = vec![0; n as usize - 4];
        socket.read_exact(&mut b).await.unwrap();
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let p = proxy.clone();
    let task = tokio::spawn(async move {
        p.invoke(
            Context::new(),
            "wait",
            vec![],
            HashMap::new(),
            HashMap::new(),
        )
        .await
    });
    received.await.unwrap();
    proxy.refresh_endpoints(vec![]);
    let error = tokio::time::timeout(Duration::from_millis(200), task)
        .await
        .expect("pending response must be released when endpoint closes")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, TarsError::ConnectionClosed));
    server.abort();
}

#[tokio::test]
async fn unsupported_transport_is_not_silently_invoked_over_tcp() {
    let proxy = ServantProxy::new(
        "App.Server.Obj",
        vec![Endpoint::udp("127.0.0.1", 12345)],
        TarsClientConfig::tcp(),
    );
    assert!(matches!(
        proxy.select_adapter(&Message::new()),
        Err(TarsError::Config(_))
    ));
    assert!(proxy.adapters.read().is_empty());
}
