use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tars_core::{
    codec::{parse_package, Buffer, PackageStatus, Reader},
    Context,
};
use tars_protocol::protocol::{RequestPacket, ResponsePacket};
use tars_rpc::{application::AppState, Application};
use tars_transport::transport::ServerProtocolHandler;
use tars_util::{ServerConfig, TarsConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Semaphore},
    time::timeout,
};

struct Echo;

struct SlowEcho {
    entered: mpsc::UnboundedSender<()>,
    release: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl ServerProtocolHandler for SlowEcho {
    fn parse_package(&self, bytes: &[u8]) -> (usize, PackageStatus) {
        parse_package(bytes)
    }
    async fn invoke(&self, ctx: &mut Context, bytes: &[u8]) -> Vec<u8> {
        self.entered.send(()).unwrap();
        let _permit = self.release.acquire().await.unwrap();
        Echo.invoke(ctx, bytes).await
    }
    fn invoke_timeout(&self, _: &[u8]) -> Vec<u8> {
        vec![]
    }
    fn get_close_msg(&self) -> Vec<u8> {
        vec![]
    }
    fn do_close(&self, _: &Context) {}
}

#[async_trait::async_trait]
impl ServerProtocolHandler for Echo {
    fn parse_package(&self, bytes: &[u8]) -> (usize, PackageStatus) {
        parse_package(bytes)
    }
    async fn invoke(&self, _: &mut Context, bytes: &[u8]) -> Vec<u8> {
        let req = RequestPacket::decode(bytes).unwrap();
        ResponsePacket::success(req.i_request_id, req.s_buffer)
            .encode()
            .unwrap()
    }
    fn invoke_timeout(&self, _: &[u8]) -> Vec<u8> {
        vec![]
    }
    fn get_close_msg(&self) -> Vec<u8> {
        vec![]
    }
    fn do_close(&self, _: &Context) {}
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
    let length = stream.read_u32().await.unwrap() as usize;
    let mut packet = vec![0; length];
    packet[..4].copy_from_slice(&(length as u32).to_be_bytes());
    stream.read_exact(&mut packet[4..]).await.unwrap();
    packet
}

async fn call(stream: &mut TcpStream, method: &str, args: Vec<u8>) -> ResponsePacket {
    let mut request = RequestPacket::new();
    request.i_request_id = 42;
    request.s_servant_name = "AdminObj".into();
    request.s_func_name = method.into();
    request.s_buffer = args;
    stream.write_all(&request.encode().unwrap()).await.unwrap();
    let response = ResponsePacket::decode(&read_packet(stream).await).unwrap();
    assert_eq!(response.i_request_id, 42);
    response
}

#[tokio::test]
async fn node_heartbeats_admin_shutdown_and_restart_release_ports() {
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_port = node_listener.local_addr().unwrap().port();
    let admin_port = free_port().await;
    let service_port = free_port().await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let node = tokio::spawn(async move {
        let (mut stream, _) = node_listener.accept().await.unwrap();
        loop {
            let request = RequestPacket::decode(&read_packet(&mut stream).await).unwrap();
            // NodeF returns int with tag 0; ZeroTag is the literal wire encoding of 0.
            let response = ResponsePacket::success(request.i_request_id, vec![0x0c]);
            stream.write_all(&response.encode().unwrap()).await.unwrap();
            tx.send(request).unwrap();
        }
    });
    let cfg = TarsConfig::parse(&format!(
        "\
<tars>\n<application>\n<server>\napp=Test\nserver=HelloServer\nmainloopticker=50\n\
local=tcp -h 127.0.0.1 -p {admin_port}\n\
node=tars.tarsnode.ServerObj@tcp -h 127.0.0.1 -p {node_port}\n\
<HelloAdapter>\nservant=Test.HelloServer.HelloObj\nendpoint=tcp -h 127.0.0.1 -p {service_port}\n\
protocol=tars\n</HelloAdapter>\n</server>\n</application>\n</tars>"
    ))
    .unwrap();
    let app = Arc::new(Application::new());
    app.set_server_config(cfg.server.clone());
    app.add_servant_from_config("Test.HelloServer.HelloObj", Echo)
        .unwrap();
    let runner = app.clone();
    let run = tokio::spawn(async move { runner.run().await });

    let mut adapters = HashSet::new();
    let mut reported_version = false;
    timeout(Duration::from_secs(3), async {
        while adapters.len() < 3 || !reported_version {
            let request = rx.recv().await.unwrap();
            assert_eq!(request.s_servant_name, "tars.tarsnode.ServerObj");
            let mut args = Reader::new(&request.s_buffer);
            match request.s_func_name.as_str() {
                "reportVersion" => {
                    assert_eq!(args.read_string(1, true).unwrap(), "Test");
                    assert_eq!(args.read_string(2, true).unwrap(), "HelloServer");
                    assert!(args.read_string(3, true).unwrap().contains("Rust"));
                    reported_version = true;
                }
                "keepAlive" => {
                    assert!(args.read_struct_begin(1, true).unwrap());
                    assert_eq!(args.read_string(0, true).unwrap(), "Test");
                    assert_eq!(args.read_string(1, true).unwrap(), "HelloServer");
                    assert_eq!(args.read_int32(2, true).unwrap(), std::process::id() as i32);
                    adapters.insert(args.read_string(3, false).unwrap());
                    args.read_struct_end().unwrap();
                }
                other => panic!("unexpected NodeF method {other}"),
            }
        }
    })
    .await
    .expect("platform never received complete startup heartbeats");
    assert_eq!(
        adapters,
        HashSet::from(["".into(), "HelloAdapter".into(), "AdminAdapter".into()])
    );
    assert_eq!(app.state(), AppState::Running);

    let mut admin = TcpStream::connect(("127.0.0.1", admin_port)).await.unwrap();
    let mut command = Buffer::new();
    command.write_string("tars.viewversion", 1).unwrap();
    let response = call(&mut admin, "notify", command.to_bytes()).await;
    assert_eq!(response.i_ret, 0);
    assert!(Reader::new(&response.s_buffer)
        .read_string(0, true)
        .unwrap()
        .contains("Rust"));
    let malformed = call(&mut admin, "notify", vec![]).await;
    assert_ne!(malformed.i_ret, 0);
    let missing = call(&mut admin, "missingMethod", vec![]).await;
    assert_ne!(missing.i_ret, 0);
    let mut service = TcpStream::connect(("127.0.0.1", service_port))
        .await
        .unwrap();
    assert_eq!(
        call(&mut service, "echo", vec![1, 2, 3]).await.s_buffer,
        vec![1, 2, 3]
    );
    let response = timeout(Duration::from_secs(2), call(&mut admin, "shutdown", vec![]))
        .await
        .unwrap();
    assert_eq!(response.i_ret, 0);
    timeout(Duration::from_secs(2), run)
        .await
        .expect("shutdown did not finish promptly")
        .unwrap()
        .unwrap();
    assert_eq!(app.state(), AppState::Stopped);
    assert_eq!(admin.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(service.read(&mut [0; 1]).await.unwrap(), 0);
    node.abort();

    // A fresh process must be able to reuse both listening ports immediately.
    let restarted = Arc::new(Application::new());
    let mut cfg = cfg.server;
    cfg.node.clear();
    restarted.set_server_config(cfg);
    restarted
        .add_servant_from_config("Test.HelloServer.HelloObj", Echo)
        .unwrap();
    let runner = restarted.clone();
    let run = tokio::spawn(async move { runner.run().await });
    timeout(Duration::from_secs(2), async {
        loop {
            if restarted.state() == AppState::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut admin = TcpStream::connect(("127.0.0.1", admin_port)).await.unwrap();
    assert_eq!(call(&mut admin, "shutdown", vec![]).await.i_ret, 0);
    timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn listener_failure_is_returned_and_other_ports_are_released() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();
    let app = Application::new();
    let admin_port = free_port().await;
    app.set_server_config(ServerConfig {
        local: format!("tcp -h 127.0.0.1 -p {admin_port}"),
        ..Default::default()
    });
    app.add_servant("Test.HelloServer.HelloObj", Echo, &addr.to_string())
        .unwrap();
    let result = timeout(Duration::from_secs(1), app.run())
        .await
        .expect("listener failure was swallowed");
    assert!(result.is_err());
    TcpListener::bind(("127.0.0.1", admin_port)).await.unwrap();
}

#[tokio::test]
async fn early_shutdown_signal_is_not_lost() {
    let app = Application::new();
    app.signal_shutdown();
    timeout(Duration::from_secs(1), app.run())
        .await
        .expect("early shutdown was lost")
        .unwrap();
    assert_eq!(app.state(), AppState::Stopped);
}

#[test]
fn config_flags_load_platform_file_and_reject_missing_values() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/hello-rs/HelloServer.conf"
    );
    for args in [
        vec!["--config".into(), path.into()],
        vec![format!("--config={path}")],
        vec!["-config".into(), path.into()],
        vec![format!("-config={path}")],
    ] {
        let app = Application::from_args(args).unwrap();
        assert_eq!(app.server_config().server, "HelloServer");
    }
    assert!(Application::from_args(["--config"]).is_err());
    assert!(Application::from_args(["--config="]).is_err());
    assert!(Application::from_args(["--config=/does/not/exist"]).is_err());
    assert!(Application::from_args(["--unknown"]).is_err());
}

#[test]
fn missing_or_unsupported_adapter_is_rejected() {
    let app = Application::new();
    assert!(app
        .add_servant_from_config("Test.HelloServer.HelloObj", Echo)
        .is_err());
    for endpoint in [
        "invalid",
        "udp -h 127.0.0.1 -p 10000",
        "ssl -h 127.0.0.1 -p 10000",
    ] {
        app.set_server_config(ServerConfig {
            adapters: HashMap::from([(
                "HelloAdapter".into(),
                tars_util::util::AdapterConfig {
                    servant: "Test.HelloServer.HelloObj".into(),
                    endpoint: endpoint.into(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        });
        assert!(app
            .add_servant_from_config("Test.HelloServer.HelloObj", Echo)
            .is_err());
    }
}

#[tokio::test]
async fn all_adapters_stop_accepting_before_inflight_requests_finish() {
    let ports = [free_port().await, free_port().await];
    let app = Arc::new(Application::new());
    let (entered, mut rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    for (index, port) in ports.into_iter().enumerate() {
        app.add_servant(
            &format!("Obj{index}"),
            SlowEcho {
                entered: entered.clone(),
                release: release.clone(),
            },
            &format!("127.0.0.1:{port}"),
        )
        .unwrap();
    }
    let runner = app.clone();
    let run = tokio::spawn(async move { runner.run().await });
    while app.state() != AppState::Running {
        tokio::task::yield_now().await;
    }
    let mut calls = Vec::new();
    for port in ports {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        calls.push(tokio::spawn(async move {
            call(&mut stream, "echo", vec![1, 2, 3]).await
        }));
    }
    for _ in 0..2 {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    app.signal_shutdown();
    let stopped_accepting = timeout(Duration::from_millis(500), async {
        for port in ports {
            loop {
                if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)).await {
                    drop(listener);
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    })
    .await;
    // Release both calls even when the assertion fails, so no blocked task is left behind.
    release.add_permits(2);
    for call in calls {
        assert_eq!(call.await.unwrap().s_buffer, vec![1, 2, 3]);
    }
    timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        stopped_accepting.is_ok(),
        "an adapter kept listening while another drained"
    );
}

#[tokio::test]
async fn cancelling_run_stops_node_heartbeats() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_port = listener.local_addr().unwrap().port();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let node = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        loop {
            let request = RequestPacket::decode(&read_packet(&mut stream).await).unwrap();
            stream
                .write_all(
                    &ResponsePacket::success(request.i_request_id, vec![0x0c])
                        .encode()
                        .unwrap(),
                )
                .await
                .unwrap();
            tx.send(()).unwrap();
        }
    });
    let port = free_port().await;
    let app = Arc::new(Application::new());
    app.set_server_config(ServerConfig {
        app: "Test".into(),
        server: "HelloServer".into(),
        local: format!("tcp -h 127.0.0.1 -p {port}"),
        node: format!("tars.tarsnode.ServerObj@tcp -h 127.0.0.1 -p {node_port}"),
        main_loop_interval: 20,
        ..Default::default()
    });
    let runner = app.clone();
    let run = tokio::spawn(async move { runner.run().await });
    for _ in 0..3 {
        timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    while rx.try_recv().is_ok() {}
    assert!(
        timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "cancelled service still reports heartbeats"
    );
    TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    node.abort();
    assert_eq!(app.server_config().app, "Test"); // caller still owns the Application
}
