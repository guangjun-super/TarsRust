use tars_core::codec::{Buffer, Reader};
use tars_protocol::protocol::queryf::EndpointF;
use tars_protocol::{RequestPacket, ResponsePacket};
use tars_registry::registry::{Registrar, TarsRegistry};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn registry(set: bool, ret: i32) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let locator = format!(
        "Custom.Registry.Query@tcp -h 127.0.0.1 -p {}",
        listener.local_addr().unwrap().port()
    );
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let n = stream.read_u32().await.unwrap() as usize;
        let mut packet = vec![0; n];
        packet[..4].copy_from_slice(&(n as u32).to_be_bytes());
        stream.read_exact(&mut packet[4..]).await.unwrap();
        let req = RequestPacket::decode(&packet).unwrap();
        assert_eq!(req.s_servant_name, "Custom.Registry.Query");
        assert_eq!(
            req.s_func_name,
            if set {
                "findObjectByIdInSameSet"
            } else {
                "findObjectByIdInSameGroup"
            }
        );
        let mut r = Reader::new(&req.s_buffer);
        assert_eq!(r.read_string(1, true).unwrap(), "App.Server.Obj");
        if set {
            assert_eq!(r.read_string(2, true).unwrap(), "sz.area.1");
        }
        let mut b = Buffer::new();
        b.write_int32(ret, 0).unwrap();
        let tag = if set { 3 } else { 2 };
        b.write_list(1, tag).unwrap();
        b.write_struct_begin(0).unwrap();
        EndpointF {
            host: "127.0.0.1".into(),
            port: 12345,
            timeout: 3000,
            istcp: 1,
            grid: 7,
            qos: 9,
            weight: 42,
            weight_type: 1,
            auth_type: 2,
            set_id: "sz.area.1".into(),
            ..Default::default()
        }
        .encode(&mut b)
        .unwrap();
        b.write_struct_end().unwrap();
        b.write_list(0, tag + 1).unwrap();
        let body = if ret == 0 {
            if set {
                include_bytes!("fixtures/same_set.bin").to_vec()
            } else {
                include_bytes!("fixtures/same_group.bin").to_vec()
            }
        } else {
            b.to_bytes()
        };
        let rsp = ResponsePacket::success(req.i_request_id, body);
        stream.write_all(&rsp.encode().unwrap()).await.unwrap();
    });
    (locator, task)
}

#[tokio::test]
async fn same_group_and_set_wire_contract() {
    for set in [false, true] {
        let (locator, task) = registry(set, 0).await;
        let registry = TarsRegistry::new(&locator).with_timeout(500);
        let result = if set {
            registry
                .query_servant_by_set("App.Server.Obj", "sz.area.1")
                .await
        } else {
            registry.query_servant("App.Server.Obj").await
        };
        task.await.unwrap();
        let (active, inactive) = result.unwrap();
        assert!(inactive.is_empty());
        let ep = &active[0];
        assert_eq!(
            (
                ep.port,
                ep.grid,
                ep.qos,
                ep.weight,
                ep.weight_type,
                ep.auth_type
            ),
            (12345, 7, 9, 42, 1, 2)
        );
    }
}

#[tokio::test]
async fn rejects_business_error() {
    let (locator, task) = registry(false, -3).await;
    let result = TarsRegistry::new(&locator)
        .with_timeout(500)
        .query_servant("App.Server.Obj")
        .await;
    task.await.unwrap();
    assert!(result.is_err());
}

#[tokio::test]
async fn stalled_query_is_cancelled_and_socket_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let locator = format!(
        "Custom.Registry.Query@tcp -h 127.0.0.1 -p {}",
        listener.local_addr().unwrap().port()
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let n = stream.read_u32().await.unwrap();
        let mut data = vec![0; n as usize - 4];
        stream.read_exact(&mut data).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.read_u8())
            .await
            .unwrap()
            .unwrap_err();
    });
    assert!(TarsRegistry::new(&locator)
        .with_timeout(50)
        .query_servant("App.Server.Obj")
        .await
        .is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn failover_visits_remaining_registry_after_stall() {
    let stalled = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (good, server) = registry(false, 0).await;
    let bad_port = stalled.local_addr().unwrap().port();
    let bad = tokio::spawn(async move {
        let (_socket, _) = stalled.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let locator = format!(
        "Custom.Registry.Query@tcp -h 127.0.0.1 -p {bad_port}:{}",
        good.split('@').nth(1).unwrap()
    );
    let (active, _) = TarsRegistry::new(&locator)
        .with_timeout(300)
        .query_servant("App.Server.Obj")
        .await
        .unwrap();
    assert_eq!(active[0].port, 12345);
    server.await.unwrap();
    bad.abort();
}

#[tokio::test]
async fn rejects_packet_errors_and_invalid_frame_lengths() {
    for malformed in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let locator = format!(
            "Query@tcp -h 127.0.0.1 -p {}",
            listener.local_addr().unwrap().port()
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            let mut request = vec![0; size as usize - 4];
            socket.read_exact(&mut request).await.unwrap();
            if malformed {
                socket.write_u32(3).await.unwrap();
            } else {
                let mut response = ResponsePacket::success(1, vec![]);
                response.i_ret = -7;
                socket.write_all(&response.encode().unwrap()).await.unwrap();
            }
        });
        assert!(TarsRegistry::new(&locator)
            .with_timeout(300)
            .query_servant("App.Server.Obj")
            .await
            .is_err());
        server.await.unwrap();
    }
}
