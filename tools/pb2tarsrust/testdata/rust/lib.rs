pub mod pb;

#[cfg(test)]
mod tests {
    use super::pb::{common::Detail, example::v1::*, legacy::Legacy};
    use async_trait::async_trait;
    use prost::Message;
    use tars::transport::ServerProtocolHandler;
    use tars::{consts, Context, RequestPacket, ResponsePacket, Result, TarsError};

    struct GreeterImpl;
    #[async_trait]
    impl Greeter for GreeterImpl {
        async fn say_hello(&self, ctx: &Context, input: HelloRequest) -> Result<HelloReply> {
            assert_eq!(ctx.get("token").as_deref(), Some("test"));
            if input.name == "fail" {
                return Err(TarsError::ServerError {
                    code: -42,
                    message: "rejected".into(),
                });
            }
            Ok(HelloReply {
                message: format!("hello {}", input.name),
            })
        }
    }

    #[test]
    fn protobuf_wire_and_types() {
        let input = HelloRequest {
            name: "Ada".into(),
            count: Some(7),
            ..Default::default()
        };
        assert_eq!(input.encode_to_vec(), b"\x0a\x03Ada\x10\x07");
        let input = HelloRequest {
            detail: Some(Detail {
                text: "detail".into(),
            }),
            choice: Some(hello_request::Choice::Child(Box::new(input))),
            next: Some(Box::new(HelloRequest::default())),
            nested: Some(hello_request::Nested { id: 9 }),
            numbers: vec![-1, 123],
            states: vec![0, 1, 999],
            r#type: vec![0, 255],
            details: [(
                "key".into(),
                Detail {
                    text: "value".into(),
                },
            )]
            .into(),
            ..Default::default()
        };
        assert_eq!(
            HelloRequest::decode(input.encode_to_vec().as_slice()).unwrap(),
            input
        );
        assert_eq!(State::Ok as i32, 1);
        let supplemental = Supplemental {
            request: Some(input),
            time: Some(super::pb::google::protobuf::Timestamp {
                seconds: 123,
                nanos: 456,
            }),
            states: [(7, State::Ok as i32)].into(),
            blobs: [(true, vec![0, 255])].into(),
            type_: [("key".into(), State::Ok as i32)].into(),
            data: vec![vec![42]],
            signed: -100,
            zigzag: -42,
            unsigned: u32::MAX,
            signed_fixed: -99,
            fixed: u64::MAX,
            ..Default::default()
        };
        assert_eq!(
            Supplemental::decode(supplemental.encode_to_vec().as_slice()).unwrap(),
            supplemental
        );
        let legacy = Legacy::default();
        assert_eq!(legacy.name(), "hello\nworld");
        assert_eq!(legacy.count(), 42);
        assert_eq!(legacy.mode() as i32, 7);
        assert_eq!(legacy.payload(), b"a\x01");
        assert_eq!(legacy.ratio(), f64::INFINITY);
        assert_eq!(
            Legacy::decode(legacy.encode_to_vec().as_slice()).unwrap(),
            legacy
        );
    }

    fn request(method: &str, buffer: Vec<u8>) -> RequestPacket {
        let mut req = RequestPacket::new();
        req.i_request_id = 123;
        req.s_func_name = method.into();
        req.s_buffer = buffer;
        req.context.insert("token".into(), "test".into());
        req
    }

    #[tokio::test]
    async fn dispatch_success_and_errors() {
        let handler = GreeterServant::new(GreeterImpl);
        let req = request(
            "SayHello",
            HelloRequest {
                name: "Ada".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        );
        let resp = ResponsePacket::decode(
            &handler
                .invoke(&mut Context::new(), &req.encode().unwrap())
                .await,
        )
        .unwrap();
        assert_eq!(resp.i_request_id, 123);
        assert_eq!(resp.i_version, 1);
        assert_eq!(resp.i_ret, 0);
        assert_eq!(resp.s_buffer, b"\x0a\x09hello Ada");
        for (method, buffer, code) in [
            ("SayHello", vec![0xff], consts::TARS_SERVER_DECODE_ERR),
            ("Missing", vec![], consts::TARS_SERVER_UNKNOWN_ERR),
            (
                "SayHello",
                HelloRequest {
                    name: "fail".into(),
                    ..Default::default()
                }
                .encode_to_vec(),
                -42,
            ),
        ] {
            let req = request(method, buffer);
            let resp = ResponsePacket::decode(
                &handler
                    .invoke(&mut Context::new(), &req.encode().unwrap())
                    .await,
            )
            .unwrap();
            assert_eq!(resp.i_ret, code);
            assert_eq!(resp.i_request_id, 123);
        }
        let timeout =
            ResponsePacket::decode(&handler.invoke_timeout(&req.encode().unwrap())).unwrap();
        assert_eq!(timeout.i_request_id, 123);
        assert_eq!(timeout.i_ret, consts::TARS_SERVER_QUEUE_TIMEOUT);
    }

    #[tokio::test]
    async fn proxy_over_tcp() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let len = stream.read_u32().await.unwrap() as usize;
            let mut packet = vec![0; len];
            packet[..4].copy_from_slice(&(len as u32).to_be_bytes());
            stream.read_exact(&mut packet[4..]).await.unwrap();
            let req = RequestPacket::decode(&packet).unwrap();
            assert_eq!(req.s_func_name, "SayHello");
            assert_eq!(req.s_buffer, b"\x0a\x03Ada");
            assert_eq!(req.status.get("custom").map(String::as_str), Some("status"));
            let reply = GreeterServant::new(GreeterImpl)
                .invoke(&mut Context::new(), &packet)
                .await;
            stream.write_all(&reply).await.unwrap();
        });
        let comm = tars::Communicator::new();
        let proxy = GreeterProxy::new(
            comm.string_to_proxy(&format!("Test.Greeter.Obj@tcp -h 127.0.0.1 -p {port}"))
                .unwrap(),
        );
        let reply = proxy
            .say_hello_with_options(
                Context::new(),
                HelloRequest {
                    name: "Ada".into(),
                    ..Default::default()
                },
                [("custom".into(), "status".into())].into(),
                [("token".into(), "test".into())].into(),
            )
            .await
            .unwrap();
        assert_eq!(reply.message, "hello Ada");
        server.await.unwrap();
    }
}
