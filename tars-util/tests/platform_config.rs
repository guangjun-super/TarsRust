use tars_util::TarsConfig;

#[test]
fn reads_node_generated_configuration() {
    let cfg = TarsConfig::parse(include_str!("../../examples/hello-rs/HelloServer.conf")).unwrap();
    assert_eq!(cfg.server.app, "Test");
    assert_eq!(cfg.server.server, "HelloServer");
    assert_eq!(cfg.server.local, "tcp -h 127.0.0.1 -p 10988 -t 3000");
    let adapter = &cfg.server.adapters["Test.HelloServer.HelloObjAdapter"];
    assert_eq!(adapter.servant, "Test.HelloServer.HelloObj");
    assert_eq!(adapter.endpoint, "tcp -h 127.0.0.1 -p 10987 -t 60000");
    assert_eq!(adapter.protocol, "tars");
    assert_eq!(adapter.max_conn, 200000);
}

#[test]
fn reads_client_and_server_options_without_treating_equals_as_separators() {
    let cfg = TarsConfig::parse(
        "\
<tars>\n<application>\n# comment\nenableset=Y\nsetdivision=test.sz.1\n\
<server>\napp=Test\nserver=HelloServer\nnode=tars.tarsnode.ServerObj@tcp -h 127.0.0.1 -p 19385\n\
basepath=/tmp/path=a\nmainloopticker=200\nreadtimeout=9000\n\
<TestAdapter>\nallow\nservant=Test.HelloServer.HelloObj\nendpoint=tcp -h 0.0.0.0 -p 10987\n\
protocol=tars\nthreads=3\nqueuecap=123\n</TestAdapter>\n</server>\n\
<client>\nlocator=tars.tarsregistry.QueryObj@tcp -h 127.0.0.1 -p 17890\n\
async-invoke-timeout=750\n</client>\n</application>\n</tars>",
    )
    .unwrap();
    assert!(cfg.server.enable_set);
    assert_eq!(cfg.server.set_division, "test.sz.1");
    assert_eq!(cfg.server.base_path, "/tmp/path=a");
    assert_eq!(cfg.server.main_loop_interval, 200);
    assert_eq!(cfg.server.read_timeout, 9000);
    assert_eq!(cfg.server.adapters["TestAdapter"].queue_cap, 123);
    assert_eq!(cfg.client.async_invoke_timeout, 750);
    assert!(cfg.client.locator.contains("QueryObj"));
}

#[test]
fn rejects_malformed_or_incomplete_platform_configuration() {
    for input in [
        "<tars>\n<application>\n<server>\n</application>\n</tars>",
        "<tars>\n<application>\n<server>\napp=Test\n</server>\n</application>\n</tars>",
        "<tars>\n<application>\n<server>\napp=Test\nserver=HelloServer\nmainloopticker=0\n</server>\n</application>\n</tars>",
        "<tars>\n<application>\n<server>\napp=Test\nserver=HelloServer\nreadtimeout=invalid\n</server>\n</application>\n</tars>",
    ] {
        assert!(TarsConfig::parse(input).is_err(), "accepted {input}");
    }
}
