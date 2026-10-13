# 接入 Tars 服务治理平台

Rust 服务沿用 TarsGo 的原生进程部署约定：tarsnode 下发配置并拉起进程，
服务向 NodeF 上报进程及 Adapter 心跳，在配置的 `local` 端口提供 AdminF。
平台的发布、停止、普通重启通过 tarsnode 完成，不需要修改平台代码。

## 服务代码

在注册业务 servant 之前读取平台启动参数，并使用平台配置的监听地址：

```rust
let app = tars_rpc::Application::from_args(std::env::args().skip(1))?;
let cfg = app.server_config();
let obj = format!("{}.{}.HelloObj", cfg.app, cfg.server);
app.add_servant_from_config(&obj, HelloServant::new(HelloImpl))?;
app.run().await
```

完整可运行示例是 `examples/hello-rs/src/bin/hello_server.rs`。
`from_args` 支持 `--config file`、`--config=file`、`-config file` 和 `-config=file`。
也可以用 `Application::from_config_file(path)` 直接加载配置。
配置格式与 TarsGo 一致，是 `<tars>/<application>/<server>` 域中的 `key=value`，不是 XML。
没有平台配置时，原有 `Application::new()` 和 `add_servant(..., address)` 仍可使用。

本地验证示例（配置未指定 node，所以不发送心跳）：

```bash
cargo run -p hello-rs --bin hello-server -- --config examples/hello-rs/HelloServer.conf
cargo run -p hello-rs --bin hello-client
```

## 构建和发布包

在与目标节点相同的操作系统、CPU 架构和运行库环境下构建；Linux 节点使用 Linux ELF
可执行文件。以下命令应在目标 Linux 构建环境执行：

```bash
cargo build --release -p hello-rs --bin hello-server
bash scripts/package-tars.sh target/release/hello-server HelloServer
```

生成 `target/tars/HelloServer.tgz`，目录结构与 TarsGo 的发布包一致：

```text
HelloServer/
└── HelloServer
```

二进制名称必须与平台服务名一致。发布包不带本地示例配置，运行时使用 tarsnode
下发的配置。交叉编译时将脚本的第一个参数替换为对应 target 目录下的二进制。

## 平台配置

1. 新建服务：应用 `Test`，服务 `HelloServer`，服务类型选择 `tars_go`（沿用 Go 的
   原生可执行文件启动约定）。自定义启动模板必须以 `--config=平台生成的配置文件路径`
   或 `--config 路径` 传入配置，不应使用 Java/Node.js 等解释器启动模板。
2. 配置对象 `Test.HelloServer.HelloObj`，Adapter 使用 TARS 协议、TCP 监听，端口由平台分配。
   平台配置中的 `app`、`server`、Adapter 的 `servant` 必须与业务注册的对象一致。
3. 保留平台生成的 `local` 管理端口和 `node` 直连对象配置，例如
   `tars.tarsnode.ServerObj@tcp -h 127.0.0.1 -p 19385`。管理端口应只允许节点管理进程访问。
4. 上传 `HelloServer.tgz`，在平台执行发布并启动；确认实际状态变为 Active。
5. 在平台执行重启：tarsnode 调用 `AdminObj.shutdown`，旧进程停止监听并排空已分发请求，
   退出后由 tarsnode 启动新的进程。确认 PID 变化、服务恢复 Active 且客户端调用正常。

## 支持范围与验证

- 启动成功后上报 `reportVersion` 和进程心跳，默认每 10 秒上报已注册业务 Adapter
  和 `AdminAdapter` 的心跳；可通过 `mainloopticker`（毫秒，必须大于 0）调整。
- `local` 管理端口支持 `shutdown`、`notify("tars.viewversion")` 和 `tars_ping`。
  未支持的 notify 命令返回说明。普通平台重启使用 `shutdown`；
  `tars.gracerestart` 的进程内无损重启未实现。
- 支持 Ctrl+C 和 Unix SIGTERM 优雅退出。任一监听端口绑定失败会返回错误，
  不会上报服务已启动；停止上报心跳后关闭监听并等待已分发调用完成，最长排空 30 秒。
- 当前平台配置的业务和管理监听只支持 TCP/TARS；UDP、SSL 和自定义协议配置返回错误。
  远程业务配置下载、远程日志、统计及其他管理命令不属于本次接入范围。
- `cargo test --workspace` 包括模拟 tarsnode 的集成测试，验证 NodeF 字段、AdminF 调用、
  端口冲突处理、早到的关闭信号和关闭后的端口复用。此测试需要允许绑定本机回环端口。
  真实平台验收按上述发布及重启步骤执行。
