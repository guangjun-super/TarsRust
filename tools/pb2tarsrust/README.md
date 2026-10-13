# pb2tarsrust

用 Go 实现的 `protoc` 插件：从 `.proto` 生成 Rust protobuf 消息和 Tars RPC
客户端/服务端代码。实现参考 TarsGo 的 `pb2tarsgo` 和本仓库 `protoc` 的 Rust
生成器。消息使用 `prost` 编解码，protobuf 字节直接放入 Tars 数据包的
`s_buffer`；外层使用 Tars v1 协议，与 TarsGo protobuf 服务的封包方式一致。

## 安装与生成

需要 Go 1.23+、`protoc`，以及 Rust 工具链来编译生成代码。

在本目录执行：

```sh
go install ./protoc-gen-tarsrust
export PATH="$(go env GOPATH)/bin:$PATH"
```

在 Rust 项目中生成代码：

```sh
mkdir -p src/pb
protoc -I proto --tarsrust_out=src/pb proto/helloworld.proto
```

`--tarsrust_out` 会在 `PATH` 中查找 `protoc-gen-tarsrust`。也可以直接指定插件：

```sh
protoc -I proto \
  --plugin=protoc-gen-tarsrust=/path/to/protoc-gen-tarsrust \
  --tarsrust_out=src/pb proto/helloworld.proto
```

通常 `--rust_out` 对应外部插件 `protoc-gen-rust`，但新版 `protoc` 已内置 Rust
生成器，会优先接管这个选项。因此此工具使用独立的 `tarsrust` 名称。

## 示例

`proto/helloworld.proto`：

```proto
syntax = "proto3";
package helloworld;

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
}
```

生成 `src/pb/helloworld.rs` 和 `src/pb/mod.rs`。在 `src/lib.rs` 或 `src/main.rs`
中声明：

```rust
pub mod pb;
```

Rust 项目依赖（按实际位置调整 `tars` 路径）：

```toml
[dependencies]
tars = { path = "/path/to/TarsRust/tars" }
prost = "0.14"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

服务端实现 `Greeter`，再把 `GreeterServant` 注册到 `Application`：

```rust
use async_trait::async_trait;
use pb::helloworld::{Greeter, GreeterServant, HelloReply, HelloRequest};
use tars::{Application, Context, Result};

struct GreeterImpl;

#[async_trait]
impl Greeter for GreeterImpl {
    async fn say_hello(&self, _ctx: &Context, input: HelloRequest) -> Result<HelloReply> {
        Ok(HelloReply { message: format!("hello {}", input.name) })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = Application::new();
    app.add_servant(
        "Test.HelloServer.GreeterObj",
        GreeterServant::new(GreeterImpl),
        "127.0.0.1:10014",
    )?;
    app.run().await
}
```

客户端使用 `GreeterProxy`：

```rust
use pb::helloworld::{GreeterProxy, HelloRequest};
use tars::{Communicator, Context, Result};

async fn call() -> Result<()> {
    let comm = Communicator::new();
    let proxy = comm.string_to_proxy(
        "Test.HelloServer.GreeterObj@tcp -h 127.0.0.1 -p 10014",
    )?;
    let client = GreeterProxy::new(proxy);
    let reply = client.say_hello(
        Context::new(), HelloRequest { name: "Ada".into() },
    ).await?;
    println!("{}", reply.message);
    Ok(())
}
```

每个方法还生成 `<method>_with_options(ctx, input, status, context)`，可以传入
Tars 的两个 `HashMap<String, String>`。服务端的 `Context` 可以读取请求的
`context` 字段。Rust 方法名转换为 snake_case，线上方法名保留 `.proto` 的原名
（例如 `SayHello`）。

## 输出与类型映射

- 每个 protobuf package 生成一个 `.rs` 文件；例如 `example.v1` 对应
  `example.v1.rs`，通过 `mod.rs` 暴露为 `pb::example::v1`。无 package 的类型放在
  `_root.rs`，直接暴露于 `pb`。
- 同一次调用中的同 package 文件合并输出。导入文件的消息和枚举也自动生成，
  包括 Timestamp、Empty 等 well-known types；无需 `prost-types`。只有命令行选中
  的文件生成服务代码。
- 把一个项目的所有入口 `.proto` 放在同一次调用中，避免后续调用覆盖 `mod.rs`
  或同 package 的文件。生成目录应专用于本插件。
- 默认完整类型路径为 `crate::pb`。使用其他入口时传入
  `--tarsrust_opt=module=crate::wire`，并声明 `pub mod wire;`。指定 `module=crate`
  时可在 crate 根使用 `include!("pb/mod.rs");`。
- 标量映射为 Rust 对应数值、`bool`、`String` 或 `Vec<u8>`；枚举字段存储 `i32`，
  保留未知数值。枚举变体移除枚举名前缀；同数值别名保留第一个变体。
- 支持 proto2/proto3、optional、required、显式默认值、packed/unpacked repeated、
  map、嵌套消息/枚举、oneof 和递归消息。嵌套类型位于所属消息的 snake_case
  子模块；只有需要打破大小递归的消息字段使用 `Box`。
- Rust 关键字通常使用 `r#` 标识符；枚举 map 字段使用 `_` 后缀，以兼容 prost
  自动生成的辅助方法。仅含下划线的名称映射为 `__`。辅助方法重名也会报错。
- 不支持流式 RPC、proto2 group/extension 和 protobuf Editions。无法表示的声明、
  Rust 名称或输出文件名冲突、未知插件选项会使生成失败，不输出部分结果。

生成代码使用 `tars` facade crate，不要求直接依赖工作区内部各个 crate。
它使用 protobuf 编码消息；`.tars` IDL 的代码仍由仓库 `protoc` 中的 `tarsc` 生成。

## 测试

在本目录运行：

```sh
go test ./...
go vet ./...
PB2TARSRUST_TEST_RUST=1 go test -v -count=1 ./...
```

普通测试需要 `protoc`，缺少时会跳过集成用例。设置 `PB2TARSRUST_TEST_RUST=1`
会编译真实生成代码，并测试 protobuf 字节、消息类型、服务分发、错误响应和
TCP 客户端调用，需要 Rust/Cargo 和可绑定的本机端口。测试 crate 放在临时目录，
构建缓存位于仓库 `target/pb2tarsrust`。
