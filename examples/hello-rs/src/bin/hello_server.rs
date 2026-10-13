use async_trait::async_trait;
use hello_rs::hello::test_app::{Hello, HelloServant};
use tars_core::{Context, Result};
use tars_rpc::Application;

struct HelloImpl;

#[async_trait]
impl Hello for HelloImpl {
    async fn hello(&self, _ctx: &Context, no: i32, name: String) -> Result<String> {
        Ok(format!("hello#{} {}", no, name))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<_> = std::env::args().skip(1).collect();
    let app = if args.first().is_some_and(|arg| arg.starts_with('-')) {
        let app = Application::from_args(&args)?;
        let config = app.server_config();
        let obj = format!("{}.{}.HelloObj", config.app, config.server);
        app.add_servant_from_config(&obj, HelloServant::new(HelloImpl))?;
        println!("hello-server: {} using platform configuration", obj);
        app
    } else {
        let addr = args
            .first()
            .map(String::as_str)
            .unwrap_or("127.0.0.1:10987");
        let obj = "Test.HelloServer.HelloObj";
        let app = Application::new();
        app.add_servant(obj, HelloServant::new(HelloImpl), addr)?;
        println!("hello-server: {} listening at tcp://{}", obj, addr);
        app
    };

    app.run().await?;
    Ok(())
}
