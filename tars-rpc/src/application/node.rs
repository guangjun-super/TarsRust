//! Minimal NodeF client, using the tags defined by TarsGo's NodeF.tars.

use super::admin::VERSION;
use crate::{communicator::Communicator, servant::ServantProxy};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tars_core::{
    codec::{Buffer, Reader},
    Context, Result, TarsError,
};
use tars_util::ServerConfig;
use tokio::sync::watch;
use tracing::warn;

pub(super) struct Node {
    proxy: Arc<ServantProxy>,
    app: String,
    server: String,
    adapters: Vec<String>,
    interval: Duration,
}

impl Node {
    pub fn new(comm: &Communicator, config: &ServerConfig, adapters: Vec<String>) -> Result<Self> {
        if config.app.is_empty() || config.server.is_empty() || config.main_loop_interval == 0 {
            return Err(TarsError::Config(
                "node reporting requires app, server and positive mainloopticker".into(),
            ));
        }
        Ok(Self {
            proxy: comm.string_to_proxy(&config.node)?,
            app: config.app.clone(),
            server: config.server.clone(),
            adapters,
            interval: Duration::from_millis(config.main_loop_interval),
        })
    }

    async fn invoke(&self, method: &str, buffer: Buffer) -> Result<()> {
        let response = self
            .proxy
            .invoke(
                Context::new(),
                method,
                buffer.to_bytes(),
                HashMap::new(),
                HashMap::new(),
            )
            .await?;
        let ret = Reader::new(&response.s_buffer).read_int32(0, true)?;
        if ret != 0 {
            return Err(TarsError::ServerError {
                code: ret,
                message: format!("NodeF::{method} failed"),
            });
        }
        Ok(())
    }

    async fn report_version(&self) -> Result<()> {
        let mut buffer = Buffer::new();
        buffer.write_string(&self.app, 1)?;
        buffer.write_string(&self.server, 2)?;
        buffer.write_string(VERSION, 3)?;
        self.invoke("reportVersion", buffer).await
    }

    async fn keep_alive(&self, adapter: &str) -> Result<()> {
        let mut buffer = Buffer::new();
        buffer.write_struct_begin(1)?;
        buffer.write_string(&self.app, 0)?;
        buffer.write_string(&self.server, 1)?;
        buffer.write_int32(std::process::id() as i32, 2)?;
        if !adapter.is_empty() {
            buffer.write_string(adapter, 3)?;
        }
        buffer.write_struct_end()?;
        self.invoke("keepAlive", buffer).await
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let reporting = async {
            if let Err(error) = self.report_version().await {
                warn!("Node version report failed: {error}");
            }
            if let Err(error) = self.keep_alive("").await {
                warn!("Node startup heartbeat failed: {error}");
            }
            let mut timer = tokio::time::interval(self.interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                for adapter in &self.adapters {
                    if let Err(error) = self.keep_alive(adapter).await {
                        warn!("Node heartbeat for {adapter} failed: {error}");
                    }
                }
            }
        };
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = reporting => {},
        }
    }
}
