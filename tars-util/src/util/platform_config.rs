//! The domain/key configuration format written by tarsnode (not XML).

use super::{AdapterConfig, ClientConfig, ServerConfig};
use std::{collections::HashMap, path::Path, str::FromStr};
use tars_core::{Result, TarsError};

/// Runtime configuration loaded from a platform-generated `.conf` file.
#[derive(Debug, Clone)]
pub struct TarsConfig {
    pub server: ServerConfig,
    pub client: ClientConfig,
}

type Domain = HashMap<String, String>;

impl TarsConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    pub fn parse(input: &str) -> Result<Self> {
        let mut domains: HashMap<String, Domain> = HashMap::new();
        let mut stack: Vec<&str> = Vec::new();
        for (line_no, line) in input.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let invalid = || TarsError::Config(format!("invalid domain at line {}", line_no + 1));
            if line.starts_with('<') {
                let tag = line
                    .strip_prefix('<')
                    .and_then(|s| s.strip_suffix('>'))
                    .ok_or_else(invalid)?;
                if let Some(close) = tag.strip_prefix('/') {
                    if stack.pop() != Some(close) {
                        return Err(invalid());
                    }
                } else {
                    if tag.is_empty() || tag.contains(['<', '>', '/', ' ']) {
                        return Err(invalid());
                    }
                    stack.push(tag);
                    if domains.insert(stack.join("/"), Domain::new()).is_some() {
                        return Err(TarsError::Config(format!(
                            "duplicate domain at line {}",
                            line_no + 1
                        )));
                    }
                }
            } else {
                let domain = domains.get_mut(&stack.join("/")).ok_or_else(invalid)?;
                let (key, value) = line.split_once('=').unwrap_or((line, ""));
                domain.insert(key.trim().to_string(), value.trim().to_string());
            }
        }
        if !stack.is_empty() {
            return Err(TarsError::Config("unclosed configuration domain".into()));
        }
        let server_path = "tars/application/server";
        let values = domains
            .get(server_path)
            .ok_or_else(|| TarsError::Config("missing tars/application/server".into()))?;
        let mut server = ServerConfig::default();
        macro_rules! strings {
            ($target:ident, $values:expr, $( $field:ident => $key:literal ),* $(,)?) => {
                $(if let Some(value) = $values.get($key) { $target.$field = value.clone(); })*
            };
        }
        macro_rules! numbers {
            ($target:ident, $values:expr, $( $field:ident => $key:literal ),* $(,)?) => {
                $(if let Some(value) = number($values, $key)? { $target.$field = value; })*
            };
        }
        strings!(server, values,
            app => "app", server => "server", local => "local", node => "node",
            base_path => "basepath", data_path => "datapath", log_path => "logpath",
            log_level => "logLevel", log => "log", config => "config", notify => "notify",
        );
        if server.app.is_empty() || server.server.is_empty() {
            return Err(TarsError::Config(
                "app and server must be configured".into(),
            ));
        }
        numbers!(server, values,
            main_loop_interval => "mainloopticker", accept_timeout => "accepttimeout",
            read_timeout => "readtimeout", write_timeout => "writetimeout",
            handle_timeout => "handletimeout", idle_timeout => "idletimeout",
            max_invoke => "maxroutine", queue_cap => "queuecap",
            tcp_read_buffer => "tcpreadbuffer", tcp_write_buffer => "tcpwritebuffer",
        );
        if server.main_loop_interval == 0 {
            return Err(TarsError::Config("mainloopticker must be positive".into()));
        }
        if let Some(value) = values.get("tcpnodelay") {
            server.tcp_no_delay = value == "1" || value.eq_ignore_ascii_case("true");
        }
        if let Some(application) = domains.get("tars/application") {
            server.enable_set = application
                .get("enableset")
                .is_some_and(|v| v.eq_ignore_ascii_case("Y"));
            strings!(server, application, set_division => "setdivision");
        }
        for (path, values) in &domains {
            if let Some(name) = path.strip_prefix("tars/application/server/") {
                if name.contains('/') {
                    continue;
                }
                let mut adapter = AdapterConfig::default();
                strings!(adapter, values, servant => "servant", endpoint => "endpoint", protocol => "protocol");
                numbers!(adapter, values,
                    max_conn => "maxconns", threads => "threads",
                    queue_cap => "queuecap", queue_timeout => "queuetimeout",
                );
                if adapter.servant.is_empty() || adapter.endpoint.is_empty() {
                    return Err(TarsError::Config(format!(
                        "adapter {name} requires servant and endpoint"
                    )));
                }
                server.adapters.insert(name.to_string(), adapter);
            }
        }
        let mut client = ClientConfig::default();
        if let Some(values) = domains.get("tars/application/client") {
            strings!(client, values, locator => "locator", stat => "stat", property => "property");
            client.endpoint_cache_path = values.get("endpoint-cache-path").cloned();
            numbers!(client, values,
                async_invoke_timeout => "async-invoke-timeout",
                refresh_endpoint_interval => "refresh-endpoint-interval",
                check_status_interval => "check-status-interval",
                report_interval => "report-interval", sample_rate => "sample-rate",
                max_sample_count => "max-sample-count", dial_timeout => "clientdialtimeout",
                idle_timeout => "clientidletimeout", read_timeout => "clientreadtimeout",
                write_timeout => "clientwritetimeout", queue_len => "clientqueuelen",
                obj_queue_max => "objqueuemax", keep_alive_interval => "keep-alive-interval",
            );
        }
        Ok(Self { server, client })
    }
}

fn number<T: FromStr>(values: &Domain, key: &str) -> Result<Option<T>> {
    values
        .get(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_| TarsError::Config(format!("invalid {key}: {value}")))
        })
        .transpose()
}
