use tars_core::{
    codec::{parse_package, Buffer, PackageStatus, Reader},
    consts, Context, Result, TarsError,
};
use tars_protocol::protocol::{RequestPacket, ResponsePacket};
use tars_transport::transport::ServerProtocolHandler;
use tokio::sync::watch;

pub(super) const VERSION: &str = concat!("TarsRust-", env!("CARGO_PKG_VERSION"));

pub(super) struct Admin {
    pub shutdown: watch::Sender<bool>,
}

impl Admin {
    fn dispatch(&self, request: &RequestPacket) -> Result<Vec<u8>> {
        match request.s_func_name.as_str() {
            "shutdown" => {
                self.shutdown.send_replace(true);
                Ok(vec![])
            }
            "notify" => {
                let command = Reader::new(&request.s_buffer).read_string(1, true)?;
                let reply = match command.trim() {
                    "tars.viewversion" => VERSION.to_string(),
                    _ => format!("{command} not support now!"),
                };
                let mut output = Buffer::new();
                output.write_string(&reply, 0)?;
                Ok(output.to_bytes())
            }
            "tars_ping" => Ok(vec![]),
            _ => Err(TarsError::ServerError {
                code: consts::TARS_SERVER_UNKNOWN_ERR,
                message: format!("unknown admin method: {}", request.s_func_name),
            }),
        }
    }
}

#[async_trait::async_trait]
impl ServerProtocolHandler for Admin {
    fn parse_package(&self, bytes: &[u8]) -> (usize, PackageStatus) {
        parse_package(bytes)
    }

    async fn invoke(&self, _: &mut Context, bytes: &[u8]) -> Vec<u8> {
        let request = match RequestPacket::decode(bytes) {
            Ok(request) => request,
            Err(error) => {
                return ResponsePacket::error(0, consts::TARS_SERVER_DECODE_ERR, &error.to_string())
                    .encode()
                    .unwrap_or_default()
            }
        };
        let mut response = match self.dispatch(&request) {
            Ok(output) => ResponsePacket::success(request.i_request_id, output),
            Err(TarsError::ServerError { code, message }) => {
                ResponsePacket::error(request.i_request_id, code, &message)
            }
            Err(error) => ResponsePacket::error(
                request.i_request_id,
                consts::TARS_SERVER_DECODE_ERR,
                &error.to_string(),
            ),
        };
        response.i_version = request.i_version;
        if request.is_oneway() {
            vec![]
        } else {
            response.encode().unwrap_or_default()
        }
    }

    fn invoke_timeout(&self, bytes: &[u8]) -> Vec<u8> {
        let id = RequestPacket::decode(bytes)
            .map(|request| request.i_request_id)
            .unwrap_or(0);
        ResponsePacket::timeout(id).encode().unwrap_or_default()
    }
    fn get_close_msg(&self) -> Vec<u8> {
        vec![]
    }
    fn do_close(&self, _: &Context) {}
}
