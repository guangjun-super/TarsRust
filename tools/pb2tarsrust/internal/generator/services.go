package generator

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func (g *generator) emitService(out *strings.Builder, s protoreflect.ServiceDescriptor) error {
	name := pascal(string(s.Name()))
	methods := s.Methods()
	for _, reserved := range []string{"new", "proxy"} {
		if err := g.reserve(string(s.FullName())+" methods", reserved, "proxy helper "+reserved); err != nil {
			return err
		}
	}
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		if m.IsStreamingClient() || m.IsStreamingServer() {
			return fmt.Errorf("%s: streaming RPCs are unsupported by Tars", m.FullName())
		}
		method := ident(snake(string(m.Name())))
		for _, n := range []string{method, ident(snake(string(m.Name())) + "_with_options")} {
			if err := g.reserve(string(s.FullName())+" methods", n, string(m.FullName())); err != nil {
				return err
			}
		}
	}
	doc(out, s)
	fmt.Fprintf(out, "#[::async_trait::async_trait]\npub trait %s: ::core::marker::Send + ::core::marker::Sync + 'static {\n", name)
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		doc(out, m)
		fmt.Fprintf(out, "async fn %s(&self, ctx: &::tars::Context, input: %s) -> ::tars::Result<%s>;\n", ident(snake(string(m.Name()))), g.types[m.Input().FullName()], g.types[m.Output().FullName()])
	}
	out.WriteString("}\n\n")
	fmt.Fprintf(out, `#[derive(Clone)]
pub struct %[1]sProxy { proxy: ::std::sync::Arc<::tars::ServantProxy> }
impl %[1]sProxy {
pub fn new(proxy: ::std::sync::Arc<::tars::ServantProxy>) -> Self { Self { proxy } }
pub fn proxy(&self) -> &::std::sync::Arc<::tars::ServantProxy> { &self.proxy }
`, name)
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		method := ident(snake(string(m.Name())))
		options := ident(snake(string(m.Name())) + "_with_options")
		input, output := g.types[m.Input().FullName()], g.types[m.Output().FullName()]
		doc(out, m)
		fmt.Fprintf(out, `pub async fn %[1]s(&self, ctx: ::tars::Context, input: %[2]s) -> ::tars::Result<%[3]s> {
self.%[4]s(ctx, input, ::std::collections::HashMap::new(), ::std::collections::HashMap::new()).await
}
pub async fn %[4]s(&self, ctx: ::tars::Context, input: %[2]s,
status: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>, context: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>) -> ::tars::Result<%[3]s> {
let resp = self.proxy.invoke(ctx, %[5]s, ::prost::Message::encode_to_vec(&input), status, context).await?;
<%[3]s as ::prost::Message>::decode(resp.s_buffer.as_slice()).map_err(|e| ::tars::TarsError::Codec(e.to_string()))
}
`, method, input, output, options, rustQuote(string(m.Name())))
	}
	out.WriteString("}\n\n")
	fmt.Fprintf(out, `pub struct %[1]sServant<__T: %[1]s> { inner: ::std::sync::Arc<__T> }
impl<__T: %[1]s> %[1]sServant<__T> {
pub fn new(inner: __T) -> Self { Self { inner: ::std::sync::Arc::new(inner) } }
pub fn from_arc(inner: ::std::sync::Arc<__T>) -> Self { Self { inner } }
async fn dispatch(&self, ctx: &::tars::Context, req: &::tars::RequestPacket) -> ::tars::Result<::prost::alloc::vec::Vec<u8>> {
match req.s_func_name.as_str() {
`, name)
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		fmt.Fprintf(out, `%[1]s => {
let input = <%[2]s as ::prost::Message>::decode(req.s_buffer.as_slice()).map_err(|e| ::tars::TarsError::Codec(e.to_string()))?;
let output = self.inner.%[3]s(ctx, input).await?;
::core::result::Result::Ok(::prost::Message::encode_to_vec(&output))
}
`, rustQuote(string(m.Name())), g.types[m.Input().FullName()], ident(snake(string(m.Name()))))
	}
	out.WriteString(`other => ::core::result::Result::Err(::tars::TarsError::Protocol(format!("unknown method: {}", other))),
}
}
}
`)
	fmt.Fprintf(out, `#[::async_trait::async_trait]
impl<__T: %[1]s> ::tars::transport::ServerProtocolHandler for %[1]sServant<__T> {
fn parse_package(&self, buff: &[u8]) -> (usize, ::tars::codec::PackageStatus) { ::tars::codec::parse_package(buff) }
async fn invoke(&self, ctx: &mut ::tars::Context, pkg: &[u8]) -> ::prost::alloc::vec::Vec<u8> {
let req = match ::tars::RequestPacket::decode(pkg) {
::core::result::Result::Ok(req) => req,
::core::result::Result::Err(e) => return ::tars::ResponsePacket::error(0, ::tars::consts::TARS_SERVER_DECODE_ERR, &e.to_string()).encode().unwrap_or_default(),
};
for (key, value) in &req.context { ctx.set(key.clone(), value.clone()); }
ctx.set_packet_type(req.c_packet_type);
let response = match self.dispatch(ctx, &req).await {
::core::result::Result::Ok(buffer) => ::tars::ResponsePacket::success(req.i_request_id, buffer),
::core::result::Result::Err(::tars::TarsError::ServerError { code, message }) => ::tars::ResponsePacket::error(req.i_request_id, code, &message),
::core::result::Result::Err(::tars::TarsError::Codec(message)) => ::tars::ResponsePacket::error(req.i_request_id, ::tars::consts::TARS_SERVER_DECODE_ERR, &message),
::core::result::Result::Err(e) => ::tars::ResponsePacket::error(req.i_request_id, ::tars::consts::TARS_SERVER_UNKNOWN_ERR, &e.to_string()),
};
response.encode().unwrap_or_default()
}
fn invoke_timeout(&self, pkg: &[u8]) -> ::prost::alloc::vec::Vec<u8> {
let id = ::tars::RequestPacket::decode(pkg).map(|req| req.i_request_id).unwrap_or(0);
::tars::ResponsePacket::timeout(id).encode().unwrap_or_default()
}
fn get_close_msg(&self) -> ::prost::alloc::vec::Vec<u8> { ::prost::alloc::vec::Vec::new() }
fn do_close(&self, _ctx: &::tars::Context) {}
}

`, name)
	return nil
}
