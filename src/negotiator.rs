//! Per-protocol negotiation: turning a raw TCP connection to a proxy into a stream tunnelled
//! to a target (a judge), ready for an HTTP request.
//!
//! The set of protocols is closed (`NGTRS` is a fixed dict of six in `negotiators.py`), so
//! this dispatches on [`Proto`] rather than using trait objects — users extend *providers*,
//! never negotiators.
//!
//! - **HTTP** is a no-op: the request goes to the proxy with an absolute-form URI.
//! - **SOCKS4/5** go through `tokio-socks`, which takes our [`TcpStream`], performs the
//!   handshake, and hands the tunnelled stream back — composing exactly the way the Python
//!   negotiators compose, but with the byte-level protocol already tested upstream.
//! - **CONNECT:80/25** send a hand-rolled `CONNECT` and check the status (25 also reads the
//!   SMTP banner).
//! - **HTTPS** does `CONNECT` then upgrades the *same* connection to TLS in place — which is
//!   why the transport is an [`enum Stream`](Stream) swapped by value, not a `&mut`: a Rust
//!   TLS connector consumes the stream and returns a differently-typed one.

use crate::error::ProxyError;
use crate::types::Proto;
use crate::utils::get_status_code;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// SMTP "service ready" reply code — the banner a `CONNECT:25` tunnel must see.
const SMTP_READY: u16 = 220;

/// The target a negotiator tunnels to (a judge). `ip` is preferred where present (SOCKS4
/// requires it); `host` is the name used for TLS SNI and the `CONNECT` authority.
#[derive(Debug, Clone)]
pub struct Target {
    pub host: String,
    pub ip: Option<IpAddr>,
    pub port: u16,
}

/// A transport to a proxied target: either the plain TCP connection or that same connection
/// upgraded to TLS. Both variants are `Unpin`, so the `AsyncRead`/`AsyncWrite` impls are a
/// plain `match` — no pin-projection. The TLS variant is boxed because `TlsStream` is much
/// larger than `TcpStream` and every plain connection would otherwise pay that size.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stream::Plain(_) => f.write_str("Stream::Plain"),
            Stream::Tls(_) => f.write_str("Stream::Tls"),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Negotiate `proto` over a freshly-connected `tcp` to reach `target`, returning a stream
/// ready for an HTTP request. `deadline` bounds each network step.
pub async fn negotiate(
    proto: Proto,
    tcp: TcpStream,
    target: &Target,
    deadline: Duration,
) -> Result<Stream, ProxyError> {
    match proto {
        // HTTP: nothing to negotiate. The checker sends an absolute-URI request to the proxy.
        Proto::Http => Ok(Stream::Plain(tcp)),
        Proto::Socks4 => socks4(tcp, target, deadline).await,
        Proto::Socks5 => socks5(tcp, target, deadline).await,
        Proto::Connect80 | Proto::Connect25 => {
            let mut tcp = http_connect(tcp, target, deadline).await?;
            if proto == Proto::Connect25 {
                read_smtp_banner(&mut tcp, deadline).await?;
            }
            Ok(Stream::Plain(tcp))
        }
        Proto::Https => {
            let tcp = http_connect(tcp, target, deadline).await?;
            tls_upgrade(tcp, &target.host, deadline).await
        }
    }
}

async fn socks4(tcp: TcpStream, target: &Target, deadline: Duration) -> Result<Stream, ProxyError> {
    // SOCKS4 has no domain support; it needs an IPv4 destination.
    let ip = match target.ip {
        Some(IpAddr::V4(v4)) => v4,
        _ => return Err(ProxyError::BadResponse),
    };
    let fut = tokio_socks::tcp::Socks4Stream::connect_with_socket(tcp, (ip, target.port));
    let s = tokio::time::timeout(deadline, fut)
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(map_socks_err)?;
    Ok(Stream::Plain(s.into_inner()))
}

async fn socks5(tcp: TcpStream, target: &Target, deadline: Duration) -> Result<Stream, ProxyError> {
    // SOCKS5 supports IPv4/IPv6/domain; prefer the resolved IP, fall back to the name.
    let fut = async {
        match target.ip {
            Some(ip) => {
                tokio_socks::tcp::Socks5Stream::connect_with_socket(tcp, (ip, target.port)).await
            }
            None => {
                tokio_socks::tcp::Socks5Stream::connect_with_socket(
                    tcp,
                    (target.host.as_str(), target.port),
                )
                .await
            }
        }
    };
    let s = tokio::time::timeout(deadline, fut)
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(map_socks_err)?;
    Ok(Stream::Plain(s.into_inner()))
}

/// Send a `CONNECT` and require a 200. Returns the tunnelled TCP stream.
async fn http_connect(
    mut tcp: TcpStream,
    target: &Target,
    deadline: Duration,
) -> Result<TcpStream, ProxyError> {
    let req = connect_request(&target.host, target.port);
    tokio::time::timeout(deadline, tcp.write_all(&req))
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(|_| ProxyError::Reset)?;
    let head = read_head(&mut tcp, deadline).await?;
    // Status is in "HTTP/1.1 200 ...": bytes 9..12. get_status_code returns 400 on garbage.
    if get_status_code(&head, 9, 12) != 200 {
        return Err(ProxyError::BadStatus);
    }
    Ok(tcp)
}

/// Read up to the end of the HTTP head (`\r\n\r\n`). A hand-rolled byte loop rather than a
/// `BufReader`: buffering would swallow bytes past the header that belong to the tunnel.
async fn read_head(tcp: &mut TcpStream, deadline: Duration) -> Result<Vec<u8>, ProxyError> {
    let mut buf = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    let read = async {
        loop {
            let n = tcp.read(&mut byte).await.map_err(|_| ProxyError::Reset)?;
            if n == 0 {
                return if buf.is_empty() {
                    Err(ProxyError::EmptyRecv)
                } else {
                    Err(ProxyError::Reset)
                };
            }
            buf.push(byte[0]);
            if buf.ends_with(b"\r\n\r\n") {
                return Ok(buf);
            }
            if buf.len() > 8192 {
                return Err(ProxyError::BadResponse); // runaway head
            }
        }
    };
    tokio::time::timeout(deadline, read)
        .await
        .map_err(|_| ProxyError::Timeout)?
}

/// After a `CONNECT:25` tunnel, the SMTP server sends a `220` banner. `negotiators.py` reads
/// three bytes and checks for `220`.
async fn read_smtp_banner(tcp: &mut TcpStream, deadline: Duration) -> Result<(), ProxyError> {
    let mut buf = [0u8; 3];
    tokio::time::timeout(deadline, tcp.read_exact(&mut buf))
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(|_| ProxyError::Reset)?;
    if get_status_code(&buf, 0, 3) != SMTP_READY {
        return Err(ProxyError::BadStatus);
    }
    Ok(())
}

async fn tls_upgrade(tcp: TcpStream, host: &str, deadline: Duration) -> Result<Stream, ProxyError> {
    let connector = tokio_rustls::TlsConnector::from(tls_config());
    // Proxies connect to whatever server they front — often expired/self-signed/mismatched
    // certs — so the verifier accepts everything. Named and isolated, as in proxy.py's
    // `_make_unverified_ssl_context_for_proxy_testing`.
    let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|_| ProxyError::BadResponse)?;
    let fut = connector.connect(server_name, tcp);
    let tls = tokio::time::timeout(deadline, fut)
        .await
        .map_err(|_| ProxyError::Timeout)?
        .map_err(|_| ProxyError::BadResponse)?;
    Ok(Stream::Tls(Box::new(tls)))
}

fn tls_config() -> Arc<rustls::ClientConfig> {
    let cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAllVerifier))
        .with_no_client_auth();
    Arc::new(cfg)
}

/// The bytes of a `CONNECT host:port HTTP/1.1` request, with IPv6 authority/Host bracketing
/// per RFC 3986. Mirrors `negotiators.py:_CONNECT_request`, including not double-bracketing a
/// caller-supplied `[v6]`. Pinned by the byte-level tests below.
pub fn connect_request(host: &str, port: u16) -> Vec<u8> {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let (authority, host_hdr) = if bare.contains(':') {
        (format!("[{bare}]:{port}"), format!("[{bare}]"))
    } else {
        (format!("{bare}:{port}"), bare.to_owned())
    };
    format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {host_hdr}\r\n\
         User-Agent: PxBroker/{}/\r\nConnection: keep-alive\r\n\r\n",
        env!("CARGO_PKG_VERSION"),
    )
    .into_bytes()
}

/// Map a `tokio-socks` error to the per-proxy error bucket. The library's rich reply-code
/// enum collapses to the Python-compatible buckets: a timeout stays a timeout, everything
/// else is a bad response (Python raised `BadResponseError` for all SOCKS failures).
fn map_socks_err(e: tokio_socks::Error) -> ProxyError {
    match e {
        tokio_socks::Error::Io(io) if io.kind() == std::io::ErrorKind::TimedOut => {
            ProxyError::Timeout
        }
        _ => ProxyError::BadResponse,
    }
}

/// A `rustls` verifier that accepts any certificate. Isolated and named so the unsafe choice
/// is auditable in one place. Correct for proxy testing: the endpoint is whatever server the
/// proxy fronts, and its certificate is not something we can or should validate.
#[derive(Debug)]
struct AcceptAllVerifier;

impl rustls::client::danger::ServerCertVerifier for AcceptAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_request_ipv4_unbracketed() {
        let req = String::from_utf8(connect_request("198.51.100.1", 443)).unwrap();
        assert!(
            req.starts_with("CONNECT 198.51.100.1:443 HTTP/1.1\r\n"),
            "{req}"
        );
        assert!(req.contains("\r\nHost: 198.51.100.1\r\n"), "{req}");
    }

    #[test]
    fn connect_request_ipv6_brackets_authority_and_host() {
        let req = String::from_utf8(connect_request("2001:db8::1", 443)).unwrap();
        assert!(
            req.starts_with("CONNECT [2001:db8::1]:443 HTTP/1.1\r\n"),
            "{req}"
        );
        assert!(req.contains("\r\nHost: [2001:db8::1]\r\n"), "{req}");
    }

    #[test]
    fn connect_request_does_not_double_bracket() {
        let req = String::from_utf8(connect_request("[2001:db8::1]", 443)).unwrap();
        assert!(
            req.contains("CONNECT [2001:db8::1]:443 HTTP/1.1\r\n"),
            "{req}"
        );
        assert!(!req.contains("[["), "{req}");
        assert!(!req.contains("]]"), "{req}");
        assert!(req.contains("\r\nHost: [2001:db8::1]\r\n"), "{req}");
    }

    #[test]
    fn connect_request_ports_25_80_443() {
        for port in [25u16, 80, 443] {
            let req = String::from_utf8(connect_request("fe80::abcd", port)).unwrap();
            assert!(
                req.contains(&format!("CONNECT [fe80::abcd]:{port} HTTP/1.1\r\n")),
                "{req}"
            );
        }
    }
}
