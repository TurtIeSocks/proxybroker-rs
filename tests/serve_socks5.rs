//! Offline tests for B12 — the SOCKS5 front-end. A client speaks SOCKS5 to the local server, which
//! relays through a SOCKS5 upstream. Raw byte frames, asserted exactly. No network (constraint C5).

#![cfg(feature = "server")]

use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig, ServerHandle};
use proxybroker::types::Proto;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A no-auth SOCKS5 *upstream* proxy that completes a CONNECT then echoes the tunnel bytes.
async fn mock_socks5_upstream() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                // Greeting → no-auth.
                let mut g = [0u8; 2];
                if sock.read_exact(&mut g).await.is_err() {
                    return;
                }
                let mut m = vec![0u8; g[1] as usize];
                let _ = sock.read_exact(&mut m).await;
                let _ = sock.write_all(&[0x05, 0x00]).await;
                // CONNECT request → success.
                let mut hdr = [0u8; 4];
                if sock.read_exact(&mut hdr).await.is_err() {
                    return;
                }
                let alen = match hdr[3] {
                    0x01 => 4,
                    0x04 => 16,
                    0x03 => {
                        let mut l = [0u8; 1];
                        let _ = sock.read_exact(&mut l).await;
                        l[0] as usize
                    }
                    _ => 0,
                };
                let mut rest = vec![0u8; alen + 2];
                let _ = sock.read_exact(&mut rest).await;
                let _ = sock.write_all(&[0x05, 0, 0, 0x01, 0, 0, 0, 0, 0, 0]).await;
                // Echo the tunnel.
                let mut buf = [0u8; 512];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 || sock.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    (addr, h)
}

async fn start(pool: Arc<Pool>, auth: Option<String>) -> ServerHandle {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
        auth,
    )
    .await
    .unwrap()
}

fn socks5_proxy(addr: SocketAddr) -> Proxy {
    let mut p = Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Socks5]));
    p.add_type(Proto::Socks5, None);
    p
}

#[tokio::test]
async fn socks5_frontend_relays_through_pool() {
    let (up, _u) = mock_socks5_upstream().await;
    let pool = Pool::from_proxies(vec![socks5_proxy(up)], PoolConfig::default());
    let handle = start(pool, None).await;
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();

    // Greeting: VER=5, 1 method, no-auth → expect 05 00.
    client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut sel = [0u8; 2];
    client.read_exact(&mut sel).await.unwrap();
    assert_eq!(sel, [0x05, 0x00], "method select");

    // CONNECT 1.2.3.4:443 (0x01BB) via IPv4 ATYP → expect the success frame.
    client
        .write_all(&[0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0x01, 0xBB])
        .await
        .unwrap();
    let mut ack = [0u8; 10];
    client.read_exact(&mut ack).await.unwrap();
    assert_eq!(
        ack,
        [0x05, 0, 0, 0x01, 0, 0, 0, 0, 0, 0],
        "connect success reply"
    );

    // The tunnel carries bytes end to end (via the echoing upstream).
    client.write_all(b"PING").await.unwrap();
    let mut echo = [0u8; 4];
    client.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"PING", "tunnel round-trip");
}

#[tokio::test]
async fn socks5_frontend_rejects_bind() {
    // A BIND (CMD=02) is rejected with REP=07 before any proxy is consumed.
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    let handle = start(pool, None).await;
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut sel = [0u8; 2];
    client.read_exact(&mut sel).await.unwrap();
    // CMD=02 (BIND).
    client
        .write_all(&[0x05, 0x02, 0x00, 0x01, 1, 2, 3, 4, 0x01, 0xBB])
        .await
        .unwrap();
    let mut rep = [0u8; 2];
    client.read_exact(&mut rep).await.unwrap();
    assert_eq!(rep, [0x05, 0x07], "command not supported");
}

#[tokio::test]
async fn socks5_frontend_auth_requires_userpass_method() {
    // With --auth, a greeting offering only no-auth (0x00) is rejected with 0xFF.
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    let handle = start(pool, Some("user:pass".into())).await;
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut sel = [0u8; 2];
    client.read_exact(&mut sel).await.unwrap();
    assert_eq!(sel, [0x05, 0xFF], "no acceptable method when auth required");
}

#[tokio::test]
async fn socks5_frontend_auth_accepts_valid_userpass() {
    // With --auth, the RFC 1929 exchange with correct creds is accepted (05 02 then 01 00), and
    // the CONNECT then succeeds.
    let (up, _u) = mock_socks5_upstream().await;
    let pool = Pool::from_proxies(vec![socks5_proxy(up)], PoolConfig::default());
    let handle = start(pool, Some("user:pass".into())).await;
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    // Greeting offering user/pass (0x02).
    client.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
    let mut sel = [0u8; 2];
    client.read_exact(&mut sel).await.unwrap();
    assert_eq!(sel, [0x05, 0x02], "user/pass selected");
    // RFC 1929: VER=01, ULEN=4 "user", PLEN=4 "pass".
    client
        .write_all(&[
            0x01, 0x04, b'u', b's', b'e', b'r', 0x04, b'p', b'a', b's', b's',
        ])
        .await
        .unwrap();
    let mut authr = [0u8; 2];
    client.read_exact(&mut authr).await.unwrap();
    assert_eq!(authr, [0x01, 0x00], "auth success");
    // CONNECT succeeds.
    client
        .write_all(&[0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0x01, 0xBB])
        .await
        .unwrap();
    let mut ack = [0u8; 10];
    client.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack[0..2], [0x05, 0x00], "connect success after auth");
}

#[tokio::test]
async fn http_frontend_still_works() {
    // A non-0x05 first byte ('G') takes the HTTP path unchanged.
    let (up, _u) = crate_http_upstream("HTTP-OK").await;
    let mut p = Proxy::new(up.ip(), up.port(), BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None);
    let pool = Pool::from_proxies(vec![p], PoolConfig::default());
    let handle = start(pool, None).await;
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp)).await;
    assert!(
        String::from_utf8_lossy(&resp).contains("HTTP-OK"),
        "HTTP front-end still relays"
    );
}

/// A plain HTTP upstream (for the non-SOCKS5-first-byte regression test).
async fn crate_http_upstream(body: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    (addr, h)
}
