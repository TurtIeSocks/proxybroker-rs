//! Offline test of the local proxy server: a client sends an HTTP request to the server, the
//! server relays it through a mock upstream proxy, and the client gets the response back
//! (constraint C5). Also checks the 502 path when the pool is empty.

#![cfg(feature = "server")]

use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::types::Proto;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A mock upstream proxy that returns a fixed HTTP response to whatever it receives.
async fn mock_upstream(body: &'static str) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
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

fn http_proxy_at(addr: std::net::SocketAddr) -> Proxy {
    let mut p = Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None);
    p
}

#[tokio::test]
async fn server_relays_http_request_through_a_pool_proxy() {
    let (upstream, _u) = mock_upstream("RELAYED-BODY").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(upstream)], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());

    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
    )
    .await
    .unwrap();

    // Act as a client of the local proxy: send an absolute-URI GET.
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();

    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp))
        .await
        .expect("read should not time out")
        .unwrap();
    let text = String::from_utf8_lossy(&resp);
    assert!(text.contains("RELAYED-BODY"), "got: {text}");
}

#[tokio::test]
async fn serve_loads_a_saved_pool() {
    // A pool loaded from NDJSON (the C2 --load path) serves without any re-checking: persist one
    // working proxy to bytes, reload via read_ndjson, fill the pool with Pool::from_proxies, and
    // prove the relay works. Fully in-memory — no temp files, no network (constraint C5).
    let (upstream, _u) = mock_upstream("LOADED-BODY").await;

    let mut buf = Vec::new();
    proxybroker::write_ndjson(&mut buf, &[http_proxy_at(upstream)]).unwrap();
    let loaded = proxybroker::read_ndjson(std::io::Cursor::new(buf)).unwrap();

    let pool = Pool::from_proxies(loaded, PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
    )
    .await
    .unwrap();

    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();

    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp))
        .await
        .expect("read should not time out")
        .unwrap();
    assert!(
        String::from_utf8_lossy(&resp).contains("LOADED-BODY"),
        "a proxy loaded from NDJSON must serve without re-checking"
    );
}

#[tokio::test]
async fn server_returns_502_when_pool_is_empty() {
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(2)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();

    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp))
        .await
        .expect("read should not time out")
        .unwrap();
    assert!(String::from_utf8_lossy(&resp).contains("502"));
}
