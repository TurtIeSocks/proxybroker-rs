//! Offline tests for B6 — the `proxycontrol` control API (history + remove). A `Host: proxycontrol`
//! request is handled locally, never consuming a pool proxy. No network (constraint C5).

#![cfg(feature = "server")]

use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig, ServerHandle};
use proxybroker::types::Proto;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

async fn start(pool: Arc<Pool>) -> ServerHandle {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
        None,
    )
    .await
    .unwrap()
}

/// Send one request line + blank line to `addr` and return the full response text.
async fn request(addr: std::net::SocketAddr, req: &str) -> String {
    let mut c = TcpStream::connect(addr).await.unwrap();
    c.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), c.read_to_end(&mut resp)).await;
    String::from_utf8_lossy(&resp).into_owned()
}

#[tokio::test]
async fn control_history_reports_serving_upstream() {
    let (up, _u) = mock_upstream("BODY").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let handle = start(pool).await;
    let addr = handle.local_addr();

    // Relay a request (recorded in history), then query the control API from the same client IP.
    let relayed = request(
        addr,
        "GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n",
    )
    .await;
    assert!(
        relayed.contains("BODY"),
        "relay should succeed: {relayed:?}"
    );

    let ctrl = request(
        addr,
        "GET http://proxycontrol/api/history/url:http://1.2.3.4/ HTTP/1.1\r\nHost: proxycontrol\r\n\r\n",
    )
    .await;
    assert!(ctrl.contains("200 OK"), "history hit → 200: {ctrl:?}");
    assert!(ctrl.contains("\"proxy\""), "JSON body: {ctrl:?}");
    assert!(
        ctrl.contains(&format!("{}:{}", up.ip(), up.port())),
        "reports the serving upstream: {ctrl:?}"
    );
    assert!(
        ctrl.contains("Access-Control-Allow-Origin: *"),
        "CORS header: {ctrl:?}"
    );
}

#[tokio::test]
async fn control_history_miss_returns_204() {
    let (up, _u) = mock_upstream("BODY").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let handle = start(pool).await;
    let ctrl = request(
        handle.local_addr(),
        "GET http://proxycontrol/api/history/url:http://never-served/ HTTP/1.1\r\nHost: proxycontrol\r\n\r\n",
    )
    .await;
    assert!(ctrl.contains("204 No Content"), "miss → 204: {ctrl:?}");
}

#[tokio::test]
async fn control_remove_returns_204_and_evicts() {
    let (up, _u) = mock_upstream("BODY").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let handle = start(pool.clone()).await;
    let ctrl = request(
        handle.local_addr(),
        &format!(
            "GET http://proxycontrol/api/remove/{}:{} HTTP/1.1\r\nHost: proxycontrol\r\n\r\n",
            up.ip(),
            up.port()
        ),
    )
    .await;
    assert!(ctrl.contains("204 No Content"), "remove → 204: {ctrl:?}");
    assert_eq!(pool.len(), 0, "the proxy is evicted from the pool");
}
