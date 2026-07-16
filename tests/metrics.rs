//! F1 — the Prometheus metrics endpoint and its counters. All offline on 127.0.0.1 (constraint C5).
#![cfg(feature = "metrics")]

use proxybroker::proxy::Proxy;
use proxybroker::server::{render_metrics, serve, serve_metrics, Pool, PoolConfig};
use proxybroker::types::Proto;
use proxybroker::{ProxyError, Resolver};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn http_only(ip: &str) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::new());
    p.add_type(Proto::Http, None);
    p
}
fn socks5(ip: &str) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 1080, BTreeSet::new());
    p.add_type(Proto::Socks5, None); // schemes() → both http AND https
    p
}
fn http_proxy_at(addr: SocketAddr) -> Proxy {
    let mut p = Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None);
    p
}
/// A bound-then-dropped port: connecting to it fails (drives a retryable relay failure).
async fn closed_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap()
}

#[tokio::test]
async fn metrics_endpoint_reports_pool_and_counters() {
    let pool = Pool::from_proxies(
        vec![http_only("1.1.1.1"), socks5("2.2.2.2")],
        PoolConfig::default(),
    );
    let handle = serve_metrics("127.0.0.1:0".parse().unwrap(), pool.clone())
        .await
        .unwrap();

    let mut s = TcpStream::connect(handle.local_addr()).await.unwrap();
    s.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf);

    assert!(
        body.contains("proxybroker_pool_size{scheme=\"http\"} 2"),
        "{body}"
    );
    // Only the SOCKS5 proxy serves https.
    assert!(
        body.contains("proxybroker_pool_size{scheme=\"https\"} 1"),
        "{body}"
    );
    assert!(body.contains("proxybroker_evictions_total 0"), "{body}");
    handle.shutdown();
}

#[tokio::test]
async fn render_metrics_is_valid_exposition() {
    let pool = Pool::from_proxies(vec![http_only("1.1.1.1")], PoolConfig::default());
    let text = render_metrics(&pool);
    for line in [
        "# TYPE proxybroker_pool_size gauge",
        "# TYPE proxybroker_pool_error_rate_avg gauge",
        "# TYPE proxybroker_pool_resp_time_avg_seconds gauge",
        "# TYPE proxybroker_evictions_total counter",
        "# TYPE proxybroker_rotations_total counter",
    ] {
        assert!(
            text.contains(line),
            "missing exposition line: {line}\n{text}"
        );
    }
    // Floats are 2-dp (matches Proxy rounding); a fresh proxy has zero error rate.
    assert!(
        text.contains("proxybroker_pool_error_rate_avg 0.00"),
        "{text}"
    );
}

#[tokio::test]
async fn evictions_counter_increments() {
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    // Unhealthy per the default config: >= min_req (5) requests and error_rate (1.0) > 0.5.
    let mut bad = Proxy::new("9.9.9.9".parse().unwrap(), 80, BTreeSet::new());
    for _ in 0..5 {
        bad.record_attempt(None, Some(ProxyError::Timeout));
    }
    pool.put_ok(bad); // put_inner sees it as unhealthy → evict, not re-pool
    assert_eq!(pool.evictions(), 1);
    assert!(pool.is_empty());
    assert!(render_metrics(&pool).contains("proxybroker_evictions_total 1"));
}

#[tokio::test]
async fn rotations_counter_increments() {
    // Two proxies, both pointing at a dead port → every relay is a retryable failure → the request
    // rotates to the next proxy each time. max_tries 2 → at least one rotation before the 502.
    let pool = Pool::from_proxies(
        vec![
            http_proxy_at(closed_addr().await),
            http_proxy_at(closed_addr().await),
        ],
        PoolConfig {
            max_tries: 2,
            ..PoolConfig::default()
        },
    );
    let pool_ref = pool.clone();
    let resolver = Arc::new(Resolver::new(Duration::from_secs(2)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(2),
        0,
        1024,
        None,
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

    assert!(pool_ref.rotations() >= 1, "expected at least one rotation");
}
