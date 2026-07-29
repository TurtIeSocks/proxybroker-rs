//! E1 gate — the concrete consumer that unlocks the connector: a real
//! `hyper_util::client::legacy::Client` making requests through `RotatingProxyConnector` to a mock
//! upstream. All sockets on 127.0.0.1, no network (constraint C5). If this could not be written /
//! pass, E1 would stay gated — it does, so the seam is proven.
#![cfg(feature = "connector")]

use http_body_util::{BodyExt, Empty};
use hyper::Uri;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use proxybroker::broker::{Broker, FindQuery};
use proxybroker::checker::RetryPolicy;
use proxybroker::connector::{RotateConfig, RotatingProxyConnector};
use proxybroker::provider::ProviderSpec;
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::types::{Proto, TypeSpec};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A mock upstream that returns a fixed HTTP 200 to whatever it receives, bumping `hits` per
/// connection so a test can count how many proxies the connector actually dialed.
async fn mock_upstream(
    body: &'static str,
    hits: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            let body = body.to_string();
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

fn client(
    connector: RotatingProxyConnector,
) -> Client<RotatingProxyConnector, Empty<bytes::Bytes>> {
    Client::builder(TokioExecutor::new()).build(connector)
}

#[tokio::test]
async fn client_routes_through_pooled_proxy() {
    let hits = Arc::new(AtomicUsize::new(0));
    let (up, _h) = mock_upstream("hello-through-proxy", hits.clone()).await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let connector = RotatingProxyConnector::from_pool(pool, resolver, RotateConfig::default());

    let resp = client(connector)
        .get(Uri::from_static("http://1.2.3.4/"))
        .await
        .expect("request through the pooled proxy");
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"hello-through-proxy");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "dialed exactly one proxy");
}

#[tokio::test]
async fn ejects_failing_proxy_and_retries() {
    // A dead proxy (bound then immediately closed → connection refused) plus a live one. The
    // request must still succeed via the live proxy within max_tries.
    let dead_addr = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        drop(l); // close it so connects are refused
        a
    };
    let hits = Arc::new(AtomicUsize::new(0));
    let (up, _h) = mock_upstream("recovered", hits.clone()).await;
    let pool = Pool::from_proxies(
        vec![http_proxy_at(dead_addr), http_proxy_at(up)],
        PoolConfig::default(),
    );
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let cfg = RotateConfig {
        max_tries: 3,
        timeout: Duration::from_secs(3),
    };
    let connector = RotatingProxyConnector::from_pool(pool, resolver, cfg);

    let resp = client(connector)
        .get(Uri::from_static("http://1.2.3.4/"))
        .await
        .expect("request succeeds via the live proxy");
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"recovered");
}

/// A mock proxy that accepts an HTTP `CONNECT`, acks it, and then answers whatever arrives through
/// the tunnel with a fixed 200 — a raw tunnel that speaks no TLS of its own.
async fn mock_connect_upstream(
    body: &'static str,
    hits: Arc<AtomicUsize>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while sock.read_exact(&mut byte).await.is_ok() {
                    head.push(byte[0]);
                    if head.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                if !head.starts_with(b"CONNECT ") {
                    return;
                }
                let _ = sock
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await;
                // Now the tunnel is open: read the tunnelled request, answer it.
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    (addr, h)
}

#[tokio::test]
async fn https_typed_proxy_is_used_through_a_raw_connect_tunnel() {
    // An `Https`-typed proxy proved it speaks CONNECT — the checker only stamps that type after a
    // successful CONNECT followed by a TLS upgrade over it. So the connector uses the CONNECT half
    // and drops the TLS half: the proxy is neither discarded (it is a working tunnel) nor allowed
    // to pre-terminate TLS with the checker's accept-all verifier. A plaintext round trip
    // completing at all is the proof no TLS was layered underneath.
    let hits = Arc::new(AtomicUsize::new(0));
    let (up, _h) = mock_connect_upstream("tunnelled", hits.clone()).await;
    let mut proxy = Proxy::new(up.ip(), up.port(), BTreeSet::from([Proto::Https]));
    proxy.add_type(Proto::Https, None); // HTTPS only — no separately-checked CONNECT/SOCKS type
    let pool = Pool::from_proxies(vec![proxy], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let connector = RotatingProxyConnector::from_pool(pool, resolver, RotateConfig::default());

    // An IP-literal https target: the pool's scheme filter admits an `Https`-typed proxy here, and
    // no DNS is needed (constraint C5). The hyper client layers no TLS, so what crosses the tunnel
    // is plaintext — which only round-trips if the connector handed back a *plain* stream.
    let resp = client(connector)
        .get(Uri::from_static("https://1.2.3.4/"))
        .await
        .expect("an Https-typed proxy is a usable CONNECT tunnel");
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"tunnelled");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "the proxy should be dialed");
}

#[tokio::test]
async fn empty_pool_is_an_error_not_a_hang() {
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let connector = RotatingProxyConnector::from_pool(pool, resolver, RotateConfig::default());

    let err = client(connector)
        .get(Uri::from_static("http://1.2.3.4/"))
        .await
        .expect_err("no proxy → error, not a hang");
    let _ = err; // hyper-util wraps it; the point is it resolves to an Err rather than blocking
}

/// A server that returns a fixed HTTP 200 body to every request (used as an external-IP stub and
/// as an empty provider page). Distinct from `mock_upstream` — it does not count hits.
async fn serve_fixed(body: &'static str) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 2048];
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

/// E1 sugar: `Broker::rotating(query, cfg)` composes `find` -> `Pool::spawn` -> `from_pool` in one
/// call. Driven end to end offline: a stubbed external-IP endpoint lets `find` build its checker,
/// an empty provider yields no proxies, and the produced connector is live — it reports the empty
/// pool (not a hang), proving the whole pipeline wired. Routing behaviour is covered by the
/// `from_pool` tests above.
#[tokio::test]
async fn broker_rotating_composes_find_into_a_live_connector() {
    let (ext_ip, _e) = serve_fixed("203.0.113.9").await; // external-IP discovery stub
    let (prov, _p) = serve_fixed("").await; // provider page listing zero proxies
    let resolver = Resolver::new(Duration::from_secs(3))
        .unwrap()
        .with_ip_endpoints(vec![format!("http://{ext_ip}/")]);
    let broker = Broker::builder()
        .providers(vec![ProviderSpec::new(
            &format!("http://{prov}/"),
            &[Proto::Http],
        )])
        .resolver(resolver)
        .build();
    let query = FindQuery {
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        max_conn: 4,
        retry: RetryPolicy::tries(1),
        ..Default::default()
    };

    let connector = broker
        .rotating(query, RotateConfig::default())
        .await
        .expect("rotating() composes find -> pool -> connector");

    // No proxies discovered → the connector is live and reports the empty pool rather than hanging.
    let err = client(connector)
        .get(Uri::from_static("http://1.2.3.4/"))
        .await
        .expect_err("empty pool should error, not hang");
    assert!(
        format!("{err:?}").to_lowercase().contains("proxy"),
        "expected a no-proxy error, got: {err:?}"
    );
}
