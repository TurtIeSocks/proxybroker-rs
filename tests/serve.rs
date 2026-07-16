//! Offline test of the local proxy server: a client sends an HTTP request to the server, the
//! server relays it through a mock upstream proxy, and the client gets the response back
//! (constraint C5). Also checks the 502 path when the pool is empty.

#![cfg(feature = "server")]

use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, ClientKey, Pool, PoolConfig, Strategy};
use proxybroker::types::{Proto, Scheme};
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A throwaway client key for non-sticky `get` calls (the strategy ignores it).
fn any_key() -> ClientKey {
    ClientKey::Ip("0.0.0.0".parse::<IpAddr>().unwrap())
}

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

/// A pooled proxy located in `cc`, HTTP-capable so it is scheme-eligible. No listener needed —
/// these tests exercise pool admission, not relaying.
fn proxy_in(ip: &str, cc: &str) -> Proxy {
    let mut p = Proxy::new(ip.parse().unwrap(), 8080, BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None);
    p.geo = Some(proxybroker::Country {
        code: cc.into(),
        name: String::new(),
    });
    p
}

#[tokio::test]
async fn pool_admits_only_allowed_countries() {
    // The admission filter screens a warm/BYO pool (from_proxies) that never went through find's
    // country filter — the whole point of B4's pool-level predicate.
    let pool = Pool::from_proxies(
        vec![proxy_in("1.1.1.1", "US"), proxy_in("2.2.2.2", "FR")],
        PoolConfig {
            countries: Some(BTreeSet::from(["US".to_string()])),
            ..PoolConfig::default()
        },
    );
    let first = pool.get(Scheme::Http, &any_key()).await;
    assert_eq!(
        first.and_then(|p| p.geo.map(|g| g.code)),
        Some("US".to_string())
    );
    assert!(
        pool.get(Scheme::Http, &any_key()).await.is_none(),
        "the FR proxy must be rejected on admission"
    );
}

#[tokio::test]
async fn pool_no_filter_admits_all() {
    // countries: None is the no-op path — both proxies are admitted.
    let pool = Pool::from_proxies(
        vec![proxy_in("1.1.1.1", "US"), proxy_in("2.2.2.2", "FR")],
        PoolConfig::default(),
    );
    assert!(pool.get(Scheme::Http, &any_key()).await.is_some());
    assert!(pool.get(Scheme::Http, &any_key()).await.is_some());
    assert!(pool.get(Scheme::Http, &any_key()).await.is_none());
}

#[tokio::test]
async fn pool_country_match_is_case_insensitive() {
    // Proxy geo code lowercase, filter uppercase — country_ok uppercases both sides.
    let pool = Pool::from_proxies(
        vec![proxy_in("1.1.1.1", "us")],
        PoolConfig {
            countries: Some(BTreeSet::from(["US".to_string()])),
            ..PoolConfig::default()
        },
    );
    assert!(pool.get(Scheme::Http, &any_key()).await.is_some());
}

#[tokio::test]
async fn sticky_returns_same_proxy_for_same_client() {
    // Pool-level proof of the session map: the same client key, across a get/put/get cycle, is
    // pinned to the same upstream address.
    let pool = Pool::from_proxies(
        vec![proxy_in("1.1.1.1", "US"), proxy_in("2.2.2.2", "US")],
        PoolConfig {
            strategy: Strategy::Sticky,
            ..PoolConfig::default()
        },
    );
    let a = ClientKey::Ip("10.0.0.1".parse::<IpAddr>().unwrap());
    let first = pool.get(Scheme::Http, &a).await.unwrap();
    let pinned = first.addr();
    pool.put(first); // healthy return — back into the pool
    let second = pool.get(Scheme::Http, &a).await.unwrap();
    assert_eq!(
        second.addr(),
        pinned,
        "same client must re-get the pinned proxy"
    );
}

#[tokio::test]
async fn sticky_pins_client_across_connections() {
    // End-to-end: two sequential connections from the same client IP (127.0.0.1), Sticky keyed on
    // IP, must relay through the SAME upstream — exercising peer capture + client_key + keyed get.
    let (u1, _a) = mock_upstream("UP-ONE").await;
    let (u2, _b) = mock_upstream("UP-TWO").await;
    let pool = Pool::from_proxies(
        vec![http_proxy_at(u1), http_proxy_at(u2)],
        PoolConfig {
            strategy: Strategy::Sticky,
            ..PoolConfig::default()
        },
    );
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
    )
    .await
    .unwrap();

    async fn one_request(addr: std::net::SocketAddr) -> String {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), c.read_to_end(&mut resp))
            .await
            .expect("read should not time out")
            .unwrap();
        String::from_utf8_lossy(&resp).into_owned()
    }

    let first = one_request(handle.local_addr()).await;
    let second = one_request(handle.local_addr()).await;
    let body = |r: &str| {
        if r.contains("UP-ONE") {
            "UP-ONE"
        } else {
            "UP-TWO"
        }
    };
    assert_eq!(
        body(&first),
        body(&second),
        "same client IP must stick to one upstream ({first:?} vs {second:?})"
    );
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
