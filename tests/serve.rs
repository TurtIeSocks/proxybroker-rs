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

/// A mock upstream returning a chosen HTTP `status` line (e.g. "403 Forbidden") with `body`.
async fn mock_status(
    status: &'static str,
    body: &'static str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
        0,
        1024,
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

/// An addr whose listener has been dropped — connecting to it fails (connection refused).
async fn closed_addr() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap() // `l` dropped at return → port closed
}

#[tokio::test]
async fn retries_next_proxy_when_first_connect_fails() {
    // A pre-commit failure (connect refused) is transparently retried on the next proxy; the
    // client sees the second proxy's body, never an error.
    let dead = closed_addr().await;
    let (good, _g) = mock_upstream("RETRIED-OK").await;
    let pool = Pool::from_proxies(
        vec![http_proxy_at(dead), http_proxy_at(good)],
        PoolConfig::default(),
    );
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
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
        String::from_utf8_lossy(&resp).contains("RETRIED-OK"),
        "a connect failure must retry the next proxy transparently"
    );
}

#[tokio::test]
async fn emits_502_after_max_tries_all_fail() {
    // Every attempt is a pre-commit failure and none commits → exactly one 502.
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
    let resolver = Arc::new(Resolver::new(Duration::from_secs(2)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(2),
        0,
        1024,
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

async fn serve_get(pool: Arc<Pool>) -> String {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
    )
    .await
    .unwrap();
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp)).await;
    String::from_utf8_lossy(&resp).into_owned()
}

#[tokio::test]
async fn retries_when_upstream_status_not_allowed() {
    // A 403 block page is not forwarded — the request retries the next proxy, which returns 200.
    let (blocked, _a) = mock_status("403 Forbidden", "BLOCKED").await;
    let (ok, _b) = mock_status("200 OK", "GOOD").await;
    let pool = Pool::from_proxies(
        vec![http_proxy_at(blocked), http_proxy_at(ok)],
        PoolConfig {
            http_allowed_codes: Some(vec![200]),
            ..PoolConfig::default()
        },
    );
    let body = serve_get(pool).await;
    assert!(body.contains("GOOD"), "should retry past the 403: {body:?}");
    assert!(
        !body.contains("BLOCKED"),
        "the 403 body must not reach the client"
    );
}

#[tokio::test]
async fn allowed_status_is_forwarded_verbatim() {
    // An allowed non-200 status is forwarded whole, including the peeked status line (no byte loss).
    let (up, _u) = mock_status("301 Moved Permanently", "REDIR-BODY").await;
    let pool = Pool::from_proxies(
        vec![http_proxy_at(up)],
        PoolConfig {
            http_allowed_codes: Some(vec![301]),
            ..PoolConfig::default()
        },
    );
    let body = serve_get(pool).await;
    assert!(
        body.contains("301 Moved Permanently"),
        "peeked status line lost: {body:?}"
    );
    assert!(body.contains("REDIR-BODY"), "body lost: {body:?}");
}

#[tokio::test]
async fn none_allowed_codes_accepts_any() {
    // With no allow-list the splice is blind (today's behaviour): a 500 is forwarded unchanged.
    let (up, _u) = mock_status("500 Internal Server Error", "ERRBODY").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let body = serve_get(pool).await;
    assert!(
        body.contains("500"),
        "500 should be forwarded when no allow-list: {body:?}"
    );
    assert!(body.contains("ERRBODY"));
}

#[tokio::test]
async fn wait_ready_returns_on_exhaustion() {
    // A too-small source must not hang startup: from_proxies is exhausted immediately, so
    // wait_ready(n) returns even though n is never met.
    let pool = Pool::from_proxies(vec![proxy_in("1.1.1.1", "US")], PoolConfig::default());
    tokio::time::timeout(Duration::from_secs(1), pool.wait_ready(5))
        .await
        .expect("wait_ready must return on exhaustion, not hang");
}

#[tokio::test]
async fn serve_waits_for_min_queue() {
    // The server does not relay until the pool holds min_queue proxies. Fed by a test-driven
    // channel (via the now-generic Pool::spawn) so we control arrivals.
    let (up, _u) = mock_upstream("READY").await;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Proxy>(4);
    let stream = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx));
    let pool = Pool::spawn(stream, PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        2, // min_queue
        1024,
    )
    .await
    .unwrap();

    // One proxy — below min_queue(2): the server must not accept/relay yet.
    tx.send(http_proxy_at(up)).await.unwrap();
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let pending = tokio::time::timeout(Duration::from_millis(300), client.read(&mut buf)).await;
    assert!(pending.is_err(), "must not relay before min_queue is met");

    // Second proxy meets min_queue → the server starts accepting and relays.
    tx.send(http_proxy_at(up)).await.unwrap();
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), client.read_to_end(&mut resp)).await;
    assert!(
        String::from_utf8_lossy(&resp).contains("READY"),
        "server relays once min_queue is met"
    );
}

#[tokio::test]
async fn backlog_sets_listen_queue() {
    // A non-default backlog exercises the TcpSocket bind path; assert it still yields a working
    // listener (backlog depth itself is not portably observable).
    let (up, _u) = mock_upstream("OK-BACKLOG").await;
    let pool = Pool::from_proxies(vec![http_proxy_at(up)], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        128, // non-default backlog
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
    assert!(String::from_utf8_lossy(&resp).contains("OK-BACKLOG"));
}

#[tokio::test(start_paused = true)]
async fn failed_proxy_is_benched_then_re_probed() {
    // A proxy that fails is benched for fail_timeout and skipped while a healthy one exists;
    // after the window elapses it re-enters selection (the re-probe). tokio's paused clock drives
    // the timer — no real sleeps.
    let pool = Pool::from_proxies(
        vec![proxy_in("1.1.1.1", "US"), proxy_in("2.2.2.2", "US")],
        PoolConfig {
            fail_timeout: Duration::from_secs(30),
            ..PoolConfig::default()
        },
    );
    let k = any_key();

    // Bench one proxy.
    let benched = pool.get(Scheme::Http, &k).await.unwrap();
    let benched_addr = benched.addr();
    pool.put_failed(benched);

    // The still-ready proxy is chosen over the benched one.
    let ready = pool.get(Scheme::Http, &k).await.unwrap();
    assert_ne!(ready.addr(), benched_addr, "a benched proxy is skipped");
    pool.put_ok(ready); // both back in the pool (one benched, one ready)

    // After the bench window, the benched proxy re-enters selection.
    tokio::time::advance(Duration::from_secs(31)).await;
    let a = pool.get(Scheme::Http, &k).await.unwrap();
    let b = pool.get(Scheme::Http, &k).await.unwrap();
    assert!(
        [a.addr(), b.addr()].contains(&benched_addr),
        "benched proxy is re-probed after fail_timeout"
    );
}

#[tokio::test(start_paused = true)]
async fn benched_proxy_is_backup_when_pool_otherwise_empty() {
    // With nothing else eligible, a benched proxy is still served (better than a 502).
    let pool = Pool::from_proxies(vec![proxy_in("1.1.1.1", "US")], PoolConfig::default());
    let k = any_key();
    let p = pool.get(Scheme::Http, &k).await.unwrap();
    pool.put_failed(p);
    assert!(
        pool.get(Scheme::Http, &k).await.is_some(),
        "a benched proxy is the backup tier when nothing else is eligible"
    );
}

#[tokio::test(start_paused = true)]
async fn persistent_unhealthy_still_evicted() {
    // Benching is additive: a proxy over the error-rate threshold (after min_req) is still hard-
    // evicted by put_*, not merely benched.
    let mut p = proxy_in("1.1.1.1", "US");
    for _ in 0..5 {
        p.record_attempt(None, Some(proxybroker::ProxyError::ConnFailed)); // 5 reqs, 100% errors
    }
    let pool = Pool::from_proxies(vec![], PoolConfig::default());
    pool.put_ok(p); // unhealthy → dropped, not pooled
    assert!(
        pool.get(Scheme::Http, &any_key()).await.is_none(),
        "a persistently unhealthy proxy is evicted"
    );
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
    pool.put_ok(first); // healthy return — back into the pool
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
        0,
        1024,
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
        0,
        1024,
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
        0,
        1024,
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
