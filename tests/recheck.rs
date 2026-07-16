//! D3 — the adaptive re-check loop re-probes pooled proxies on cadence and throttles starts to the
//! rate ceiling. Tiny real intervals keep it fast; loopback mocks keep it offline (constraint C5).
//! (Paused time can't drive it — the checker's reqwest judge probe fails under `start_paused`.)
#![cfg(all(feature = "server", feature = "store-sqlite"))]

use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{Pool, PoolConfig};
use proxybroker::types::{Proto, TypeSpec};
use proxybroker::{RecheckConfig, SqliteStore, Store};
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static DBN: AtomicU32 = AtomicU32::new(0);
fn tmp_db() -> std::path::PathBuf {
    let n = DBN.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("pxb-recheck-{}-{n}.db", std::process::id()))
}

/// Echo server that replies with the marker-substituted page and, when given a counter, bumps it
/// on every connection (so a test can count re-check starts across several proxies).
async fn echo_server(
    body: &'static str,
    counter: Option<Arc<AtomicUsize>>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let counter = counter.clone();
            tokio::spawn(async move {
                if let Some(c) = &counter {
                    c.fetch_add(1, Ordering::SeqCst);
                }
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let marker = req
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("user-agent:"))
                    .and_then(|l| l.rsplit('/').next())
                    .map(|m| m.trim().to_string())
                    .unwrap_or_default();
                let body = body.replace("{marker}", &marker);
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

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
const HIGH_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

async fn make_checker(judge: SocketAddr) -> Arc<Checker> {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    let cfg = CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        retry: RetryPolicy::tries(1),
        ..Default::default()
    };
    Arc::new(Checker::new(cfg, resolver, &client, real).await.unwrap())
}

fn http_proxy(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]))
}

// Real (not paused) time with tiny intervals: paused time breaks the checker's reqwest judge probe
// (Checker::new → NoJudges). The re-check loop itself uses raw sockets, so short real waits are
// deterministic enough.
#[tokio::test]
async fn scheduler_reprobes_on_cadence() {
    let (judge, _j) = echo_server(JUDGE_PAGE, None).await;
    let hits = Arc::new(AtomicUsize::new(0));
    let (proxy_addr, _p) = echo_server(HIGH_PAGE, Some(hits.clone())).await;
    let checker = make_checker(judge).await;

    let pool = Pool::from_proxies(vec![http_proxy(proxy_addr)], PoolConfig::default());
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(tmp_db()).unwrap());
    let cfg = RecheckConfig {
        min_interval: Duration::from_millis(50),
        rate_per_sec: 100.0,
        ..Default::default()
    };
    let _h = proxybroker::spawn_rechecker(pool.clone(), checker, store.clone(), cfg);

    // First due is min_interval ± 50% jitter (≤ 75ms); give it room to fire + run its loopback I/O.
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "the pooled proxy should have been re-checked"
    );
    assert!(
        store
            .load()
            .unwrap()
            .iter()
            .any(|p| p.addr() == proxy_addr.to_string()),
        "the re-check outcome should be persisted"
    );
}

#[tokio::test]
async fn rate_ceiling_caps_starts() {
    // 20 proxies come due at once; the token bucket must throttle STARTS to ~rate/sec regardless of
    // backlog. All echo through one shared counter, so it counts total re-check starts.
    let (judge, _j) = echo_server(JUDGE_PAGE, None).await;
    let hits = Arc::new(AtomicUsize::new(0));
    let mut proxies = Vec::new();
    let mut _servers = Vec::new();
    for _ in 0..20 {
        let (addr, h) = echo_server(HIGH_PAGE, Some(hits.clone())).await;
        proxies.push(http_proxy(addr));
        _servers.push(h);
    }
    let checker = make_checker(judge).await;
    let pool = Pool::from_proxies(proxies, PoolConfig::default());
    let store: Arc<dyn Store> = Arc::new(SqliteStore::open(tmp_db()).unwrap());
    let cfg = RecheckConfig {
        min_interval: Duration::from_millis(50),
        rate_per_sec: 5.0, // ≤ 5 starts/sec
        ..Default::default()
    };
    let _h = proxybroker::spawn_rechecker(pool.clone(), checker, store, cfg);

    // Over ~1s, at most ~5 starts should fire despite 20 due proxies — well below the backlog.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let started = hits.load(Ordering::SeqCst);
    assert!(started >= 1, "some re-checks should have started");
    assert!(
        started <= 12,
        "rate ceiling should throttle starts well below the 20-proxy backlog, got {started}"
    );
}
