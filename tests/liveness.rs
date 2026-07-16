//! A2 — judge-less liveness mode. When no judge verifies but the caller supplies a liveness URL,
//! the checker degrades to a plain fetch-through-the-proxy 200 check instead of failing with
//! `NoJudges`. Fully offline (constraint C5): a "bad judge" that never echoes the real IP forces
//! an empty judge pool, and a mock proxy returns the status under test.

use proxybroker::checker::{Checker, CheckerConfig};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{AnonLevel, Proto, TypeSpec};
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A server that returns `response` verbatim to any connection. Serves as a bad judge (probed by
/// reqwest) or a mock proxy (hit by the checker's liveness GET).
async fn serve(response: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    (addr, h)
}

// A judge that answers 200 but never echoes the real external IP → the eager probe rejects it,
// leaving the judge pool empty.
const BAD_JUDGE: &str = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope";
const OK_200: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
const BAD_503: &str =
    "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

fn cfg(
    judge: SocketAddr,
    liveness: Option<String>,
    levels: Option<Vec<AnonLevel>>,
) -> CheckerConfig {
    CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec {
            proto: Proto::Http,
            levels,
        }],
        timeout: Duration::from_secs(3),
        max_tries: 2,
        liveness_url: liveness,
        ..Default::default()
    }
}

async fn new_checker(cfg: CheckerConfig) -> Result<Checker, proxybroker::Error> {
    let resolver = std::sync::Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    Checker::new(cfg, resolver, &client, real).await
}

fn http_proxy_at(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]))
}

#[tokio::test]
async fn all_judges_down_degrades_to_liveness() {
    let (bad_judge, _j) = serve(BAD_JUDGE).await;
    let (proxy_addr, _p) = serve(OK_200).await;
    let liveness = format!("http://{proxy_addr}/health");
    // Judge pool is empty, but the liveness URL lets Checker::new succeed.
    let checker = new_checker(cfg(bad_judge, Some(liveness), None))
        .await
        .expect("liveness URL should let the checker build");

    let mut proxy = http_proxy_at(proxy_addr);
    assert!(
        checker.check(&mut proxy).await,
        "liveness 200 should confirm"
    );
    // Confirmed via liveness → HTTP with anonymity level None (unclassifiable without a judge).
    assert_eq!(proxy.types().get(&Proto::Http), Some(&None));
}

#[tokio::test]
async fn no_liveness_url_still_errors() {
    let (bad_judge, _j) = serve(BAD_JUDGE).await;
    let err = new_checker(cfg(bad_judge, None, None)).await.unwrap_err();
    assert!(
        matches!(err, proxybroker::Error::NoJudges),
        "parity: no liveness → NoJudges"
    );
}

#[tokio::test]
async fn liveness_bad_status_is_not_working() {
    let (bad_judge, _j) = serve(BAD_JUDGE).await;
    let (proxy_addr, _p) = serve(BAD_503).await;
    let checker = new_checker(cfg(bad_judge, Some(format!("http://{proxy_addr}/")), None))
        .await
        .unwrap();
    let mut proxy = http_proxy_at(proxy_addr);
    assert!(
        !checker.check(&mut proxy).await,
        "503 through the proxy is not working"
    );
}

#[tokio::test]
async fn liveness_ignores_anon_filter() {
    // A level filter can never be satisfied in liveness mode (level is always None).
    let (bad_judge, _j) = serve(BAD_JUDGE).await;
    let (proxy_addr, _p) = serve(OK_200).await;
    let checker = new_checker(cfg(
        bad_judge,
        Some(format!("http://{proxy_addr}/")),
        Some(vec![AnonLevel::High]),
    ))
    .await
    .unwrap();
    let mut proxy = http_proxy_at(proxy_addr);
    assert!(
        !checker.check(&mut proxy).await,
        "liveness level None cannot satisfy a High filter"
    );
}
