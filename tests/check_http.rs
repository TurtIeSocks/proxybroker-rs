//! Offline end-to-end test of the HTTP check path: a mock judge (for the eager probe) and a
//! mock HTTP proxy (that echoes the forwarded request) let the whole checker run — connect,
//! negotiate (no-op for HTTP), test request, validate, classify anonymity — with no internet
//! (constraint C5).

use proxybroker::checker::{Checker, CheckerConfig};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{AnonLevel, Proto, TypeSpec};
use std::collections::{BTreeSet, HashSet};
use std::net::IpAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A server that reads an HTTP request, recovers our marker from the echoed `User-Agent`
/// (`PxBroker/<ver>/<marker>`), and replies with `body_template` with `{marker}` substituted.
/// Serves as both a judge (probed by reqwest) and an HTTP proxy (hit by the checker).
async fn echo_server(
    body_template: &'static str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let marker = req
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("user-agent:"))
                    .and_then(|l| l.rsplit('/').next())
                    .map(|m| m.trim().to_string())
                    .unwrap_or_default();
                let body = body_template.replace("{marker}", &marker);
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

/// A judge page must echo the marker AND a real external IP (so the probe verifies).
const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

fn cfg(judge_url: &str) -> CheckerConfig {
    CheckerConfig {
        judges: vec![judge_url.to_string()],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        max_tries: 2,
        post: false,
        strict: false,
    }
}

async fn make_checker(judge: std::net::SocketAddr) -> Checker {
    let resolver = Resolver::new(Duration::from_secs(3)).unwrap();
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    Checker::new(cfg(&format!("http://{judge}/")), &resolver, &client, real)
        .await
        .expect("judge should verify")
}

fn http_proxy_at(addr: std::net::SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]))
}

#[tokio::test]
async fn high_anonymity_proxy_is_confirmed() {
    // Judge (for the probe) echoes the real IP; proxy page does NOT contain the real IP and
    // has no extra via/proxy → High.
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let checker = make_checker(judge).await;

    let (proxy_addr, _p) = echo_server(
        "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
         Referer=https://www.google.com/ Cookie=cookie=ok",
    )
    .await;
    let mut proxy = http_proxy_at(proxy_addr);

    assert!(
        checker.check(&mut proxy).await,
        "high-anon proxy should pass"
    );
    assert_eq!(
        proxy.types().get(&Proto::Http),
        Some(&Some(AnonLevel::High))
    );
}

#[tokio::test]
async fn transparent_proxy_is_detected() {
    // The proxy page leaks the host's real external IP → Transparent.
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let checker = make_checker(judge).await;

    let (proxy_addr, _p) = echo_server(
        "X-Forwarded-For=203.0.113.9 seen 8.8.8.8 UA=PxBroker/x/{marker} \
         Referer=https://www.google.com/ Cookie=cookie=ok",
    )
    .await;
    let mut proxy = http_proxy_at(proxy_addr);

    assert!(checker.check(&mut proxy).await);
    assert_eq!(
        proxy.types().get(&Proto::Http),
        Some(&Some(AnonLevel::Transparent))
    );
}

#[tokio::test]
async fn invalid_response_fails_the_check() {
    // Proxy page omits the cookie echo → response invalid → not working.
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let checker = make_checker(judge).await;

    let (proxy_addr, _p) =
        echo_server("REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} Referer=https://www.google.com/")
            .await;
    let mut proxy = http_proxy_at(proxy_addr);

    assert!(!checker.check(&mut proxy).await);
    assert!(proxy.types().is_empty());
}

#[tokio::test]
async fn no_judges_is_an_error() {
    // A judge that never echoes the real IP cannot verify → Checker::new fails with NoJudges.
    let (bad_judge, _j) = echo_server("nothing useful here UA=PxBroker/x/{marker}").await;
    let resolver = Resolver::new(Duration::from_secs(2)).unwrap();
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    let err = Checker::new(
        cfg(&format!("http://{bad_judge}/")),
        &resolver,
        &client,
        real,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, proxybroker::Error::NoJudges));
}
