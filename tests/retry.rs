//! A5 — the retry policy controls WHICH errors retry. A flaky mock proxy fails its first
//! connection (a transient reset/empty-recv) and echoes a valid page thereafter: with a policy
//! that retries the transient set the check passes on the second attempt; with the default
//! timeout-only policy the first non-timeout error is fatal. Fully offline (constraint C5).

use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use proxybroker::ProxyError;
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

/// An echo server: recovers our marker from the `User-Agent` and replies with `body` (with
/// `{marker}` substituted). Used as the judge for the eager probe.
async fn echo_server(body: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                reply_echo(&mut sock, body).await;
            });
        }
    });
    (addr, h)
}

/// A proxy whose first `fail_first` connections are dropped immediately (→ a transient
/// Reset/EmptyRecv at the checker), and which echoes a valid proxy page on every later connection.
async fn flaky_proxy(fail_first: usize) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if n < fail_first {
                    // Drop the connection without responding: the checker sees a reset/empty recv.
                    drop(sock);
                    return;
                }
                reply_echo(
                    &mut sock,
                    "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
                     Referer=https://www.google.com/ Cookie=cookie=ok",
                )
                .await;
            });
        }
    });
    (addr, h)
}

async fn reply_echo(sock: &mut tokio::net::TcpStream, body_template: &str) {
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
}

async fn make_checker(judge: SocketAddr, retry: RetryPolicy) -> Checker {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    let cfg = CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        retry,
        ..Default::default()
    };
    Checker::new(cfg, resolver, &client, real)
        .await
        .expect("judge should verify")
}

fn proxy_at(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]))
}

#[tokio::test]
async fn reset_is_retried_when_policy_includes_it() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (proxy_addr, _p) = flaky_proxy(1).await; // first connection fails, then it works
    let retry = RetryPolicy {
        max_tries: 2,
        retry_on: HashSet::from([ProxyError::Reset, ProxyError::EmptyRecv]),
        ..Default::default()
    };
    let checker = make_checker(judge, retry).await;
    let mut proxy = proxy_at(proxy_addr);
    assert!(
        checker.check(&mut proxy).await,
        "a transient reset should be retried and the second attempt succeeds"
    );
}

#[tokio::test]
async fn default_policy_does_not_retry_reset() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (proxy_addr, _p) = flaky_proxy(1).await;
    // Default policy retries only Timeout, so the first reset/empty-recv is fatal.
    let checker = make_checker(judge, RetryPolicy::tries(2)).await;
    let mut proxy = proxy_at(proxy_addr);
    assert!(
        !checker.check(&mut proxy).await,
        "a non-timeout error is not retried by the default policy"
    );
}
