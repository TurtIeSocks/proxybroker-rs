//! F3 — the checker emits one structured `proxybroker::check` event per terminal outcome. Captured
//! in-process via a JSON `tracing-subscriber` writing to a shared buffer; no network beyond the
//! loopback mock judge/proxy (constraint C5). Gated on `cli` (the tracing-subscriber dependency).
#![cfg(feature = "cli")]

use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use std::collections::{BTreeSet, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn echo_server(body: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
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

/// Accepts but never responds — the checker's read times out (ProxyError::Timeout).
async fn blackhole() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _held = sock;
                std::future::pending::<()>().await;
            });
        }
    });
    (addr, h)
}

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
const HIGH_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

async fn make_checker(judge: SocketAddr, timeout: Duration) -> Checker {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from(["203.0.113.9".parse().unwrap()]);
    let cfg = CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout,
        retry: RetryPolicy::tries(2),
        ..Default::default()
    };
    Checker::new(cfg, resolver, &client, real).await.unwrap()
}

fn http_proxy(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::new())
}

// A MakeWriter that appends every log line to a shared buffer.
#[derive(Clone)]
struct BufMaker(Arc<Mutex<Vec<u8>>>);
impl io::Write for BufMaker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl tracing_subscriber::fmt::MakeWriter<'_> for BufMaker {
    type Writer = BufMaker;
    fn make_writer(&self) -> BufMaker {
        self.clone()
    }
}

/// Run `f` with a JSON subscriber capturing `proxybroker::check` events, return the parsed events.
async fn capture<F, Fut>(f: F) -> Vec<serde_json::Value>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let buf = Arc::new(Mutex::new(Vec::new()));
    let sub = tracing_subscriber::fmt()
        .json()
        .with_writer(BufMaker(buf.clone()))
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "proxybroker::check=info",
        ))
        .finish();
    {
        let _guard = tracing::subscriber::set_default(sub);
        f().await;
    }
    let bytes = buf.lock().unwrap().clone();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect()
}

#[tokio::test]
async fn check_emits_structured_outcome_event() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (proxy_addr, _p) = echo_server(HIGH_PAGE).await;
    let checker = make_checker(judge, Duration::from_secs(3)).await;

    let events = capture(|| async {
        let mut proxy = http_proxy(proxy_addr);
        assert!(checker.check(&mut proxy).await);
    })
    .await;

    let working = events
        .iter()
        .find(|e| e["fields"]["outcome"] == "working")
        .expect("a working outcome event");
    assert_eq!(working["fields"]["proto"], "HTTP");
    assert_eq!(working["fields"]["addr"], proxy_addr.to_string());
    assert!(working["fields"]["rtt"].is_number(), "rtt: {working}");
}

#[tokio::test]
async fn timeout_outcome_is_labelled() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (dead, _d) = blackhole().await;
    let checker = make_checker(judge, Duration::from_millis(400)).await;

    let events = capture(|| async {
        let mut proxy = http_proxy(dead);
        assert!(!checker.check(&mut proxy).await);
    })
    .await;

    assert!(
        events.iter().any(|e| e["fields"]["outcome"] == "timeout"),
        "expected a timeout outcome event, got: {events:?}"
    );
}
