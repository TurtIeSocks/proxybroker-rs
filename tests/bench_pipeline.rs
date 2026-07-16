//! F5 — proves the criterion bench fixture (`benches/check_pipeline.rs`) is deterministic and
//! network-free before criterion ever runs it: stand up the same mock judge + mock proxy on
//! 127.0.0.1, run exactly one check, and assert it confirms HTTP (constraint C5).

use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
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

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
const HIGH_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

async fn make_checker(judge: SocketAddr) -> Checker {
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
    Checker::new(cfg, resolver, &client, real).await.unwrap()
}

fn http_proxy(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::new())
}

#[tokio::test]
async fn bench_fixture_runs_one_check_offline() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (proxy_addr, _p) = echo_server(HIGH_PAGE).await;
    let checker = make_checker(judge).await;
    let mut proxy = http_proxy(proxy_addr);
    assert!(checker.check(&mut proxy).await, "fixture check should pass");
    assert!(proxy.types().contains_key(&Proto::Http));
}
