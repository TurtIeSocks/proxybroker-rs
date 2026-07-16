//! A4 — capability profiling and filtering. Under `--relaxed-validity` a proxy that forwards the
//! request (marker + IP) but strips our Cookie is accepted and profiled; `--require-cookie` then
//! drops it from the stream. Fully offline (constraint C5), reusing the `echo_server` pattern.

use futures_util::StreamExt;
use proxybroker::broker::{Broker, FindQuery};
use proxybroker::checker::{Checker, CheckerConfig, RetryPolicy};
use proxybroker::resolver::Resolver;
use proxybroker::types::{Caps, Proto, TypeSpec};
use proxybroker::Proxy;
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const REAL_IP: &str = "203.0.113.9";

async fn echo_server(body_template: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
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

async fn stubbed_resolver() -> (Resolver, tokio::task::JoinHandle<()>) {
    let (ext_ip, h) = echo_server(REAL_IP).await;
    let r = Resolver::new(Duration::from_secs(3))
        .unwrap()
        .with_ip_endpoints(vec![format!("http://{ext_ip}/")]);
    (r, h)
}

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
// Forwards marker + IP + Referer, but strips the Cookie: invalid under strict validity, but a
// working (High) proxy with a `referer_echo`-only profile under --relaxed-validity.
const NO_COOKIE_PAGE: &str =
    "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} Referer=https://www.google.com/";

fn http_proxy(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::new())
}

async fn relaxed_checker(judge: SocketAddr) -> Checker {
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let client = reqwest::Client::new();
    let real: HashSet<IpAddr> = HashSet::from([REAL_IP.parse().unwrap()]);
    let cfg = CheckerConfig {
        judges: vec![format!("http://{judge}/")],
        types: vec![TypeSpec::any(Proto::Http)],
        timeout: Duration::from_secs(3),
        retry: RetryPolicy::tries(1),
        relaxed_validity: true,
        ..Default::default()
    };
    Checker::new(cfg, resolver, &client, real)
        .await
        .expect("judge should verify")
}

#[tokio::test]
async fn cookie_stripping_proxy_is_profiled() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let checker = relaxed_checker(judge).await;
    let (proxy_addr, _p) = echo_server(NO_COOKIE_PAGE).await;
    let mut proxy = http_proxy(proxy_addr);

    assert!(
        checker.check(&mut proxy).await,
        "relaxed validity accepts a proxy that strips the cookie"
    );
    assert_eq!(
        proxy.caps(),
        Caps {
            cookie_echo: false,
            referer_echo: true,
        },
        "the stripped cookie + forwarded referer are recorded"
    );
}

#[tokio::test]
async fn require_cookie_filters() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (proxy_addr, _p) = echo_server(NO_COOKIE_PAGE).await;
    let (resolver, _ext) = stubbed_resolver().await;
    let broker = Broker::builder()
        .providers(vec![])
        .resolver(resolver)
        .build();

    let query = FindQuery {
        types: vec![TypeSpec::any(Proto::Http)],
        judges: vec![format!("http://{judge}/")],
        timeout: Duration::from_secs(3),
        max_conn: 8,
        retry: RetryPolicy::tries(1),
        relaxed_validity: true,
        require_cookie: true,
        ..Default::default()
    };
    let input = futures_util::stream::iter(vec![http_proxy(proxy_addr)]);
    let out: Vec<_> = broker
        .check(input, query)
        .await
        .expect("check should start")
        .collect()
        .await;

    assert!(
        out.is_empty(),
        "a working but cookie-stripping proxy is filtered out by --require-cookie"
    );
}
