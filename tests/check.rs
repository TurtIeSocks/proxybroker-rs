//! Offline end-to-end test of `Broker::check`: feed a user-supplied list of proxy addresses
//! (not scraped from providers) through the same check pipeline as `find`. Mock judge + mock
//! HTTP proxies + a stubbed resolver keep it fully offline (constraint C5).

use futures_util::StreamExt;
use proxybroker::broker::{Broker, FindQuery};
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use proxybroker::Proxy;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const REAL_IP: &str = "203.0.113.9";

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

async fn stubbed_resolver() -> (Resolver, tokio::task::JoinHandle<()>) {
    let (ext_ip, h) = echo_server(REAL_IP).await;
    let r = Resolver::new(Duration::from_secs(3))
        .unwrap()
        .with_ip_endpoints(vec![format!("http://{ext_ip}/")]);
    (r, h)
}

const HIGH_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

fn http_proxy(addr: std::net::SocketAddr) -> Proxy {
    // A user-supplied proxy: no expected_types (checker treats it as "check all requested").
    Proxy::new(addr.ip(), addr.port(), BTreeSet::new())
}

fn query(judge: std::net::SocketAddr, limit: Option<usize>) -> FindQuery {
    FindQuery {
        types: vec![TypeSpec::any(Proto::Http)],
        judges: vec![format!("http://{judge}/")],
        limit,
        timeout: Duration::from_secs(3),
        max_conn: 8,
        max_tries: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn check_streams_working_proxies() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (p1, _a) = echo_server(HIGH_PAGE).await;
    let (p2, _b) = echo_server(HIGH_PAGE).await;
    let (resolver, _ext) = stubbed_resolver().await;

    let broker = Broker::builder()
        .providers(vec![])
        .resolver(resolver)
        .build();

    let input = futures_util::stream::iter(vec![http_proxy(p1), http_proxy(p2)]);
    let proxies: Vec<_> = broker
        .check(input, query(judge, None))
        .await
        .expect("check should start")
        .collect()
        .await;

    let mut addrs: Vec<String> = proxies.iter().map(|p| p.addr()).collect();
    addrs.sort();
    let mut want = [format!("{p1}"), format!("{p2}")];
    want.sort();
    assert_eq!(
        addrs, want,
        "both user-supplied proxies must be checked and streamed"
    );
    assert!(proxies.iter().all(|p| p.is_working()));
}

#[tokio::test]
async fn check_respects_the_limit() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (p1, _a) = echo_server(HIGH_PAGE).await;
    let (p2, _b) = echo_server(HIGH_PAGE).await;
    let (p3, _c) = echo_server(HIGH_PAGE).await;
    let (resolver, _ext) = stubbed_resolver().await;

    let broker = Broker::builder()
        .providers(vec![])
        .resolver(resolver)
        .build();

    let input = futures_util::stream::iter(vec![http_proxy(p1), http_proxy(p2), http_proxy(p3)]);
    let proxies: Vec<_> = broker
        .check(input, query(judge, Some(1)))
        .await
        .expect("check should start")
        .collect()
        .await;

    assert_eq!(proxies.len(), 1, "limit of 1 must yield exactly one");
}

#[tokio::test]
async fn check_requires_types() {
    let broker = Broker::builder().providers(vec![]).build();
    let err = broker
        .check(
            futures_util::stream::iter(Vec::<Proxy>::new()),
            FindQuery {
                types: vec![],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, proxybroker::Error::NoTypes));
}
