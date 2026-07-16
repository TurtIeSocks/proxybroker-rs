//! Offline end-to-end test of `Broker::find`: mock providers list the addresses of mock
//! HTTP proxies, a mock judge lets the checker's eager probe pass, and a stubbed resolver
//! makes external-IP discovery return the address the judge echoes — so the whole pipeline
//! (Semaphore, TaskTracker, limit, dedup) runs with no internet (constraint C5).

use futures_util::StreamExt;
use proxybroker::broker::{Broker, FindQuery};
use proxybroker::provider::ProviderSpec;
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The external IP the stubbed resolver reports and the judge echoes, so the judge verifies.
const REAL_IP: &str = "203.0.113.9";

/// Echoes the marker + given page; used for judge (probe), proxy (check), and ext-IP stub.
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

/// A provider page listing the given proxy addresses, one per line.
async fn provider_listing(addrs: &[std::net::SocketAddr]) -> (String, tokio::task::JoinHandle<()>) {
    let body: String = addrs.iter().map(|a| format!("{a}\n")).collect();
    let leaked: &'static str = Box::leak(body.into_boxed_str());
    let (addr, h) = echo_server(leaked).await;
    (format!("http://{addr}/"), h)
}

/// A resolver whose external-IP discovery returns REAL_IP (from a mock endpoint), so the
/// judge — which echoes REAL_IP — verifies offline.
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

fn base_query(judge: std::net::SocketAddr, limit: Option<usize>) -> FindQuery {
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
async fn find_streams_working_checked_proxies() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (p1, _a) = echo_server(HIGH_PAGE).await;
    let (p2, _b) = echo_server(HIGH_PAGE).await;
    let (prov, _pr) = provider_listing(&[p1, p2]).await;
    let (resolver, _ext) = stubbed_resolver().await;

    let broker = Broker::builder()
        .providers(vec![ProviderSpec::new(&prov, &[Proto::Http])])
        .resolver(resolver)
        .build();

    let proxies: Vec<_> = broker
        .find(base_query(judge, None))
        .await
        .expect("find should start")
        .collect()
        .await;

    let mut addrs: Vec<String> = proxies.iter().map(|p| p.addr()).collect();
    addrs.sort();
    let mut want = [format!("{p1}"), format!("{p2}")];
    want.sort();
    assert_eq!(addrs, want, "both working proxies must be streamed");
    assert!(proxies.iter().all(|p| p.is_working()));
}

#[tokio::test]
async fn find_respects_the_limit() {
    let (judge, _j) = echo_server(JUDGE_PAGE).await;
    let (p1, _a) = echo_server(HIGH_PAGE).await;
    let (p2, _b) = echo_server(HIGH_PAGE).await;
    let (p3, _c) = echo_server(HIGH_PAGE).await;
    let (prov, _pr) = provider_listing(&[p1, p2, p3]).await;
    let (resolver, _ext) = stubbed_resolver().await;

    let broker = Broker::builder()
        .providers(vec![ProviderSpec::new(&prov, &[Proto::Http])])
        .resolver(resolver)
        .build();

    let proxies: Vec<_> = broker
        .find(base_query(judge, Some(1)))
        .await
        .expect("find should start")
        .collect()
        .await;

    assert_eq!(proxies.len(), 1, "limit of 1 must yield exactly one");
}

#[tokio::test]
async fn find_requires_types() {
    let broker = Broker::builder().providers(vec![]).build();
    let err = broker
        .find(FindQuery {
            types: vec![],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, proxybroker::Error::NoTypes));
}
