//! A6 — honeypot detection end to end. A mock proxy that injects a header into its echoed page is
//! flagged (`--trust-check`) and filtered (`--require-trusted`); a proxy that merely re-gzips a
//! clean page stays trusted (the canary is compared *after* decompression). Offline (constraint C5).

use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::StreamExt;
use proxybroker::broker::{Broker, FindQuery};
use proxybroker::resolver::Resolver;
use proxybroker::types::{Proto, TypeSpec};
use proxybroker::{Proxy, RetryPolicy, TrustSignal};
use std::collections::BTreeSet;
use std::io::Write as _;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const REAL_IP: &str = "203.0.113.9";

fn marker_of(req: &str) -> String {
    req.lines()
        .find(|l| l.to_ascii_lowercase().starts_with("user-agent:"))
        .and_then(|l| l.rsplit('/').next())
        .map(|m| m.trim().to_string())
        .unwrap_or_default()
}

/// Reply with `body_template` ({marker} substituted), optionally gzip-encoded.
async fn server(
    body_template: &'static str,
    gzip: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let body = body_template
                    .replace("{marker}", &marker_of(&String::from_utf8_lossy(&buf[..n])));
                let (payload, enc): (Vec<u8>, &str) = if gzip {
                    let mut e = GzEncoder::new(Vec::new(), Compression::default());
                    e.write_all(body.as_bytes()).unwrap();
                    (e.finish().unwrap(), "Content-Encoding: gzip\r\n")
                } else {
                    (body.into_bytes(), "")
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\n{enc}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&payload).await;
                let _ = sock.flush().await;
            });
        }
    });
    (addr, h)
}

async fn stubbed_resolver() -> (Resolver, tokio::task::JoinHandle<()>) {
    let (ext_ip, h) = server(REAL_IP, false).await;
    let r = Resolver::new(Duration::from_secs(3))
        .unwrap()
        .with_ip_endpoints(vec![format!("http://{ext_ip}/")]);
    (r, h)
}

const JUDGE_PAGE: &str = "REMOTE_ADDR=203.0.113.9 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";
// Valid (marker + IP + Referer + Cookie) but injects a header the checker never sent.
const INJECT_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok\nX-Ad-Inject: 1\n";
// Valid and clean — no injected `Name: value` line.
const CLEAN_PAGE: &str = "REMOTE_ADDR=8.8.8.8 UA=PxBroker/x/{marker} \
    Referer=https://www.google.com/ Cookie=cookie=ok";

fn http_proxy(addr: SocketAddr) -> Proxy {
    Proxy::new(addr.ip(), addr.port(), BTreeSet::new())
}

fn query(judge: SocketAddr, trust_check: bool, require_trusted: bool) -> FindQuery {
    FindQuery {
        types: vec![TypeSpec::any(Proto::Http)],
        judges: vec![format!("http://{judge}/")],
        timeout: Duration::from_secs(3),
        max_conn: 8,
        retry: RetryPolicy::tries(1),
        trust_check,
        require_trusted,
        ..Default::default()
    }
}

async fn run(
    proxy_page: &'static str,
    gzip: bool,
    trust_check: bool,
    require_trusted: bool,
) -> Vec<Proxy> {
    let (judge, _j) = server(JUDGE_PAGE, false).await;
    let (proxy_addr, _p) = server(proxy_page, gzip).await;
    let (resolver, _ext) = stubbed_resolver().await;
    let broker = Broker::builder()
        .providers(vec![])
        .resolver(resolver)
        .build();
    let input = futures_util::stream::iter(vec![http_proxy(proxy_addr)]);
    broker
        .check(input, query(judge, trust_check, require_trusted))
        .await
        .expect("check should start")
        .collect()
        .await
}

#[tokio::test]
async fn injecting_proxy_is_recorded() {
    let out = run(INJECT_PAGE, false, true, false).await;
    assert_eq!(
        out.len(),
        1,
        "trust_check records the verdict but does not filter"
    );
    assert!(
        out[0]
            .trust()
            .signals
            .contains(&TrustSignal::InjectedHeader),
        "verdict: {:?}",
        out[0].trust().signals
    );
}

#[tokio::test]
async fn require_trusted_filters_the_injector() {
    let out = run(INJECT_PAGE, false, false, true).await;
    assert!(
        out.is_empty(),
        "--require-trusted drops the header-injecting proxy"
    );
}

#[tokio::test]
async fn gzip_reencode_stays_trusted() {
    // The proxy re-gzips a clean page; the canary survives once decompressed, so it is trusted and
    // passes --require-trusted (guards against comparing raw bytes).
    let out = run(CLEAN_PAGE, true, false, true).await;
    assert_eq!(out.len(), 1, "a clean gzip-re-encoding proxy stays trusted");
    assert!(out[0].trust().trusted());
}
