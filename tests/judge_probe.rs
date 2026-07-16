//! Offline test of judge probing: a mock judge echoes the request's User-Agent (with our
//! marker) plus a "real" external IP, and we check that probe verifies it and records the
//! via/proxy baseline (constraint C5).

use proxybroker::judge::{Judge, JudgePool};
use proxybroker::resolver::Resolver;
use proxybroker::types::JudgeScheme;
use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A mock judge. Reads the request (to recover the marker from the User-Agent) and replies
/// with a page that echoes the marker, the given `real_ip`, and `extra` text (to exercise the
/// via/proxy counting).
async fn mock_judge(
    real_ip: &'static str,
    extra: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                // Recover our marker from the echoed User-Agent: "PxBroker/<ver>/<marker>".
                let marker = req
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("user-agent:"))
                    .and_then(|l| l.rsplit('/').next())
                    .map(|m| m.trim().to_string())
                    .unwrap_or_default();
                let body = format!("REMOTE_ADDR={real_ip} User-Agent=PxBroker/x/{marker} {extra}");
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
    (format!("http://{addr}/"), h)
}

fn ips(s: &str) -> HashSet<IpAddr> {
    HashSet::from([s.parse().unwrap()])
}

#[tokio::test]
async fn probe_verifies_and_records_baseline_marks() {
    // Page echoes the real IP and (via the mock) our marker, plus one "via" and two "proxy".
    let (url, _srv) = mock_judge("203.0.113.9", "via=1 proxy proxy").await;
    let resolver = Resolver::new(Duration::from_secs(3)).unwrap();
    let client = reqwest::Client::new();

    let mut judge = Judge::parse(&url).unwrap();
    let ok = judge
        .probe(
            &resolver,
            &client,
            &ips("203.0.113.9"),
            Duration::from_secs(3),
        )
        .await;

    assert!(ok, "judge echoing real IP + marker must verify");
    assert_eq!(judge.marks.via, 1);
    assert_eq!(judge.marks.proxy, 2);
    assert!(judge.ip.is_some());
}

#[tokio::test]
async fn probe_fails_when_real_ip_absent() {
    // The page echoes a DIFFERENT IP than the host's real one → transparent-detection would
    // be impossible, so the judge is rejected.
    let (url, _srv) = mock_judge("198.51.100.1", "").await;
    let resolver = Resolver::new(Duration::from_secs(3)).unwrap();
    let client = reqwest::Client::new();

    let mut judge = Judge::parse(&url).unwrap();
    let ok = judge
        .probe(
            &resolver,
            &client,
            &ips("203.0.113.9"),
            Duration::from_secs(3),
        )
        .await;
    assert!(!ok);
}

#[tokio::test]
async fn probe_all_groups_working_judges() {
    let (url, _srv) = mock_judge("203.0.113.9", "").await;
    let resolver = Resolver::new(Duration::from_secs(3)).unwrap();
    let client = reqwest::Client::new();

    let pool = JudgePool::probe_all(
        vec![Judge::parse(&url).unwrap()],
        &resolver,
        &client,
        &ips("203.0.113.9"),
        Duration::from_secs(3),
    )
    .await;

    assert!(!pool.is_empty());
    assert!(pool.random(JudgeScheme::Http).is_some());
    assert_eq!(pool.counts().0, 1);
}
