//! Offline test for external-IP discovery: point the resolver at local mock endpoints that
//! echo canned IPs, and check the set is collected and deduplicated (constraint C5).

use proxybroker::resolver::Resolver;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn echo_ip(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 512];
                let _ = sock.read(&mut buf).await;
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

#[tokio::test]
async fn external_ips_collects_distinct_ips_from_endpoints() {
    // Two endpoints report the same v4, a third reports a v6 → the set has two entries.
    let (u1, _a) = echo_ip("203.0.113.7\n").await;
    let (u2, _b) = echo_ip("203.0.113.7").await;
    let (u3, _c) = echo_ip("2001:db8::42").await;

    let r = Resolver::new(Duration::from_secs(5))
        .unwrap()
        .with_ip_endpoints(vec![u1, u2, u3]);
    let ips = r.external_ips().await.unwrap();

    assert_eq!(ips.len(), 2, "{ips:?}");
    assert!(ips.contains(&"203.0.113.7".parse().unwrap()));
    assert!(ips.contains(&"2001:db8::42".parse().unwrap()));
}

#[tokio::test]
async fn external_ips_errors_when_no_endpoint_answers() {
    let r = Resolver::new(Duration::from_secs(2))
        .unwrap()
        .with_ip_endpoints(vec!["http://127.0.0.1:1/".to_string()]);
    assert!(r.external_ips().await.is_err());
}
