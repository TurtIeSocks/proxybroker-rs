//! Offline integration test for the provider fetch path. Spins a real local HTTP server
//! (so `reqwest` genuinely does a request/response), points a `ProviderSpec` at it, and
//! checks extraction — no internet, satisfying the "testable offline" constraint (C5).

use proxybroker::provider::{fetch, ProviderSpec};
use proxybroker::types::Proto;
use std::convert::Infallible;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Minimal one-shot HTTP/1.1 server: accepts connections and replies with `body` to every
/// request until the test drops it. Enough to exercise reqwest end-to-end.
async fn serve_body(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await; // drain the request line/headers
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _: Result<(), Infallible> = {
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                    Ok(())
                };
            });
        }
    });
    (format!("http://{addr}/"), handle)
}

#[tokio::test]
async fn fetch_extracts_proxies_from_a_live_response() {
    let (url, _server) = serve_body("8.8.8.8:8080\n1.1.1.1:3128\nnot a proxy\n").await;
    let mut spec = ProviderSpec::new(&url, &[Proto::Http]);
    spec.timeout = 5;

    let client = reqwest::Client::new();
    let got = fetch(&spec, &client).await;

    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0].host, "8.8.8.8");
    assert_eq!(got[0].port, 8080);
    assert!(got[0].protocols.contains(&Proto::Http));
    assert_eq!(got[1].host, "1.1.1.1");
    assert_eq!(got[1].port, 3128);
}

#[tokio::test]
async fn fetch_failure_yields_no_proxies_not_an_error() {
    // Nothing is listening on this port → connection refused. A dead provider must yield an
    // empty list, never propagate an error that would sink the whole grab.
    let spec = {
        let mut s = ProviderSpec::new("http://127.0.0.1:1/", &[Proto::Http]);
        s.timeout = 2;
        s
    };
    let got = fetch(&spec, &reqwest::Client::new()).await;
    assert!(got.is_empty());
}

#[test]
fn bundled_registry_parses_and_is_nonempty() {
    let reg = proxybroker::provider::bundled_registry();
    assert!(
        reg.len() >= 10,
        "expected the live providers, got {}",
        reg.len()
    );
    // The proxyscrape SOCKS entries must carry the right protocol (the missing-comma bug fix).
    let socks5 = reg
        .iter()
        .find(|s| s.url.contains("proxytype=socks5"))
        .expect("proxyscrape socks5 present");
    assert_eq!(socks5.protocols, vec![Proto::Socks5]);
}
