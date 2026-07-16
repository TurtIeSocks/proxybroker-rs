//! Offline test of the CONNECT negotiation: a local mock proxy accepts the CONNECT and
//! replies with a status, and we check that `negotiate` reads it correctly (constraint C5).
//! SOCKS is delegated to tokio-socks (tested upstream); this covers our hand-rolled path.

use proxybroker::negotiator::{negotiate, Stream, Target};
use proxybroker::types::Proto;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A mock proxy that reads the CONNECT request line and replies with `status_line`.
async fn mock_proxy(
    status_line: &'static str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            // Read until the request head ends.
            let mut buf = Vec::new();
            let mut b = [0u8; 1];
            while sock.read(&mut b).await.map(|n| n > 0).unwrap_or(false) {
                buf.push(b[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let resp = format!("{status_line}\r\n\r\n");
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
            // Keep the socket open briefly so the client can finish reading.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    (addr, h)
}

fn target() -> Target {
    Target {
        host: "judge.example".into(),
        ip: None,
        port: 80,
    }
}

#[tokio::test]
async fn connect_succeeds_on_200() {
    let (addr, _srv) = mock_proxy("HTTP/1.1 200 Connection established").await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    let s = negotiate(
        Proto::Connect80,
        tcp,
        &target(),
        Duration::from_secs(2),
        None,
    )
    .await
    .expect("CONNECT 200 should succeed");
    assert!(matches!(s, Stream::Plain(_)));
}

#[tokio::test]
async fn connect_fails_on_403() {
    let (addr, _srv) = mock_proxy("HTTP/1.1 403 Forbidden").await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    let err = negotiate(
        Proto::Connect80,
        tcp,
        &target(),
        Duration::from_secs(2),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err, proxybroker::ProxyError::BadStatus);
}

#[tokio::test]
async fn http_negotiation_is_a_noop() {
    // HTTP does not negotiate; it must return the plain stream without touching the wire.
    let (addr, _srv) = mock_proxy("HTTP/1.1 200 OK").await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    let s = negotiate(Proto::Http, tcp, &target(), Duration::from_secs(2), None)
        .await
        .unwrap();
    assert!(matches!(s, Stream::Plain(_)));
}
