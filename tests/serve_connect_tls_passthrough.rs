//! Proof: a CONNECT client tunnelling through an `Https`-typed pool proxy must get an **opaque**
//! byte tunnel. `choose_proto` prefers `Proto::Https` for a `Scheme::Https` request, and that
//! negotiation does `CONNECT` *plus a TLS upgrade of its own* — so the client's end-to-end TLS
//! would be sent as application data inside the server's TLS session. This test drives the path
//! with a plaintext marker instead of a real ClientHello: if the tunnel is opaque the marker
//! arrives verbatim; if the server terminated TLS it never does.

#![cfg(feature = "server")]

use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::types::Proto;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A mock upstream proxy that speaks HTTP `CONNECT`: it acks with `200`, then records whatever the
/// caller sends through the tunnel. That recording is the assertion surface — it tells us whether
/// the bytes on the wire were the client's own or a TLS handshake our server injected.
async fn mock_connect_proxy(
    seen: Arc<Mutex<Vec<u8>>>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                // Read the CONNECT request head.
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while sock.read_exact(&mut byte).await.is_ok() {
                    head.push(byte[0]);
                    if head.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                if !head.starts_with(b"CONNECT ") {
                    return;
                }
                let _ = sock
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await;
                let _ = sock.flush().await;
                // Whatever comes next is the tunnel payload.
                let mut buf = [0u8; 1024];
                if let Ok(n) = sock.read(&mut buf).await {
                    seen.lock().unwrap().extend_from_slice(&buf[..n]);
                }
            });
        }
    });
    (addr, h)
}

#[tokio::test]
async fn connect_client_gets_an_opaque_tunnel_through_an_https_typed_proxy() {
    proxybroker::install_default_crypto_provider();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (upstream, _u) = mock_connect_proxy(seen.clone()).await;

    // An `Https`-typed proxy — the shape any proxy that passes the HTTPS check ends up with, and
    // the first candidate `choose_proto` picks for a CONNECT (Scheme::Https) request.
    let mut proxy = Proxy::new(upstream.ip(), upstream.port(), BTreeSet::new());
    proxy.add_type(Proto::Https, None);
    let pool = Pool::from_proxies(vec![proxy], PoolConfig::default());

    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        128,
        None,
    )
    .await
    .unwrap();

    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"CONNECT secure.example:443 HTTP/1.1\r\nHost: secure.example:443\r\n\r\n")
        .await
        .unwrap();

    // Read the server's ack.
    let mut ack = Vec::new();
    let mut byte = [0u8; 1];
    while client.read_exact(&mut byte).await.is_ok() {
        ack.push(byte[0]);
        if ack.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let ack = String::from_utf8_lossy(&ack).into_owned();
    assert!(
        ack.starts_with("HTTP/1.1 200"),
        "CONNECT should be acked, got: {ack:?}"
    );

    // Stand in for the client's ClientHello. A correct opaque tunnel forwards it verbatim.
    client.write_all(b"CLIENT-OWNED-TLS-MARKER").await.unwrap();
    client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let got = seen.lock().unwrap().clone();
    assert_eq!(
        got,
        b"CLIENT-OWNED-TLS-MARKER".to_vec(),
        "upstream must receive the client's own bytes, not a server-injected TLS handshake; got {:?}",
        String::from_utf8_lossy(&got)
    );
}
