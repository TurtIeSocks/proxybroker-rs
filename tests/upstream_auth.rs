//! Offline tests for B8 — relaying through authenticated upstreams. A mock SOCKS5 server proves
//! the RFC 1929 username/password exchange; a capture mock proves the HTTP-forward
//! `Proxy-Authorization` injection. No network (constraint C5).

#![cfg(feature = "server")]

use proxybroker::negotiator::{negotiate, Target};
use proxybroker::proxy::Proxy;
use proxybroker::resolver::Resolver;
use proxybroker::server::{serve, Pool, PoolConfig};
use proxybroker::types::Proto;
use proxybroker::Credentials;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// A minimal SOCKS5 *server* that requires RFC 1929 user/pass, reports the credentials it saw, and
/// completes a CONNECT so `negotiate` returns Ok.
async fn mock_socks5_server() -> (SocketAddr, oneshot::Receiver<(String, String)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Greeting: VER=05, NMETHODS, methods[].
        let mut hdr = [0u8; 2];
        sock.read_exact(&mut hdr).await.unwrap();
        let mut methods = vec![0u8; hdr[1] as usize];
        sock.read_exact(&mut methods).await.unwrap();
        assert!(
            methods.contains(&0x02),
            "client must offer user/pass (0x02)"
        );
        sock.write_all(&[0x05, 0x02]).await.unwrap(); // select user/pass

        // RFC 1929: VER=01, ULEN, user, PLEN, pass.
        let mut vu = [0u8; 2];
        sock.read_exact(&mut vu).await.unwrap();
        let mut user = vec![0u8; vu[1] as usize];
        sock.read_exact(&mut user).await.unwrap();
        let mut pl = [0u8; 1];
        sock.read_exact(&mut pl).await.unwrap();
        let mut pass = vec![0u8; pl[0] as usize];
        sock.read_exact(&mut pass).await.unwrap();
        sock.write_all(&[0x01, 0x00]).await.unwrap(); // auth success

        // CONNECT request: VER=05, CMD, RSV, ATYP, addr, port.
        let mut req = [0u8; 4];
        sock.read_exact(&mut req).await.unwrap();
        let addr_len = match req[3] {
            0x01 => 4,
            0x04 => 16,
            0x03 => {
                let mut l = [0u8; 1];
                sock.read_exact(&mut l).await.unwrap();
                l[0] as usize
            }
            _ => 0,
        };
        let mut rest = vec![0u8; addr_len + 2];
        sock.read_exact(&mut rest).await.unwrap();
        // Success reply: bound addr 0.0.0.0:0.
        sock.write_all(&[0x05, 0, 0, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();

        let _ = tx.send((
            String::from_utf8_lossy(&user).into_owned(),
            String::from_utf8_lossy(&pass).into_owned(),
        ));
        let _ = sock.read(&mut [0u8; 16]).await; // hold the tunnel briefly
    });
    (addr, rx)
}

#[tokio::test]
async fn socks5_upstream_auth_sends_rfc1929() {
    let (addr, rx) = mock_socks5_server().await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    let target = Target {
        host: "1.2.3.4".into(),
        ip: Some("1.2.3.4".parse().unwrap()),
        port: 80,
    };
    let creds = Credentials {
        username: "user".into(),
        password: "pass".into(),
    };
    let res = negotiate(
        Proto::Socks5,
        tcp,
        &target,
        Duration::from_secs(3),
        Some(&creds),
    )
    .await;
    assert!(
        res.is_ok(),
        "socks5 negotiation with creds should succeed: {res:?}"
    );
    let (u, p) = tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .expect("server should report creds")
        .unwrap();
    assert_eq!(
        (u.as_str(), p.as_str()),
        ("user", "pass"),
        "RFC 1929 creds mismatch"
    );
}

/// A mock HTTP upstream that reports the exact request bytes it received, then answers 200.
async fn mock_capture() -> (SocketAddr, oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap_or(0);
        let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
            .await;
        let _ = sock.flush().await;
    });
    (addr, rx)
}

async fn http_proxy_with_auth(addr: SocketAddr, auth: Option<Credentials>) -> Proxy {
    let mut p = Proxy::new(addr.ip(), addr.port(), BTreeSet::from([Proto::Http]));
    p.add_type(Proto::Http, None);
    match auth {
        Some(c) => p.with_auth(c),
        None => p,
    }
}

async fn forwarded_request(auth: Option<Credentials>) -> String {
    let (up, rx) = mock_capture().await;
    let pool = Pool::from_proxies(
        vec![http_proxy_with_auth(up, auth).await],
        PoolConfig::default(),
    );
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
        None,
    )
    .await
    .unwrap();
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"GET http://1.2.3.4/ HTTP/1.1\r\nHost: 1.2.3.4\r\n\r\n")
        .await
        .unwrap();
    let _ = client.read(&mut [0u8; 64]).await; // drain the response
    tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .expect("upstream should report the forwarded request")
        .unwrap()
}

#[tokio::test]
async fn http_forward_injects_proxy_authorization() {
    let creds = Credentials {
        username: "user".into(),
        password: "pass".into(),
    };
    let fwd = forwarded_request(Some(creds)).await;
    // Basic base64("user:pass") == dXNlcjpwYXNz, inserted right after the request line.
    assert!(
        fwd.starts_with(
            "GET http://1.2.3.4/ HTTP/1.1\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n"
        ),
        "{fwd:?}"
    );
}

#[tokio::test]
async fn no_creds_omits_proxy_authorization() {
    let fwd = forwarded_request(None).await;
    assert!(!fwd.contains("Proxy-Authorization"), "{fwd:?}");
}

#[tokio::test]
async fn connect_ack_carries_x_proxy_info() {
    // B7: a CONNECT client gets X-Proxy-Info on the `200 Connection established` head, pre-tunnel.
    // A SOCKS5 upstream (with auth, so it offers 0x02) serves the Https scheme and completes the
    // tunnel; the relay then writes the ACK to the client.
    let (up, _rx) = mock_socks5_server().await;
    let mut proxy = Proxy::new(up.ip(), up.port(), BTreeSet::from([Proto::Socks5]));
    proxy.add_type(Proto::Socks5, None);
    let proxy = proxy.with_auth(Credentials {
        username: "u".into(),
        password: "p".into(),
    });
    let pool = Pool::from_proxies(vec![proxy], PoolConfig::default());
    let resolver = Arc::new(Resolver::new(Duration::from_secs(3)).unwrap());
    let handle = serve(
        "127.0.0.1:0".parse().unwrap(),
        pool,
        resolver,
        Duration::from_secs(3),
        0,
        1024,
        None,
    )
    .await
    .unwrap();
    let mut client = TcpStream::connect(handle.local_addr()).await.unwrap();
    client
        .write_all(b"CONNECT 1.2.3.4:443 HTTP/1.1\r\nHost: 1.2.3.4:443\r\n\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
        .await
        .expect("ack should not time out")
        .unwrap();
    let head = String::from_utf8_lossy(&buf[..n]);
    assert!(head.contains("200 Connection established"), "{head:?}");
    assert!(
        head.contains(&format!("X-Proxy-Info: {}:{}", up.ip(), up.port())),
        "{head:?}"
    );
}
