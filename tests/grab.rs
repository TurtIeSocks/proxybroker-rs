//! Offline integration test for `Broker::grab`: dedup, limit, and cancellation, all driven
//! by local mock provider servers so nothing touches the internet (constraint C5).

use futures_util::StreamExt;
use proxybroker::broker::{Broker, GrabQuery};
use proxybroker::provider::ProviderSpec;
use proxybroker::types::Proto;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serve `body` to every request until dropped. Returns its URL.
async fn mock_provider(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
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
async fn grab_streams_deduplicated_proxies_from_all_providers() {
    // Two providers, overlapping on 8.8.8.8:80 — it must appear once.
    let (u1, _a) = mock_provider("8.8.8.8:80\n1.1.1.1:3128\n").await;
    let (u2, _b) = mock_provider("8.8.8.8:80\n9.9.9.9:53\n").await;

    let broker = Broker::builder()
        .providers(vec![
            ProviderSpec::new(&u1, &[Proto::Http]),
            ProviderSpec::new(&u2, &[Proto::Http]),
        ])
        .build();

    let proxies: Vec<_> = broker.grab(GrabQuery::default()).collect().await;

    let mut addrs: Vec<String> = proxies.iter().map(|p| p.addr()).collect();
    addrs.sort();
    assert_eq!(addrs, ["1.1.1.1:3128", "8.8.8.8:80", "9.9.9.9:53"]);
}

#[tokio::test]
async fn grab_respects_the_limit() {
    let (u1, _a) = mock_provider("1.1.1.1:80\n2.2.2.2:80\n3.3.3.3:80\n4.4.4.4:80\n").await;
    let broker = Broker::builder()
        .providers(vec![ProviderSpec::new(&u1, &[Proto::Http])])
        .build();

    let proxies: Vec<_> = broker
        .grab(GrabQuery {
            countries: None,
            limit: Some(2),
        })
        .collect()
        .await;

    assert_eq!(proxies.len(), 2);
}

#[tokio::test]
async fn dropping_the_stream_stops_grabbing() {
    let (u1, _a) = mock_provider("1.1.1.1:80\n2.2.2.2:80\n3.3.3.3:80\n").await;
    let broker = Broker::builder()
        .providers(vec![ProviderSpec::new(&u1, &[Proto::Http])])
        .build();

    let mut stream = broker.grab(GrabQuery::default());
    // Take one, then drop the stream. The source task must not panic or hang; dropping the
    // receiver closes the channel and the task exits on its next send.
    let first = stream.next().await;
    assert!(first.is_some());
    drop(stream);
    // Give the runtime a tick for the source task to observe the closed channel.
    tokio::task::yield_now().await;
}
