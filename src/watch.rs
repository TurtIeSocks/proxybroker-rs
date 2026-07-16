//! E3 — live-reload the `serve --load` file (`all(server, watch)`).
//!
//! Watch the NDJSON pool file (Wave 1 C2) with `notify`; on change, re-parse it and reconcile the
//! running [`Pool`] — additions join, removals drop — without restarting the server. Shares the
//! `Pool` add/remove seam with the D3 re-checker, so both can mutate one live pool concurrently
//! (the pool's own mutex serializes them).

use crate::proxy::{read_ndjson, Proxy};
use crate::server::Pool;
use std::collections::BTreeSet;
use std::io::BufReader;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Coalesce a burst of write events into one re-parse: fire only after this long with no new event.
const DEBOUNCE: Duration = Duration::from_millis(250);
/// How often the drain task checks the event queue (notify is OS-driven, so a coarse poll is fine).
const POLL: Duration = Duration::from_millis(50);

/// Reconcile the pool to exactly `desired`: add proxies in `desired` but not pooled, remove pooled
/// proxies absent from `desired`. Pure over [`Pool`]'s public API — no I/O, no clock — so it is the
/// deterministic, directly-testable core of the watcher.
pub fn reconcile(pool: &Pool, desired: Vec<Proxy>) {
    let current = pool.addrs();
    let want: BTreeSet<(IpAddr, u16)> = desired.iter().map(|p| (p.host, p.port)).collect();
    for (host, port) in &current {
        if !want.contains(&(*host, *port)) {
            pool.remove_addr(*host, *port);
        }
    }
    for proxy in desired {
        if !current.contains(&(proxy.host, proxy.port)) {
            pool.add(proxy);
        }
    }
}

/// Handle to a running watcher; `shutdown` or drop stops it (and releases the OS watch).
pub struct WatchHandle {
    // Dropping the watcher unregisters the OS watch, so it must outlive the drain task — hold it.
    _watcher: notify::RecommendedWatcher,
    cancel: CancellationToken,
}

impl WatchHandle {
    /// Stop watching.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Spawn a filesystem watcher on `path`; on each debounced change, re-parse the NDJSON file (the C2
/// loader) and [`reconcile`] the pool. A parse error (e.g. a half-written file) is logged and the
/// pool left untouched — a bad write never empties it. Stops when the returned handle is dropped or
/// [`WatchHandle::shutdown`] is called.
pub fn spawn_watch(pool: Arc<Pool>, path: PathBuf) -> std::io::Result<WatchHandle> {
    use notify::{RecursiveMode, Watcher};

    let cancel = CancellationToken::new();

    let (tx, rx) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = tx.send(res);
    })
    .map_err(to_io)?;

    // Watch the parent directory, not the file: editors replace-on-save (remove+create of the path),
    // which a file-level watch would miss. Filter events down to our target by file name — notify
    // reports canonical absolute paths (e.g. /private/tmp on macOS), so compare names, not paths.
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let dir = dir.unwrap_or_else(|| Path::new("."));
    watcher
        .watch(dir, RecursiveMode::NonRecursive)
        .map_err(to_io)?;

    let target = path.file_name().map(|n| n.to_os_string());
    let task_cancel = cancel.clone();
    tokio::spawn(async move {
        let mut pending: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = task_cancel.cancelled() => break,
                _ = tokio::time::sleep(POLL) => {}
            }
            // Drain everything queued since the last tick; note if any event touched our file.
            let mut touched = false;
            while let Ok(res) = rx.try_recv() {
                if let Ok(event) = res {
                    if event
                        .paths
                        .iter()
                        .any(|p| p.file_name().map(|n| n.to_os_string()) == target)
                    {
                        touched = true;
                    }
                }
            }
            if touched {
                pending = Some(Instant::now()); // (re)start the debounce window
            }
            if let Some(since) = pending {
                if since.elapsed() >= DEBOUNCE {
                    pending = None;
                    match reload(&path) {
                        Ok(desired) => reconcile(&pool, desired),
                        Err(e) => tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "watch: re-parse failed, keeping current pool"
                        ),
                    }
                }
            }
        }
    });

    Ok(WatchHandle {
        _watcher: watcher,
        cancel,
    })
}

/// Re-run the C2 NDJSON loader over the file.
fn reload(path: &Path) -> std::io::Result<Vec<Proxy>> {
    read_ndjson(BufReader::new(std::fs::File::open(path)?))
}

fn to_io(e: notify::Error) -> std::io::Error {
    std::io::Error::other(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::PoolConfig;
    use crate::types::Proto;

    fn proxy(ip: &str, port: u16) -> Proxy {
        Proxy::new(ip.parse().unwrap(), port, BTreeSet::from([Proto::Http]))
    }

    #[test]
    fn reconcile_removes_then_adds_disjoint_sets() {
        let pool = Pool::from_proxies(vec![proxy("1.1.1.1", 1)], PoolConfig::default());
        reconcile(&pool, vec![proxy("2.2.2.2", 2)]);
        assert_eq!(
            pool.addrs(),
            BTreeSet::from([("2.2.2.2".parse().unwrap(), 2)])
        );
    }
}
