//! Pure core of the `proxybroker top` terminal dashboard (F4).
//!
//! [`DashboardState`] holds everything the dashboard needs to draw a frame; [`DashboardState::apply`]
//! folds in a fresh [`Pool`](crate::server::Pool) snapshot, [`render`] draws it with ratatui, and
//! [`DashboardState::on_key`] turns a keypress into a state mutation. All three are pure/offline —
//! no terminal, no I/O, no tokio — so they are unit-tested directly (`render` via ratatui's
//! `TestBackend`). The event loop, raw-mode terminal guard, and the `top` CLI subcommand that drive
//! these are a separate, non-pure controller layer built on top of this module.

use crate::proxy::Proxy;
use crate::server::PoolSnapshot;
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;

/// One row of the dashboard's proxy table — a display-ready projection of a [`Proxy`].
pub struct Row {
    pub addr: String,
    pub protos: String,
    pub error_rate: f64,
    pub resp_time: f64,
    pub country: String,
}

/// Which column [`DashboardState::rows`] is sorted by. `ErrorRate`/`RespTime` sort ascending
/// (best/fastest first); `Addr`/`Country` sort lexicographically.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Addr,
    ErrorRate,
    RespTime,
    Country,
}

/// Ring-buffer capacity for a proxy's response-time history (one sparkline's worth).
const RING_CAP: usize = 60;

/// Everything [`render`] needs to draw one frame. Rebuilt from a live `Pool` by [`Self::apply`];
/// mutated in place by [`Self::on_key`].
pub struct DashboardState {
    pub rows: Vec<Row>,
    pub snapshot: PoolSnapshot,
    pub sort: SortKey,
    /// Per-proxy response-time history, keyed by `(host, port)` so it survives a proxy dropping
    /// out and back into the pool. Capped at [`RING_CAP`] — oldest sample drops first.
    pub history: HashMap<(IpAddr, u16), VecDeque<f64>>,
    pub selected: usize,
}

impl Default for DashboardState {
    fn default() -> Self {
        DashboardState {
            rows: Vec::new(),
            snapshot: PoolSnapshot::default(),
            sort: SortKey::RespTime,
            history: HashMap::new(),
            selected: 0,
        }
    }
}

impl DashboardState {
    /// Rebuild [`Self::rows`] from a live pool snapshot: one [`Row`] per proxy, each proxy's
    /// current `avg_resp_time()` pushed onto its history ring, then re-sorted by [`Self::sort`]
    /// and [`Self::selected`] clamped to the new row count.
    pub fn apply(&mut self, proxies: &[Proxy], snapshot: PoolSnapshot) {
        self.rows = proxies
            .iter()
            .map(|p| Row {
                addr: p.addr(),
                protos: p
                    .types()
                    .keys()
                    .map(|proto| proto.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                error_rate: p.error_rate(),
                resp_time: p.avg_resp_time(),
                country: p.geo.as_ref().map(|c| c.code.clone()).unwrap_or_default(),
            })
            .collect();

        for p in proxies {
            let ring = self.history.entry((p.host, p.port)).or_default();
            ring.push_back(p.avg_resp_time());
            while ring.len() > RING_CAP {
                ring.pop_front();
            }
        }

        self.snapshot = snapshot;
        self.sort_rows();
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
    }

    /// Re-sort [`Self::rows`] by the current [`Self::sort`] key. Factored out of [`Self::apply`]
    /// so [`Self::on_key`] can re-sort without re-fetching the pool.
    fn sort_rows(&mut self) {
        match self.sort {
            SortKey::Addr => self.rows.sort_by(|a, b| a.addr.cmp(&b.addr)),
            SortKey::ErrorRate => self
                .rows
                .sort_by(|a, b| a.error_rate.total_cmp(&b.error_rate)),
            SortKey::RespTime => self
                .rows
                .sort_by(|a, b| a.resp_time.total_cmp(&b.resp_time)),
            SortKey::Country => self.rows.sort_by(|a, b| a.country.cmp(&b.country)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Pool, PoolConfig};
    use crate::types::Proto;
    use std::collections::BTreeSet;

    fn proxy(ip: &str, rt: f64) -> Proxy {
        let mut p = Proxy::new(ip.parse().unwrap(), 80, BTreeSet::new());
        p.add_type(Proto::Http, None);
        p.record_attempt(Some(rt), None); // a runtime so avg_resp_time reflects `rt`
        p
    }

    #[test]
    fn apply_snapshot_updates_rings_and_sorts() {
        let pool = Pool::from_proxies(
            vec![proxy("2.2.2.2", 0.9), proxy("1.1.1.1", 0.1)],
            PoolConfig::default(),
        );
        let mut st = DashboardState {
            sort: SortKey::RespTime,
            ..Default::default()
        };
        st.apply(&pool.proxies(), pool.snapshot());
        assert_eq!(st.rows[0].addr, "1.1.1.1:80", "fastest first under RespTime");
        st.apply(&pool.proxies(), pool.snapshot());
        assert_eq!(
            st.history[&("1.1.1.1".parse().unwrap(), 80)].len(),
            2,
            "ring grew per apply"
        );
    }
}
