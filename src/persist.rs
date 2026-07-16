//! D2 — durable cross-run proxy state in one SQLite table (`persist` feature).
//!
//! Waves 1–6 keep a proxy alive only for one process. Wave 1's file `--save`/`--load` (C2) restores
//! *which* proxies to try, but a flat snapshot cannot accumulate an EWMA across runs or record
//! "last seen 3 days ago" — a read-modify-write per check. This escalates to SQLite **only** for
//! that history, one denormalized table (no per-attempt rows — we only ever read aggregates), and
//! only behind a feature gate so pure-library users stay zero-dep.

use crate::error::Error;
use crate::proxy::Proxy;
use crate::types::{AnonLevel, Proto};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Current on-disk schema version, written to `PRAGMA user_version`. Bump + add a migration arm
/// when the single table changes.
pub const SCHEMA_VERSION: i64 = 1;

// EWMA smoothing `ALPHA = 0.3` for the rolling success/latency probabilities — inlined as the
// `0.3`/`0.7` literals in the upsert SQL below (a constant, not a flag, per the lazy principle).

/// The durable state store: one SQLite connection behind a `Mutex` (rusqlite `Connection` is `Send`
/// but not `Sync`; the mutex makes `Arc<Store>` shareable across tasks).
pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    /// Open (creating if absent) the state DB at `path`, running any pending migration and setting
    /// `PRAGMA user_version` to [`SCHEMA_VERSION`].
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Store, Error> {
        let conn = Connection::open(path).map_err(db)?;
        migrate(&conn)?;
        Ok(Store {
            conn: Mutex::new(conn),
        })
    }

    /// Fold one finished proxy's current-session outcome into its durable row (upsert on
    /// `(host, port)`): accumulate requests/errors, move the success + latency EWMAs toward this
    /// session's sample, bump `uptime_checks` when working, set `last_seen = now`.
    pub fn upsert(&self, proxy: &Proxy) -> Result<(), Error> {
        let now = unix_now();
        let types_json = types_to_json(proxy.types());
        let sample = f64::from(u8::from(proxy.is_working())); // 1.0 working, 0.0 not
        let errors_total: u32 = proxy.errors().values().sum();
        let uptime = i64::from(proxy.is_working());
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO proxies
                 (host, port, types, requests, errors, ewma_success, avg_latency,
                  first_seen, last_seen, uptime_checks)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9)
             ON CONFLICT(host, port) DO UPDATE SET
                 -- Keep the prior confirmed types on a failing re-check (empty sample), just as
                 -- avg_latency below keeps the prior value on a zero sample — otherwise a transient
                 -- failure would erase a proxy's types and make it unselectable on warm start.
                 types        = CASE WHEN excluded.types <> '[]'
                                     THEN excluded.types ELSE proxies.types END,
                 requests     = proxies.requests + excluded.requests,
                 errors       = proxies.errors + excluded.errors,
                 ewma_success = 0.3 * excluded.ewma_success + 0.7 * proxies.ewma_success,
                 avg_latency  = CASE WHEN excluded.avg_latency > 0
                                     THEN 0.3 * excluded.avg_latency + 0.7 * proxies.avg_latency
                                     ELSE proxies.avg_latency END,
                 last_seen    = excluded.last_seen,
                 uptime_checks = proxies.uptime_checks + excluded.uptime_checks",
            params![
                proxy.host.to_string(),
                proxy.port,
                types_json,
                proxy.requests(),
                errors_total,
                sample,
                proxy.avg_resp_time(),
                now,
                uptime,
            ],
        )
        .map_err(db)?;
        Ok(())
    }

    /// Reconstruct every remembered proxy for a warm start, seeding priority-relevant aggregates
    /// (requests, errors, avg latency) and confirmed types onto each [`Proxy`] via
    /// [`Proxy::restored`].
    pub fn load(&self) -> Result<Vec<Proxy>, Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT host, port, types, requests, errors, avg_latency FROM proxies")
            .map_err(db)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, f64>(5)?,
                ))
            })
            .map_err(db)?;
        let mut out = Vec::new();
        for row in rows {
            let (host, port, types_json, requests, errors, avg) = row.map_err(db)?;
            let Ok(host) = host.parse() else { continue }; // skip an unparseable stored host
            out.push(Proxy::restored(
                host,
                port as u16,
                types_from_json(&types_json),
                requests as u32,
                errors as u32,
                avg,
            ));
        }
        Ok(out)
    }
}

fn migrate(conn: &Connection) -> Result<(), Error> {
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(db)?;
    // A `match` on the read version, not a migration framework. Add a `1 => ...` arm on the next
    // schema change and bump SCHEMA_VERSION.
    if version < 1 {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS proxies (
                 host          TEXT NOT NULL,
                 port          INTEGER NOT NULL,
                 types         TEXT NOT NULL,
                 requests      INTEGER NOT NULL DEFAULT 0,
                 errors        INTEGER NOT NULL DEFAULT 0,
                 ewma_success  REAL NOT NULL DEFAULT 0.0,
                 avg_latency   REAL NOT NULL DEFAULT 0.0,
                 first_seen    INTEGER NOT NULL,
                 last_seen     INTEGER NOT NULL,
                 uptime_checks INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (host, port)
             );
             PRAGMA user_version = 1;",
        )
        .map_err(db)?;
    }
    let _ = SCHEMA_VERSION; // referenced so a bump without a new arm is a visible reminder
    Ok(())
}

fn db(e: rusqlite::Error) -> Error {
    Error::Persist(e.to_string())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Serialize confirmed types the same shape the `Proxy` JSON uses: `[{"type":"HTTP","level":"High"}]`.
fn types_to_json(types: &BTreeMap<Proto, Option<AnonLevel>>) -> String {
    let arr: Vec<_> = types
        .iter()
        .map(|(p, l)| {
            serde_json::json!({ "type": p.as_str(), "level": l.map(|x| x.as_str()).unwrap_or("") })
        })
        .collect();
    serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into())
}

fn types_from_json(s: &str) -> BTreeMap<Proto, Option<AnonLevel>> {
    let mut map = BTreeMap::new();
    let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(s) else {
        return map;
    };
    for v in arr {
        let Some(t) = v["type"].as_str() else {
            continue;
        };
        let Ok(proto) = t.parse::<Proto>() else {
            continue;
        };
        let level = v["level"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<AnonLevel>().ok());
        map.insert(proto, level);
    }
    map
}
