//! D2 — durable cross-run proxy state. A backend-agnostic [`Store`] trait plus the bundled
//! `store-sqlite` backend (`SqliteStore`).
//!
//! Waves 1–6 keep a proxy alive only for one process. Wave 1's file `--save`/`--load` (C2) restores
//! *which* proxies to try, but a flat snapshot cannot accumulate an EWMA across runs or record
//! "last seen 3 days ago" — a read-modify-write per check. This escalates to a real store **only**
//! for that history, behind feature gates so pure-library users stay zero-dep.
//!
//! The **write seam** is already a plain `Arc<dyn Fn(&Proxy)>` observer on the broker
//! ([`crate::broker::CheckObserver`]), so any backend plugs in via `Broker::with_observer` + a
//! warm-start `Store::load`. This trait just names the contract; the bundled SQLite backend is the
//! reference impl. A `store-redis` backend is planned for Wave 9.

use crate::error::Error;
use crate::proxy::Proxy;

/// The canonical EWMA fold (`alpha·sample + (1-alpha)·prev`). SqliteStore's SQL and RedisStore's Lua
/// both replicate this with the literal alpha = 0.3; this is the single Rust definition tests measure
/// against. `#[cfg(test)]`: neither backend calls this at runtime (the fold happens in SQL/Lua so the
/// upsert stays one atomic round-trip) — it exists solely as the oracle `fold_ewma_arithmetic` pins.
#[cfg(test)]
pub(crate) fn fold_ewma(prev: f64, sample: f64, alpha: f64) -> f64 {
    alpha * sample + (1.0 - alpha) * prev
}

/// The persistence contract (D2): remember proxies across runs. Implemented by the bundled
/// [`SqliteStore`] (`store-sqlite`); a Redis backend is planned for Wave 9. Library users can impl
/// this for any store and wire it via `Broker::with_observer` + [`Store::load`].
pub trait Store: Send + Sync {
    /// Fold one finished proxy's current-session outcome into its durable record (upsert on
    /// `(host, port)`): accumulate requests/errors, move the success/latency EWMAs, bump uptime.
    fn upsert(&self, proxy: &Proxy) -> Result<(), Error>;

    /// Reconstruct every remembered proxy for a warm start, seeding priority-relevant aggregates.
    fn load(&self) -> Result<Vec<Proxy>, Error>;
}

#[cfg(feature = "store-sqlite")]
mod sqlite {
    use super::Store;
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
    // `0.3`/`0.7` literals in the upsert SQL (a constant, not a flag, per the lazy principle).

    /// The bundled SQLite [`Store`] backend: one connection behind a `Mutex` (rusqlite `Connection`
    /// is `Send` but not `Sync`; the mutex makes `Arc<SqliteStore>` shareable across tasks). One
    /// denormalized table, no per-attempt rows — we only ever read aggregates.
    pub struct SqliteStore {
        conn: Mutex<Connection>,
    }

    impl SqliteStore {
        /// Open (creating if absent) the state DB at `path`, running any pending migration and
        /// setting `PRAGMA user_version` to [`SCHEMA_VERSION`].
        pub fn open(path: impl AsRef<std::path::Path>) -> Result<SqliteStore, Error> {
            let conn = Connection::open(path).map_err(db)?;
            // The D3 re-checker opens a SECOND connection to this same DB alongside the D2 upsert
            // observer. WAL lets a writer and readers coexist and, crucially, avoids the
            // rollback-journal writer deadlock that returns SQLITE_BUSY *immediately* (un-retryable
            // by busy_timeout); the timeout then makes a contended writer wait its turn rather than
            // error. Without this a dropped write silently loses a re-check outcome (the scheduler
            // discards upsert errors).
            conn.busy_timeout(std::time::Duration::from_secs(5))
                .map_err(db)?;
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(db)?;
            migrate(&conn)?;
            Ok(SqliteStore {
                conn: Mutex::new(conn),
            })
        }
    }

    impl Store for SqliteStore {
        fn upsert(&self, proxy: &Proxy) -> Result<(), Error> {
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
                     -- avg_latency keeps the prior value on a zero sample — otherwise a transient
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

        fn load(&self) -> Result<Vec<Proxy>, Error> {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT host, port, types, requests, errors, avg_latency, ewma_success \
                     FROM proxies",
                )
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
                        row.get::<_, f64>(6)?,
                    ))
                })
                .map_err(db)?;
            let mut out = Vec::new();
            for row in rows {
                let (host, port, types_json, requests, errors, avg, ewma) = row.map_err(db)?;
                let Ok(host) = host.parse() else { continue }; // skip an unparseable stored host
                out.push(
                    Proxy::restored(
                        host,
                        port as u16,
                        types_from_json(&types_json),
                        requests as u32,
                        errors as u32,
                        avg,
                    )
                    .with_ewma(Some(ewma)),
                );
            }
            Ok(out)
        }
    }

    fn migrate(conn: &Connection) -> Result<(), Error> {
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(db)?;
        // A `match` on the read version, not a migration framework. Add a `1 => ...` arm on the
        // next schema change and bump SCHEMA_VERSION.
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

    /// Serialize confirmed types the shape the `Proxy` JSON uses: `[{"type":"HTTP","level":"High"}]`.
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
}

#[cfg(feature = "store-sqlite")]
pub use sqlite::{SqliteStore, SCHEMA_VERSION};

#[cfg(feature = "persist")]
mod memory {
    //! An in-memory [`Store`](super::Store) — the D2 contract with no database (D3). Gated on
    //! `persist` alone (no backend dependency), so `serve --recheck` can keep an adaptive re-check's
    //! decay/score bookkeeping without a `--state` SQLite/Redis backend. The state lives only for
    //! the process; nothing survives a restart.
    //!
    //! [`MemoryStore::upsert`](super::MemoryStore::upsert) reproduces
    //! [`sqlite::SqliteStore`](super::sqlite)'s `ON CONFLICT` fold byte-for-byte — alpha = 0.3 new +
    //! 0.7 prior on the success/latency EWMAs, accumulate requests/errors/uptime, and keep prior
    //! confirmed types on a failing (empty) sample — so a MemoryStore-backed re-check decays
    //! identically to the durable path.

    use super::Store;
    use crate::error::Error;
    use crate::proxy::Proxy;
    use crate::types::{AnonLevel, Proto};
    use std::collections::{BTreeMap, HashMap};
    use std::net::IpAddr;
    use std::sync::Mutex;

    /// One remembered proxy's durable aggregates — the same columns [`sqlite::SqliteStore`](super::sqlite)
    /// keeps per `proxies` row, folded in memory instead of in SQL. `ewma_success` is tracked for a
    /// faithful fold even though `load` (like both durable backends) reconstructs via
    /// [`Proxy::restored`] and so never surfaces it.
    struct Record {
        types: BTreeMap<Proto, Option<AnonLevel>>,
        requests: u32,
        errors: u32,
        ewma_success: f64,
        avg_latency: f64,
        uptime_checks: u64,
    }

    /// The in-memory [`Store`]: a `HashMap<(host, port), Record>` behind a `Mutex` (so `Arc<MemoryStore>`
    /// is shareable across the re-check tasks). No backend dependency — `serve --recheck` without
    /// `--state` folds into this instead of a database.
    #[derive(Default)]
    pub struct MemoryStore {
        map: Mutex<HashMap<(IpAddr, u16), Record>>,
    }

    impl MemoryStore {
        /// A fresh, empty in-memory store.
        pub fn new() -> MemoryStore {
            MemoryStore::default()
        }
    }

    impl Store for MemoryStore {
        fn upsert(&self, proxy: &Proxy) -> Result<(), Error> {
            let sample = f64::from(u8::from(proxy.is_working())); // 1.0 working, 0.0 not
            let uptime = u64::from(proxy.is_working());
            let errors_total: u32 = proxy.errors().values().sum();
            let latency = proxy.avg_resp_time();
            let mut map = self.map.lock().unwrap();
            map.entry((proxy.host, proxy.port))
                .and_modify(|rec| {
                    // CONFLICT arm — mirror SqliteStore's `UPDATE SET`.
                    if !proxy.types().is_empty() {
                        // Keep the prior confirmed types on a failing (empty) sample, just as
                        // avg_latency keeps its prior on a zero sample — else a transient failure
                        // erases a proxy's types and makes it unselectable on warm start.
                        rec.types = proxy.types().clone();
                    }
                    rec.requests = rec.requests.saturating_add(proxy.requests());
                    rec.errors = rec.errors.saturating_add(errors_total);
                    rec.ewma_success = 0.3 * sample + 0.7 * rec.ewma_success;
                    if latency > 0.0 {
                        rec.avg_latency = 0.3 * latency + 0.7 * rec.avg_latency;
                    }
                    rec.uptime_checks = rec.uptime_checks.saturating_add(uptime);
                })
                .or_insert_with(|| Record {
                    // INSERT arm — seed from the first sample (SqliteStore's VALUES row): the raw
                    // latency (even 0) and the raw sample as the initial EWMA.
                    types: proxy.types().clone(),
                    requests: proxy.requests(),
                    errors: errors_total,
                    ewma_success: sample,
                    avg_latency: latency,
                    uptime_checks: uptime,
                });
            Ok(())
        }

        fn load(&self) -> Result<Vec<Proxy>, Error> {
            let map = self.map.lock().unwrap();
            let out = map
                .iter()
                .map(|(&(host, port), rec)| {
                    Proxy::restored(
                        host,
                        port,
                        rec.types.clone(),
                        rec.requests,
                        rec.errors,
                        rec.avg_latency,
                    )
                    .with_ewma(Some(rec.ewma_success))
                })
                .collect();
            Ok(out)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::BTreeMap;

        /// A working proxy (non-empty types → `is_working()` true → success sample 1.0) with the
        /// given aggregates. `Proxy::restored` seeds the private stat fields the fold reads.
        fn working(host: &str, port: u16, requests: u32, errors: u32, avg: f64) -> Proxy {
            let mut types = BTreeMap::new();
            types.insert(Proto::Http, None);
            Proxy::restored(host.parse().unwrap(), port, types, requests, errors, avg)
        }

        /// A failing proxy (empty types → `is_working()` false → success sample 0.0).
        fn failing(host: &str, port: u16, requests: u32, errors: u32) -> Proxy {
            Proxy::restored(
                host.parse().unwrap(),
                port,
                BTreeMap::new(),
                requests,
                errors,
                0.0,
            )
        }

        fn ewma_of(store: &MemoryStore, host: &str, port: u16) -> f64 {
            store.map.lock().unwrap()[&(host.parse().unwrap(), port)].ewma_success
        }

        #[test]
        fn upsert_then_load_round_trips() {
            let store = MemoryStore::new();
            store
                .upsert(&working("1.2.3.4", 8080, 3, 1, 250.0))
                .unwrap();
            let loaded = store.load().unwrap();
            assert_eq!(
                loaded.len(),
                1,
                "one upserted proxy round-trips through load"
            );
            let p = &loaded[0];
            assert_eq!(p.host, "1.2.3.4".parse::<IpAddr>().unwrap());
            assert_eq!(p.port, 8080);
            assert_eq!(p.requests(), 3);
            assert!(p.types().contains_key(&Proto::Http));
            // First upsert seeds avg_latency from the raw sample (SqliteStore's VALUES row).
            assert!((p.avg_resp_time() - 250.0).abs() < 1e-9);
        }

        #[test]
        fn first_upsert_seeds_ewma_from_sample() {
            let store = MemoryStore::new();
            store
                .upsert(&working("1.2.3.4", 8080, 1, 0, 100.0))
                .unwrap();
            assert!(
                (ewma_of(&store, "1.2.3.4", 8080) - 1.0).abs() < 1e-12,
                "a working first sample seeds ewma = 1.0"
            );
        }

        #[test]
        fn second_upsert_folds_ewma_alpha_0_3() {
            // working (1.0) then failing (0.0): ewma = 0.3*0.0 + 0.7*1.0 = 0.7, matching
            // SqliteStore's `0.3*excluded + 0.7*prior` and the `fold_ewma` oracle.
            let store = MemoryStore::new();
            store
                .upsert(&working("1.2.3.4", 8080, 1, 0, 100.0))
                .unwrap();
            store.upsert(&failing("1.2.3.4", 8080, 1, 1)).unwrap();
            assert!((ewma_of(&store, "1.2.3.4", 8080) - 0.7).abs() < 1e-12);
            let rec = store.map.lock().unwrap();
            let r = &rec[&("1.2.3.4".parse::<IpAddr>().unwrap(), 8080)];
            assert_eq!(r.requests, 2, "requests accumulate");
            assert_eq!(r.errors, 1, "errors accumulate");
            assert!(
                r.types.contains_key(&Proto::Http),
                "a failing (empty) sample must not erase confirmed types"
            );
            assert!(
                (r.avg_latency - 100.0).abs() < 1e-9,
                "a zero-latency sample leaves avg_latency unchanged (CASE latency>0)"
            );
        }

        #[test]
        fn latency_ewma_folds_with_same_alpha() {
            // Two positive-latency working samples: 0.3*200 + 0.7*100 = 130, observable via load().
            let store = MemoryStore::new();
            store
                .upsert(&working("9.9.9.9", 3128, 1, 0, 100.0))
                .unwrap();
            store
                .upsert(&working("9.9.9.9", 3128, 1, 0, 200.0))
                .unwrap();
            let p = store.load().unwrap().into_iter().next().unwrap();
            assert!((p.avg_resp_time() - 130.0).abs() < 1e-9);
        }
    }
}

#[cfg(feature = "persist")]
pub use memory::MemoryStore;

#[cfg(feature = "store-redis")]
mod redis {
    //! The bundled Redis [`Store`](super::Store) backend (Wave 9). One blocking
    //! `redis::Connection` behind a `Mutex`, mirroring [`sqlite::SqliteStore`](super::sqlite)'s
    //! connection handling — `redis::Connection` is `Send` but not `Sync`. The atomic upsert fold
    //! that `SqliteStore` does with SQL `ON CONFLICT` is a Lua `EVAL` here: Redis runs a script
    //! single-threaded, so [`UPSERT_LUA`] is race-free without a WATCH/MULTI transaction.

    use super::Store;
    use crate::error::Error;
    use crate::proxy::Proxy;
    use crate::types::{AnonLevel, Proto};
    use ::redis::Commands;
    use std::collections::BTreeMap;
    use std::net::IpAddr;
    use std::sync::Mutex;

    /// Per-proxy Redis key: a hash of the durable fields (`host`, `port`, `types`, `requests`,
    /// `errors`, `ewma_success`, `avg_latency`, `first_seen`, `last_seen`, `uptime_checks`).
    fn proxy_key(host: &IpAddr, port: u16) -> String {
        format!("proxybroker:proxy:{host}:{port}")
    }

    /// The set of every known `"host:port"` member — how `load` enumerates proxies without a
    /// Redis `SCAN` (which would need a cursor loop, and could double/miss keys under concurrent
    /// writes to a live set).
    const SET_KEY: &str = "proxybroker:proxies";

    /// Schema marker key, checked/set on `RedisStore::open` — a lighter analogue of SQLite's
    /// `PRAGMA user_version`.
    const SCHEMA_KEY: &str = "proxybroker:schema";

    /// Current schema version string, written to [`SCHEMA_KEY`].
    const SCHEMA_VERSION: &str = "1";

    /// The atomic upsert: fold one check's outcome into the proxy hash and register it in
    /// [`SET_KEY`]. Reproduces `SqliteStore::upsert`'s `ON CONFLICT` arithmetic byte-for-byte
    /// (alpha = 0.3), including the confirmed-types guard — a failing re-check's empty `"[]"`
    /// types must not erase previously confirmed types, or a transient failure makes a proxy
    /// unselectable on warm start. Redis runs Lua scripts single-threaded, so this whole fold is
    /// one atomic round-trip — no WATCH/MULTI needed even with two `RedisStore`s on a shared
    /// fleet upserting concurrently.
    ///
    /// `KEYS[1]` = proxy hash key, `KEYS[2]` = the proxies set key.
    /// `ARGV`: 1=host, 2=port, 3=types_json, 4=sample, 5=working, 6=latency, 7=now, 8=requests,
    /// 9=errors.
    const UPSERT_LUA: &str = r#"
local h = redis.call('HGETALL', KEYS[1])
local cur = {}
for i=1,#h,2 do cur[h[i]] = h[i+1] end
local function num(x) return tonumber(x) or 0 end
local first = (next(cur) == nil)
local sample, working, latency = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local ewma   = first and sample or (0.3*sample + 0.7*num(cur.ewma_success))
local avglat = first and latency or ((latency > 0) and (0.3*latency + 0.7*num(cur.avg_latency)) or num(cur.avg_latency))
local types  = (ARGV[3] ~= '[]') and ARGV[3] or (cur.types or ARGV[3])
redis.call('HSET', KEYS[1],
  'host', ARGV[1], 'port', ARGV[2], 'types', types,
  'requests', num(cur.requests) + tonumber(ARGV[8]),
  'errors',   num(cur.errors)   + tonumber(ARGV[9]),
  'ewma_success', ewma, 'avg_latency', avglat,
  'first_seen', first and ARGV[7] or (cur.first_seen or ARGV[7]),
  'last_seen', ARGV[7],
  'uptime_checks', num(cur.uptime_checks) + working)
redis.call('SADD', KEYS[2], ARGV[1] .. ':' .. ARGV[2])
return 1
"#;

    /// The bundled Redis [`Store`] backend: one blocking connection behind a `Mutex` (mirrors
    /// [`sqlite::SqliteStore`](super::sqlite::SqliteStore) — `redis::Connection` is `Send` but not
    /// `Sync`, so the mutex is what makes `Arc<RedisStore>` shareable). The key layout already
    /// namespaces everything under `proxybroker:*`, so there is no separate prefix field.
    pub struct RedisStore {
        conn: Mutex<::redis::Connection>,
    }

    impl RedisStore {
        /// Open a connection to `url` (`redis://host:port/db`), checking/setting the schema
        /// marker key ([`SCHEMA_KEY`]) — Redis has no `PRAGMA user_version`, so a plain string key
        /// stands in for SQLite's migration guard. A present-but-different value is a hard error:
        /// there is only one schema version so far, but this keeps a future bump from silently
        /// misreading old-shape hashes.
        pub fn open(url: &str) -> Result<RedisStore, Error> {
            let client = ::redis::Client::open(url).map_err(db)?;
            let mut conn = client.get_connection().map_err(db)?;
            let existing: Option<String> = conn.get(SCHEMA_KEY).map_err(db)?;
            if let Some(v) = &existing {
                if v != SCHEMA_VERSION {
                    return Err(Error::Persist(format!(
                        "redis store schema mismatch: found {v:?}, expected {SCHEMA_VERSION:?} \
                         — wipe the proxybroker:* keys or point --state at a fresh database"
                    )));
                }
            }
            let _: () = conn.set(SCHEMA_KEY, SCHEMA_VERSION).map_err(db)?;
            Ok(RedisStore {
                conn: Mutex::new(conn),
            })
        }
    }

    impl Store for RedisStore {
        fn upsert(&self, proxy: &Proxy) -> Result<(), Error> {
            let now = unix_now();
            let types_json = types_to_json(proxy.types());
            let sample = f64::from(u8::from(proxy.is_working())); // 1.0 working, 0.0 not
            let working = i64::from(proxy.is_working());
            let errors_total: u32 = proxy.errors().values().sum();
            let key = proxy_key(&proxy.host, proxy.port);

            let script = ::redis::Script::new(UPSERT_LUA);
            let mut conn = self.conn.lock().unwrap();
            // One atomic round-trip: Redis runs the whole fold single-threaded, so this is race-
            // free against a second `RedisStore` (possibly another process) upserting concurrently.
            let _: i64 = script
                .key(key)
                .key(SET_KEY)
                .arg(proxy.host.to_string())
                .arg(proxy.port)
                .arg(types_json)
                .arg(sample)
                .arg(working)
                .arg(proxy.avg_resp_time())
                .arg(now)
                .arg(proxy.requests())
                .arg(errors_total)
                .invoke(&mut *conn)
                .map_err(db)?;
            Ok(())
        }

        fn load(&self) -> Result<Vec<Proxy>, Error> {
            let mut conn = self.conn.lock().unwrap();
            let members: Vec<String> = conn.smembers(SET_KEY).map_err(db)?;
            let mut out = Vec::new();
            for member in members {
                // Members are `host:port`; `rsplit_once` takes the LAST colon, which is always the
                // port separator even for a bare (unbracketed) IPv6 host.
                let Some((host_s, port_s)) = member.rsplit_once(':') else {
                    tracing::warn!(
                        "redis store: malformed proxies-set member {member:?}, skipping"
                    );
                    continue;
                };
                let Ok(host) = host_s.parse::<IpAddr>() else {
                    tracing::warn!("redis store: unparseable host {host_s:?}, skipping");
                    continue;
                };
                let Ok(port) = port_s.parse::<u16>() else {
                    tracing::warn!("redis store: unparseable port {port_s:?}, skipping");
                    continue;
                };
                let hash: std::collections::HashMap<String, String> =
                    conn.hgetall(proxy_key(&host, port)).map_err(db)?;
                if hash.is_empty() {
                    // The set member survived a crash/partial write with no matching hash.
                    tracing::warn!("redis store: missing hash for {host}:{port}, skipping");
                    continue;
                }
                let requests = hash
                    .get("requests")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let errors = hash.get("errors").and_then(|s| s.parse().ok()).unwrap_or(0);
                let avg_latency = hash
                    .get("avg_latency")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let types_json = hash.get("types").map(String::as_str).unwrap_or("[]");
                let ewma = hash.get("ewma_success").and_then(|s| s.parse().ok());
                out.push(
                    Proxy::restored(
                        host,
                        port,
                        types_from_json(types_json),
                        requests,
                        errors,
                        avg_latency,
                    )
                    .with_ewma(ewma),
                );
            }
            Ok(out)
        }
    }

    fn db(e: ::redis::RedisError) -> Error {
        Error::Persist(e.to_string())
    }

    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Serialize confirmed types the shape the `Proxy` JSON uses: `[{"type":"HTTP","level":"High"}]`.
    /// Replicated from `sqlite::types_to_json` byte-for-byte (kept private to each backend module
    /// rather than extracted, so neither backend's internals move — see the task brief).
    fn types_to_json(types: &BTreeMap<Proto, Option<AnonLevel>>) -> String {
        let arr: Vec<_> = types
            .iter()
            .map(|(p, l)| {
                serde_json::json!({ "type": p.as_str(), "level": l.map(|x| x.as_str()).unwrap_or("") })
            })
            .collect();
        serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into())
    }

    /// Replicated from `sqlite::types_from_json` byte-for-byte.
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

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Pure — no server. Pins the key format and that the Lua literally carries the alpha =
        /// 0.3 fold (a typo turning `0.3`/`0.7` into e.g. `0.5`/`0.5` would silently change the
        /// smoothing without any other test catching it).
        #[test]
        fn redis_key_layout() {
            let host: IpAddr = "1.2.3.4".parse().unwrap();
            assert_eq!(proxy_key(&host, 8080), "proxybroker:proxy:1.2.3.4:8080");
            assert!(UPSERT_LUA.contains("0.3"));
            assert!(UPSERT_LUA.contains("0.7"));
        }
    }
}

#[cfg(feature = "store-redis")]
pub use redis::RedisStore;

#[cfg(test)]
mod tests {
    #[test]
    fn fold_ewma_arithmetic() {
        assert!((super::fold_ewma(0.0, 1.0, 0.3) - 0.3).abs() < 1e-12);
        assert!((super::fold_ewma(1.0, 0.0, 0.3) - 0.7).abs() < 1e-12);
        let e = super::fold_ewma(super::fold_ewma(0.0, 1.0, 0.3), 1.0, 0.3); // 0.51
        assert!((e - 0.51).abs() < 1e-12);
    }
}
