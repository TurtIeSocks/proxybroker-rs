# Persistence (`--state`)

By default proxybroker-rs keeps a proxy's history for a single process. A flat
`find --save`/`serve --load` snapshot restores *which* proxies to try, but it
cannot accumulate a success EWMA across runs or record "last seen 3 days ago".
Durable cross-run state (internally "D2") escalates to a real store **only** for
that history, behind feature gates so pure-library users stay zero-dependency.
The relevant module is `proxybroker::persist`.

## The `Store` trait

Every backend implements one small, synchronous contract:

```rust
pub trait Store: Send + Sync {
    /// Fold one finished proxy's current-session outcome into its durable
    /// record (upsert on `(host, port)`): accumulate requests/errors, move the
    /// success/latency EWMAs, bump uptime.
    fn upsert(&self, proxy: &Proxy) -> Result<(), Error>;

    /// Reconstruct every remembered proxy for a warm start, seeding
    /// priority-relevant aggregates.
    fn load(&self) -> Result<Vec<Proxy>, Error>;
}
```

The trait itself lives behind the `persist` feature and pulls in no backend
dependency. A pure-library user can implement `Store` for any backend of their
own; the `store-sqlite` and `store-redis` features each provide a concrete one.

## The observer hook

The write seam is already a plain observer on the broker — a
`CheckObserver`, i.e. `Arc<dyn Fn(&Proxy) + Send + Sync>` — so any `Store`
plugs in without new machinery. You install it with `Broker::builder()` /
`with_observer`, and warm-start the pool from `Store::load`:

```rust
use std::sync::Arc;
use proxybroker::persist::{SqliteStore, Store};
use proxybroker::Broker;

let store = Arc::new(SqliteStore::open("proxies.db")?);

// Warm start: reconstruct remembered proxies before find runs.
let history = store.load()?;

// Fold every checked proxy's outcome back into the store as it finishes.
let upsert = store.clone();
let broker = Broker::builder()
    .with_observer(Some(Arc::new(move |p| {
        let _ = upsert.upsert(p);
    })))
    .build();
```

`with_observer` takes an `Option<CheckObserver>`; passing `None` (the default)
disables it. The broker calls the observer once per finished check.

## CLI: `--state <PATH_OR_URL>`

The binary wires all of this up from one flag on [`find`](../cli/find.md) and
[`serve`](../cli/serve.md):

```sh
# SQLite: a filesystem path.
proxybroker --state proxies.db find --types HTTP --limit 20

# Redis: a redis:// or rediss:// URL.
proxybroker --state redis://127.0.0.1/0 serve --port 8888
```

`--state` remembers proxies across runs: it warm-starts the pool from stored
history and folds each fresh check back in. The backend is chosen by the spec —
a `redis://` / `rediss://` URL selects Redis, anything else is treated as a
SQLite file path. If the matching backend feature is not compiled in, the CLI
prints a hint (`--state <spec>: a file path needs --features store-sqlite`)
rather than silently doing nothing.

`--state` gives adaptive re-checking (`--recheck`) a **durable** score to
re-check into, so scores survive restarts. But `--recheck` no longer *requires*
`--state`: without it, the re-checker folds into an in-memory
[`MemoryStore`](#memorystore-persist) instead (scores reset on restart). See
[serve](../cli/serve.md) for the `--recheck*` cadence flags.

## Warm start: `Proxy::restored`

Both backends reconstruct proxies through `Proxy::restored`:

```rust
pub fn restored(
    host: IpAddr,
    port: u16,
    types: BTreeMap<Proto, Option<AnonLevel>>,
    requests: u32,
    errors_total: u32,
    avg_resp_time: f64,
) -> Proxy
```

The reconstruction is **lossy on the error histogram, faithful on the error
rate**: `errors_total` is seeded under a single bucket, so per-bucket
breakdowns are gone but `error_rate()` is exact. `avg_resp_time` is seeded as one
runtime sample, so `avg_resp_time()` returns it. Warm start only needs
`priority()`, never the per-bucket breakdown — so nothing priority-relevant is
lost.

## `SqliteStore` (`store-sqlite`)

The bundled SQLite backend. `rusqlite` is compiled with its `bundled` feature —
SQLite is statically linked from source, so there is no dependency on a system
`libsqlite3` (this matches the static-musl goal). One denormalized `proxies`
table keyed on `(host, port)`, no per-attempt rows.

```rust
use proxybroker::persist::SqliteStore;

let store = SqliteStore::open("proxies.db")?; // creates the file if absent
```

`open` sets `journal_mode = WAL` and a 5-second `busy_timeout` so the re-checker
(which opens a second connection to the same DB) and the upsert observer can
write concurrently without an un-retryable `SQLITE_BUSY`. The single connection
sits behind a `Mutex` (rusqlite's `Connection` is `Send` but not `Sync`), which
makes `Arc<SqliteStore>` shareable across tasks.

The atomic EWMA fold is done in SQL with `ON CONFLICT(host, port) DO UPDATE` —
`ewma_success = 0.3 * excluded.ewma_success + 0.7 * proxies.ewma_success` — so
each check is one atomic round-trip. A failing re-check's empty types are
guarded: the prior confirmed types are kept rather than erased.

### `SCHEMA_VERSION`

```rust
pub const SCHEMA_VERSION: i64 = 1;
```

The current on-disk schema version, written to `PRAGMA user_version`. `open`
runs `migrate`, which reads the stored version and creates the table if it is
below 1. A schema change bumps this constant and adds a migration arm.

## `MemoryStore` (`persist`)

The in-memory backend: a `HashMap<(host, port), Record>` behind a `Mutex` (so
`Arc<MemoryStore>` is shareable across the re-check tasks), gated on the
`persist` feature alone — **no backend dependency**. It exists so `serve
--recheck` can keep an adaptive re-check's decay/score bookkeeping without a
`--state` SQLite/Redis backend; the state lives only for the process and nothing
survives a restart.

```rust
use proxybroker::persist::MemoryStore;

let store = MemoryStore::new(); // empty; no file, no connection
```

Its `upsert` fold reproduces [`SqliteStore`](#sqlitestore-store-sqlite)'s
`ON CONFLICT` arithmetic byte-for-byte — alpha = 0.3 new + 0.7 prior on the
success/latency EWMAs, accumulate requests/errors/uptime, and keep the prior
confirmed types on a failing (empty) sample — so a MemoryStore-backed re-check
decays identically to the durable path. `load` reconstructs through
[`Proxy::restored`](#warm-start-proxyrestored) like both durable backends.

This is what the CLI selects when `--recheck` runs without `--state` (or on a
`persist`-only build with no backend compiled in); it prints
`re-checking into memory only (no durable --state); scores reset on restart`.

## `RedisStore` (`store-redis`)

The Redis backend (added in Wave 9). One blocking `redis::Connection` behind a
`Mutex`, mirroring `SqliteStore`'s connection handling (`redis::Connection` is
`Send` but not `Sync`). It works with local Redis over `redis://` and managed
Redis (ElastiCache / Upstash / Redis Cloud) over `rediss://`; TLS uses the
crate's ring-only rustls with bundled webpki roots, so there is no OS cert-store
dependency.

```rust
use proxybroker::persist::RedisStore;

let store = RedisStore::open("redis://127.0.0.1/0")?;
```

The atomic upsert that `SqliteStore` does with SQL `ON CONFLICT` is a Lua `EVAL`
here: Redis runs scripts single-threaded, so the whole read-modify-write fold is
race-free without a WATCH/MULTI transaction, even with two `RedisStore`s on a
shared fleet upserting concurrently. It reproduces the SQLite arithmetic
byte-for-byte (alpha = 0.3), including the confirmed-types guard.

Each proxy is a Redis hash under `proxybroker:proxy:<host>:<port>`; a set at
`proxybroker:proxies` enumerates members for `load`. A string key
`proxybroker:schema` stands in for SQLite's `PRAGMA user_version`; a
present-but-different value is a hard error (`redis store schema mismatch`)
rather than a silent misread of old-shape hashes.

## Feature gates

| Feature | Enables | Pulls in |
| --- | --- | --- |
| `persist` | The `Store` trait + observer machinery + `MemoryStore`, no backend | — |
| `store-sqlite` | `SqliteStore`, `SCHEMA_VERSION` (implies `persist`) | `rusqlite` (bundled) |
| `store-redis` | `RedisStore` (implies `persist`) | `redis` (blocking, `script` + rustls) |

See [feature flags](../architecture/feature-flags.md) for the full matrix. The
`Store` trait is synchronous, so `store-redis` uses redis-rs's **blocking**
`Connection` — no tokio-comp — matching the trait's shape.

Errors from either backend surface as `Error::Persist(String)`.
