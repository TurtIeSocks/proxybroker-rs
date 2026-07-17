# Proxy

`Proxy` is the library's value type: an address, the protocols it is expected to (and confirmed to)
support, timing/error statistics, and geolocation. It is **not** a connection handle — the socket
lives in the checker/negotiator and is passed in. `Proxy` is plain data plus a few recording
methods.

## Fields

| Field | Type | Notes |
| --- | --- | --- |
| `host` | `IpAddr` | Public. |
| `port` | `u16` | Public. |
| `expected_types` | `BTreeSet<Proto>` | Public. Protocols to check, from the provider. |
| `geo` | `Option<Country>` | Public. `None` when geo is disabled or the lookup missed. |
| `asn` | `Option<Asn>` | Public. `None` unless a `--asn-db` was supplied and resolved this IP. |
| `types` | `BTreeMap<Proto, Option<AnonLevel>>` | Private — read via `types()`. Confirmed protocols and, for HTTP, the measured anonymity level. |
| `requests` / `errors` / `runtimes` | — | Private stats — read via `requests()`, `errors()`, `error_rate()`, `avg_resp_time()`, `percentile()`. |
| `auth` | `Option<Credentials>` | Private — read via `auth()`. Upstream proxy credentials; never serialized. |
| `caps` / `trust` | — | Private — read via `caps()`/`capabilities()` and `trust()`. |

Construct one with `Proxy::new(host, port, expected_types)`. The address renders with `addr()`,
which brackets IPv6 per RFC 3986:

```rust
use proxybroker::{Proto, Proxy};
use std::collections::BTreeSet;

let mut p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::from([Proto::Http]));
p.add_type(Proto::Http, None); // mark HTTP confirmed-working
assert_eq!(p.addr(), "1.2.3.4:8080");
```

### Key methods

| Method | Returns | Meaning |
| --- | --- | --- |
| `addr()` | `String` | `host:port`, IPv6 bracketed. |
| `types()` | `&BTreeMap<Proto, Option<AnonLevel>>` | Confirmed protocols + anonymity levels. |
| `add_type(proto, level)` | | Record that `proto` works. |
| `remove_type(proto)` | | Drop a confirmed protocol (strict-mode filtering). |
| `is_working()` | `bool` | True once any protocol is confirmed. |
| `schemes()` | `Vec<Scheme>` | Transport schemes served (HTTP / HTTPS families). |
| `error_rate()` | `f64` | `0.0..=1.0`, rounded to 2 dp. |
| `avg_resp_time()` | `f64` | Mean successful round-trip, seconds, 2 dp. |
| `percentile(q)` | `f64` | The `q`-quantile of runtimes (numpy "linear"). |
| `priority()` | `(f64, f64)` | Pool-ordering key `(error_rate, avg_resp_time)`, lower is better. |
| `record_attempt(runtime, err)` | | Fold one attempt into the stats. Timeout runtimes are excluded from `avg_resp_time`. |

## Geo model: Country and Region

`geo` is an `Option<Country>`. The bundled DB-IP Country-Lite database resolves the country only, so
`region`/`city` stay `None` for it — they populate only from a richer MaxMind **City** database
opened via `--geo-db`. The JSON shape is fixed either way.

```rust
pub struct Country {
    pub code: String,           // ISO country code, e.g. "US"
    pub name: String,           // e.g. "United States"
    pub region: Option<Region>, // subdivision — City DB only
    pub city: Option<String>,   // City DB only
}

pub struct Region {
    pub code: String, // ISO 3166-2, e.g. "CA"
    pub name: String,
}
```

## ASN model: Asn

`asn` is network-ownership attribution, orthogonal to `Country` (geolocation and ownership are
independent facts about an IP). It is populated only from a user-supplied ASN database (`--asn-db`);
nothing ASN-shaped is bundled.

```rust
pub struct Asn {
    pub number: u32,         // e.g. 15169
    pub org: Option<String>, // e.g. "Google LLC"; None when the DB omits it
}
```

## The v1 JSON contract

`Proxy` implements `serde::Serialize` and `serde::Deserialize` by hand. `Serialize` matches
proxybroker2's `as_json` — a nested object, not a flat struct — so
`serde_json::to_string(&proxy)` is the replacement for the Python `as_json()` method. The top-level
key set is **frozen at v1**: `host`, `port`, `geo`, `asn`, `types`, `avg_resp_time`, `error_rate`.

```json
{
  "host": "1.2.3.4",
  "port": 80,
  "geo": {
    "country": { "code": "US", "name": "United States" },
    "region":  { "code": "",   "name": "" },
    "city": null
  },
  "asn": null,
  "types": [ { "type": "HTTP", "level": "High" } ],
  "avg_resp_time": 0.0,
  "error_rate": 0.0
}
```

The round-trip is **lossy on stats by design**. `Serialize` emits the computed `avg_resp_time` /
`error_rate` for humans, but `Deserialize` restores only the persistent identity — host, port, geo,
asn, confirmed types — and leaves `expected_types` / `requests` / `errors` / `runtimes` empty. A
loaded proxy's timing history restarts. `asn` serializes as `null` when no `--asn-db` resolved it;
an empty country `code` maps back to `geo: None`.

Only additive, always-present, backward-compatible fields may join without a format bump; a breaking
change must bump the `--format` variant (e.g. `json2`).

## Credentials redaction

`Credentials` (username/password for an authenticated upstream proxy) is carried on `Proxy` and
applied by the negotiator (SOCKS5 RFC 1929) and the server (HTTP `Proxy-Authorization`). It is
**never serialized** — secrets stay out of `--format json` — and its `Debug` impl is redacted, so a
`Proxy` debug print cannot leak the secret. Attach it builder-style:

```rust
use proxybroker::{Credentials, Proto, Proxy};
use std::collections::BTreeSet;

let creds = Credentials { username: "user".into(), password: "pass".into() };
let p = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::from([Proto::Socks5]))
    .with_auth(creds);
assert!(p.auth().is_some());
```

Because `auth` is never serialized, it is never deserialized either — a loaded proxy always has
`auth: None`.

## Caps and trust

Two more profiles ride on a checked proxy, neither serialized:

- **`Capabilities`** (from `capabilities()`): `cookie_echo`, `referer_echo`, and `connect25` (a
  confirmed SMTP tunnel, derived from the types). `caps()` returns the raw
  [`Caps`](../architecture/checking.md) — the two header-echo flags — OR-accumulated across
  protocols. These back the `--require-cookie` / `--require-referer` / `--require-connect25` filters.
- **Trust** (from `trust()`): the honeypot verdict. Empty (trusted) unless `--trust-check` ran.
  Backs `--require-trusted`.

## Saving and loading: read_ndjson / write_ndjson

The library ships flat-file persistence as NDJSON — one `serde_json` object per line, the exact
bytes `--format json` emits. This is the minimal persistence step (no schema/index/migration).

```rust
use proxybroker::{read_ndjson, write_ndjson, Proto, Proxy};
use std::collections::BTreeSet;
use std::io::Cursor;

let mut a = Proxy::new("1.2.3.4".parse().unwrap(), 8080, BTreeSet::new());
a.add_type(Proto::Http, None);

let mut buf = Vec::new();
write_ndjson(&mut buf, &[a])?;                     // W: std::io::Write
let back = read_ndjson(Cursor::new(buf))?;         // R: std::io::BufRead
assert_eq!(back.len(), 1);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`write_ndjson` is generic over `std::io::Write`; `read_ndjson` over `std::io::BufRead`. `read_ndjson`
skips blank lines and aborts on the first malformed line with a `std::io::ErrorKind::InvalidData`
error.

## See also

- [Broker](./broker.md) — produces a stream of `Proxy` values.
- [Pool](./pool.md) — a rotating pool of checked proxies.
