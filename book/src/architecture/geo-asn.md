# Geolocation & ASN

When built with the `geo` feature, checked proxies can be attributed to a country (and, with a richer
database, a region and city) and to the Autonomous System that owns their IP. This page covers the
`GeoDb` lookup, the bundled database, and the two user-supplied database hooks.

Geo support is [feature-gated](./feature-flags.md): `geo` compiles the lookup code, and `geo-bundled`
additionally embeds the default country database. Both are on in the default build.

## `GeoDb`

`GeoDb` is an opened MaxMind-format (MMDB) database. There are three ways to obtain one:

```rust
use proxybroker::GeoDb;

let db = GeoDb::bundled()?;              // the embedded DB-IP Country Lite (needs `geo-bundled`)
let db = GeoDb::open("/path/to.mmdb")?;  // any MMDB you supply
```

It exposes two lookups:

```rust
db.lookup(ip)     // -> Option<Country>
db.lookup_asn(ip) // -> Option<Asn>
```

A single decode path serves both a Country-only and a full City database: `geoip2::City`'s fields are
all optional, so the bundled Country record decodes cleanly with empty `region`/`city`, and those
fields fill in only when the supplied database actually carries them.

## The data model

`lookup` returns a [`Country`](../library/proxy.md); `lookup_asn` returns an `Asn`. Both hang off the
[`Proxy`](../library/proxy.md) as `proxy.geo` and `proxy.asn`.

```rust
pub struct Country {
    pub code: String,           // ISO country code, e.g. "US"
    pub name: String,           // English country name
    pub region: Option<Region>, // subdivision — City DB only
    pub city: Option<String>,   // city name — City DB only
}

pub struct Region {
    pub code: String,           // ISO 3166-2 subdivision code
    pub name: String,
}

pub struct Asn {
    pub number: u32,            // autonomous system number, e.g. 15169
    pub org: Option<String>,    // owning organization, e.g. "Google LLC"
}
```

`Country` and `Asn` are **orthogonal**: geolocation and network ownership are independent facts about
an IP, and come from independent databases.

## The bundled database — DB-IP Country Lite

The embedded database is **DB-IP Country Lite** (CC BY 4.0), not MaxMind GeoLite2. It resolves an IP
to a **country only** — `region` and `city` are always `None` from the bundled data, and `lookup_asn`
always returns `None` (it carries no ASN fields). Shipping only country data is a licensing hygiene
constraint, covered in [data & licensing](../data-and-licensing.md).

Using the bundled data triggers the CC BY 4.0 attribution duty, which the crate surfaces in
`--version`, the README, and `LICENSE-DATA`:

> IP Geolocation by DB-IP (https://db-ip.com)

## `--geo-db` — bring a City database

Pass any MaxMind-format country/city database to override the bundled one. A full **City** database
populates `region` and `city`, which the bundled Country-Lite cannot:

```sh
proxybroker --geo-db /path/to/GeoLite2-City.mmdb find --types HTTP --format json
```

You may lawfully use your own MaxMind GeoLite2 copy under your own license key — the crate simply does
not *bundle* MaxMind data. Both flags are `global`, so they precede the subcommand.

## `--asn-db` — attribute the Autonomous System

ASN lives in a **separate** database from country/city (e.g. `GeoLite2-ASN.mmdb`). It is opt-in and
unbundled — no ASN data ships with the crate — so `proxy.asn` stays `None` unless you supply one:

```sh
proxybroker --asn-db /path/to/GeoLite2-ASN.mmdb find --types HTTP --format json
```

Each proxy then carries an `asn` object (`{ "number": 15169, "org": "Google LLC" }`, or `null` when
nothing resolved) in `--format json`, and the `{{asn}}` / `{{asn_org}}` tokens work in
`--output-format` templates. `--geo-db` and `--asn-db` are independent and can be combined.

Only **country** data is bundled. Region/city (via `--geo-db` City DB) and ASN (via `--asn-db`) are
hooks: the code ships, the data does not.
