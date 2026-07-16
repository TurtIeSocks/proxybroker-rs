# Bundled geolocation data

`dbip-country-lite.mmdb` — **IP Geolocation by DB-IP (https://db-ip.com)**, licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). See `../LICENSE-DATA`.

Committed to git on purpose. The alternatives are worse:

- **Fetching in `build.rs`** breaks offline builds and docs.rs (no network at build time),
  and makes the build non-reproducible.
- **Gitignoring it** means a fresh clone cannot build with default features, since
  `geo-bundled` is on by default and `Cargo.toml` `include`s the file when publishing.

This is what `tor-geoip-db` (Tor Project / arti) does with its own vendored geo data, and
CC BY 4.0 imposes no update-or-destroy obligation, so an immutable committed copy is
inherently compliant. (This is precisely what bundling MaxMind GeoLite2 could *not* be —
see `../LICENSE-DATA`.)

## Refreshing

DB-IP publishes monthly. Refresh per release — hygiene, not a legal duty:

```sh
curl -sSLO https://download.db-ip.com/free/dbip-country-lite-$(date +%Y-%m).mmdb.gz
gunzip -c dbip-country-lite-*.mmdb.gz > data/dbip-country-lite.mmdb
cargo test --lib          # the geo test asserts real lookups against it
```

Update the build date recorded in `../LICENSE-DATA` when you do.
