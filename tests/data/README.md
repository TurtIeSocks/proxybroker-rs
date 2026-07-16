# Test fixtures — `tests/data/`

Test-only data. **Never packaged**: the crate's `Cargo.toml` `include` list ships only
`src/**`, `examples/**`, `data/*`, and the licenses — not `tests/**` — so nothing here reaches
crates.io. (Verify with `cargo package --list`.)

## `city-test.mmdb`

A MaxMind **City**-format database used by `tests/geo_city.rs` to prove that a user-supplied City
DB populates `geo.region` / `geo.city` (C7), while the bundled DB-IP Country-Lite does not.

- **Source:** `test-data/GeoIP2-City-Test.mmdb` from
  [maxmind/MaxMind-DB](https://github.com/maxmind/MaxMind-DB) (`main`), vendored verbatim.
- **License:** Apache License 2.0 (the MaxMind-DB repository's license; the test databases are
  generated from that repo's own `source-data/` and carried under the same terms).
- **Nature:** synthetic sample data MaxMind publishes specifically for exercising MMDB *readers*
  — not real geolocation data. It is the de-facto standard fixture across MMDB reader libraries.
- **Why vendored:** the `maxminddb` crate references these files via a git submodule and does not
  ship them in its published package, so the fixture is committed here directly.

This is **not** the runtime geo database. The bundled runtime DB is DB-IP Country-Lite
(CC BY 4.0), lives at `data/dbip-country-lite.mmdb`, and is Country-resolution only.
