# Data & licensing

proxybroker-rs keeps a strict line between its **code** and its **bundled data**, because they carry
different licenses with different obligations. Getting this wrong is precisely the mistake the upstream
project made; the separation below is deliberate.

## Two licenses, two files

| Artifact | License | File |
| --- | --- | --- |
| All source code | Apache License 2.0 | [`LICENSE`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/LICENSE) |
| Bundled geo database (`data/dbip-country-lite.mmdb`) | CC BY 4.0 | [`LICENSE-DATA`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/LICENSE-DATA) |

The blanket Apache-2.0 grant on the code does **not** extend to the geo data. `LICENSE-DATA` covers
only the bundled MMDB and nothing else; the two never imply each other. Attribution and the statement
of changes for the derivative work live in [`NOTICE`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/NOTICE).

## Why DB-IP Country Lite, not GeoLite2

The Python original bundles MaxMind's GeoLite2-Country database inside its distributed package.
proxybroker-rs does **not** redistribute any MaxMind data. It bundles **DB-IP Country Lite** instead,
for a concrete legal reason:

- **GeoLite2's EULA is update-or-destroy.** It obliges licensees to destroy superseded copies within
  30 days of a new release. A published crates.io version is **immutable** — it can never be
  destroyed — so bundling GeoLite2 in a published crate cannot be brought into compliance by
  attribution, feature flags, or any other means.
- **DB-IP Country Lite is CC BY 4.0.** It imposes no update-or-destroy duty and carries no ShareAlike
  obligation. Its only condition is attribution.

The required attribution, which the crate surfaces in `--version`, the README, and `LICENSE-DATA`:

> IP Geolocation by DB-IP (https://db-ip.com)

## The bundled data is country-only

The embedded database resolves an IP to a **country** and nothing more. Region, city, and ASN are
never populated from the bundled data — they are unbundled hooks: the [lookup code](./architecture/geo-asn.md)
ships, the data does not.

- **Region / city** populate only from a user-supplied MaxMind **City** database via `--geo-db`.
- **ASN** (number + organization) populates only from a separate ASN database via `--asn-db`.

You may lawfully use your own MaxMind GeoLite2 copy under your own license key — pass it with
`--geo-db` / `--asn-db`. The crate simply never *redistributes* MaxMind data.

## Shipping zero geo data

If you do not want the CC BY 4.0 attribution duty at all, build without the bundled database. The
[feature flags](./architecture/feature-flags.md) that control this:

```sh
# geo code, but no bundled database (bring your own with --geo-db):
cargo build --no-default-features --features cli,server,geo

# no geo at all — no code, no data, no attribution duty:
cargo build --no-default-features --features cli,server
```

`geo-bundled` is the only feature that embeds licensed data; turning it off (or building
`--no-default-features`) ships a crate free of any third-party data. See
[`NOTICE`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/NOTICE) and
[`LICENSE-DATA`](https://github.com/TurtIeSocks/proxybroker-rs/blob/main/LICENSE-DATA) for the full
terms and the statement of changes from the original work.
