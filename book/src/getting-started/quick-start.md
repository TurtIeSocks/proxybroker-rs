# Quick Start

This page walks through the most common commands. It assumes `proxybroker` is on your `PATH`
(see [Installation](./installation.md)). For every flag, see the full
[CLI Reference](../cli/overview.md).

## Grab proxies (no checking)

The fastest path: scrape the providers and print addresses, **without** verifying them.

```sh
proxybroker grab --limit 10
```

Output is `host:port`, one per line. Results are unverified — many will not work. Use
`grab` when you want raw candidates and will check them yourself. See [grab](../cli/grab.md).

## Find checked HTTP proxies

`find` scrapes, checks that each proxy actually works, and classifies its anonymity level.
`--types` is required.

```sh
proxybroker find --types HTTP HTTPS --limit 10
```

Only working proxies are emitted. Add `--show-stats` for an aggregate summary on stderr:

```sh
proxybroker find --types HTTP --limit 20 --show-stats
```

Restrict by country and anonymity level (`--lvl` applies to HTTP):

```sh
proxybroker find --types HTTP --countries US GB --lvl "High Anonymous"
```

See [find](../cli/find.md) for the full flag set (`--dnsbl`, `--judges`, `--timeout`,
`--max-conn`, retry knobs, and more).

## Serve a rotating proxy

Run a local proxy server that finds working proxies and rotates through them. Point any HTTP
client at it and each request goes out through a pooled upstream.

```sh
proxybroker serve --types HTTP --host 127.0.0.1:8888
```

Then use it like any HTTP proxy:

```sh
curl --proxy http://127.0.0.1:8888 https://example.com
```

The pool tops itself up to `--limit` working proxies (default 100) and picks an upstream per
request according to `--strategy` (`best`, `round-robin`, `random`, or `sticky`). See
[serve](../cli/serve.md) for selection strategies, health thresholds, authentication, and
country filtering.

## Check a list you already have

Verify proxies from a file or stdin instead of scraping. Input is `host:port` addresses.

```sh
# from a file
proxybroker check --types HTTP --infile proxies.txt

# from stdin
cat proxies.txt | proxybroker check --types HTTP SOCKS5
```

Only working proxies are emitted, exactly as with `find`. See [check](../cli/check.md).

## Machine-readable output

Every command that emits proxies supports `--format`. For example, NDJSON (one JSON object
per line):

```sh
proxybroker find --types SOCKS5 --limit 10 --format json
```

Other formats include `json-array`, `csv`, `url` (`scheme://host:port`), and a custom
`--output-format` template. See [Output Formats](../cli/output-formats.md).

## Save and reload

Append every working proxy to an NDJSON file with `--save`, then reload it later without
re-checking via `check --load` or `serve --load`:

```sh
proxybroker find --types HTTP --limit 50 --save working.ndjson
proxybroker serve --load working.ndjson
```

## Next steps

- Browse the full [CLI Reference](../cli/overview.md) for every subcommand and flag.
- Embed the broker in your own program via the [Library Guide](../library/broker.md).
- Understand how checking works in [The Checking Pipeline](../architecture/checking.md).
