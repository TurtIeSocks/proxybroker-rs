# Checking

The `Checker` validates one proxy across the protocols it is expected to
support, and classifies how much it reveals about the client. This page walks the pipeline a proxy
travels from a raw address to a confirmed, classified result.

## The pipeline

```
Proxy (host, port)
   │
   ├─ DNSBL check ............ rejected early if listed in any --dnsbl zone
   │
   ▼  for each protocol in (expected ∩ requested), in Proto::ALL order
connect TCP ──► negotiate ──► send test request ──► read + validate ──► classify
   │              │                                                        │
resolver       negotiator                                          anonymity + trust
```

Each protocol is checked independently; a proxy can confirm several. The set actually checked is the
**intersection** of the protocols the provider claimed (`expected_types`) and the protocols the caller
requested — iterated in the fixed `Proto::ALL` order, never `HashMap` order (which is randomized and
would make check order, and therefore emitted bytes, nondeterministic). An empty expected set means
"unknown", so all requested protocols are checked.

## Resolver

Before any judge can be used it must be reachable, and the host's own external IP must be known — it is
the **anonymity baseline**. The `Resolver` resolves host names via hickory
(IP literals pass straight through, including leading-zero IPv4) and discovers this machine's external
IPs by probing a set of IP-echo endpoints concurrently. `external_ips` returns a **set**: on a
dual-stack host both the IPv4 and IPv6 addresses, because the anonymity check must trip if *either*
appears in a judge's echo.

## Negotiator

The `negotiator` turns a fresh TCP connection into a stream tunnelled to
the target, dispatching on `Proto`. The protocol set is closed — six variants —
so this is a `match`, not a trait object (users extend *providers*, never protocols):

| `Proto` | Wire name | Negotiation |
| --- | --- | --- |
| `Http` | `HTTP` | No-op; the request goes to the proxy with an absolute-form URI. |
| `Https` | `HTTPS` | `CONNECT`, then a TLS upgrade of the *same* connection in place. |
| `Socks4` | `SOCKS4` | `tokio-socks` handshake; requires an IPv4 destination. |
| `Socks5` | `SOCKS5` | `tokio-socks` handshake; IPv4/IPv6/domain, optional RFC 1929 auth. |
| `Connect80` | `CONNECT:80` | Hand-rolled `CONNECT`, require HTTP 200. |
| `Connect25` | `CONNECT:25` | `CONNECT`, then read and check the SMTP `220` banner. |

`CONNECT:25` has **no test request** — a granted tunnel plus the `220` banner is the whole check.
Everything else proceeds to a request/response round-trip against a judge.

## Judges

A *judge* is an endpoint that echoes the request headers and the client IP back, so the checker can
see what the proxy forwarded. The `JudgePool` is probed **eagerly** when the
checker is built and owned by it: `Checker::new` returns `Error::NoJudges` if none verify, so `check`
is simply unconstructible before the baseline exists. There is no process-global judge state and no
`asyncio.Event` — the deadlock trap of a naive port does not exist here.

Judges are grouped by `JudgeScheme` (`Http` / `Https` / `Smtp`). Each protocol
routes to a random working judge of the scheme its `judge_scheme()` selects; `HTTPS` uses an HTTPS
judge, `CONNECT:25` an SMTP judge, everything else an HTTP judge. On probe, an HTTP/HTTPS judge is kept
only if it returns 200 **and** echoes one of the host's real external IPs **and** echoes a random
marker — and its baseline `via`/`proxy` counts are recorded for the anonymity comparison below.

## Anonymity levels

Only `HTTP` carries anonymity information (the judge sees the client's headers directly; tunnelled
protocols hide them). The measured `AnonLevel`, ordered worst → best:

| Level | Meaning |
| --- | --- |
| `Transparent` | One of the host's real external IPs appeared in the judge's response. |
| `Anonymous` | The real IP is hidden, but `via`/`proxy` counts exceed the judge's baseline (marked as proxied). |
| `High` | Indistinguishable from a direct request. |

Because the ordering is worst → best, a filter like `level >= AnonLevel::Anonymous` is meaningful. A
request narrows protocols and, optionally, levels via `TypeSpec`; `--strict`
requires the measured level to match exactly.

## Judge-less liveness mode

If no judge comes up (all unreachable) the checker normally fails with `Error::NoJudges`. When the
caller supplies a liveness URL (`CheckerConfig::liveness_url`, the CLI `--liveness-url`), the checker
instead falls back to a plain fetch-through-the-proxy: a `GET` that must return **200**. This is
graceful degradation — the proxy is confirmed *working* but its anonymity is **unclassifiable** without
a judge, so it carries level `None`. Combining `--liveness-url` with an anonymity-level filter
therefore yields nothing.

## Honeypot / trust verdict

With `--trust-check` (`CheckerConfig::trust_check`), each judge round-trip is assessed for hostility
and a `TrustReport` is recorded. It reports *specific* signals rather than a
bare "untrusted" boolean:

| `TrustSignal` | Fires when |
| --- | --- |
| `CanaryMismatch` | Our nonce marker did not survive the round-trip verbatim (content tampering). |
| `InjectedHeader` | The echoed request carried a header name we never sent (injection). |
| `CertMismatch` | Reserved for the optional cert-pin follow-up; the dependency-free core never emits it. |

An empty report means trusted. The check is an **opt-in heuristic** with documented false-positive
guards: `Via` / `X-Forwarded-For` are the *anonymity* signal and are allow-listed so they do not
double-count as injection, and the injected-header scan only detects against judges that echo raw
`Name: value` request headers (the bundled defaults emit JSON or `HTTP_NAME = value`, so the scan is
inert — safe, but pass a raw-header-echo judge via `--judges` for real detection).
