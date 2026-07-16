# Wave 3 — Serving: auth, control, protocols

*Theme: turn the working rotating server (`src/server.rs`) into a production tool — a client
can authenticate to it, it can authenticate to paid upstreams, it can be introspected and
steered live, it tells each client which upstream served the request, and it speaks SOCKS5 as
a front-end, not only HTTP/CONNECT.*

All five features land in `src/server.rs` (the `handle_client` → `relay` seam), with smaller
touches in `src/negotiator.rs` (B8), `src/proxy.rs` (B8), `src/utils.rs` (a base64 helper
shared by B8+B9), and `src/bin/proxybroker.rs` (`ServeArgs` flags for B9). Everything stays
behind the existing `server` feature; no new crate dependency is added (base64 is hand-rolled
— see the shared refactor).

## Build order (dependency-respecting)

1. **Shared refactor R0** — a `base64_encode` helper in `utils.rs`, and a `Frontend` enum
   field on `ClientRequest` that generalizes the relay's client-ack from "branch on `scheme`"
   to "branch on how the client spoke to us". Lands first because B7, B9, B8 and B12 all read
   from it. Tiny; ships as part of B9's commit (its first consumer) unless it grows.
2. **B9 — local-server client auth** (`--auth user:pass`). Isolated gate in `handle_client`
   before selection; introduces R0's base64 helper. No relay-body changes → safest first.
3. **B8 — upstream proxy auth** (SOCKS5 RFC 1929 + HTTP `Proxy-Authorization`). Touches
   `Proxy`, `negotiate`, `connect_request`; independent of the relay body. Reuses R0 base64.
4. **B7 — `X-Proxy-Info` injection**. Introduces the relay's manual upstream→client first-
   response rewrite (replacing `copy_bidirectional` in that one direction for HTTP). This is
   the relay reshape that B6 and B12 then build on.
5. **B6 — `proxycontrol` control API**. Adds a shared `History` map (populated at the same
   relay-success point B7 touches), `Pool::remove`, and a `path` field on `ClientRequest`.
   Intercepts in `handle_client` before selection.
6. **B12 — SOCKS5 front-end**. Biggest surface; reuses the relay shape settled by B7 and the
   `Frontend` ack-branch from R0. Done last.

Every feature ships its failing test first (TDD) and one conventional commit.

---

## R0 — Shared refactor (base64 + `Frontend`)

**Goal.** Provide the two primitives Wave 3 reuses: a dependency-free base64 encoder, and a
`Frontend` discriminant on the parsed client request so the relay knows what acknowledgement
to send the client independent of the target `Scheme`.

**Public surface.**
- `src/utils.rs`: `pub fn base64_encode(input: &[u8]) -> String` — standard RFC 4648 alphabet,
  `=` padding. ~16 lines, no dep. (Encode-only is all Wave 3 needs: B9 compares the client's
  header against a pre-encoded expected string; B8 emits `Basic <b64>`. No decoder required.)
- `src/server.rs` (private): extend the existing `struct ClientRequest` (server.rs:214) with
  ```rust
  enum Frontend { HttpForward, HttpConnect, Socks5 }
  ```
  and a `frontend: Frontend` field. `parse_client_request` sets it (`HttpForward` for the
  plain-HTTP branch at server.rs:355, `HttpConnect` for the CONNECT branch at server.rs:346;
  `Socks5` added by B12). `relay` (server.rs:281) branches its client-ack on `req.frontend`
  instead of on `req.scheme` (the current `match req.scheme` at server.rs:297).

**Design.** `scheme` keeps its two current jobs — `pool.get(req.scheme)` (server.rs:243) and
`choose_proto(&proxy, req.scheme)` (server.rs:248). `frontend` takes over the *ack* decision
only. After R0 the relay's post-negotiate block reads:
```rust
match req.frontend {
    Frontend::HttpConnect => client.write_all(CONNECT_OK).await?,      // 200 (+X-Proxy-Info in B7)
    Frontend::Socks5      => client.write_all(&socks5_success()).await?, // B12
    Frontend::HttpForward => upstream.write_all(&req.raw).await?,        // forward buffered request
}
```
This is behaviour-preserving today (`HttpConnect`⇔`Https`, `HttpForward`⇔`Http`), so the two
existing `tests/serve.rs` cases stay green.

**Offline test plan.** Unit test in `utils.rs`: `base64_encode_matches_rfc4648` — assert
`base64_encode(b"user:pass") == "dXNlcjpwYXNz"`, plus the RFC 4648 §10 vectors
(`""→""`, `"f"→"Zg=="`, `"fo"→"Zm8="`, `"foo"→"Zm9v"`, `"foob"→"Zm9vYg=="`). No I/O.

**Acceptance criteria.**
- [ ] `base64_encode` passes the RFC vectors.
- [ ] `ClientRequest.frontend` set on both existing parse branches; relay branches on it.
- [ ] `tests/serve.rs` (both cases) still green with zero behavioural change.

**Risks / principle-flags.** ⚠ "no speculative abstraction" — a hand-rolled base64 could be
seen as reinventing a crate. Mitigation: it is encode-only, ~16 lines, and avoids pulling a
dependency into an always-compiled module for one header; if a decoder is ever needed
(unlikely), swap in the `base64` crate then. The `Frontend` enum is not speculative — it has
three real variants by end of wave.

**Effort.** S (folded into B9's commit).

---

## B6 — `proxycontrol` control API

**Goal.** Introspect and steer a live server without restart: `GET
http://proxycontrol/api/history/url:<url>` reports which upstream last served `<url>` for the
calling client, and `GET http://proxycontrol/api/remove/<ip:port>` evicts a proxy. Parity with
`proxybroker2 server.py:320`.

**Public surface.**
- `src/server.rs`:
  - `pub fn remove(&self, host: IpAddr, port: u16) -> bool` on `impl Pool` — drops every
    matching proxy from `self.state`; returns whether any were removed. (`Pool` already holds
    `state: Mutex<Vec<Proxy>>`, server.rs:56, so this is a `retain` + a changed-flag.)
  - A private `History` type: `struct History(Mutex<HashMap<String, String>>)` with
    `record(&self, key: String, proxy: String)` and `get(&self, key: &str) -> Option<String>`.
    Created in `serve` (server.rs:179), wrapped `Arc`, passed into `handle_client`.
- No new CLI flag and no `ServeArgs` change — the control API is always on, as in Python.

**Design.**
- **Interception point.** In `handle_client` (server.rs:225), immediately after
  `parse_client_request` returns and *before* the `for _ in 0..max_tries` selection loop
  (server.rs:242), add: `if req.host == "proxycontrol" { return serve_control(&mut client,
  &req, &pool, &history, client_peer_ip).await; }`. Mirrors `server.py:320`
  (`if headers["Host"] == "proxycontrol"`).
- **Path parsing.** `parse_client_request` must retain the request-target token. Add
  `path: String` to `ClientRequest` (R0's struct), set to the `uri` already parsed at
  server.rs:344. Python splits `headers["Path"].split("/", 5)[3:]` → `[api, operation,
  params]`; a client sends `GET http://proxycontrol/api/remove/1.2.3.4:8080`, so the token is
  `http://proxycontrol/api/...`. `serve_control` strips the `http://proxycontrol` authority
  and splits the remaining `/api/<operation>/<params>` on `/` (into 3).
- **`remove`.** `params` = `ip:port`; reuse `split_host_port(params, 0)` (server.rs:377) to
  parse, call `pool.remove(ip, port)`, reply `HTTP/1.1 204 No Content\r\n\r\n` (parity:
  server.py:329 always replies 204 regardless of hit — keep that).
- **`history` (`url:<url>`).** `params` = `url:<url>`; split once on `:` → (`"url"`, `url`).
  Key = `format!("{client_peer_ip}-{url}")` (Python: `f"{peername[0]}-{url}"`,
  server.py:337). On hit: `HTTP/1.1 200 OK` + `Content-Type: application/json` +
  `Content-Length` + `Access-Control-Allow-Origin: *` + `Access-Control-Allow-Credentials:
  true`, body `{"proxy": "<ip:port>"}\r\n` (byte-for-byte with server.py:346-360, including
  the trailing `\r\n` counted in `Content-Length`). On miss: `204 No Content`.
- **Populating history.** On the relay-success arm of `handle_client` (server.rs:251, the
  `Ok(())` branch), before `pool.put(proxy)`, record
  `history.record(format!("{client_peer_ip}-{}", req.path), format!("{}:{}", proxy.host,
  proxy.port))`. Python keys the *write* on `headers['Path']` (server.py:390) and the *read*
  on the `url` query param — so the client must query with the exact request-target it used;
  we preserve that.
- **`client_peer_ip`.** `client.peer_addr()?.ip().to_string()`, captured once at the top of
  `handle_client`.

**Offline test plan** (new `tests/serve_control.rs`, mirroring `tests/serve.rs`'s mock
upstream). First failing test: **`control_history_reports_serving_upstream`**.
1. `control_history_reports_serving_upstream` — start server over a pool with one mock HTTP
   upstream. Client A sends `GET http://1.2.3.4/ HTTP/1.1` (relayed, recorded). Same client
   connection origin (same 127.0.0.1 source — bind the client socket so peer IP is stable)
   then sends `GET http://proxycontrol/api/history/url:http://1.2.3.4/ HTTP/1.1`; assert `200`
   and body contains `"proxy"` with the mock upstream's `ip:port`.
2. `control_history_miss_returns_204` — query a URL never served → `204 No Content`.
3. `control_remove_evicts_proxy` — pool with two mock upstreams; `GET
   http://proxycontrol/api/remove/<ip:port>` for one → `204`; assert `Pool::remove` returned
   true and a follow-up relay never lands on the removed addr (unit-level: call
   `pool.remove(...)` then `pool.get(Http)` in a loop, assert the evicted addr never appears).
4. Unit `pool_remove_drops_matching` — `Pool::from_proxies` with 3 proxies, `remove` one,
   assert length 2 and the right one gone.

**Acceptance criteria.**
- [ ] `Host: proxycontrol` intercepted before selection; never consumes a pool proxy.
- [ ] `remove` → 204, proxy gone from pool.
- [ ] `history` hit → 200 + the exact JSON + CORS headers; miss → 204.
- [ ] History populated on every successful relay, keyed `peer_ip-<request-target>`.
- [ ] Non-control traffic unaffected (`tests/serve.rs` green).

**Risks / deviations / principle-flags.**
- ⚠ *Unbounded map* vs Python's `TTLCache(maxsize=10000, ttl=600)` (server.py:26). Deviation:
  ship a plain `HashMap` with a **hard cap** (drop-oldest via an `IndexMap` FIFO, or simply
  clear when `len > 10_000`). No time-based TTL — "ephemeral by design" says don't grow a
  background timer for this. Record the deviation in `decisions.md`.
- ⚠ *IPv6 proxy addr formatting.* Python emits `host:port` unbracketed; we use
  `format!("{}:{}", proxy.host, proxy.port)` (v6 unbracketed too, matching Python) rather than
  `proxy.addr()` (which brackets v6). Note it; the control API is a Python-parity surface.
- Open question: should `remove` 404 when nothing matched? Python always 204s. Keep 204 for
  parity; the `bool` return is available if a caller ever wants stricter semantics.

**Effort.** S/M.

---

## B7 — `X-Proxy-Info` response-header injection

**Goal.** Every client learns which upstream served its request: an `X-Proxy-Info: <ip:port>`
header on the response path. Parity with `server.py` `inject_resp_header` (server.py:393) /
`_inject_headers` (server.py:527).

**Public surface.** No lib API change and (recommended) no flag — always on, as in Python. If
we decide it should be gateable, add `ServeArgs.inject_proxy_info: bool` (default `true`); see
Open Question.

**Design.** The current relay does `copy_bidirectional(client, upstream)` (server.rs:315),
which cannot see or rewrite bytes. Split it:
- **HTTPS / CONNECT client (`Frontend::HttpConnect`).** Deviation from Python (which tries to
  rewrite the first *encrypted* tunnel chunk — a no-op/garbage on real TLS): send the header
  **pre-tunnel**, on the acknowledgement we already write (server.rs:301):
  `HTTP/1.1 200 Connection established\r\nX-Proxy-Info: <ip:port>\r\n\r\n`. Cleanly visible to
  any client that reads the CONNECT response head; no tunnel corruption.
- **Plain HTTP client (`Frontend::HttpForward`).** After forwarding `req.raw` to the upstream,
  manually pump the upstream→client direction for the *first* response chunk: read once, find
  the first `\r\n` (end of status line), splice in `X-Proxy-Info: <ip:port>\r\n`, write the
  rewritten head+remainder to the client, then `copy_bidirectional` (or two `tokio::io::copy`
  tasks) for the rest. Mirrors `_inject_headers` (server.py:527: split on first `\r\n`, insert
  header lines, rejoin). Guard: if the first read yields no `\r\n` within a small cap (e.g.
  8 KiB), forward unmodified rather than stall.
- **`Frontend::Socks5` (B12).** Opaque tunnel — no injection (SOCKS5 CONNECT carries arbitrary
  bytes). Same pre-tunnel option is meaningless; skip.

Concretely, `relay` gains a helper `splice_with_injection(client, upstream, header:
Option<Vec<u8>>)`: `None` → today's `copy_bidirectional`; `Some(line)` → the read-first-chunk-
rewrite-then-splice path above. Only the `HttpForward` arm passes `Some`.

**Offline test plan** (extend `tests/serve.rs`). First failing test:
**`http_response_carries_x_proxy_info`**.
1. `http_response_carries_x_proxy_info` — mock upstream returns a normal 200; client sends a
   plain-HTTP GET; assert the client's received bytes contain
   `X-Proxy-Info: <upstream ip:port>` on its own header line, *after* the status line and
   *before* the body, and the body (`RELAYED-BODY`) is intact.
2. `connect_ack_carries_x_proxy_info` — client sends `CONNECT example.com:443`; the mock
   upstream accepts the CONNECT (returns 200); assert the client reads
   `HTTP/1.1 200 Connection established\r\nX-Proxy-Info: <ip:port>\r\n\r\n` before any tunnel
   bytes. (Mock upstream that speaks a minimal CONNECT-200 then echoes.)
3. `injection_preserves_body_boundaries` — upstream body contains an embedded `\r\n\r\n`;
   assert only the response head is rewritten and body bytes are byte-identical.

**Acceptance criteria.**
- [ ] Plain-HTTP responses carry exactly one `X-Proxy-Info` line inserted after the status
      line; body unchanged.
- [ ] CONNECT clients get the header on the `200 Connection established` head, pre-tunnel.
- [ ] Malformed/short first chunk → forwarded unmodified, no hang.
- [ ] Bidirectional relay still works after the first-chunk rewrite (`tests/serve.rs` green).

**Risks / deviations / principle-flags.**
- ⚠ *Deviation (documented):* HTTPS injection is pre-tunnel on the CONNECT head, not an
  in-stream rewrite. Python's in-stream rewrite is effectively broken for real TLS; ours is
  correct and visible. Record in `decisions.md`.
- ⚠ Replacing `copy_bidirectional` for one direction adds a read/parse hot-path. Keep it to
  the *first* chunk only, then hand back to a zero-copy splice — no per-byte scanning of the
  body.
- Open question: always-on (parity) vs `--inject-proxy-info`/`--no-inject`. Recommend
  always-on for parity + laziness; a flag is a one-line add if a consumer objects to the
  rewritten bytes.

**Effort.** S/M (the relay reshape is the cost, not the header).

---

## B8 — Upstream proxy auth (SOCKS5 RFC 1929 + HTTP `Proxy-Authorization`)

**Goal.** Relay through authenticated/paid upstreams: SOCKS5 username/password (RFC 1929) and
HTTP `Proxy-Authorization: Basic` on CONNECT/forward.

**Public surface.**
- `src/proxy.rs`:
  ```rust
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Credentials { pub username: String, pub password: String }
  ```
  and a field `auth: Option<Credentials>` on `Proxy` (default `None`), with
  `pub fn with_auth(mut self, creds: Credentials) -> Self` (builder-style) and
  `pub fn auth(&self) -> Option<&Credentials>`. Scraped proxies stay `None`; creds arrive only
  via BYO/URL loading (`scheme://user:pass@host:port`, Wave 1 C1) — this feature just carries
  and applies them. **Not serialized** (keep secrets out of `--format json`); note it.
- `src/negotiator.rs`: thread creds into `negotiate`:
  ```rust
  pub async fn negotiate(proto, tcp, target, deadline, creds: Option<&Credentials>) -> Result<Stream, ProxyError>
  ```
  `connect_request(host, port, creds: Option<&Credentials>)` — append
  `Proxy-Authorization: Basic <base64(user:pass)>\r\n` when `Some`.

**Design.**
- **SOCKS5** (negotiator.rs:139 `socks5`): when `creds.is_some()`, call
  `Socks5Stream::connect_with_password_and_socket(tcp, target, &c.username, &c.password)`
  (verified present: `tokio-socks-0.5.3/src/tcp/socks5.rs:171`) instead of
  `connect_with_socket` (negotiator.rs:144). Same timeout/`map_socks_err` wrapping. Note
  RFC 1929 caps username/password at 1–255 bytes (the lib validates: socks5.rs:189); surface
  an over-length cred as `ProxyError::BadResponse` (it maps through `map_socks_err`).
- **SOCKS4** — RFC has no user/pass auth; ignore `creds` (SOCKS4 already needs an IPv4 target,
  negotiator.rs:127). Document that SOCKS4 upstream auth is unsupported.
- **HTTP CONNECT** (negotiator.rs `http_connect` → `connect_request`, negotiator.rs:250): add
  the `Proxy-Authorization` header line when `creds.is_some()`, using `utils::base64_encode`.
- **Plain HTTP forward** (server.rs:307, the `Scheme::Http` arm): the buffered client request
  `req.raw` goes to the upstream verbatim. Inject `Proxy-Authorization` by rewriting `req.raw`
  before the write — insert the header after the request line (same first-`\r\n` splice B7
  uses). Only when the chosen proxy has `auth`.
- **Call-site plumbing.** Two `negotiate` callers:
  - `src/server.rs:295` — pass `proxy.auth()`.
  - `src/checker.rs:210` — pass `None` (checked proxies are public candidates; the checker has
    no creds). One-line change.

**Offline test plan.** First failing test: **`socks5_upstream_auth_sends_rfc1929`**.
1. `connect_request_includes_proxy_authorization` (unit, negotiator.rs tests): 
   `connect_request("h", 443, Some(&Credentials{user,pass}))` contains
   `\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n`; the existing four `connect_request_*`
   tests updated to pass `None` (single-function refactor).
2. `socks5_upstream_auth_sends_rfc1929` (integration, `tests/upstream_auth.rs`): a mock TCP
   server that speaks the SOCKS5 server side — reads greeting, requires method `0x02`, reads
   the RFC 1929 `01 ULEN user PLEN pass`, asserts `user:pass`, replies `01 00` (success), then
   `00`-reply to the CONNECT and echoes. Build a `Proxy` with `SOCKS5` type +
   `with_auth(...)`, relay through it, assert success. No network.
3. `http_connect_upstream_auth` — mock upstream reads the `CONNECT` head, asserts
   `Proxy-Authorization: Basic ...`, returns 200; relay a CONNECT client through it.
4. `no_creds_omits_header` — `connect_request(.., None)` has no `Proxy-Authorization`;
   `checker.rs` path unaffected.

**Acceptance criteria.**
- [ ] SOCKS5 upstream with creds → RFC 1929 exchange (method `0x02`, `01 ULEN.. PLEN..`).
- [ ] HTTP CONNECT/forward upstream with creds → `Proxy-Authorization: Basic <b64>`.
- [ ] `creds = None` produces byte-identical output to today (checker path unchanged).
- [ ] SOCKS4 + creds documented as unsupported; no panic.
- [ ] Credentials never appear in `serde_json` output.

**Risks / deviations / principle-flags.**
- ⚠ Secrets in a `Clone` value type (`Proxy`) — acceptable; flag that `Credentials` is
  excluded from `Serialize` and from `Debug` of `Proxy` if the derive would leak it (add a
  manual `Debug` for `Credentials` that prints `Credentials { .. }`).
- ⚠ `negotiate` signature change ripples to `checker.rs` — one line, `None`. Preferred over a
  second `negotiate_with_auth` (avoids a near-duplicate public fn).

**Effort.** M.

---

## B9 — Local-server client auth (`--auth user:pass`)

**Goal.** Gate the local server: clients must present `Proxy-Authorization: Basic <b64>` or get
`407 Proxy Authentication Required`. Lets the server be exposed on a shared host.

**Public surface.**
- `src/bin/proxybroker.rs` `ServeArgs` (server.rs:76 struct): 
  ```rust
  /// Require clients to authenticate: Proxy-Authorization: Basic base64(user:pass).
  #[arg(long, value_name = "USER:PASS")]
  auth: Option<String>,
  ```
- `src/server.rs`: `serve` gains an `auth: Option<String>` param (the expected `user:pass`),
  threaded into `handle_client`. (Prefer a param over a `PoolConfig` field — auth is a server
  concern, not a pool-eviction concern.)

**Design.**
- At the top of `handle_client`, after `parse_client_request` returns and *before* selection
  (and, order-wise, alongside B6's `proxycontrol` intercept — auth first, then control), if
  `auth.is_some()`:
  - Pre-compute once (in `serve`, not per request) `expected = format!("Basic {}",
    base64_encode(user_pass.as_bytes()))`.
  - `parse_client_request` must expose the client's `Proxy-Authorization` header. Add
    `proxy_auth: Option<String>` to `ClientRequest`, populated in the HTTP parse branch by the
    same header scan that finds `Host:` (server.rs:357) — look for a line starting
    `proxy-authorization:` (case-insensitive), take the trimmed value.
  - Compare `req.proxy_auth.as_deref() == Some(expected)`; on mismatch/absent, write
    `HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic
    realm="proxybroker"\r\n\r\n` and return without consuming a pool proxy.
- Constant-time compare is overkill here (the secret is a shared static string, not a
  per-user hash); a plain `==` is the lazy-that-holds choice. Note it.
- `proxycontrol` requests: gate them too (auth before control) so introspection isn't open;
  match Python's ordering — Python checks control *before* auth, but Python has no client-auth
  feature, so this is a new decision. Recommend **auth-first** (control endpoints reveal pool
  membership). Flag as a deliberate choice.

**Offline test plan** (extend `tests/serve.rs` or new `tests/serve_auth.rs`). First failing
test: **`missing_credentials_get_407`**.
1. `missing_credentials_get_407` — start server with `auth = Some("user:pass")`; client sends
   a plain GET with no `Proxy-Authorization`; assert response contains `407` and
   `Proxy-Authenticate: Basic`; assert the mock upstream received **nothing** (no proxy
   consumed).
2. `valid_credentials_relay` — same server; client sends
   `Proxy-Authorization: Basic dXNlcjpwYXNz`; assert the relay succeeds (`RELAYED-BODY`).
3. `wrong_credentials_get_407` — `Basic <b64 of "user:wrong">` → `407`.
4. `no_auth_configured_is_open` — `auth = None` → today's behaviour (open relay), regression
   guard.

**Acceptance criteria.**
- [ ] `--auth user:pass` → clients without/with-wrong creds get `407` + `Proxy-Authenticate`.
- [ ] Correct creds relay normally.
- [ ] `407` path never checks out a pool proxy.
- [ ] `--auth` absent → unchanged open behaviour.
- [ ] Expected string encoded once at startup, not per request.

**Risks / deviations / principle-flags.**
- ⚠ Ordering deviation from Python (auth-before-control vs Python's control-only). Documented
  choice; the safer default.
- ⚠ Plain `==` compare (not constant-time). Acceptable for a shared static secret; note it in
  `decisions.md`. If per-user auth ever lands, revisit.
- `--auth` value is a process-arg (visible in `ps`). Standard for proxy CLIs; document, don't
  solve here.

**Effort.** S/M.

---

## B12 — SOCKS5 front-end for the local server

**Goal.** Clients can speak SOCKS5 to the local server (not only HTTP/CONNECT): greeting →
no-auth (or RFC 1929 when `--auth` set) → CONNECT request → the server resolves the target and
reuses the existing relay.

**Public surface.** No new lib API or flag — `parse_client_request` auto-detects the protocol
from the first byte (`0x05` ⇒ SOCKS5, else HTTP). The listener already accepts any TCP client.

**Design.**
- **Protocol sniff.** `parse_client_request` currently reads byte-by-byte until `\r\n\r\n`
  (server.rs:322). Restructure: read the **first byte**; if `0x05`, dispatch to a new
  `parse_socks5_request(client, timeout, auth)`; else seed the CRLF buffer with that byte and
  continue the existing HTTP loop unchanged.
- **`parse_socks5_request`** (new, in `server.rs`):
  1. **Greeting:** first byte is `VER=0x05`; read `NMETHODS`, then that many method bytes.
  2. **Method select:** if `auth.is_none()` and `0x00` (no-auth) offered → reply `05 00`. If
     `auth.is_some()`: require `0x02` (user/pass) → reply `05 02`, then read RFC 1929
     `01 ULEN user PLEN pass`, compare to the expected `user:pass`, reply `01 00` (ok) or
     `01 01` (fail, then close). If no acceptable method → reply `05 FF`, close. (This makes
     B9's `--auth` cover the SOCKS5 front-end too, symmetric with the HTTP `407`.)
  3. **Connect request:** read `VER=05 CMD=01 RSV=00 ATYP`. `ATYP`: `01`→4-byte IPv4,
     `03`→1-byte len + domain, `04`→16-byte IPv6; then 2-byte big-endian port. Only `CMD=01`
     (CONNECT); reply `05 07` (command not supported) for BIND/UDP and close.
  4. Build `ClientRequest { scheme: Scheme::Https, frontend: Frontend::Socks5, host, port,
     raw: Vec::new(), path: <host:port>, proxy_auth: None }`. `Scheme::Https` because a SOCKS5
     CONNECT is an opaque tunnel — `choose_proto` (server.rs:269) then prefers a tunnelling
     upstream proto (`Https/Socks5/Socks4/Connect80`), exactly right.
- **Ack (relay, via R0's `Frontend` branch).** For `Frontend::Socks5`, after the upstream
  tunnel is negotiated, reply the SOCKS5 success frame to the client instead of the HTTP-200:
  `05 00 00 01 00 00 00 00 00 00` (success, bound addr `0.0.0.0:0`), then `copy_bidirectional`
  (no header injection — opaque). On upstream failure the outer retry loop already writes the
  client a `502`; for SOCKS5 that byte sequence isn't a valid reply, so on final failure send a
  SOCKS5 error reply (`05 01 ...`) instead — branch the terminal error path on `req.frontend`.
- **Target resolution.** Reuse the existing `resolver.resolve(&req.host)` (server.rs:235); a
  domain `ATYP=03` resolves there, an IP literal passes straight through (`Resolver::resolve`
  short-circuits IP literals, resolver.rs).

**Offline test plan** (new `tests/serve_socks5.rs`). First failing test:
**`socks5_frontend_relays_through_pool`**. Use a raw client socket writing the byte frames
(no SOCKS client lib needed — assert exact bytes).
1. `socks5_frontend_relays_through_pool` — pool with one mock HTTP-CONNECT upstream (a mock
   that accepts a CONNECT and echoes). Client writes greeting `05 01 00`; assert server replies
   `05 00`. Client writes CONNECT-to-IPv4 `05 01 00 01 <ip> <port>`; assert reply
   `05 00 00 01 00 00 00 00 00 00`. Client then writes a payload; assert it round-trips through
   the tunnel.
2. `socks5_frontend_domain_atyp` — `ATYP=03` domain (`127.0.0.1` as a name resolved by the
   test resolver, or an IP literal domain string) → resolves and relays.
3. `socks5_frontend_rejects_bind` — `CMD=02` → reply `05 07`, connection closed.
4. `socks5_frontend_auth` — server with `--auth`; greeting offering only `0x00` → `05 FF`;
   greeting offering `0x02` + correct RFC 1929 creds → `05 02`,`01 00`, then relay; wrong creds
   → `01 01`.
5. `http_frontend_still_works` — a first byte that isn't `0x05` (e.g. `G` of `GET`) takes the
   HTTP path (regression: `tests/serve.rs` green).

**Acceptance criteria.**
- [ ] SOCKS5 greeting/method-select/connect/reply implemented for `CMD=01`, all three `ATYP`.
- [ ] Successful relay through the same pool + `relay` core, tunnel bytes intact.
- [ ] `--auth` gates the SOCKS5 front-end via RFC 1929 (`0x02`), symmetric with HTTP `407`.
- [ ] `BIND`/`UDP ASSOCIATE` rejected with `05 07`.
- [ ] Non-`0x05` first byte → unchanged HTTP/CONNECT path.
- [ ] Terminal upstream failure sends a SOCKS5 error reply, not an HTTP `502`, to a SOCKS5
      client.

**Risks / deviations / principle-flags.**
- ⚠ New surface with hand-rolled binary framing — but it's the *server* side of a closed,
  fixed protocol (matches negotiator.rs's "protocol set is closed" stance, negotiator.rs:5).
  No trait, no abstraction; one `parse_socks5_request` fn + one ack branch.
- ⚠ The terminal error path in `handle_client` (server.rs:263) writes an HTTP `502`
  unconditionally; must branch on `frontend` so a SOCKS5 client gets a SOCKS5 error frame.
  Easy to miss — called out in the acceptance list.
- Open question: bind-address in the success reply. `0.0.0.0:0` is the common, accepted stub;
  returning the real upstream addr leaks pool membership. Recommend the stub.
- Open question (auth): supporting `0x02` when `--auth` is set couples B12 to B9. If B9 ships
  first (recommended order), B12 gets it for free; if a maintainer wants B12 independent, ship
  no-auth-only first and add `0x02` in the same commit as the coupling. Recommend coupling.

**Effort.** M/L.

---

## What must stay green (no regressions)

- **`tests/serve.rs`** — both `server_relays_http_request_through_a_pool_proxy` and
  `server_returns_502_when_pool_is_empty`. The `Frontend` refactor (R0) and the relay reshape
  (B7) must preserve: plain-HTTP relay round-trips the body, and an empty/exhausted pool still
  yields a client-visible `502` (for HTTP front-ends; SOCKS5 front-ends now get a SOCKS5 error
  frame — a new, tested path, not a regression).
- **`server.rs` unit tests** — `best_for_*`, `tied_response_times_do_not_panic`,
  `split_host_port_variants`. `Pool::remove` (B6) must not disturb `best_for` ordering or the
  `swap_remove`-based `get` (server.rs:113/119).
- **`negotiator.rs` unit tests** — the four `connect_request_*` byte-level tests. B8 changes
  `connect_request`'s signature (adds `creds`), so these are updated to pass `None` and must
  still assert the exact same bytes when `creds = None`.
- **`checker.rs`** — the checker's `negotiate` call (checker.rs:210) gains a `None` argument;
  all existing check/anonymity tests (`tests/check_http.rs`, `tests/find.rs`) must stay green,
  proving the auth plumbing is inert on the check path.
- **`proxy.rs` serialization** — `serializes_to_python_as_json_shape` must be unchanged: B8's
  `Credentials` field is excluded from `Serialize`, so the JSON shape and field count stay put.
- **Feature gating** — everything remains under `server` (and `cli` for the `--auth` flag); a
  `--no-default-features` library build is unaffected.
