# Wave 4 — Output & integration

*Make the CLI a universal building block. Every feature lands on the output path — the
`Format` enum and `write_stream` in `src/bin/proxybroker.rs` — except C5 (also `stats.rs` +
`types.rs`) and C7 (`geo.rs` + `proxy.rs`).*

The theme: stop making the caller reshape our output. Today the only machine format is
NDJSON (`Format::Json`, one object per line) and the only human format is `host:port`.
Downstream tools want `scheme://host:port`, CSV, a real `[...]` JSON array `jq .` can parse,
a machine-readable stats summary, an arbitrary field template, and — for users who bring a
richer geo DB — region/city. All of that is presentation; none of it touches the check
engine.

## Build order (respect the dependencies)

1. **Shared refactor — the `Emitter`** (land first). `write_stream` currently hard-codes
   "one `render()` line per proxy" in two near-identical sink loops (file async / stdout
   blocking). C4 (array wrapping) and C6 (templating) both need per-stream state and a
   header/prefix/suffix, which the stateless `Format::render(self, &Proxy) -> String` can't
   express. Introduce a small stateful `Emitter` that owns the format + optional template +
   array-separator state and yields byte chunks. Do this once, before adding formats, so the
   two sink loops don't each grow a copy of the wrapping logic.
2. **C3 — `--format url` / `--format csv`.** Two new stateless line formats. Proves the
   `Emitter` line path. `find_and_save.rs` already hand-rolls the URL form.
3. **C4 — `--format json-array` + stream mode.** The one *structural* format; exercises the
   `Emitter`'s prefix/separator/suffix state. ⚠ schema-versioning mitigation lands here.
4. **C6 — `--output-format "{{proxy}}/{{country}}/{{duration}}"`.** A third line format (a
   template), rendered through the same `Emitter` line path.
5. **C5 — `#[derive(Serialize)] for Stats` + `--stats-format json`.** Independent of the
   `Emitter` (stats go to **stderr**, not the proxy stream). Requires a `Serialize` impl for
   `AnonLevel` first.
6. **C7 — region/city from a user-supplied City DB.** Independent of the `Emitter`; touches
   `geo.rs` (lookup) and `proxy.rs` (`Country` model + `Serialize`). ⚠ CC BY 4.0 hygiene.

C3/C4/C6 are strictly ordered (each builds on the `Emitter`). C5 and C7 can be built at any
point after the refactor; they're placed last only because they reach outside the output path.

---

## Shared refactor — the `Emitter`

**Goal.** Replace the stateless `Format::render` with a stateful `Emitter` that can emit a
one-time prefix (CSV header, JSON-array `[`), a per-proxy chunk (line, or comma-separated
array element), and a one-time suffix (JSON-array `]`), so both sink loops in `write_stream`
share one source of format truth.

**Public surface (all in `src/bin/proxybroker.rs`, private to the bin — no lib API change).**

```rust
#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Default,   // host:port          (unchanged)
    Txt,       // host:port          (unchanged, alias of default)
    Json,      // one object per line — NDJSON (unchanged; the current `Json`)
    JsonArray, // C4: a single [ {...}, {...} ] document        (value: json-array)
    Url,       // C3: scheme://host:port
    Csv,       // C3: host,port,protocols,anon,country,resp_time,error_rate
}

/// Owns the format, the optional --output-format template, and array-separator state.
struct Emitter<'a> {
    format: Format,
    template: Option<&'a str>, // C6: when Some, overrides `format` (line output)
    started: bool,             // array-separator / first-element state
}

impl<'a> Emitter<'a> {
    fn new(format: Format, template: Option<&'a str>) -> Self;
    /// One-time bytes before the first proxy: CSV header line, or `[` for json-array.
    fn prefix(&self) -> Option<String>;
    /// One proxy → a chunk that already includes its own newline (line formats) or its
    /// leading separator (json-array). Mutates `started`.
    fn item(&mut self, p: &Proxy) -> String;
    /// One-time trailing bytes: `]\n` for json-array; `None` otherwise.
    fn suffix(&self) -> Option<String>;
}
```

**Design.**
- `write_stream` (`src/bin/proxybroker.rs:362`) keeps its deliberate two-branch split — file
  is async `tokio::fs::File`, stdout is a blocking `std::io::stdout().lock()`; the existing
  comment (`:367`) explains why they aren't unified behind a trait object. Each branch becomes:
  write `emitter.prefix()` once, loop `emitter.item(&proxy)` per proxy, write `emitter.suffix()`
  once. The mechanics differ per sink (~5 lines each); the *format* logic lives only in `Emitter`.
- The old `Format::render(self, &Proxy) -> String` (`:198`) collapses into `Emitter::item`
  for line formats. `Default | Txt` → `proxy.addr()`; `Json` → `serde_json::to_string(proxy).unwrap()`.
- `write_stream`'s signature gains the template: `write_stream(stream, format, template: Option<&str>, outfile)`.
  Both call sites (`grab` `:316`, `find` `:346`) pass `args.output_format.as_deref()`.
- The file branch's `count`/`wrote N proxies` log (`:378`) is preserved (count the `item` calls).

**Offline test plan** (new `#[cfg(test)] mod tests` in `src/bin/proxybroker.rs` — the bin has
none today; these are pure in-process, no sockets). Helper `fn proxy_fixture() -> Proxy` builds
a `Proxy` with a geo, one HTTP type at `High`, one recorded runtime.
- `emitter_default_is_addr_per_line` — `Format::Default`, `prefix`/`suffix` are `None`,
  `item` == `"1.2.3.4:8080\n"`. **First failing test.**
- `emitter_json_is_ndjson` — `Format::Json`, `item` is the existing single-line object + `\n`,
  no prefix/suffix (locks the NDJSON default so C4 can't regress it).

**Acceptance criteria.**
- [ ] `Format::render` removed; all format logic in `Emitter`.
- [ ] `Default`/`Txt`/`Json` output is **byte-identical** to before (existing behaviour frozen).
- [ ] Both sink branches drive `prefix`/`item`/`suffix`; no format `match` outside `Emitter`.
- [ ] `write_stream` count log unchanged.

**Risks / deviations / principle-flags.**
- ⚠ *Lazy-that-holds:* `Emitter` is one struct with concrete methods, not a trait — there is
  exactly one implementation and no extension point. It exists only because two sink loops
  need shared stateful formatting; that's real duplication, not speculative abstraction.
- First unit tests inside the bin. `cargo test` compiles the bin with `cfg(test)`; integration
  tests in `tests/` can't import bin symbols, so in-file unit tests are the offline home for
  pure formatting logic (no `assert_cmd`/process spawning, matching the project's lean deps).

**Effort:** S.

---

## C3 — `--format url` and `--format csv`

**Goal.** Emit `scheme://host:port` (the form `find_and_save.rs` hand-rolls) and a flat CSV
row per proxy, so results drop straight into shells, `curl --proxy`, and spreadsheets.

**Public surface.** Two new `Format` variants (above): `url`, `csv`. Available to **both**
`--format` on `grab` (`GrabArgs`, `:110`) and `find` (`FindArgs`, `:129`) — they share the
one `Format` enum. No lib API change.

**Design.**
- **`url`** — mirrors `examples/find_and_save.rs:29-34` exactly: `https` if
  `proxy.schemes().contains(&Scheme::Https)` (`src/proxy.rs:130`), else `http`; then
  `format!("{scheme}://{}", proxy.addr())`. A grabbed (unchecked) proxy has empty
  `types()` ⇒ empty `schemes()` ⇒ falls back to `http`. Add a private
  `fn scheme_str(p: &Proxy) -> &'static str` in the bin (`Scheme` has no `Display`/`as_str`;
  inlining the two-arm choice is lazier than widening the lib's `types.rs`).
- **`csv`** — header `host,port,protocols,anon,country,resp_time,error_rate` (via
  `Emitter::prefix`). Per row:
  - `host` = `proxy.host` (`IpAddr::to_string`, unbracketed — the port is its own column).
  - `port` = `proxy.port`.
  - `protocols` = confirmed `proxy.types()` (`src/proxy.rs:82`) keys joined with `|`
    (BTreeMap order = enum order `HTTP,HTTPS,SOCKS4,SOCKS5,CONNECT:80,CONNECT:25`, deterministic).
    `|` is chosen precisely so the field never contains a comma.
  - `anon` = `proxy.types().get(&Proto::Http).and_then(|l| *l).map(AnonLevel::as_str).unwrap_or("")`.
  - `country` = `proxy.geo.as_ref().map(|c| c.code.as_str()).unwrap_or("")`.
  - `resp_time` = `proxy.avg_resp_time()` (`:112`), `error_rate` = `proxy.error_rate()` (`:103`),
    each `{}`-formatted (already `round2`-rounded).
- **No `csv` crate.** Every field is comma-free by construction (`|`-joined protocols, ISO
  code only, numeric stats, colon-bearing proto names carry no comma). A quoting layer would
  be config-for-a-constant. Recorded as a deviation with its guard below.

**Offline test plan** (bin unit tests; pure, no sockets).
- `url_format_prefixes_scheme` — a proxy with only `Proto::Http` renders `http://1.2.3.4:8080`;
  add `Proto::Socks5` (⇒ `Scheme::Https` per `schemes_follow_protocol_families`,
  `src/proxy.rs:288`) ⇒ `https://...`. **First failing test.**
- `csv_header_and_row` — `Emitter::prefix` is the exact header; a fixture row is
  `1.2.3.4,8080,HTTP,High,US,<rt>,<er>` (assert the split has 7 fields, none containing `,`).
- `csv_unchecked_proxy_has_empty_type_columns` — grabbed proxy (no types, no geo) ⇒
  `1.2.3.4,8080,,,,0,0`.

**Acceptance criteria.**
- [ ] `--format url` matches `find_and_save.rs` byte-for-byte (sans trailing newline handled by `Emitter`).
- [ ] `--format csv` emits the exact header once, then one comma-free row per proxy.
- [ ] Both formats work on `grab` and `find`.
- [ ] `find_and_save.rs` optionally simplified to note `--format url` supersedes its hand-rolled loop (doc comment only; keep the example self-contained).

**Risks / deviations / principle-flags.**
- ⚠ *No CSV quoting.* Guarded by `csv_header_and_row` asserting exactly 7 comma-free fields.
  If a future column can hold a comma (e.g. `country_name`), that test fails loudly — the
  signal to add quoting *then*, not now.

**Effort:** S.

---

## C4 — NDJSON / JSON-array toggle + stream mode

**Goal.** Add a true `[ {...}, {...} ]` document so `jq .` parses the whole output, while
keeping NDJSON (`--format json`) as the streaming default. Fixes the recurring "why won't
`jq .` read this".

**Public surface.** New `Format` variant `json-array` (above). `--format json` stays NDJSON
(one object per line, unchanged); `--format json-array` wraps. No lib API change.

**Design.**
- `Emitter::prefix` → `Some("[".into())` for `JsonArray`.
- `Emitter::item` for `JsonArray`: `serde_json::to_string(p).unwrap()`, prefixed with `,`
  when `self.started`, then set `started = true`. No trailing newline per element.
- `Emitter::suffix` → `Some("]\n".into())` for `JsonArray`.
- Empty stream ⇒ `prefix` + `suffix` only ⇒ `[]\n` (valid empty array; `jq .` yields `[]`).
- Streaming is preserved: elements are written as proxies arrive (the bracket/commas are
  interleaved, not buffered) — no `Vec<Proxy>` is collected. This is the "stream mode": a
  well-formed array emitted incrementally.
- The per-object schema is **unchanged** — identical bytes to `Format::Json`, just wrapped.

**Offline test plan** (bin unit tests).
- `json_array_emits_bracketed_comma_separated` — feed two fixture proxies through
  `prefix`/`item`×2/`suffix`, concatenate, assert `serde_json::from_str::<Vec<Value>>` yields
  2 elements and the raw string starts `[` ends `]\n` with exactly one `,` between elements.
  **First failing test.**
- `json_array_empty_stream_is_empty_array` — no `item` calls ⇒ `"[]\n"`, parses to `[]`.
- `ndjson_still_one_object_per_line` — `Format::Json` unchanged (guards the toggle).

**Acceptance criteria.**
- [ ] `--format json-array` output parses as a single JSON array (`jq .` / `serde_json::from_str::<Vec<_>>`).
- [ ] `--format json` remains NDJSON, byte-identical to today.
- [ ] Array is emitted incrementally (no full-stream buffering).
- [ ] Per-object shape identical between `json` and `json-array`.

**Risks / deviations / principle-flags.**
- ⚠ *Schema versioning* (roadmap principle register, C4 row: "version the JSON schema before
  consumers depend on it"). **Do not add a per-object `version` field** — that would break
  `proxy.py:as_json` parity (`src/proxy.rs:180`) and every `jq` consumer. Instead **freeze the
  object shape as v1** and lock it with a golden test: extend
  `serializes_to_python_as_json_shape` (`src/proxy.rs:298`) to assert the *complete* shape
  (`host, port, geo.{country,region,city}, types[], avg_resp_time, error_rate`) so the schema
  cannot drift silently. Record "Proxy JSON = v1, frozen" in `docs/systematic-refactor/decisions.md`.
  A future breaking change is signalled by a top-level `--format` variant bump (e.g. `json2`),
  not an in-band field.

**Effort:** S.

---

## C6 — Custom output template (`--output-format`)

**Goal.** Render each proxy through a user template like
`--output-format "{{proxy}}/{{country}}/{{duration}}"` (mubeng parity), so users compose any
line shape without a downstream `awk`.

**Public surface.**

```
--output-format <TEMPLATE>   (Option<String>, default None) on GrabArgs and FindArgs
```

When set, it overrides `--format` and produces one templated line per proxy. Lib API: a
testable free function

```rust
// src/bin/proxybroker.rs
fn render_template(tmpl: &str, p: &Proxy) -> String;
```

**Design.**
- A **closed** token set (fasttemplate-style sequential replace — tokens are distinct and
  non-overlapping, so `str::replace` per token is correct and needs no parser):
  | token | source (`src/proxy.rs`) |
  |---|---|
  | `{{proxy}}` | `p.addr()` (`:74`) |
  | `{{host}}` | `p.host` |
  | `{{port}}` | `p.port` |
  | `{{scheme}}` | `scheme_str(p)` (C3 helper) |
  | `{{protocols}}` | `p.types()` keys joined `|` (C3 CSV rule) |
  | `{{anon}}` | HTTP level or `""` (C3 CSV rule) |
  | `{{country}}` | `p.geo` code or `""` |
  | `{{duration}}` | `p.avg_resp_time()` (`:112`) — mubeng's "duration"/response-time |
  | `{{error_rate}}` | `p.error_rate()` (`:103`) |
  Unknown `{{...}}` tokens are left **literally** (predictable, config-free; documented).
- Wired via `Emitter::template`: `Emitter::new(format, template)`. When `template.is_some()`,
  `item` returns `render_template(t, p) + "\n"` and `prefix`/`suffix` are suppressed
  (a template is always line output; it ignores `json-array` wrapping). `--output-format`
  therefore takes precedence over `--format`; documented on the flag's help text.
- Lives in the bin (presentation, `cli`-gated already); unit-tested in-file.

**Offline test plan** (bin unit tests).
- `template_renders_known_fields` — `render_template("{{proxy}}/{{country}}/{{duration}}", fx)`
  == `"1.2.3.4:8080/US/<rt>"`. **First failing test.**
- `template_leaves_unknown_tokens_literal` — `"{{nope}}"` renders `"{{nope}}"`.
- `output_format_overrides_format` — `Emitter::new(Format::JsonArray, Some("{{host}}"))`
  yields no `[`/`]` and one `{{host}}`-rendered line per proxy.

**Acceptance criteria.**
- [ ] Every documented token resolves against the real `Proxy` getter.
- [ ] Unknown tokens pass through unchanged.
- [ ] `--output-format` overrides `--format` (including `json-array`), on both `grab` and `find`.

**Risks / deviations / principle-flags.**
- ⚠ *Lazy-that-holds:* a closed token table + sequential `replace`, **not** a general template
  engine (no `fasttemplate`/`tinytemplate` dep). Tokens are non-overlapping literals, so this
  is correct and minimal. If nested/conditional templating is ever demanded, revisit then.

**Effort:** S.

---

## C5 — `Serialize for Stats` + `--stats-format json`

**Goal.** Emit the run summary as JSON for CI/dashboards, alongside the existing human
`Display` — a one-derive machine-readable summary.

**Public surface.**
- `stats.rs`: `#[derive(serde::Serialize)]` added to `Stats` (`src/stats.rs:14`).
- `types.rs`: **new** `impl serde::Serialize for AnonLevel` (required — see Design).
- CLI: `--stats-format <text|json>` on `FindArgs` (`ValueEnum`, default `text`).

```rust
#[derive(Clone, Copy, ValueEnum)]
enum StatsFormat { Text, Json }
```

**Design.**
- `Stats` (`src/stats.rs:14`) already derives `Debug, Clone, Default, PartialEq`. Adding
  `Serialize` needs every field serializable:
  - `by_protocol: BTreeMap<Proto, usize>` — `Proto` serializes to its wire string
    (`src/types.rs:115`, `serialize_str`), valid as a JSON map key. ✅
  - `by_anonymity: BTreeMap<AnonLevel, usize>` — **`AnonLevel` has no `Serialize` today**
    (`src/types.rs:132`, derives stop at `Hash`). Add `impl Serialize for AnonLevel` mirroring
    `Proto`'s (`serialize_str(self.as_str())`, `src/types.rs:143` `as_str`), so it's a valid
    string map key. This is the one prerequisite edit.
  - `by_country: BTreeMap<String, usize>`, `errors: BTreeMap<&'static str, u32>`,
    `total/working: usize`, `avg_resp_time: f64` — all trivially serializable.
- CLI branch: `find` (`src/bin/proxybroker.rs:348-356`) currently, under `--show-stats`, does
  `eprint!("\n{s}")` (Display, stderr). Replace with a `match args.stats_format`:
  `Text` → `eprint!("\n{s}")` (unchanged); `Json` → `eprintln!("{}", serde_json::to_string(&s)?)`.
  Stats stay on **stderr** so they never mix with the proxy stream on stdout — orthogonal to
  `--format`. `--stats-format` only takes effect together with `--show-stats` (documented);
  no implicit enabling.

**Offline test plan.**
- `stats.rs` unit test `stats_serializes_to_json` — build `Stats::from_proxies(&[...])`
  (reuse the module's `proxy()` helper, `:173`), `serde_json::to_value(&s)`, assert
  `v["total"]`, `v["working"]`, `v["by_protocol"]["HTTP"]`, `v["by_anonymity"]["High"]`,
  `v["by_country"]["US"]`, `v["avg_resp_time"]`. **First failing test** (won't compile until
  the `AnonLevel`/`Stats` derives land — TDD red).
- `types.rs` unit test `anon_level_serializes_as_wire_name` — `serde_json::to_string(&AnonLevel::High)`
  == `"\"High\""` (matches `proto_roundtrips` style, `:240`).

**Acceptance criteria.**
- [ ] `Stats` and `AnonLevel` implement `Serialize`; `by_protocol`/`by_anonymity` keys are the
      wire names (`HTTP`, `High`), not serde's default enum spelling.
- [ ] `--stats-format json` prints a single JSON object to **stderr**; `text` is unchanged.
- [ ] `--stats-format` is inert without `--show-stats`.
- [ ] Human `Display` (`src/stats.rs:114`) untouched.

**Risks / deviations / principle-flags.** None significant. `AnonLevel: Serialize` is a
genuine gap the derive forces us to close, useful independently.

**Effort:** S.

---

## C7 — Region/city from a user-supplied City DB

**Goal.** When the user passes a MaxMind **City** DB via `--geo-db`, populate `geo.region` and
`geo.city` in the JSON; the **bundled** DB stays DB-IP Country-Lite and yields empty
region/city, exactly as today.

**Public surface (lib).** `src/proxy.rs` — extend the `Country` model:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Country {
    pub code: String,
    pub name: String,
    pub region: Option<Region>, // populated only from a user City DB
    pub city: Option<String>,   // populated only from a user City DB
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Region { pub code: String, pub name: String }
```

`GeoDb::lookup` signature is unchanged (`fn lookup(&self, ip: IpAddr) -> Option<Country>`,
`src/geo.rs:44`). No new CLI flag — `--geo-db` already exists (`src/bin/proxybroker.rs:47`).

**Design.**
- **One code path for both DB kinds.** `maxminddb::geoip2::City` (0.29,
  `.../maxminddb-0.29.0/src/geoip2.rs:177`) has all-`Option`/`default` fields: `country`,
  `subdivisions: Vec<Subdivision>` (`iso_code`, `names.english`), `city: {names.english}`.
  Decoding a **Country-only** record (the bundled DB-IP) as `City` populates `country` and
  leaves `subdivisions`/`city` empty — so `lookup` can always decode `City` and the richer
  fields fill in only when the DB carries them. Rewrite `lookup` (currently decodes
  `geoip2::Country`, `src/geo.rs:45`):
  ```rust
  let rec: maxminddb::geoip2::City = self.reader.lookup(ip).ok()?.decode().ok()??;
  let code = rec.country.iso_code?;
  let name = rec.country.names.english.unwrap_or(code).to_owned();
  let region = rec.subdivisions.first().map(|s| Region {
      code: s.iso_code.unwrap_or_default().to_owned(),
      name: s.names.english.unwrap_or_default().to_owned(),
  }).filter(|r| !(r.code.is_empty() && r.name.is_empty()));
  let city = rec.city.names.english.map(str::to_owned);
  Some(Country { code: code.to_owned(), name, region, city })
  ```
- **`Serialize` update** (`src/proxy.rs:186-199`): replace the hardcoded empty region/city with
  the real values, keeping the exact shape (`region.{code,name}` as strings, empty when absent;
  `city` = the string or `Null`):
  ```rust
  let region = self.geo.as_ref().and_then(|c| c.region.as_ref());
  // "region": { "code": region.map_or("", |r| &r.code), "name": region.map_or("", |r| &r.name) }
  // "city":   self.geo.as_ref().and_then(|c| c.city.as_deref())  // -> string or null
  ```
  Update the module doc comment (`src/proxy.rs:16-22`) that currently asserts region/city are
  "always empty".
- **Bundled DB stays Country-Lite; no City data shipped.** No change to `data/`,
  `include_bytes!`, or the Cargo `include` list. The hard constraint holds by construction —
  we only *read* richer fields from whatever DB is opened.

**Offline test plan.**
- **Fixture:** a tiny City-format `.mmdb` at `tests/data/city-test.mmdb`, **not** in the crate
  `include` list (`Cargo.toml` ships only `src/**`, `examples/**`, `data/*`, licenses — never
  `tests/**`), so nothing City-shaped is published. Record provenance + license in
  `tests/data/README.md`. (Open Question below on sourcing it.)
- `tests/geo_city.rs::city_db_populates_region_and_city` — `GeoDb::open("tests/data/city-test.mmdb")`,
  look up a known fixture IP, assert `country.region` is `Some` and `country.city` is `Some`;
  serialize a `Proxy` carrying it and assert `v["geo"]["region"]["code"]` and `v["geo"]["city"]`
  are non-empty/non-null. **First failing test.**
- `src/geo.rs` test `bundled_country_db_has_no_region_city` (under `feature = "geo-bundled"`) —
  `GeoDb::bundled().lookup("8.8.8.8")` returns `region == None && city == None` (proves the
  bundled DB stays country-only — the hard constraint, under test). Existing
  `bundled_db_resolves_known_ips` (`src/geo.rs:60`) must still pass (guards that City-decode of
  a Country DB still yields `code`/`name`).
- `src/proxy.rs` golden test (the C4 schema lock) extended to assert region/city serialize as
  empty-string/`Null` for a country-only `Country`.

**Acceptance criteria.**
- [ ] A user City DB fills `geo.region.{code,name}` and `geo.city` in the JSON.
- [ ] The bundled DB yields `region:{code:"",name:""}`, `city:null` — byte-identical to today's output.
- [ ] No City data added to `data/` or the published crate (`cargo package --list` shows no `tests/`).
- [ ] `Country`/`Region` derive `Default`; the three existing `Country { code, name }` literals
      (`src/proxy.rs:304`, `src/stats.rs:179`, plus test helpers) compile via `..Default::default()`.

**Risks / deviations / principle-flags.**
- ⚠ *CC BY 4.0 / data hygiene* (roadmap register, C7). Mitigation baked in: bundled DB
  unchanged, richer fields read-only from a user DB, fixture excluded from the package. The
  `bundled_country_db_has_no_region_city` test makes the constraint executable.
- ⚠ *City-decode of a Country DB.* The design relies on `geoip2::City` decoding cleanly against
  a Country-only record (all City fields are optional/defaulted, so it should). The existing
  `bundled_db_resolves_known_ips` test is the guard; if maxminddb 0.29 rejects the decode,
  fall back to try-`City`-then-`Country` in `lookup`. Verify on first run.

**Open Question — City fixture sourcing.** The offline test needs a real City `.mmdb`. Options:
(a) vendor MaxMind's public synthetic `GeoIP2-City-Test.mmdb` (ships in the `maxminddb` crate's
`test-data/`, Apache-2.0 test data) into `tests/data/` with provenance noted; (b) generate a
minimal synthetic City DB via an mmdb-writer as a one-off and commit the bytes. (a) is lazier
and already in our dependency tree's test-data; (b) has zero third-party provenance question.
Recommend (a) with a `tests/data/README.md` recording the source and that it's test-only /
never packaged. Decide before writing the test.

**Effort:** S.

---

## What must stay green

- **All existing tests.** Notably `tests/find.rs`, `tests/grab.rs`, `tests/serve.rs`,
  `tests/check_http.rs` (offline mock-server pipeline), and the in-module suites in
  `src/proxy.rs`, `src/stats.rs`, `src/types.rs`, `src/geo.rs`.
- **NDJSON default frozen.** `--format json` must remain one object per line, byte-identical —
  the entire C4 value is *adding* an array mode without touching the streaming default.
- **`host:port` default frozen.** `--format default` / `txt` output unchanged (the `Emitter`
  refactor is behaviour-preserving).
- **Proxy JSON schema = v1.** `serializes_to_python_as_json_shape` (`src/proxy.rs:298`) still
  passes; C7 only fills previously-empty region/city, and only when a City DB is supplied —
  the country-only shape is unchanged.
- **`Stats` Display.** `--show-stats` text output (`src/stats.rs:114`) unchanged; JSON is a new
  branch, not a replacement.
- **Bundled geo stays Country-Lite.** `GeoDb::bundled` region/city empty; `data/` and the crate
  `include` list untouched; `cargo package` ships no City data.
- **Feature gates.** `geo`/`geo-bundled` still compile/pass under `--no-default-features`
  combinations; the new formatting code is `cli`-gated (lives in the bin) and adds **no** new
  dependency.
