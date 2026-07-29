# Security policy

## Reporting a vulnerability

Report privately through [GitHub Security Advisories](https://github.com/TurtIeSocks/proxybroker-rs/security/advisories/new).
Please don't open a public issue for anything exploitable.

Include what you need to reproduce it: version or commit, the command or API call,
and what you observed. If you have a patch, attach it and it'll be credited.

Expect a first reply within a few days. This is a small project without an on-call
rotation, so response times are best effort rather than a guarantee.

## Supported versions

Fixes go to the latest release. Older versions are not patched. Upgrading is the
supported path.

## Threat model

Worth stating plainly, because it changes what counts as a vulnerability.

**Public proxies are untrusted by design.** This tool finds, checks and serves
third-party proxies operated by strangers. A proxy that logs traffic, injects
content, or lies about its anonymity level is behaving exactly as the threat model
expects. Never send credentials, tokens, or private data through a proxy this tool
discovered. That a public proxy can see plaintext traffic is not a bug here.

What is in scope:

- The local server (`serve`): anything that lets a client reach a resource it
  should not, bypass `--auth`, or make the process crash or exhaust memory.
- The checker and negotiator: anything where a hostile proxy or judge response
  compromises the client running this tool, rather than merely returning bad data.
- Anonymity classification: a proxy reported as `High` anonymity while leaking the
  client IP is a real bug, since callers act on that label.
- TLS handling: any path where a client's end-to-end TLS gets terminated or
  downgraded without that being explicit.
- The installer and container image: checksum handling, privilege, and anything
  that lets an attacker substitute a binary.

What is out of scope:

- Malicious behaviour by a discovered proxy, as above.
- Rate limits or bans from provider sites.
- Denial of service against a server you deliberately exposed to the internet.
  The listener defaults to `127.0.0.1` and `--auth` exists; binding it publicly
  without auth is a deployment choice.

## Notes for operators

The server binds `127.0.0.1:8888` by default. If you change that, use `--auth`.
An open proxy on a public interface will be found and abused, usually within hours.

The container image runs as UID 65532 and needs no writable filesystem. Run it
`--read-only` with `--cap-drop=ALL` unless you have a reason not to.

Release binaries ship with SHA-256 checksums, and `install.sh` verifies them. It
fails closed: no checksum tool means no install.
