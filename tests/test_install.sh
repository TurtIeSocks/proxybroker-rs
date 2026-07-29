#!/bin/sh
# Contract test for install.sh.
#
# The installer's job is to fetch two specific filenames from a GitHub Release. Nothing in CI used
# to check that those filenames are the ones release.yml actually publishes, so a drift between the
# two shipped silently: the installer asked for `<bin>-<ver>-<target>.tar.gz.sha256` while releases
# carried `<bin>-<ver>-<target>.sha256`. Every platform 404'd on the checksum and, under `set -e`,
# the installer died. Shellcheck was clean throughout — a lint cannot see a URL that does not exist.
#
# So this asserts the contract from both ends, offline: the names the installer builds, and the
# names release.yml is configured to produce.
#
# Run: sh tests/test_install.sh
set -eu

unset CDPATH
repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
installer="$repo_root/install.sh"
release_workflow="$repo_root/.github/workflows/release.yml"
failures=0

fail() {
	echo "FAIL: $*" >&2
	failures=$((failures + 1))
}

ok() {
	echo "ok: $*"
}

# Run the installer far enough to compute asset names, then stop before any network call. `curl` is
# shadowed by a stub that records the URLs it was asked for; PATH is prepended so the stub wins.
stub_dir=$(mktemp -d)
trap 'rm -rf "$stub_dir"' EXIT
urls="$stub_dir/urls"
: >"$urls"

cat >"$stub_dir/curl" <<STUB
#!/bin/sh
# Record any URL argument, write an empty file where -o points, and succeed.
out=""
for arg in "\$@"; do
	case "\$arg" in
	https://*) printf '%s\n' "\$arg" >>"$urls" ;;
	esac
done
prev=""
for arg in "\$@"; do
	if [ "\$prev" = "-o" ]; then out="\$arg"; fi
	prev="\$arg"
done
[ -n "\$out" ] && : >"\$out"
exit 0
STUB
chmod +x "$stub_dir/curl"

# A resolvable version keeps the "latest release" lookup off the network. The run is expected to
# fail once it hits the (empty) checksum file — by then both URLs have been recorded.
PATH="$stub_dir:$PATH" \
	PROXYBROKER_VERSION=v9.9.9 \
	PROXYBROKER_BIN_DIR="$stub_dir/bin" \
	sh "$installer" >/dev/null 2>&1 || true

archive_url=$(grep '\.tar\.gz$' "$urls" 2>/dev/null | head -1 || true)
checksum_url=$(grep '\.sha256$' "$urls" 2>/dev/null | head -1 || true)

[ -n "$archive_url" ] || fail "installer requested no .tar.gz archive"
[ -n "$checksum_url" ] || fail "installer requested no .sha256 checksum"

archive=$(basename "${archive_url:-none}")
checksum=$(basename "${checksum_url:-none}")

# The bug this test exists for: the checksum name must be its own `<stem>.sha256`, never the
# archive name with `.sha256` glued on.
case "$checksum" in
*.tar.gz.sha256) fail "checksum name is archive+.sha256 ($checksum) — release.yml publishes <stem>.sha256" ;;
*) ok "checksum is not archive+.sha256" ;;
esac

expected_checksum="${archive%.tar.gz}.sha256"
if [ "$checksum" = "$expected_checksum" ]; then
	ok "checksum name matches the archive stem ($checksum)"
else
	fail "expected $expected_checksum, installer asked for $checksum"
fi

# Both must be built from the same version the caller pinned, or the installer is fetching a
# different release than it reports.
case "$archive" in
*v9.9.9*) ok "archive carries the requested version" ;;
*) fail "archive name lost PROXYBROKER_VERSION: $archive" ;;
esac
case "$checksum" in
*v9.9.9*) ok "checksum carries the requested version" ;;
*) fail "checksum name lost PROXYBROKER_VERSION: $checksum" ;;
esac

# The other end of the contract. release.yml drives asset naming through a `checksum: sha256`
# setting, which produces `<stem>.sha256` alongside the archive. If that ever becomes an explicit
# name, this assertion is the reminder to re-check the installer against it.
if [ -f "$release_workflow" ]; then
	if grep -q 'checksum: sha256' "$release_workflow"; then
		ok "release.yml still publishes checksums as <stem>.sha256"
	else
		fail "release.yml no longer sets 'checksum: sha256' — re-verify the installer's naming"
	fi
else
	fail "release workflow not found at $release_workflow"
fi

# Fail closed when no SHA-256 tool exists: a curl|sh installer must not proceed unverified.
if grep -q 'refusing to install unverified' "$installer"; then
	ok "installer fails closed without a sha256 tool"
else
	fail "installer no longer refuses to install without checksum verification"
fi

if [ "$failures" -eq 0 ]; then
	echo "install.sh contract: all checks passed"
	exit 0
fi
echo "install.sh contract: $failures check(s) failed" >&2
exit 1
