#!/bin/sh
# proxybroker installer — downloads the release binary for your OS/arch, verifies its checksum,
# and installs it to a user directory. No sudo, no build toolchain.
#
#   curl -fsSL https://raw.githubusercontent.com/TurtIeSocks/proxybroker-rs/main/install.sh | sh
#
# Environment overrides:
#   PROXYBROKER_VERSION   release tag to install (default: the latest release)
#   PROXYBROKER_BIN_DIR   install directory     (default: $HOME/.local/bin)
set -eu

REPO="TurtIeSocks/proxybroker-rs"
BIN="proxybroker"
BIN_DIR="${PROXYBROKER_BIN_DIR:-$HOME/.local/bin}"

err() {
	echo "install.sh: $*" >&2
	exit 1
}
command -v curl >/dev/null 2>&1 || err "curl is required"
command -v tar >/dev/null 2>&1 || err "tar is required"

# Resolve the version (default: latest release tag).
VERSION="${PROXYBROKER_VERSION:-}"
if [ -z "$VERSION" ]; then
	VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" |
		grep '"tag_name"' | head -1 | cut -d'"' -f4)
fi
[ -n "$VERSION" ] || err "could not resolve the latest release version"

# Detect OS/arch and map to a release target triple.
os=$(uname -s)
arch=$(uname -m)
case "$os" in
Linux)
	case "$arch" in
	x86_64 | amd64) target="x86_64-unknown-linux-musl" ;;
	aarch64 | arm64) target="aarch64-unknown-linux-musl" ;;
	*) err "unsupported architecture: $arch" ;;
	esac
	;;
Darwin)
	case "$arch" in
	x86_64 | amd64) target="x86_64-apple-darwin" ;;
	arm64 | aarch64) target="aarch64-apple-darwin" ;;
	*) err "unsupported architecture: $arch" ;;
	esac
	;;
*) err "unsupported OS: $os (on Windows, download the .zip from the Releases page)" ;;
esac

asset="$BIN-$VERSION-$target.tar.gz"
base="https://github.com/$REPO/releases/download/$VERSION"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $asset ..."
curl -fsSL "$base/$asset" -o "$tmp/$asset"
curl -fsSL "$base/$asset.sha256" -o "$tmp/$asset.sha256"

# Verify the checksum before touching the install directory. The .sha256 file may be a bare hash
# or "<hash>  <file>"; take the first field and re-form the line the checker expects.
echo "Verifying checksum ..."
hash=$(cut -d' ' -f1 <"$tmp/$asset.sha256")
[ -n "$hash" ] || err "empty checksum"
if command -v sha256sum >/dev/null 2>&1; then
	(cd "$tmp" && echo "$hash  $asset" | sha256sum -c - >/dev/null) || err "checksum mismatch"
elif command -v shasum >/dev/null 2>&1; then
	(cd "$tmp" && echo "$hash  $asset" | shasum -a 256 -c - >/dev/null) || err "checksum mismatch"
else
	echo "install.sh: no sha256 tool found; skipping verification" >&2
fi

echo "Installing to $BIN_DIR ..."
tar -xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$BIN_DIR"
install -m 0755 "$tmp/$BIN" "$BIN_DIR/$BIN"

# CC BY 4.0: the binary embeds the DB-IP geo database, so the attribution must travel with it.
echo ""
echo "Installed $BIN $VERSION to $BIN_DIR/$BIN"
echo "IP Geolocation by DB-IP (https://db-ip.com), licensed CC BY 4.0"
case ":$PATH:" in
*":$BIN_DIR:"*) ;;
*) echo "Note: $BIN_DIR is not on your PATH — add it, e.g.  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
