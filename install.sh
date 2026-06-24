#!/usr/bin/env bash
# install.sh — download + install latest backlog-server release for this OS/arch.
#
# Private repo: uses `gh release download` (gh CLI required, must be authed).
# Per DESIGN.md §2: install.sh reinstall is the upgrade path. No self-update in the binary.
#
# Usage:
#   ./install.sh                # latest release
#   ./install.sh v0.3.1         # specific tag
#   PREFIX=/opt/local ./install.sh   # override install dir
set -euo pipefail

REPO="${BACKLOG_REPO:-docean552-star/backlog-server}"
TAG="${1:-}"

# --- platform detection ---------------------------------------------------------
uname_s=$(uname -s)
uname_m=$(uname -m)
case "$uname_s" in
  Darwin) os="darwin" ;;
  Linux)  os="linux" ;;
  *) echo "unsupported OS: $uname_s (this script handles darwin/linux; windows users grab the zip manually)" >&2; exit 1 ;;
esac
case "$uname_m" in
  x86_64|amd64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "unsupported arch: $uname_m" >&2; exit 1 ;;
esac

# --- gh CLI required (private repo, no anonymous downloads) ---------------------
if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI not found. Install: https://cli.github.com/" >&2
  exit 1
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "gh not authenticated. Run: gh auth login" >&2
  exit 1
fi

# --- resolve target tag ---------------------------------------------------------
if [ -z "$TAG" ]; then
  TAG=$(gh release view --repo "$REPO" --json tagName -q .tagName)
  if [ -z "$TAG" ]; then
    echo "no releases found in $REPO" >&2
    exit 1
  fi
fi
VERSION="${TAG#v}"
asset="backlog-server_${VERSION}_${os}_${arch}.tar.gz"

# --- pick install dir -----------------------------------------------------------
if [ -n "${PREFIX:-}" ]; then
  bindir="$PREFIX/bin"
elif [ -w /usr/local/bin ] 2>/dev/null; then
  bindir="/usr/local/bin"
else
  bindir="$HOME/.local/bin"
fi
mkdir -p "$bindir"

# --- download + extract ---------------------------------------------------------
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

echo "Downloading $REPO@$TAG ($asset) into $tmpdir"
gh release download "$TAG" --repo "$REPO" --pattern "$asset" --dir "$tmpdir"
tar -xzf "$tmpdir/$asset" -C "$tmpdir"

# --- install --------------------------------------------------------------------
install -m 0755 "$tmpdir/backlog-server" "$bindir/backlog-server"
echo "Installed: $bindir/backlog-server ($TAG)"

case ":$PATH:" in
  *":$bindir:"*) ;;
  *) echo "WARN: $bindir is not in \$PATH. Add: export PATH=\"$bindir:\$PATH\"" >&2 ;;
esac

"$bindir/backlog-server" help 2>&1 | head -2 || true
