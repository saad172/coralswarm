#!/usr/bin/env bash
#
# check_embedding_gateway.sh — the "every embedding goes through the gateway"
# grep-guard.
#
# THE INVARIANT: no code calls an embedding provider except the gateway
# (`packages/core/src/embed_gateway.rs`), and no code but the backend's
# gateway module (`backend/src/embedding.rs`) uses the core gateway directly.
# The gateway owns the input size bound (split, never truncate), batching,
# error classification (only 429/5xx/transport are retried) and how a paid
# call is billed. A provider call anywhere else skips all four, which is how
# an over-limit Gmail atom drew a provider 400 that was retried forever (prod,
# 2026-10-23; PR #1449).
#
# WHAT ENFORCES IT FIRST: the compiler. Every `EmbedProvider` /
# `GatedEmbed` method takes a `ProviderPermit`, whose constructor is
# `pub(crate)` in coralswarm-core and called only by the gateway, so a
# provider call outside packages/core does not compile. This guard is the
# backstop for what visibility cannot say across crates, and for a future
# edit that widens that visibility:
#
#   1. `embed_gateway` named in CODE outside packages/core and
#      backend/src/embedding.rs (callers use `crate::embedding` /
#      `coralswarm_backend::embedding`, which adds metering).
#   2. `ProviderPermit::new` in CODE outside packages/core/src/embed_gateway.rs,
#      except in a core file's trailing `#[cfg(test)]` module (core's own
#      transport tests drive a provider with a permit).
#   3. A provider method CALL (`.embed_batch(` / `.embed_batch_gated(`) in
#      CODE outside packages/core/src/embed_gateway.rs, with the same core
#      test-module exception. Implementations (`fn embed_batch<`) are fine
#      anywhere; only calls are refused.
#
# WHAT A GREEN RUN DOES NOT PROVE: it is a lint on names. A provider method
# renamed, or reached through a macro, evades it; the permit is what stops
# those. Comments are skipped (this header names everything it forbids).
#
# Usage:
#   scripts/check_embedding_gateway.sh              # CI mode; non-zero on violation
#   scripts/check_embedding_gateway.sh --self-test  # prove each rule fires
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Rust files to scan under ROOT: every workspace crate, never build output,
# vendored code or agent worktrees.
rust_files() {
  local root="$1"
  (cd "$root" && find . -name '*.rs' \
      -not -path '*/target/*' -not -path './vendor/*' -not -path './archive/*' \
      -not -path '*/.claude/*' -not -path '*/node_modules/*' -print | sed 's|^\./||' | sort)
}

# Print `file:line: text` for every CODE line of FILE matching PERL_RE.
# Full-line comments are skipped, a trailing `//` comment stripped (when the
# `//` is not inside a string literal) and string literals blanked. With
# CUT_TESTS=1, scanning stops
# at the first `#[cfg(test)]` line: the file's trailing test module.
scan() {
  local root="$1" file="$2" re="$3" cut_tests="$4"
  SCAN_FILE="$file" SCAN_RE="$re" CUT_TESTS="$cut_tests" perl -ne '
    BEGIN { $re = qr/$ENV{SCAN_RE}/; }
    exit 0 if $ENV{CUT_TESTS} && /^\s*#\[cfg\(test\)\]/;
    my $line = $_;
    next if $line =~ m{^\s*//};
    my $i = index($line, "//");
    if ($i >= 0) {
      my $prefix = substr($line, 0, $i);
      my $quotes = () = $prefix =~ /"/g;
      $line = $prefix if $quotes % 2 == 0;
    }
    $line =~ s/"(?:[^"\\]|\\.)*"/""/g;          # string literal contents
    if ($line =~ $re) { chomp(my $t = $_); $t =~ s/^\s+//; print "$ENV{SCAN_FILE}:$.: $t\n"; }
  ' "$root/$file"
}

check_tree() {
  local root="$1"
  local hits=""
  local file
  while IFS= read -r file; do
    case "$file" in
      packages/core/src/embed_gateway.rs) continue ;;
    esac
    # Rule 1: the core gateway is the backend gateway module's alone.
    case "$file" in
      packages/core/*|backend/src/embedding.rs) ;;
      *) hits+="$(scan "$root" "$file" '\bembed_gateway\b' 0)"$'\n' ;;
    esac
    # Rules 2 and 3: nobody else makes a permit or calls a provider. Inside
    # core, a file's trailing test module may.
    local cut=0
    case "$file" in packages/core/*) cut=1 ;; esac
    hits+="$(scan "$root" "$file" 'ProviderPermit::new\b' "$cut")"$'\n'
    hits+="$(scan "$root" "$file" '\.embed_batch(_gated)?\s*\(' "$cut")"$'\n'
  done < <(rust_files "$root")
  printf '%s' "$hits" | sed '/^$/d'
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$tmp/backend/src" "$tmp/packages/core/src" "$tmp/cli/src"
  # Allowed: the gateway module, the core gateway, a core test module, an
  # implementation, and comments naming the forbidden things.
  cat > "$tmp/backend/src/embedding.rs" <<'RS'
use coralswarm_core::embed_gateway::{self as gateway};
RS
  cat > "$tmp/packages/core/src/embed_gateway.rs" <<'RS'
fn send() { provider.embed_batch(inputs, ProviderPermit::new()); }
RS
  cat > "$tmp/packages/core/src/embed_ort.rs" <<'RS'
impl EmbedProvider for OrtEmbedder {
    fn embed_batch<'a>(&'a self, inputs: &'a [EmbedInput], _: ProviderPermit) -> F<'a> { todo!() }
}
#[cfg(test)]
mod tests {
    fn t() { e.embed_batch(&inputs, ProviderPermit::new()); }
}
RS
  cat > "$tmp/backend/src/fine.rs" <<'RS'
// Not `embed_gateway::submit` nor `.embed_batch(` directly: see crate::embedding.
let v = crate::embedding::submit(p, purpose, &texts).await; // not .embed_batch(x)
let s = "a string with // and .embed_batch( in it";
RS
  local clean
  clean="$(check_tree "$tmp")"
  if [ -n "$clean" ]; then
    echo "SELF-TEST FAIL: allowed shapes were flagged:" >&2
    echo "$clean" >&2
    return 1
  fi
  # Each rule must fire.
  cat > "$tmp/backend/src/rogue.rs" <<'RS'
use coralswarm_core::embed_gateway::submit;
RS
  cat > "$tmp/cli/src/rogue.rs" <<'RS'
let v = embedder.embed_batch(&inputs, permit).await;
let w = cloud.embed_batch_gated(&inputs, &gate, permit).await;
RS
  cat > "$tmp/packages/core/src/embed_openrouter.rs" <<'RS'
fn sneak(&self) { self.embed_batch(&inputs, ProviderPermit::new()); }
RS
  local found
  found="$(check_tree "$tmp")"
  local expect
  for expect in "backend/src/rogue.rs:1" "cli/src/rogue.rs:1" "cli/src/rogue.rs:2" "packages/core/src/embed_openrouter.rs:1"; do
    if ! grep -q "^$expect:" <<<"$found"; then
      echo "SELF-TEST FAIL: expected a violation at $expect, got:" >&2
      echo "$found" >&2
      return 1
    fi
  done
  if [ "$(grep -c 'embed_openrouter.rs:1' <<<"$found")" -lt 2 ]; then
    echo "SELF-TEST FAIL: a permit minted outside the gateway AND its call must both be flagged" >&2
    return 1
  fi
  echo "check_embedding_gateway self-test: ok"
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit $?
fi

violations="$(check_tree "$REPO_ROOT")"
if [ -n "$violations" ]; then
  echo "FAIL: embedding call outside the gateway (see scripts/check_embedding_gateway.sh):" >&2
  echo "$violations" >&2
  echo >&2
  echo "Use crate::embedding::submit / submit_one (backend) or" >&2
  echo "coralswarm_backend::embedding (other crates) with an explicit Purpose." >&2
  exit 1
fi
echo "check_embedding_gateway: ok (every provider call goes through the gateway)"
