#!/usr/bin/env bash
#
# check_managed_front_door.sh — the "every managed call resolves its plane
# through one resolver" grep-guard (COR-114).
#
# THE INVARIANT: which content plane an account's managed work is admitted
# and billed on is decided in ONE place, `backend/src/managed/plane.rs`
# (`resolve_liability_plane` / `PlanePermit`). Managed admission (the gate,
# its fences, principal selection) and billing reconciliation both read it,
# so they cannot disagree. Before it, four admission checks and billing each
# inlined "consumer means `clerk_org_id IS NULL`", which refused every
# personal org (an org id by design) every managed purpose.
#
# WHAT ENFORCES IT FIRST: the compiler. `PlanePermit`'s fields are private
# and `PlanePermit::admit` is its only constructor; `ManagedGate` cannot be
# built without one (its `door`), every fence and the funded reservation's
# plane label take it, and the fence dispatch matches every `Subject`
# exhaustively, so a new managed purpose does not compile until its fence
# takes the permit. This guard is the backstop for what types cannot say:
#
#   1. An inline plane decision in the managed or billing modules (every
#      `.rs` under backend/src/managed/ except plane.rs and test modules,
#      backend/src/workers/billing_derivation.rs,
#      backend/src/billing/managed_reservation.rs):
#        - selecting `clerk_org_id` from `accounts`,
#        - `clerk_org_id IS [NOT] NULL`,
#        - `is_org_account`,
#        - the old Rust shape `(TenantId::Consumer, Some(None))`.
#      A revocation join (`JOIN accounts a ON a.clerk_org_id=r.clerk_org_id`)
#      is not a plane decision and is not flagged.
#   2. A `PlanePermit { ... }` struct literal outside plane.rs.
#   3. A production `impl SendGate for` in backend/src other than
#      `ManagedGate` (backend/src/managed/gate.rs): any other paid gate would
#      be a managed send that never met the door. Test-only gates (an item
#      under `#[cfg(test)]`, a module declared under it, `pg_tests` files)
#      are allowed.
#
# WHAT A GREEN RUN DOES NOT PROVE: it is a line-based lint. A plane decision
# split across SQL lines, or reached through a renamed column, evades rule 1;
# the permit types are what stop those. Comments are skipped (this header
# names everything it forbids).
#
# Usage:
#   scripts/check_managed_front_door.sh              # CI mode; non-zero on violation
#   scripts/check_managed_front_door.sh --self-test  # prove each rule fires
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

backend_files() {
  local root="$1"
  (cd "$root" && find backend/src -name '*.rs' -print 2>/dev/null | sort)
}

# Print `file:line: text` for every CODE line of FILE matching PERL_RE.
# Full-line comments are skipped and a trailing `//` comment stripped when
# it is not inside a string literal; string contents are KEPT (rule 1 is
# about SQL). With TEST_AWARE=1, lines after a `#[cfg(test)]` that opens a
# `mod`, and the item right after a lone `#[cfg(test)]`, are skipped.
scan() {
  local root="$1" file="$2" re="$3" test_aware="$4"
  SCAN_FILE="$file" SCAN_RE="$re" TEST_AWARE="$test_aware" perl -ne '
    BEGIN { $re = qr/$ENV{SCAN_RE}/; $cfg = 0; $in_test = 0; }
    my $line = $_;
    next if $line =~ m{^\s*//};
    if ($ENV{TEST_AWARE}) {
      next if $in_test;
      if ($line =~ /^\s*#\[cfg\(test\)\]\s*$/) { $cfg = 1; next; }
      if ($cfg) {
        $cfg = 0;
        if ($line =~ /^\s*(pub(\([^)]*\))?\s+)?mod\s+\w+/) { $in_test = 1; next; }
        next;    # the single item a lone #[cfg(test)] gates
      }
    }
    my $i = index($line, "//");
    if ($i >= 0) {
      my $prefix = substr($line, 0, $i);
      my $quotes = () = $prefix =~ /"/g;
      $line = $prefix if $quotes % 2 == 0;
    }
    if ($line =~ $re) { chomp(my $t = $_); $t =~ s/^\s+//; print "$ENV{SCAN_FILE}:$.: $t\n"; }
  ' "$root/$file"
}

PLANE_RE='(?i:\bSELECT\b[^;]*\bclerk_org_id\b[^;]*\bFROM\s+accounts\b)|(?i:\bclerk_org_id\s+IS\s+(NOT\s+)?NULL\b)|\bis_org_account\b|TenantId::Consumer\s*,\s*Some\(\s*None'

check_tree() {
  local root="$1"
  local hits=""
  local file
  while IFS= read -r file; do
    case "$file" in
      */pg_tests.rs|*/pg_tests/*|*/tests.rs) continue ;;
    esac
    # Rule 1: no inline plane decision where admission and billing live.
    case "$file" in
      backend/src/managed/plane.rs) ;;
      backend/src/managed/*|backend/src/workers/billing_derivation.rs|backend/src/billing/managed_reservation.rs)
        hits+="$(scan "$root" "$file" "$PLANE_RE" 1)"$'\n' ;;
    esac
    # Rule 2: only the resolver makes a permit.
    case "$file" in
      backend/src/managed/plane.rs) ;;
      *) hits+="$(scan "$root" "$file" '\bPlanePermit\s*\{' 0)"$'\n' ;;
    esac
    # Rule 3: ManagedGate is the only production SendGate.
    case "$file" in
      backend/src/managed/gate.rs) ;;
      *) hits+="$(scan "$root" "$file" '\bimpl\b[^{;]*\bSendGate\s+for\b' 1)"$'\n' ;;
    esac
  done < <(backend_files "$root")
  printf '%s' "$hits" | sed '/^$/d'
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$tmp/backend/src/managed/gate" "$tmp/backend/src/workers" "$tmp/backend/src/llm"
  # Allowed: the resolver itself, the gate's own impl, a revocation join,
  # comments, and test-only gates.
  cat > "$tmp/backend/src/managed/plane.rs" <<'RS'
const SQL: &str = "SELECT a.clerk_org_id,a.is_org_account FROM accounts a WHERE a.id=$1";
fn admit() -> PlanePermit { PlanePermit { account, plane, tenant } }
RS
  cat > "$tmp/backend/src/managed/gate.rs" <<'RS'
impl SendGate for ManagedGate<'_> {
}
RS
  cat > "$tmp/backend/src/managed/fine.rs" <<'RS'
// Not `SELECT clerk_org_id FROM accounts`, nor `clerk_org_id IS NULL`: see plane.rs.
let revoked = "SELECT EXISTS(SELECT 1 FROM org_revocations r JOIN accounts a ON a.clerk_org_id=r.clerk_org_id WHERE a.id=$1)";
let permit = PlanePermit::admit(conn, account, tenant).await?;
#[cfg(test)]
impl SendGate for EmbeddingGate<'_> {
}
RS
  cat > "$tmp/backend/src/llm/openai.rs" <<'RS'
fn prod() {}
#[cfg(test)]
mod tests {
    impl SendGate for RecordingGate {
    }
}
RS
  cat > "$tmp/backend/src/managed/gate/pg_tests.rs" <<'RS'
sqlx::query("SELECT clerk_org_id FROM accounts WHERE id=$1");
RS
  local clean
  clean="$(check_tree "$tmp")"
  if [ -n "$clean" ]; then
    echo "SELF-TEST FAIL: allowed shapes were flagged:" >&2
    echo "$clean" >&2
    return 1
  fi
  # Each rule must fire.
  cat > "$tmp/backend/src/managed/rogue.rs" <<'RS'
sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1 FOR SHARE")
sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id IS NULL ORDER BY id")
"SELECT clerk_org_id,is_org_account FROM accounts WHERE id=$1",
    (TenantId::Consumer, Some(None)) => true,
RS
  cat > "$tmp/backend/src/workers/billing_derivation.rs" <<'RS'
let org: Option<String> = sqlx::query_scalar("select clerk_org_id from accounts where id=$1")
RS
  cat > "$tmp/backend/src/workers/forge.rs" <<'RS'
let permit = PlanePermit { account, plane, tenant };
impl SendGate for SideDoor {
}
RS
  local found
  found="$(check_tree "$tmp")"
  local expect
  for expect in "backend/src/managed/rogue.rs:1" "backend/src/managed/rogue.rs:2" "backend/src/managed/rogue.rs:3" \
                "backend/src/managed/rogue.rs:4" "backend/src/workers/billing_derivation.rs:1" \
                "backend/src/workers/forge.rs:1" "backend/src/workers/forge.rs:2"; do
    if ! grep -q "^$expect:" <<<"$found"; then
      echo "SELF-TEST FAIL: expected a violation at $expect, got:" >&2
      echo "$found" >&2
      return 1
    fi
  done
  echo "check_managed_front_door self-test: ok"
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit $?
fi

violations="$(check_tree "$REPO_ROOT")"
if [ -n "$violations" ]; then
  echo "FAIL: managed plane decided outside the front door (see scripts/check_managed_front_door.sh):" >&2
  echo "$violations" >&2
  echo >&2
  echo "Resolve the plane with crate::managed::plane (resolve_liability_plane /" >&2
  echo "PlanePermit::admit), and send managed calls through ManagedGate." >&2
  exit 1
fi
echo "check_managed_front_door: ok (one plane resolver; ManagedGate is the only managed send gate)"
