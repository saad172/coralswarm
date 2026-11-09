#!/usr/bin/env bash
#
# check_rls_coverage.sh — the RLS-scope coverage gate (org-tenancy Phase P.1;
# extended in the follow-up that closed its Store-mediated blind spot — see
# "TWO WAYS A FILE CAN TOUCH AN RLS TABLE" below).
#
# STATUS (2026-08-04): OPS-4 HAS RUN. Verified against production:
# `DATABASE_URL`'s user IS `coralswarm_app`, `rolbypassrls = f`, and an
# unscoped query against an RLS-covered table genuinely ERRORS
# (`unrecognized configuration parameter "app.account_id"`) instead of
# silently returning all rows. Any comment still claiming the app connects as
# the owner/BYPASSRLS role — including 0083_consumer_rls.sql's own header,
# which is append-only and cannot be corrected in place — is stale; the RLS
# backstop is live enforcement on the consumer plane. (The TENANT plane's own
# cutover, OPS-4-T, is separate — see check_tenant_rls_coverage.sh.) This
# gate's job is therefore no longer pre-cutover readiness but preventing
# REGRESSIONS: an unscoped query path added today hard-errors in production.
#
# Original rationale: before OPS-4 could switch the backend's DB role to the
# least-privilege `coralswarm_app` (activating the RLS backstop from
# backend/migrations_control/0083_consumer_rls.sql), every query path that
# touches an RLS-covered consumer table had to run inside
# `tenancy::scope::begin_account_scoped` — otherwise the strict
# `current_setting('app.account_id')` predicate hard-errors that path at
# runtime. This script makes "100% of query paths converted" MACHINE-CHECKED
# instead of a manual claim:
#
#   1. The 36-table roster is derived AT RUNTIME from 0083's own
#      `CREATE POLICY account_isolation* ON <table>` lines, so the roster can
#      never drift from the actual policies.
#   2. Every .rs file under backend/src/ (EXCEPT backend/src/store/ — see
#      below) that can reach an RLS-covered table (see "TWO WAYS" below) must
#      appear in the checked-in manifest (scripts/rls_coverage.manifest) with
#      a status:
#        pending   — known, not yet converted (passes CI, counted in the
#                    summary so progress is visible on every run)
#        converted — the file's RLS-table queries run via
#                    begin_account_scoped; the file must actually contain a
#                    `begin_account_scoped` call or this FAILS (regression
#                    guard: a refactor that drops the scoping un-converts the
#                    file, and this catches it). ESCAPE HATCH, for the
#                    `ocean_stats_scoped` shape only (see the KNOWN EXCEPTION
#                    note further down): a `converted` entry carrying a
#                    `# converted:` comment line directly above it is accepted
#                    without that call. Same reviewer-owned discipline as
#                    `exempt` below — writing the rationale down is the cost of
#                    the exception, and the comment is what a future reader
#                    (and reviewer) sees instead of an unexplained hole.
#        exempt    — the file's RLS-table references are, on review, all
#                    deliberate non-scoped surfaces: queries that only ever
#                    execute against an ORG TENANT pool (whole-database
#                    isolation, no 0083 policies exist there), or calls into
#                    the audited SECURITY DEFINER carve-outs of
#                    0084/0086_bootstrap_scope_functions. An exempt entry MUST
#                    carry a `# exempt:` comment line directly above it in the
#                    manifest naming why (reviewer-owned, like `converted`'s
#                    per-diff caveat) — the script checks the comment exists.
#   3. FAILURES: a file that touches an RLS table but is absent from the
#      manifest (new unreviewed query surface); a `converted` file with no
#      begin_account_scoped call; a manifest entry whose file no longer
#      exists; a malformed/duplicate manifest line.
#
# TWO WAYS A FILE CAN TOUCH AN RLS TABLE:
#
#   (a) DIRECT SQL — the file's code references an RLS-covered table in a SQL
#       keyword position (FROM/JOIN/INTO/UPDATE <table>). This was the
#       original (P.1) detection and is unchanged.
#
#   (b) VIA THE `Store` TRAIT — the file calls a `Store` method whose SQL
#       touches an RLS table, e.g. `store.list_nodes(...)`. This was P.1's
#       blind spot: `backend/src/store/mod.rs` is excluded from THIS script's
#       own scan (see below), so a file that reaches `nodes`/`atoms`/etc.
#       exclusively through `Store` — never spelling out `FROM nodes` itself —
#       was invisible to (a) alone. The RLS-scoped `Store` rollout
#       (org-tenancy PR B.0, chunks 1-3) gives this a clean, self-maintaining
#       signal: every `Store` trait method whose SQL touches an RLS table now
#       has a `_scoped` sibling (`list_nodes` / `list_nodes_scoped`, etc.) —
#       see that trait's own "RLS-scoped transaction surface" doc comments.
#       So the RLS-touching METHOD roster is derived the same way the TABLE
#       roster is: at runtime, from the trait declarations themselves (every
#       method name X such that both `X` and `X_scoped` are declared), not
#       hand-maintained — it can't silently drift as chunk 4+ / future Store
#       methods land.
#
#   A file touches an RLS table if EITHER (a) or (b) matches.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (same honesty bar as
# check_no_ambient_pool.sh): this is a grep, not a data-flow proof.
#   * Dynamically-built SQL (format!/push_str'd table names) is invisible to
#     the keyword-adjacency pattern — the staging soak under the real
#     `coralswarm_app` role is the backstop for that (an unscoped query
#     hard-errors there; it cannot silently leak).
#   * `converted` means "the file calls begin_account_scoped somewhere", not
#     "every query in it flows through the scoped transaction" — reviewers
#     own that per-diff, exactly like the ambient-pool guard's rebind caveat.
#     ONE KNOWN EXCEPTION: `Store::ocean_stats_scoped` deliberately does NOT
#     take a caller-supplied transaction (it's a pure-read fan-out that opens
#     its own independent `begin_account_scoped` transactions internally —
#     see its doc comment in store/mod.rs) — a file whose ONLY RLS-touching
#     call is `ocean_stats_scoped` is genuinely converted without containing
#     a literal `begin_account_scoped` itself, and would need a documented
#     manifest exception rather than tripping the "converted but no
#     begin_account_scoped" failure. Such a file DOES exist as of org-tenancy
#     phase B.5a (`backend/src/self_context.rs`), so the escape hatch this
#     note anticipated is now implemented rather than hypothetical: a
#     `# converted: <why>` comment line directly above the manifest entry
#     waives the literal-call check for that one path, exactly like
#     `# exempt:` does for `exempt`. It is deliberately NOT a blanket opt-out
#     — every use is a line in the manifest a reviewer reads.
#   * The Store-method pattern matches `.method_name(` — a literal `.` then
#     the exact method name then `(`, so it cannot be fooled by another
#     method sharing a *prefix* with an RLS-touching one (`.get_node(` does
#     not match inside `.get_node_digest(`), but it COULD, in principle,
#     match a same-named method on some unrelated type. In practice every
#     RLS-touching Store method name was checked against the codebase when
#     this detection was added and none collided — a future name collision
#     would just be a (safe-direction) false positive, resolved the same way
#     as any other: a documented manifest exception.
#   * Full-line `//` comments are stripped before matching (both for table/
#     method references and for the begin_account_scoped check), so doc
#     comments quoting SQL or method names don't create false entries — a
#     trailing same-line comment containing SQL/a method call can still
#     false-positive (erring toward a false positive, never a false
#     negative).
#   * backend/src/store/ is deliberately OUT of scope for BOTH detection
#     methods: it is the shared query layer that RECEIVES already-scoped
#     transactions (the `_scoped` variants themselves — scoping happens at
#     the route/worker/service call sites this gate covers, not inside
#     Store's own implementation, which necessarily calls itself via
#     `self.other_method(...)` and would otherwise trip its own pattern).
#     backend/tests/ is out of scope too (not production query paths).
#
# Usage:
#   scripts/check_rls_coverage.sh           # CI mode; non-zero exit on failure
#   scripts/check_rls_coverage.sh --seed    # (re)generate the manifest:
#                                           # existing statuses preserved, new
#                                           # matching files added as pending,
#                                           # entries whose files no longer
#                                           # match are dropped
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MIGRATION="backend/migrations_control/0083_consumer_rls.sql"
STORE_MOD="backend/src/store/mod.rs"
MANIFEST="scripts/rls_coverage.manifest"
SRC_ROOT="backend/src"
EXCLUDE_DIR="backend/src/store"

MODE="verify"
[ "${1:-}" = "--seed" ] && MODE="seed"

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }

# ── 1a. Derive the table roster from the migration itself ───────────────────
if [ ! -f "$MIGRATION" ]; then
  echo "FAIL: $MIGRATION not found (run from the repo root)" >&2
  exit 1
fi
# node_promotions carries four split policies (account_isolation_select/
# insert/update/delete) instead of one bare `account_isolation` — the
# `[a-z_]*` suffix plus `sort -u` folds them onto one roster entry.
TABLES=$(grep -E '^CREATE POLICY account_isolation[a-z_]* ON [a-z_]+' "$MIGRATION" \
  | sed -E 's/^CREATE POLICY [a-z_]+ ON ([a-z_]+).*/\1/' | sort -u)

# ── 1a-extra: tables whose account_isolation-shaped policy was added by a
# LATER migration, not 0083 itself (review round 1, finding 6). $MIGRATION
# above is a single fixed file — it was never extended into a multi-file scan
# across every later account_isolation-adding migration (that would be the
# more thorough fix, but a MUCH wider blast radius: it would also retroactively
# pull `reef_identifiers`/`conflict_dismissal`/`membership_events`/etc. into
# this gate's roster for the first time, each a separate, unreviewed change
# this PR did not set out to make). Instead: an explicit, reviewer-owned
# supplement, same discipline as `scripts/check_schema_parity.sh`'s
# `CONTROL_ONLY_RLS_TABLES` — a human adds a line here, with the migration
# that introduced the policy, in the SAME PR that ships it.
#   reef_judgement — backend/migrations_control/0139_reef_judgement.sql
#     (judgement-graph resolver slice PR1)
#   llm_usage_pending — backend/migrations_control/0149_llm_usage_pending.sql
#     (usage-metering PR4's reserve/finalize choke point)
#   credit_ledger, credit_balances, grants, enforcement_events —
#     backend/migrations_control/0150_credit_ledger.sql (usage-metering PR8:
#     ledger, balances, plans/grants, enforcement-observation log). Added in
#     usage-metering PR10, not PR8/PR9 — a real gap those two PRs left open
#     (PR9's `workers/billing_derivation.rs` already writes `credit_ledger`/
#     `credit_balances` through `begin_account_scoped` and was already
#     `converted` in the manifest below, so this addition is retroactively
#     satisfied by existing code, not a new requirement on it). `plans` is
#     deliberately NOT listed — migration 0150's own header: "`plans` gets NO
#     RLS policy" (global reference data, no per-account boundary).
# NOTE: newline-separated (not space-separated) — this is fed straight into a
# `printf '%s\n%s\n'` that treats the whole variable as ONE `%s` substitution,
# so a space-separated list would fold onto a single bogus roster entry
# instead of two real ones.
#   grant_seat_tiers — backend/migrations_control/0168_grant_seat_tiers.sql
#     (billing self-serve launch PR B, schema remodel): the per-grant seat-
#     tier breakdown, RLS-covered via a one-hop subquery policy back to its
#     parent `grants` row (`grant_id IN (SELECT id FROM grants WHERE
#     account_id = current_setting('app.account_id')::uuid)` — the same
#     shape `0083_consumer_rls.sql`'s header documents for every table
#     reached only through a foreign key; see that migration's header and
#     0168's own header for the full reasoning). No denormalized `account_id`
#     column on this table — an earlier version of 0168 carried one on the
#     mistaken belief that a `USING` policy cannot join to another table;
#     that belief was wrong and the denormalization was removed.
# KNOWN ROSTER GAP (recorded 2026-08-25, #711 review — deliberately NOT fixed
# in that PR): at least six RLS-policied tables from later migrations are
# missing from this supplement, so a file whose ONLY RLS surface is one of
# them passes this gate green and would hard-error at runtime under
# coralswarm_app:
#   google_watch_channels (0180), gmail_watched_threads (0186),
#   outlook_everything_mode (0192), microsoft_calendar_sync_state (0189),
#   query_audit_log (0191), connector_audit_log (0193)
# Adding them will make the gate demand manifest entries for every file that
# reaches them (including the four query/connector-audit route+service files,
# currently in NO manifest entry) — do that as its own reviewed change, per
# this list's own contract below ("in the SAME PR that ships it" was already
# missed for these six). Until then this comment is the tracked record.
#   slack_unmatched_email_notices — backend/migrations_control/
#     0227_slack_unmatched_email_notices.sql (chat-per-user plan PR 6,
#     "email the unmatched"): the persisted, per-recipient email cooldown.
#     Added HERE, in the same PR that ships the table, per this list's own
#     contract — unlike the six-table gap above, this table is BRAND NEW
#     (this PR), so widening the roster costs exactly one new manifest entry
#     (services/unmatched_recipient_email.rs, its only writer) rather than
#     retroactively demanding entries from unrelated pre-existing files.
#   org_join_requests — backend/migrations_control/0238_org_join_requests.sql
#     (CS-437 / #1021): the join-request queue. Same "added in the same PR
#     that ships the table" contract as slack_unmatched_email_notices —
#     production readers/writers live in services/org_join_requests.rs
#     (`converted`); routes/org_join_requests.rs only touches this table
#     inside `#[cfg(test)]` fixture reads (already `exempt` for its
#     test-only grants/grant_seat_tiers seeding).
#   org_join_request_digests — backend/migrations_control/
#     0240_org_join_request_digests.sql (CS-438, D13): the admin digest-send
#     ledger. Same "added in the same PR that ships the table" contract as
#     org_join_requests immediately above — production reads/writes live in
#     workers/org_join_request_digest.rs (`converted`, see that manifest
#     entry for the full shape).
#   org_install_capture_grace (0257), gmail_hidden_messages (0277),
#   google_meet_captures (0269), outlook_capture_receipts (0263),
#   outlook_schedule_intents (0266), teams_meeting_captures /
#   teams_meeting_contexts / teams_meeting_installations (0260): later
#   non-0083 policies that shipped without being registered here. Added by
#   the RLS roster sync (#1169), together with their manifest entries.
#   managed_receipt_recovery_queue — backend/migrations_control/
#     0345_managed_receipt_recovery_queue.sql: the funded receipts the billing
#     worker's managed recovery has not confirmed on control yet. Added in the
#     same PR that ships the table; read and drained only by
#     workers/billing_derivation.rs inside its account-scoped financial
#     transaction (the enqueue is a SECURITY DEFINER trigger).
#   0367_extra_usage_schema.sql: extra_usage_settings, extra_usage_balances and
#     extra_usage_periods (strict account_isolation policy). Added in the PR
#     that first reads them from Rust (settings launch 3.3,
#     routes/billing_manage.rs `GET /v1/billing/summary`, inside
#     begin_account_scoped). The other 0367 tables join when their first Rust
#     caller lands.
#   0370_account_deletions.sql: account_deletions (strict account_isolation
#     policy). Added with its first Rust callers (settings launch 2.4,
#     services/account_deletion.rs inside begin_account_scoped; the cross-
#     account paths go through 0370's SECURITY DEFINER functions).
#   extra_usage_settings, extra_usage_balances, extra_usage_periods,
#   extra_usage_reloads, extra_usage_ledger, billing_notices —
#     backend/migrations_control/0367_extra_usage_schema.sql (settings launch
#     plan PR 3.1). Registered here by PR 3.2, the first PR whose Rust reads
#     and writes them, per this list's own contract (0367's header: "no
#     manifest change until Rust touches them"). 3.2's only code is
#     billing/funding_policy/db.rs, which runs inside the caller's
#     account-scoped, entry-fenced transaction. `extra_usage_rates` is
#     deliberately NOT listed: reference data with no RLS, like `plans`.
#   connector_sources — backend/migrations_control/0406_connector_sources.sql
#     (connectors redesign B1): per-member source state for the generic
#     connector catalog. Registered in the PR that first reads it from Rust
#     (connectors/catalog/facts.rs, inside begin_account_scoped).
# Read-model storage/subscriptions and durable graph/user fences are shared
# framework tables with explicit member/system scopes (control 0411).
EXTRA_RLS_TABLES="read_models
read_model_triggers
read_model_graph_fences
read_model_user_fences
source_items
source_version_chunks
source_versions
semantic_atoms
semantic_atom_passages
node_semantic_atoms
semantic_atom_windows
source_retraction_outbox
assertion_disagreements
corpus_enrichment_runs
corpus_enrichment_units
reef_judgement
llm_usage_pending
credit_ledger
credit_balances
query_billing_state
managed_credit_reservations
grants
enforcement_events
grant_seat_tiers
slack_unmatched_email_notices
org_join_requests
org_join_request_digests
org_domains
connector_health
org_auto_add_settings
org_auto_adds
chat_cold_start_notices
google_calendar_sync_attempts
microsoft_calendar_sync_attempts
org_install_capture_grace
gmail_hidden_messages
google_meet_captures
outlook_capture_receipts
outlook_schedule_intents
teams_meeting_captures
teams_meeting_contexts
teams_meeting_installations
managed_receipt_recovery_queue
extra_usage_settings
extra_usage_balances
extra_usage_periods
account_deletions
extra_usage_reloads
extra_usage_ledger
org_trials
billing_notices
connector_sources"
TABLES=$(printf '%s\n%s\n' "$TABLES" "$EXTRA_RLS_TABLES" | sort -u)

TABLE_COUNT=$(echo "$TABLES" | grep -c . || true)
if [ "$TABLE_COUNT" -lt 30 ]; then
  echo "FAIL: parsed only $TABLE_COUNT tables from $MIGRATION plus \$EXTRA_RLS_TABLES — the" >&2
  echo "CREATE POLICY pattern this script greps for no longer matches the migration; fix the" >&2
  echo "parser (or the migration formatting) before trusting this gate." >&2
  exit 1
fi

# One alternation regex over the whole roster, used to shortlist files fast.
TABLE_ALT=$(echo "$TABLES" | paste -sd'|' -)
# Keyword adjacency: FROM/JOIN/INTO/UPDATE, then spaces / string-quote chars /
# a line-continuation backslash (multi-line Rust string literals flatten to
# `FROM \  graphs`), then the table name at a non-identifier boundary.
SQL_PATTERN="(FROM|JOIN|INTO|UPDATE)[\"' \\\\]+(${TABLE_ALT})([^a-z_]|\$)"

# ── 1b. Derive the RLS-touching Store method roster from the trait itself ───
# (see "TWO WAYS" in the header). Every method with a `_scoped` sibling
# touches an RLS table — that pairing IS the roster, derived at runtime so it
# tracks the trait exactly, the same way TABLES tracks the migration.
if [ ! -f "$STORE_MOD" ]; then
  echo "FAIL: $STORE_MOD not found (run from the repo root)" >&2
  exit 1
fi
TRAIT_END_LINE=$(grep -n '^pub struct PostgresStore' "$STORE_MOD" | head -n1 | cut -d: -f1)
if [ -z "$TRAIT_END_LINE" ]; then
  echo "FAIL: could not find 'pub struct PostgresStore' in $STORE_MOD — the RLS-touching-" >&2
  echo "Store-method parser anchors on it to know where the trait declarations end; fix" >&2
  echo "the parser (or the file's structure) before trusting this gate." >&2
  exit 1
fi
ALL_TRAIT_METHODS=$(head -n "$((TRAIT_END_LINE - 1))" "$STORE_MOD" \
  | grep -E '^    async fn [a-zA-Z_][a-zA-Z0-9_]*\(' \
  | sed -E 's/^    async fn ([a-zA-Z_][a-zA-Z0-9_]*)\(.*/\1/' | sort -u)
SCOPED_BASE_NAMES=$(echo "$ALL_TRAIT_METHODS" | grep -E '_scoped$' | sed -E 's/_scoped$//' | sort -u)
# Intersect: a `_scoped` name's base MUST also be a declared trait method
# (always true in practice — every `_scoped` sibling pairs with exactly one
# unscoped method — but this is a runtime derivation, so intersect
# defensively rather than assume the invariant holds forever).
RLS_METHODS=$(comm -12 <(echo "$SCOPED_BASE_NAMES") <(echo "$ALL_TRAIT_METHODS"))
RLS_METHOD_COUNT=$(echo "$RLS_METHODS" | grep -c . || true)
if [ "$RLS_METHOD_COUNT" -lt 50 ]; then
  echo "FAIL: parsed only $RLS_METHOD_COUNT RLS-touching Store methods (methods with a" >&2
  echo "_scoped sibling) from $STORE_MOD — the trait-declaration pattern this script greps" >&2
  echo "for no longer matches; fix the parser (or the trait's formatting) before trusting" >&2
  echo "this gate." >&2
  exit 1
fi

METHOD_ALT=$(echo "$RLS_METHODS" | paste -sd'|' -)
# A non-identifier boundary (or start of the flattened line), the exact
# method name, an OPTIONAL `_scoped` suffix, then an open paren:
#   .list_nodes(              — ordinary unscoped dot-call
#   .list_nodes_scoped(       — ordinary scoped dot-call
#   list_nodes_scoped(        — a BARE (non-dot) call, the shape route files
#                                use inside the `scoped_store!` dispatch macro
#                                (org-tenancy PR B.2): `scoped_store!(store,
#                                tx, list_nodes / list_nodes_scoped(args))` —
#                                the unscoped half there is followed by ` / `,
#                                never `(`, so only the `_scoped(` half
#                                matches; that's sufficient, since it's the
#                                same call site either way.
# The boundary requirement (not a bare `\b`, for BSD/macOS grep portability —
# same reasoning as SQL_PATTERN's own trailing-boundary trick) also means
# this can't match a trait/impl DECLARATION (`    async fn method_name(` has
# a space before the name, which IS a valid boundary char — but store/mod.rs
# is excluded from this scan entirely, so its own declarations never reach
# this check). Same anti-prefix-collision property as before: `.get_node(`
# doesn't match inside `.get_node_digest(`, since neither `(` nor `_scoped(`
# follows `get_node` there.
STORE_PATTERN="(^|[^a-zA-Z0-9_])(${METHOD_ALT})(_scoped)?\\("

# Strip full-line // comments and flatten a file to one line for matching.
flatten() {
  perl -ne 'print unless m{^\s*//}' "$1" | tr '\n' ' '
}

# ── 2. Scan the source tree ──────────────────────────────────────────────────
# NOTE: no bash-4 features (associative arrays, etc.) anywhere in this script —
# macOS ships bash 3.2 and this must run locally as well as in CI.
touching_files="$(mktemp)"
manifest_norm="$(mktemp)"
per_file_status="$(mktemp)"
# Paths whose 'converted' entry carries a '# converted: <why>' waiver comment
# (the documented ocean_stats_scoped escape hatch — see the header).
waived_paths="$(mktemp)"
trap 'rm -f "$touching_files" "$manifest_norm" "$per_file_status" "$waived_paths"' EXIT

while IFS= read -r file; do
  case "$file" in
    "$EXCLUDE_DIR"/*) continue ;;
  esac
  flat="$(flatten "$file")"
  if echo "$flat" | grep -Eq "$SQL_PATTERN"; then
    echo "$file" >> "$touching_files"
    continue
  fi
  if echo "$flat" | grep -Eq "$STORE_PATTERN"; then
    echo "$file" >> "$touching_files"
    continue
  fi
done < <(find "$SRC_ROOT" -name '*.rs' | sort)

# ── seed mode: write the manifest and exit ───────────────────────────────────
if [ "$MODE" = "seed" ]; then
  {
    echo "# RLS-scope conversion manifest — consumed by scripts/check_rls_coverage.sh."
    echo "# Format: <path> <status>   status: pending | converted | exempt"
    echo "#"
    echo "# Every backend/src file (store/ excluded) that can reach an RLS-covered table"
    echo "# (roster derived from ${MIGRATION}) — either via direct SQL"
    echo "# or via a Store method with a _scoped sibling — must be listed. Flip an entry to"
    echo "# 'converted' in the SAME PR that routes its queries through"
    echo "# tenancy::scope::begin_account_scoped — the script verifies the call is actually"
    echo "# present (one documented exception: Store::ocean_stats_scoped manages its own"
    echo "# scoping internally — see the script header). A file whose RLS-table references"
    echo "# are ALL deliberate non-scoped surfaces (org-tenant-pool-only queries, or the"
    echo "# audited SECURITY DEFINER carve-outs) gets 'exempt' with a '# exempt: <why>'"
    echo "# comment directly above it. Regenerate with: scripts/check_rls_coverage.sh --seed"
    while IFS= read -r file; do
      status="pending"
      if [ -f "$MANIFEST" ]; then
        prev=$(awk -v f="$file" '$1 == f { print $2 }' "$MANIFEST" | head -n1)
        [ -n "$prev" ] && status="$prev"
      fi
      if [ "$status" = "exempt" ]; then
        # Carry the required "# exempt:" rationale comment through a reseed
        # (verify mode fails an exempt entry without one).
        prev_comment=$(grep -B1 "^$file exempt$" "$MANIFEST" | head -n1)
        case "$prev_comment" in
          '# exempt:'*) echo "$prev_comment" ;;
          *) echo "# exempt: (rationale lost by --seed — restore it from git history)" ;;
        esac
      fi
      echo "$file $status"
    done < "$touching_files"
  } > "$MANIFEST.tmp"
  mv "$MANIFEST.tmp" "$MANIFEST"
  echo "seeded $MANIFEST ($(grep -cE '^[^#]' "$MANIFEST" || true) entries, table roster: $TABLE_COUNT," \
       "Store-method roster: $RLS_METHOD_COUNT)"
  exit 0
fi

# ── 3. Verify against the manifest ───────────────────────────────────────────
if [ ! -f "$MANIFEST" ]; then
  echo "FAIL: $MANIFEST missing — run scripts/check_rls_coverage.sh --seed" >&2
  exit 1
fi

# Normalize + validate the manifest into "<path> <status>" lines (bash-3.2
# compatible: temp file + awk lookups, no associative arrays).
lineno=0
prev_line=""
while IFS= read -r line; do
  lineno=$((lineno + 1))
  case "$line" in ''|'#'*) prev_line="$line"; continue ;; esac
  path=$(echo "$line" | awk '{print $1}')
  status=$(echo "$line" | awk '{print $2}')
  extra=$(echo "$line" | awk '{print $3}')
  if [ -z "$path" ] || [ -z "$status" ] || [ -n "$extra" ]; then
    err "manifest line $lineno malformed (want '<path> <status>'): $line"
    continue
  fi
  if [ "$status" != "pending" ] && [ "$status" != "converted" ] && [ "$status" != "exempt" ]; then
    err "manifest line $lineno: unknown status '$status' for $path (want pending|converted|exempt)"
    continue
  fi
  if [ "$status" = "exempt" ] && [ "${prev_line#\# exempt:}" = "$prev_line" ]; then
    err "manifest line $lineno: $path is 'exempt' but the line above is not a '# exempt: <why>' comment"
    continue
  fi
  # A `# converted: <why>` comment directly above a converted entry waives the
  # "must contain a literal begin_account_scoped" check below — the documented
  # `ocean_stats_scoped` escape hatch (see the header). Recorded per path so
  # step 3a can consult it; the comment itself is the review artifact.
  if [ "$status" = "converted" ] && [ "${prev_line#\# converted:}" != "$prev_line" ]; then
    echo "$path" >> "$waived_paths"
  fi
  if grep -q "^$path " "$manifest_norm"; then
    err "manifest line $lineno: duplicate entry for $path"
    continue
  fi
  if [ ! -f "$path" ]; then
    err "manifest lists $path but the file does not exist — remove/rename the entry"
    continue
  fi
  echo "$path $status" >> "$manifest_norm"
  prev_line="$line"
done < "$MANIFEST"

# 3a. Every touching file must be in the manifest; converted files must still
#     call begin_account_scoped.
while IFS= read -r file; do
  status=$(awk -v f="$file" '$1 == f { print $2 }' "$manifest_norm")
  if [ -z "$status" ]; then
    flat="$(flatten "$file")"
    # A file can land here via EITHER pattern alone, so either grep may
    # legitimately find zero matches — `|| true` on each keeps that from
    # tripping `set -e`/`pipefail` (a bare `var=$(pipeline)` whose pipeline
    # exits non-zero DOES abort under `set -e`, even though downstream
    # awk/sort/paste stages ran fine on empty input).
    matched_tables=$( (echo "$flat" | grep -Eo "$SQL_PATTERN" || true) | awk '{print $NF}' | tr -cd 'a-z_\n' | sort -u | paste -sd', ' -)
    matched_methods=$( (echo "$flat" | grep -Eo "$STORE_PATTERN" || true) | sed -E 's/^[^a-zA-Z0-9_]//; s/\($//' | sort -u | paste -sd', ' -)
    err "$file can reach an RLS-covered table but is NOT in $MANIFEST —
      new (or newly-detected) query surface must be reviewed and added, e.g.:
        echo \"$file pending\" >> $MANIFEST
      (or run scripts/check_rls_coverage.sh --seed).
      Matched tables (direct SQL): ${matched_tables:-none}
      Matched Store methods (RLS-touching, via _scoped sibling): ${matched_methods:-none}"
    continue
  fi
  if [ "$status" = "converted" ] && ! grep -qx "$file" "$waived_paths"; then
    # The class-aware request read openers preserve the same account/user/
    # system-actor GUCs as their write-capable predecessors. Recognize both;
    # the read-audit guard separately rejects class-blind openers in page reads.
    if ! flatten "$file" | grep -Eq 'begin_account_scoped|begin_member_scoped|begin_tenant_scoped|begin_tenant_system_scoped|begin_request_(account|tenant|system)_read'; then
      err "$file is marked 'converted' in $MANIFEST but contains no begin_account_scoped
      (or begin_member_scoped) call — the scoping was removed (regression), never existed,
      or (rare — see the script header's ocean_stats_scoped note) this file's ONLY
      RLS-touching call is a Store method that manages its own scoping internally.
      Re-convert the file, flip the entry back to 'pending', or (for the documented
      ocean_stats_scoped case only) put a '# converted: <why>' comment line directly above
      the manifest entry."
      continue
    fi
  fi
  echo "$(dirname "$file") $status" >> "$per_file_status"
done < "$touching_files"

# 3b. Manifest entries whose file no longer references any RLS table: a
#     notice, not a failure — the grep can't distinguish "queries removed"
#     from "queries went dynamic" (see header), so a human removes the entry.
while read -r path _status; do
  if ! grep -qx "$path" "$touching_files"; then
    echo "note: $path is in $MANIFEST but no longer references any RLS-covered table" \
         "(directly or via a Store method) in a greppable position — remove the entry if" \
         "its queries are truly gone."
  fi
done < "$manifest_norm"

converted=$(awk '$2 == "converted"' "$per_file_status" | wc -l | tr -d ' ')
pending=$(awk '$2 == "pending"' "$per_file_status" | wc -l | tr -d ' ')
exempt=$(awk '$2 == "exempt"' "$per_file_status" | wc -l | tr -d ' ')
total=$((converted + pending + exempt))
echo
echo "RLS scope coverage: $converted/$total files converted ($pending pending, $exempt exempt; table roster: $TABLE_COUNT" \
     "from $(basename "$MIGRATION"); Store-method roster: $RLS_METHOD_COUNT with a _scoped sibling)"
awk '{ total[$1]++; if ($2 != "pending") conv[$1]++ }
     END { for (d in total) printf "  %s: %d/%d converted-or-exempt\n", d, conv[d], total[d] }' \
  "$per_file_status" | sort

if [ "$fail" -ne 0 ]; then
  echo "rls-coverage gate: FAILED" >&2
  exit 1
fi
echo "rls-coverage gate: OK (grep-strength check — see header for what a green run does not prove)"
