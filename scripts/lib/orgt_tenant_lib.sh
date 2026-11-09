#!/usr/bin/env bash
#
# orgt_tenant_lib.sh — shared helpers for the concierge tenant provisioning
# scripts (onboard_tenant.sh / offboard_tenant.sh / verify_tenant_isolation.sh /
# export_member_ocean.sh).
#
# Source it; do not execute directly. Requires DATABASE_URL to point at the
# CONTROL-plane Postgres (which holds `tenant_databases`).
#
# Control-plane role split (OPS-4, migration 0083_consumer_rls): DATABASE_URL
# now points at the least-privilege `coralswarm_app` role in real deployments,
# which is SELECT-only on `tenant_databases` (INSERT/UPDATE/DELETE revoked).
# Read helpers below (orgt_routing_status, orgt_account_id_for_org, the
# offboard-script lookup) still go straight through `$DATABASE_URL` — SELECT
# is unaffected. WRITE helpers (orgt_routing_insert_provisioning,
# orgt_routing_set_status) instead go through `$ORGT_CONTROL_WRITE_DSN`,
# which resolves to the elevated owner DSN `MIGRATOR_DATABASE_URL` (the same
# one the `migrate_control`/`migrate_tenants` release-command binaries use)
# when set, falling back to `DATABASE_URL` for pre-OPS-4/dev/CI environments
# where that IS still the owner role. A real (non-dry-run) onboarding/
# offboarding run against a post-cutover control DB requires
# MIGRATOR_DATABASE_URL to be set for the writes to succeed.
#
# Honesty note: the routing-table, migrate, fingerprint, and DEK-provisioning
# steps are fully implemented here. External-service steps (Neon project
# create, Fly secret set) run the real CLI commands but are gated behind
# --dry-run for rehearsal. One step is intentionally delegated to its blocking
# PR and is NOT faked:
#   * cross-tenant isolation probes -> PR 1.2 (the binding gate must be deployed)

set -euo pipefail

ORGT_DRY_RUN="${ORGT_DRY_RUN:-0}"

orgt_log()  { printf '\033[36m[orgt]\033[0m %s\n' "$*" >&2; }
orgt_warn() { printf '\033[33m[orgt] WARN:\033[0m %s\n' "$*" >&2; }
orgt_die()  { printf '\033[31m[orgt] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Run a command, or just print it under --dry-run.
orgt_run() {
  if [ "$ORGT_DRY_RUN" = "1" ]; then
    printf '\033[90m[dry-run] %s\033[0m\n' "$*" >&2
  else
    "$@"
  fi
}

orgt_require_bin() {
  command -v "$1" >/dev/null 2>&1 || orgt_die "required command not found: $1"
}

orgt_require_control_db() {
  [ -n "${DATABASE_URL:-}" ] || orgt_die "DATABASE_URL (control-plane connection) must be set"
  orgt_require_bin psql
}

# The elevated control-plane DSN the WRITE helpers below use. `MIGRATOR_
# DATABASE_URL` (the owner role) wins when set and non-empty; otherwise falls
# back to `DATABASE_URL` (correct pre-OPS-4/dev/CI, where DATABASE_URL still
# IS the owner role). Mirrors `migrator_database_url()` in
# `backend/src/lib.rs` / the `migrate_control`/`migrate_tenants` binaries.
ORGT_CONTROL_WRITE_DSN="${MIGRATOR_DATABASE_URL:-${DATABASE_URL:-}}"

# Call once, after `orgt_require_control_db`, before any control-plane WRITE
# (routing insert/set-status). Dies if neither DSN is configured at all;
# warns (does not die) when only DATABASE_URL is available, since that is a
# legitimate pre-OPS-4/dev/CI shape but a real post-cutover run needs
# MIGRATOR_DATABASE_URL for the writes to succeed against `coralswarm_app`.
orgt_require_control_write_db() {
  [ -n "$ORGT_CONTROL_WRITE_DSN" ] || orgt_die \
    "MIGRATOR_DATABASE_URL (preferred, the elevated owner DSN) or DATABASE_URL must be set to write tenant_databases"
  if [ -z "${MIGRATOR_DATABASE_URL:-}" ]; then
    orgt_warn "MIGRATOR_DATABASE_URL is not set; routing writes will use DATABASE_URL instead — \
this only works if DATABASE_URL is still the elevated owner role (pre-OPS-4/dev/CI). Once the \
control DB's DATABASE_URL points at the least-privilege coralswarm_app role, set \
MIGRATOR_DATABASE_URL to the owner DSN or these writes will fail with permission denied."
  fi
}

# Derive the Fly-secret / env-var name from a slug: uppercase, hyphens -> _.
# Slugs are already constrained by the tenant_databases charset CHECK, so the
# result is always a valid env identifier; db_secret_ref UNIQUE rejects any
# post-normalization collision at insert time.
orgt_secret_ref() {
  local slug="$1"
  printf 'TENANT_DB_%s' "$(printf '%s' "$slug" | tr 'a-z-' 'A-Z_')"
}

# Resolve a tenant DSN by its `db_secret_ref` into the shell variable named by
# $2, WITHOUT printing it (issue #1514). Same chain and ref gate as every
# per-tenant operator binary (`tenancy::secrets::OperatorDsnResolver`):
#   1. the environment variable named by the ref (a concierge tenant's Fly
#      secret, or an operator export), else
#   2. `migrate_tenants --resolve-dsn <REF>`, which reads the KEK-encrypted
#      `tenant_db_credentials` row (a self-serve tenant has no Fly secret).
#      It needs DATABASE_URL (the app role holds SELECT on that table) or
#      MIGRATOR_DATABASE_URL, plus MASTER_KEK_HEX, all simply inherited.
#      ORGT_MIGRATE_TENANTS_BIN overrides the binary (`/app/migrate_tenants`
#      on a backend machine); otherwise it is `cargo run` from this checkout.
# The binary's stdout is the DSN alone and is captured, never echoed; its
# stderr names only the ref and the reason. A miss DIES: no tenant is ever
# skipped silently, and nothing falls through to libpq's PG* defaults.
#
# Call at STATEMENT level (`orgt_resolve_tenant_dsn "$REF" VAR`), never inside
# `$(...)`, where `orgt_die` would only exit the subshell.
orgt_resolve_tenant_dsn() {
  local ref="$1" out="$2" dsn=""
  [[ "$ref" =~ ^TENANT_DB_[A-Z0-9_]+$ ]] ||
    orgt_die "refusing to resolve a tenant DSN: the secret ref is not a TENANT_DB_ name (value not shown)"
  [[ "$out" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || orgt_die "orgt_resolve_tenant_dsn: bad output variable name"
  if [ -n "${!ref:-}" ]; then
    dsn="${!ref}"
  else
    local cmd=()
    if [ -n "${ORGT_MIGRATE_TENANTS_BIN:-}" ]; then
      cmd=("$ORGT_MIGRATE_TENANTS_BIN")
    else
      orgt_require_bin cargo
      cmd=(cargo run -q -p coralswarm-backend --bin migrate_tenants --)
    fi
    if ! dsn="$("${cmd[@]}" --resolve-dsn "$ref")"; then
      orgt_die "could not resolve $ref: not in the environment, and not readable from tenant_db_credentials \
(needs DATABASE_URL or MIGRATOR_DATABASE_URL, plus MASTER_KEK_HEX)"
    fi
  fi
  case "$dsn" in
    postgres://?*|postgresql://?*) ;;
    *) orgt_die "the resolved value for $ref is not a postgres DSN (not shown)" ;;
  esac
  printf -v "$out" '%s' "$dsn"
}

# --- control-plane SQL helpers (always real; read-only ones ignore --dry-run) ---

orgt_ctl_sql() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -Atc "$1"; }

orgt_account_id_for_org() {
  # accounts.clerk_org_id is set when the org account is provisioned (Clerk
  # webhook / first admin login). Empty result => not yet provisioned.
  #
  # THIS RESOLVES A PERSONAL ORG TOO, AND THAT IS DELIBERATE. It is a lookup,
  # not a policy gate: it answers "which account carries this org id", and for
  # a person whose account has been relabelled by
  # `bin/adopt_personal_org_account.rs` the answer is their own home account.
  # Callers that must not act on a personal home call
  # `orgt_refuse_personal_org` FIRST, at statement level.
  #
  # Why the refusal is not inlined here: both call sites are
  # `X="$(orgt_account_id_for_org "$ORG" || true)"`. `orgt_die` runs `exit 1`,
  # which inside `$(...)` exits only the COMMAND-SUBSTITUTION SUBSHELL — the
  # `|| true` then swallows it, the variable comes back empty, and the script
  # dies several lines later with an unrelated message ("no control accounts
  # row ... nothing to migrate"). Fail-closed by accident, with the wrong
  # reason printed. A refusal has to be its own statement to be readable.
  orgt_ctl_sql "SELECT id FROM accounts WHERE clerk_org_id = $(orgt_sql_lit "$1") LIMIT 1"
}

# --- the personal-org refusal (shared; call at STATEMENT level) --------------
#
# THE HAZARD. A legacy shared-DB org and a PERSONAL org are indistinguishable
# in `accounts`. Both are a row with a non-null `clerk_org_id`. The only thing
# that tells them apart is a row in `personal_orgs` (control migration 0208).
#
# So the obvious evacuation query — "every account with a non-null
# `clerk_org_id` still on the consumer plane" — selects every personal home in
# the fleet, the founder's own included, and hands them to a script whose job
# is to move an org's content into a fresh tenant database and leave the
# original behind, unreachable. That must be impossible, not merely
# discouraged.
#
# TWO SIGNALS THAT LOOK USABLE AND ARE NOT:
#
#   * `accounts.is_org_account` is NOT independent. A trigger at
#     `migrations_control/0169_account_members_seat_tier.sql:342` sets
#     `NEW.is_org_account := (NEW.clerk_org_id IS NOT NULL)` on every INSERT
#     and UPDATE. Testing it is testing `clerk_org_id IS NOT NULL` under
#     another name, so it is `true` for every personal org as well. The
#     `--self-test` block in `migrate_shared_db_org.sh` installs this exact
#     trigger on its fixture and asserts the point, so it stays proved rather
#     than asserted in a comment.
#
#   * "personal means `clerk_org_id IS NULL`" is NOT true any more.
#     `bin/adopt_personal_org_account.rs` RELABELS the person's existing
#     account (`UPDATE accounts SET clerk_org_id = ...`) rather than migrating
#     ~213k rows onto the empty account Clerk's webhook made. After it runs, an
#     adopted user has NO `clerk_org_id IS NULL` account at all.
#
# `personal_orgs` is therefore the only discriminator, which is why this
# function exists and why it fails closed.
#
# FAIL CLOSED. Every path that is not a proved-absent `personal_orgs` row is a
# refusal: no `DATABASE_URL`, no `psql`, a missing table (a control DB older
# than 0208), a permission error, a non-numeric result. "The check could not
# run" and "the check said no" reach the same outcome by design — the only way
# to proceed is a query that ran and returned zero.
#
# READ-ONLY, AND IT IGNORES `--dry-run` ON PURPOSE. Like `orgt_routing_status`
# above, this is a SELECT. A rehearsal that skipped the check would rehearse
# the wrong thing: the whole point is that an operator sees the refusal during
# the dry run, before the real one.
#
# Usage: orgt_refuse_personal_org <clerk_org_id> [what-is-being-refused]
orgt_refuse_personal_org() {
  local org="$1" what="${2:-this operation}"

  [ -n "$org" ] || orgt_die \
    "refusing $what: no clerk_org_id was given to the personal-org check (fail closed)"

  if [ -z "${DATABASE_URL:-}" ]; then
    orgt_die "refusing $what for org $org: DATABASE_URL is unset, so the personal_orgs check \
CANNOT RUN. This check is the only thing separating a legacy shared-DB org from a person's own \
ocean, so a check that cannot run is a refusal, never a pass. Set DATABASE_URL to the control-plane \
DSN and try again."
  fi
  if ! command -v psql >/dev/null 2>&1; then
    orgt_die "refusing $what for org $org: psql is not installed, so the personal_orgs check \
CANNOT RUN (fail closed)."
  fi

  local n rc=0
  n="$(orgt_ctl_sql \
    "SELECT count(*) FROM personal_orgs WHERE clerk_org_id = $(orgt_sql_lit "$org")" 2>&1)" || rc=$?

  if [ "$rc" -ne 0 ] || ! [[ "$n" =~ ^[0-9]+$ ]]; then
    orgt_die "refusing $what for org $org: the personal_orgs check FAILED TO RUN \
(psql exit $rc, output: ${n:-<empty>}). A control database with no \`personal_orgs\` table predates \
migration 0208 and cannot tell a legacy shared-DB org apart from somebody's personal ocean, so \
there is no safe way to continue. Fix the check, do not bypass it."
  fi

  if [ "$n" -ne 0 ]; then
    orgt_die "REFUSED: org $org has a row in \`personal_orgs\` — it is the Clerk organization that \
STANDS FOR ONE PERSON'S OWN OCEAN (control migration 0208), not a legacy shared-DB org. \
$what would act on that person's home account as if it were org content. \
Note that \`accounts.is_org_account\` is TRUE for this org and \`clerk_org_id\` is NOT NULL, because \
a trigger derives the first from the second and \`adopt_personal_org_account\` relabelled the \
person's existing account — neither signal can tell you what \`personal_orgs\` just did. \
If you believe this org is genuinely a legacy shared-DB org, the \`personal_orgs\` row is the thing \
to investigate; do not remove this check."
  fi
}

# --- the canonical evacuation query -----------------------------------------
#
# Prints the SQL that answers precondition 3 of `docs/plans/database-architecture.md`
# ("Evacuate legacy shared-DB orgs from the consumer plane, before sharding").
# The same text appears in that document between its
# `<!-- BEGIN canonical-evacuation-query -->` markers, and
# `migrate_shared_db_org.sh --self-test` compares the two so the copy in the
# plan cannot drift away from the copy that runs.
#
# THE LOAD-BEARING LINE IS THE `personal_orgs` EXCLUSION. Everything else is
# narrowing; that one line is the difference between an evacuation list and a
# list of every person's home ocean.
#
# `graphs` lives in the shared consumer content tree, which is the same
# physical database as the control tree today — that colocation is precisely
# what "shared-DB org" means and what this precondition ends. `orgt_ctl_sql`
# reaches both, as the existing step-0 content check already relies on.
orgt_evacuation_candidates_sql() {
  cat <<'SQL'
SELECT a.id AS account_id,
       a.clerk_org_id,
       (SELECT count(*) FROM graphs g WHERE g.account_id = a.id) AS shared_graphs
  FROM accounts a
 WHERE a.clerk_org_id IS NOT NULL
   -- The whole point. A personal org is an ordinary org-labelled account in
   -- every other respect, so without this line the founder's own home is the
   -- first row of the result.
   AND NOT EXISTS (SELECT 1 FROM personal_orgs p
                    WHERE p.clerk_org_id = a.clerk_org_id)
   -- Already in the tenancy pipeline (any status) => not stranded on the
   -- consumer plane, and not this script's business. `gate.rs` gives the same
   -- meaning to the absence of a row: 403 "not provisioned for service".
   AND NOT EXISTS (SELECT 1 FROM tenant_databases t
                    WHERE t.clerk_org_id = a.clerk_org_id)
   -- Nothing to evacuate is not a candidate; it is an onboard_tenant.sh job.
   AND EXISTS (SELECT 1 FROM graphs g WHERE g.account_id = a.id)
 ORDER BY shared_graphs DESC, a.clerk_org_id
SQL
}

# The rows the query above deliberately leaves out, so the exclusion is
# VISIBLE to the operator instead of silent. An empty candidate list next to
# "excluded N personal orgs" is a different message from an empty list alone.
orgt_evacuation_excluded_sql() {
  cat <<'SQL'
SELECT a.id AS account_id,
       a.clerk_org_id,
       p.clerk_user_id,
       (SELECT count(*) FROM graphs g WHERE g.account_id = a.id) AS shared_graphs
  FROM accounts a
  JOIN personal_orgs p ON p.clerk_org_id = a.clerk_org_id
   -- Mirrors the same two narrowing predicates as the candidate query above,
   -- so this is exactly the complement: a personal org that would otherwise
   -- have been a candidate, not every personal org regardless of shape.
 WHERE NOT EXISTS (SELECT 1 FROM tenant_databases t
                    WHERE t.clerk_org_id = a.clerk_org_id)
   AND EXISTS (SELECT 1 FROM graphs g WHERE g.account_id = a.id)
 ORDER BY a.clerk_org_id
SQL
}

orgt_routing_status() {
  orgt_ctl_sql "SELECT status FROM tenant_databases WHERE clerk_org_id = $(orgt_sql_lit "$1") LIMIT 1"
}

# Single-quote-escape a value for inline SQL (helpers use bound params where they
# can; this is for the few literal lookups).
#
# BUG FIX (found during member-private-oceans PR 9): the previous body,
# `${1//\'/\'\'}`, does NOT double a single quote the way its own comment and
# every caller assumed. Bash's `${var//pattern/replacement}` does not treat a
# backslash before an ordinary character in `replacement` as an escape (only
# `\&` and `\\` are special there) — so the literal 4-character sequence
# `\'\'` in the source is what gets substituted in verbatim: a real embedded
# BACKSLASH followed by two literal quotes, not two literal quotes alone.
# Every existing caller (slug, clerk_org_id, account_id, secret_ref, region)
# is charset-constrained and never contains a quote, so this never fired in
# practice — but it is the exact class of thing this project's own review
# culture calls out (see this file's "quoting footgun" note): a latent
# escaping bug in shared SQL-building code, waiting for the first genuinely
# free-form caller. `export_member_ocean.sh`'s member-email argument is that
# first caller (real-world emails can legally contain an apostrophe, e.g.
# `o'brien@example.com`), and the old body would corrupt exactly that input
# into a query PostgreSQL then rejects with a syntax error (fails safe against
# injection — the stray backslash cannot re-open the string early under
# `standard_conforming_strings = on`, the default since PG 9.1 — but it is
# simply wrong for correctness).
#
# The fix routes the quote character through a variable so the pattern and
# replacement are each a plain, unambiguous single-quote character (no
# backslash anywhere in the substitution), which doubles every embedded quote
# exactly once with no stray characters.
orgt_sql_lit() {
  local q="'"
  printf "'%s'" "${1//$q/$q$q}"
}

orgt_routing_insert_provisioning() {
  local org="$1" account_id="$2" slug="$3" secret_ref="$4" region="$5"
  # Same `orgt_sql_lit` guard the read helpers above already use (security
  # review P2-2 on PR #282) — these values are charset-constrained today
  # (slug/secret_ref by the `tenant_databases` CHECK constraints, org/account_id
  # by Clerk/Postgres id formats), so this is defense-in-depth, not a live
  # injection fix, but it keeps every write helper in this file consistent
  # with the read helpers' escaping instead of relying on that constraint
  # holding forever.
  local region_sql
  if [ -n "$region" ]; then
    region_sql="$(orgt_sql_lit "$region")"
  else
    region_sql='NULL'
  fi
  # WRITE — routed through the elevated $ORGT_CONTROL_WRITE_DSN, not
  # $DATABASE_URL (see the "Control-plane role split" note at the top of this
  # file): INSERT on `tenant_databases` is revoked from `coralswarm_app` by
  # migration 0083.
  orgt_run psql "$ORGT_CONTROL_WRITE_DSN" -v ON_ERROR_STOP=1 -c \
    "BEGIN;
     SELECT set_config('app.account_id',$(orgt_sql_lit "$account_id"),true);
     SELECT public.billing_enter_account($(orgt_sql_lit "$account_id")::uuid);
     INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, region, status)
     VALUES ($(orgt_sql_lit "$org"), $(orgt_sql_lit "$account_id"), $(orgt_sql_lit "$slug"), $(orgt_sql_lit "$secret_ref"), $region_sql, 'provisioning')
     ON CONFLICT (clerk_org_id) DO NOTHING;
     COMMIT;"
}

orgt_routing_set_status() {
  # Note: avoid the local name `status` — it is a readonly special variable in
  # zsh, which breaks if this lib is sourced into a zsh shell.
  local org="$1" new_status="$2"
  # `orgt_sql_lit`-guarded — see the note in `orgt_routing_insert_provisioning`.
  # WRITE — routed through the elevated $ORGT_CONTROL_WRITE_DSN, not
  # $DATABASE_URL: UPDATE on `tenant_databases` is revoked from
  # `coralswarm_app` by migration 0083 (see the note atop this file).
  orgt_run psql "$ORGT_CONTROL_WRITE_DSN" -v ON_ERROR_STOP=1 -c \
    "UPDATE tenant_databases SET status = $(orgt_sql_lit "$new_status"), updated_at = now() \
     WHERE clerk_org_id = $(orgt_sql_lit "$org")"
}

# Insert the single-row tenant_meta fingerprint into the TENANT database (via
# its DSN), asserting the org matches. Idempotent for the SAME org (re-running
# is a safe no-op); FAILS CLOSED if `tenant_meta` already carries a DIFFERENT
# org_id (CodeRabbit review, PR #282): an unconditional
# `ON CONFLICT DO UPDATE SET org_id = EXCLUDED.org_id` would silently RELABEL
# another org's tenant database onto the new org — exactly the wrong-DSN /
# DSN-swap scenario this fingerprint exists to catch (and would defeat, since
# `TenantRegistry`'s own pool-build assert just checks `tenant_meta.org_id ==
# expected`, which a silent relabel makes trivially pass for the wrong DB).
#
# The `WHERE tenant_meta.org_id = EXCLUDED.org_id` clause makes the UPDATE arm
# a no-op whenever the existing row's org_id differs, so `RETURNING org_id`
# comes back EMPTY in exactly that case (a real query failure would already
# have aborted the script via `ON_ERROR_STOP` before this check ever runs) —
# empty result here can only mean "a differently-fingerprinted row already
# exists", which this function treats as an abort, never a silent overwrite.
# `orgt_sql_lit`-guarded — see the note in `orgt_routing_insert_provisioning`.
orgt_tenant_set_fingerprint() {
  local tenant_dsn="$1" org="$2"
  local sql
  sql="INSERT INTO tenant_meta (id, org_id) VALUES (true, $(orgt_sql_lit "$org")) \
       ON CONFLICT (id) DO UPDATE SET org_id = EXCLUDED.org_id \
       WHERE tenant_meta.org_id = EXCLUDED.org_id \
       RETURNING org_id"
  if [ "$ORGT_DRY_RUN" = "1" ]; then
    printf '\033[90m[dry-run] psql <tenant DSN> -v ON_ERROR_STOP=1 -c "%s"\033[0m\n' "$sql" >&2
    return 0
  fi
  local result
  result="$(psql "$tenant_dsn" -v ON_ERROR_STOP=1 -Atqc "$sql")"
  if [ -z "$result" ]; then
    local existing
    existing="$(psql "$tenant_dsn" -v ON_ERROR_STOP=1 -Atqc \
      "SELECT org_id FROM tenant_meta WHERE id = true")"
    orgt_die "tenant_meta is already fingerprinted for a DIFFERENT org ('$existing' != '$org') — \
refusing to relabel this tenant database (DSN-swap guard); this database does not belong to '$org'"
  fi
}

# Idempotently insert the org's commons "org" ocean into the TENANT database
# (via its DSN) — belt-and-braces alongside the code-path self-heal in
# `backend/src/tenancy/registry.rs`'s `TenantRegistry::ensure_org_ocean`
# (called from `build()`, i.e. every time this org's tenant pool is ever
# resolved — the request path, `/mcp`, the tenant-harness worker, and THIS
# script's own step 7 `orgt_provision_dek` call, which itself triggers a pool
# build via `routes::admin::rotate_dek`). That means the ocean would already
# get created as a side effect of step 7 even without this step — this one
# exists so the ocean shows up as an EXPLICIT, auditable onboarding action
# rather than an implicit side effect of a differently-named step, and so
# onboarding still creates it even if a future refactor ever changes what
# rotate-dek touches.
#
# Same idempotency shape as `orgt_tenant_set_fingerprint` just above:
# `graphs`' `uq_graphs_one_org_per_account` partial unique index
# (`... WHERE kind = 'org'`, `migrations_tenant/0001_tenant_baseline.sql`)
# makes `ON CONFLICT ... DO NOTHING` safe to re-run, and — critically — safe
# to run BEFORE OR AFTER the code-path self-heal fires: whichever gets there
# first creates the row, the other is a no-op, never a duplicate and never an
# error. `orgt_sql_lit`-guarded — see the note in
# `orgt_routing_insert_provisioning` (this file's values are UUID/plain-text
# name inputs, not user-facing free text, so this is defense-in-depth here
# too, not a live injection fix).
orgt_tenant_ensure_org_ocean() {
  local tenant_dsn="$1" account_id="$2" name="$3"
  local sql
  sql="INSERT INTO graphs (account_id, name, kind) \
       VALUES ($(orgt_sql_lit "$account_id"), $(orgt_sql_lit "$name"), 'org') \
       ON CONFLICT (account_id) WHERE kind = 'org' DO NOTHING"
  if [ "$ORGT_DRY_RUN" = "1" ]; then
    printf '\033[90m[dry-run] psql <tenant DSN> -v ON_ERROR_STOP=1 -c "%s"\033[0m\n' "$sql" >&2
    return 0
  fi
  psql "$tenant_dsn" -v ON_ERROR_STOP=1 -c "$sql"
}

# --- Clerk slug parity (the frontend's ONLY source of the org host) ----------
#
# `tenant_databases.slug` MUST equal the Clerk organization slug. This is not a
# cosmetic convention — it is a hard reachability requirement:
#
#   * the web app builds the org host from `useOrganization().organization.slug`
#     (Clerk's slug) and redirects the user to `<slug>.<WEB_APEX_DOMAIN>`;
#   * it has NO alternative source for that slug: `/v1/me` sits behind
#     `require_auth`, which sits behind the binding gate, so an org user landing
#     on the apex is rejected BEFORE any server-side lookup could hand back the
#     right host, and there is no unauthenticated org -> host endpoint;
#   * the gate resolves the routing row by the SUBDOMAIN SLUG, so if the two
#     ever disagree the redirect lands on a host with no routing row -> 404,
#     with no server-side reconciliation anywhere in the stack.
#
# `onboard_tenant.sh` takes <slug> and <clerk_org_id> as two independent
# arguments, so a single typo in either silently produces an org that can never
# be reached. This check closes that hole by asking Clerk itself.
#
# Requires `CLERK_SECRET_KEY` (the Clerk Backend API secret, same var the
# backend uses) and `jq`. The secret is passed via curl `--config` on an fd, not
# `-H` on argv, so it never lands in `ps`/`/proc/<pid>/cmdline` — same idiom as
# `orgt_provision_dek` above.
ORGT_CLERK_API_BASE="${ORGT_CLERK_API_BASE:-https://api.clerk.com/v1}"

# Fetch one Clerk organization as JSON. Dies (with the API's own error body) on
# any non-200 — a 404 here means the org id itself is wrong.
orgt_clerk_get_org() {
  local org="$1"
  orgt_require_bin curl
  orgt_require_bin jq
  [ -n "${CLERK_SECRET_KEY:-}" ] || orgt_die \
    "CLERK_SECRET_KEY must be set to verify slug parity against Clerk (see .env.example)"
  local resp code body
  # `--config` reads the Authorization header from a process-substitution fd,
  # never argv. Status code is appended on its own trailing line so a non-200
  # can still surface Clerk's error body.
  resp="$(curl -sS -w $'\n%{http_code}' \
    --config <(printf 'header = "Authorization: Bearer %s"\n' "$CLERK_SECRET_KEY") \
    "$ORGT_CLERK_API_BASE/organizations/$org")" \
    || orgt_die "Clerk Backend API request failed (org=$org)"
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  [ "$code" = "200" ] || orgt_die "Clerk Backend API returned HTTP $code for org '$org': $body"
  printf '%s' "$body"
}

# Assert that Clerk's own record for <clerk_org_id> has EXACTLY <slug>.
#
# Two assertions, not one. `GET /organizations/{id}` accepts either an org id OR
# an org slug in the path, so checking only `.slug` would happily pass when the
# operator swapped the two arguments (`onboard_tenant.sh acme acme` resolves and
# matches) — and we would then write the SLUG into `tenant_databases.clerk_org_id`,
# which never matches a real JWT `org_id`. Asserting `.id` too makes the argument
# order itself part of the contract.
orgt_verify_clerk_slug() {
  local org="$1" slug="$2"
  local json clerk_id clerk_slug
  json="$(orgt_clerk_get_org "$org")"
  clerk_id="$(printf '%s' "$json" | jq -r '.id // empty')"
  clerk_slug="$(printf '%s' "$json" | jq -r '.slug // empty')"
  [ -n "$clerk_slug" ] || orgt_die \
    "Clerk org '$org' has no slug — give the organization a slug in Clerk before onboarding \
(the web app builds the tenant host from it and has no fallback)"
  [ "$clerk_id" = "$org" ] || orgt_die \
    "Clerk resolved '$org' to organization id '$clerk_id' — the second argument must be the \
Clerk ORG ID (org_...), not the slug; did you pass the arguments in the wrong order?"
  [ "$clerk_slug" = "$slug" ] || orgt_die \
    "slug parity FAILED: Clerk org '$org' has slug '$clerk_slug', but this run was given \
'$slug'. Onboarding with a mismatched slug creates a tenant that can NEVER be reached — the \
web app redirects org users to '<clerk-slug>.<web apex>' and the binding gate resolves the \
routing row by that subdomain, so it would 404 with no server-side reconciliation. Re-run with \
'$clerk_slug', or rename the org in Clerk first."
  orgt_log "slug parity OK: Clerk org $clerk_id -> slug '$clerk_slug'"
}

# POST-ONBOARDING HALF (was a known gap here, now CLOSED in backend/src): the
# check above is onboarding-time only, so an admin RENAMING the org's slug in
# Clerk afterwards used to re-open the identical breakage — the web app starts
# redirecting to the NEW slug's host while `tenant_databases.slug` still holds
# the old one, every org user 404s, and nothing reconciles it. The Clerk
# `organization.updated` handler (`backend/src/routes/clerk_webhook.rs` ->
# `tenancy::routing::reconcile_slug`) now reconciles the rename into
# `tenant_databases.slug` and drops the old slug, the new slug, AND the org-id
# key from the routing cache. It refuses (loudly, leaving routing untouched) a
# rename into a reserved/malformed slug or one another tenant already owns, and
# ignores out-of-order/replayed deliveries via the `clerk_slug_updated_at`
# watermark (migration 0088). Proofs: `backend/tests/org_slug_rename_pg.rs`.
#
# It does NOT make this check redundant: reconciliation only ever repairs a row
# that already exists, so an onboarding-time typo still produces a tenant that
# can never be reached — no `organization.updated` will ever name it.

# Provision (first call, while the routing row is still `provisioning` — the
# rotate-dek endpoint's status gate allows that; see routes/admin.rs's module
# doc) or rewrap (subsequent calls — same DEK, re-wrapped under the current
# KEK; never a NEW key unless the caller explicitly passes force) an org's
# tenant-DB DEK via the internal admin endpoint (org-tenancy PR 1.3:
# POST /internal/accounts/{account_id}/rotate-dek?tenant=<clerk_org_id>). The
# key itself is generated and KEK-wrapped server-side — it is never a raw
# value in shell args, env, or history. `INTERNAL_TOKEN` is required and is
# NEVER echoed, including under --dry-run — and (PR 1.3 security review, P2)
# it is passed to curl via `--config`/a header file, NOT `-H` on argv, so it
# never appears in `ps`/`/proc/<pid>/cmdline` on a shared host.
#
# `ORGT_INTERNAL_BASE_URL` defaults to loopback because that is where a LOCAL
# backend binds its internal router (`lib.rs`'s `INTERNAL_BIND_ADDR`, default
# `127.0.0.1`). That default is for local development ONLY.
#
# It is NOT where the deployed backend listens. Production sets
# `INTERNAL_BIND_ADDR=fly-local-6pn`, so the internal router is bound to that
# machine's private-network address and NOTHING listens on its loopback —
# running this on the backend host without an override now fails. Reach a
# deployed backend one of two ways, and set `ORGT_INTERNAL_BASE_URL` to match:
#
#   * from a laptop — `fly proxy <local-port>:8081 -a coralswarm-backend`,
#     then `http://127.0.0.1:<local-port>`
#   * on the machine itself — `http://[fly-local-6pn]:8081` (that name resolves
#     via the machine's own `/etc/hosts`; it does not resolve anywhere else)
#
# This comment previously said the internal router was "loopback-only by
# design". That misread `lib.rs::internal_app`'s doc, which always documented
# the override, and it is now wrong about production besides.
orgt_provision_dek() {
  local account_id="$1" org="$2"
  local base="${ORGT_INTERNAL_BASE_URL:-http://127.0.0.1:8081}"
  orgt_require_bin curl
  [ -n "${INTERNAL_TOKEN:-}" ] || orgt_die "INTERNAL_TOKEN must be set to provision the org DEK"
  if [ "$ORGT_DRY_RUN" = "1" ]; then
    printf '\033[90m[dry-run] curl -X POST %s/internal/accounts/%s/rotate-dek?tenant=%s --config <header file, X-Internal-Token: <redacted>>\033[0m\n' \
      "$base" "$account_id" "$org" >&2
    return 0
  fi
  # `--config` reads the header from a process-substitution fd, not argv, so
  # the token never shows up in `ps`/`/proc/<pid>/cmdline`.
  #
  # `-w '%{http_code}'` instead of `-f`: `-f` collapses every 4xx/5xx into one
  # non-zero exit, and this call has exactly one failure mode an operator hits
  # repeatedly and cannot diagnose from "request failed" — a token that belongs
  # to a DIFFERENT deployment than `$base`. See the 401/403 arm.
  # Not named `status`: that identifier is READ-ONLY in zsh, so a Mac operator
  # sourcing this lib interactively to debug would hit a confusing failure that
  # has nothing to do with the request.
  local http_status
  http_status="$(curl -s -o /dev/null -w '%{http_code}' \
    -X POST "$base/internal/accounts/$account_id/rotate-dek?tenant=$org" \
    --config <(printf 'header = "X-Internal-Token: %s"\n' "$INTERNAL_TOKEN"))" \
    || http_status="000"

  case "$http_status" in
    2??) return 0 ;;
    401|403)
      # The trap this arm exists for: `INTERNAL_TOKEN` is PER-DEPLOYMENT, and
      # the repo's `.env` carries the LOCAL value. Sourcing `.env` and then
      # pointing `ORGT_INTERNAL_BASE_URL` at a deployed backend is the natural
      # thing to do and always yields this, because a deployed backend's token
      # is its own app secret — which the platform will not read back, so it
      # cannot be re-derived from the environment at all.
      orgt_die "DEK provisioning was REJECTED (HTTP $http_status) by $base — the token was wrong, not the request.
  INTERNAL_TOKEN is per-deployment. The repo's .env holds the LOCAL value; a deployed
  backend has its own app secret, which the platform will not read back. Export the
  token belonging to THIS base URL, not whatever .env last set.
  (account_id=$account_id org=$org)"
      ;;
    000)
      # curl never got an HTTP response at all. The most likely cause is
      # pointing at loopback while the target backend binds its internal
      # router somewhere else — see this function's header.
      orgt_die "DEK provisioning could not REACH $base (no HTTP response).
  The internal router is never on the public port. Where it IS depends on the target:
  a LOCAL backend binds loopback (the default here); the DEPLOYED backend binds its
  private-network address (INTERNAL_BIND_ADDR=fly-local-6pn), so its loopback has
  nothing on it at all. For a deployed backend set ORGT_INTERNAL_BASE_URL to either
  http://127.0.0.1:<port> behind 'fly proxy <port>:8081 -a coralswarm-backend', or
  http://[fly-local-6pn]:8081 when running on the machine itself.
  (account_id=$account_id org=$org)"
      ;;
    *)
      orgt_die "DEK provisioning request failed (HTTP $http_status, account_id=$account_id org=$org)"
      ;;
  esac
}
