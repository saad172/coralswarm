#!/usr/bin/env bash
#
# tenant_restore_drill.sh — the automated half of a tenant restore drill.
#
# .github/workflows/tenant-restore-drill.yml runs this. It restores ONE nightly
# tenant dump into a THROWAWAY target and proves the copy is usable. The
# operator-run phases (fetch, restore, verify, sample-decrypt) stay in
# scripts/rehearse_tenant_restore.sh; this script adds what a workflow needs
# around them:
#
#   scratch-roles     create, in a scratch Postgres, the roles the dump's RLS
#                     policies name. A fresh container has none of them, and
#                     CREATE POLICY ... TO <role> fails without them. Reads the
#                     dump's schema only.
#   neon-create       create a throwaway Neon BRANCH of the tenant's project
#                     and an EMPTY database on it; export TARGET_DSN (masked,
#                     direct endpoint) and the branch id for neon-delete.
#   neon-delete       delete that branch. Runs under `if: always()`. Refuses
#                     any branch whose name does not start with restore-drill-.
#   sanity            assert the restored copy is whole: every table in the
#                     dump exists, the migration ledger is present with no
#                     failed rows, rows came back, and pg_restore reported no
#                     errors beyond the allow-list. Prints exact row counts.
#   hold-check        prove a held copy under restore-hold/ matches its source:
#                     same size, same recorded SHA-256. Read-only.
#   --self-test       pure assertions, no I/O. Runs in CI.
#
# NEVER PRINTS a DSN, password or key. TARGET_DSN is masked with
# ::add-mask:: before it reaches $GITHUB_ENV.
#
# ENV VARS:
#   TARGET_DSN            sanity / scratch-roles: the restored database.
#   DRILL_DUMP            sanity / scratch-roles: the decrypted dump file.
#   DRILL_RESTORE_LOG     sanity: pg_restore's captured stderr.
#   DRILL_MIN_TABLES      sanity: floor on restored tables (default 30).
#   NEON_API_KEY          neon-create / neon-delete.
#   NEON_PROJECT_ID       neon-create / neon-delete: the TENANT's project.
#   DRILL_BRANCH_NAME     neon-create: default restore-drill-<run id>.
#   DRILL_BRANCH_ID       neon-delete: set by neon-create via $GITHUB_ENV.
#   BACKUP_S3_*           hold-check.
#   DRILL_HOLD_KEY        hold-check: restore-hold/<slug>/<name>.
#   DRILL_SOURCE_KEY      hold-check: tenant/<slug>/<name> (may be pruned).

set -euo pipefail
umask 077

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  printf 'ERROR: this script requires bash >= 4\n' >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '[restore-drill] %s\n' "$*" >&2; }
die() { printf '[restore-drill] ERROR: %s\n' "$*" >&2; exit 1; }

PSQL="${PG_BIN_DIR:+$PG_BIN_DIR/}psql"
PG_RESTORE="${PG_BIN_DIR:+$PG_BIN_DIR/}pg_restore"
NEON_API="${NEON_API:-https://console.neon.tech/api/v2}"
BRANCH_PREFIX="restore-drill-"

# ---------------------------------------------------------------------------
# Pure functions (unit-tested by --self-test)
# ---------------------------------------------------------------------------

# policy_roles — reads plain SQL (pg_restore -f -) on stdin and prints each
# role named in a CREATE POLICY ... TO clause, one per line, sorted, unique.
# PUBLIC and the CURRENT_USER/SESSION_USER keywords are not roles to create.
policy_roles() {
  perl -ne 'next unless /^CREATE POLICY /; print "$1\n" if / TO (.+?)(?: USING| WITH CHECK|;|$)/' \
    | tr ',' '\n' | sed -E 's/^ +//; s/ +$//; s/^"(.*)"$/\1/' \
    | { grep -vixE 'public|current_user|session_user|current_role' || true; } \
    | { grep . || true; } | sort -u
}

# migration_floor_decision <restored-max> <repo-max> -> ok | older | newer | refuse
# The restored ledger's highest version against the highest file in
# backend/migrations_tenant at the checked-out ref. "older" is normal for an
# old dump; "newer" means the dump came from code ahead of this checkout.
# Non-numeric input is a refusal.
migration_floor_decision() {
  local got="${1:-}" repo="${2:-}"
  case "$got" in ''|*[!0-9]*) printf 'refuse'; return 0 ;; esac
  case "$repo" in ''|*[!0-9]*) printf 'refuse'; return 0 ;; esac
  got=$((10#$got)); repo=$((10#$repo))
  if [ "$got" -lt 1 ]; then printf 'refuse'
  elif [ "$got" -eq "$repo" ]; then printf 'ok'
  elif [ "$got" -lt "$repo" ]; then printf 'older'
  else printf 'newer'
  fi
}

# restore_error_lines — reads pg_restore stderr on stdin and prints the error
# lines NOT on the allow-list. The allow-list is narrow on purpose: a restore
# that "mostly worked" must not read as a pass.
#   - `schema "public" already exists`: the target's own public schema.
#   - `must be owner of extension` / `extension ... already exists`: Neon
#     pre-installs some extensions on a new database.
#   - `pg_restore: warning: errors ignored on restore: N`: the summary line.
restore_error_lines() {
  grep -E '^pg_restore: error:' \
    | grep -vE 'schema "public" already exists' \
    | grep -vE 'must be owner of extension|extension "[^"]+" already exists' \
    || true
}

# branch_name_decision <name> -> ok | refuse. Only throwaway drill branches
# may be created or deleted by this script.
branch_name_decision() {
  case "$1" in
    "$BRANCH_PREFIX"?*) [[ "$1" =~ ^restore-drill-[a-z0-9-]+$ ]] && printf 'ok' || printf 'refuse' ;;
    *) printf 'refuse' ;;
  esac
}

# repo_tenant_migration_max -> highest NNNN in backend/migrations_tenant.
repo_tenant_migration_max() {
  local dir="$HERE/../backend/migrations_tenant"
  find "$dir" -maxdepth 1 -name '[0-9][0-9][0-9][0-9]_*.sql' -exec basename {} \; \
    | cut -c1-4 | sort -n | tail -1 | sed 's/^0*//'
}

self_test() {
  local FAILED=0
  t() {
    if [ "$2" = "$3" ]; then log "PASS: $1"; else
      printf '[restore-drill] FAIL: %s\n  want: %s\n  got:  %s\n' "$1" "$2" "$3" >&2; FAILED=1; fi
  }
  local sql
  sql="$(printf '%s\n' \
    'CREATE POLICY p1 ON public.atoms TO coralswarm_tenant_app USING ((graph_id = 1));' \
    'CREATE POLICY p2 ON public.nodes FOR SELECT TO coralswarm_tenant_app, "neondb_owner" USING (true);' \
    'CREATE POLICY p3 ON public.jobs TO PUBLIC USING (true);' \
    'CREATE POLICY p4 ON public.jobs USING (true);' \
    'CREATE TABLE public.atoms (id bigint);' \
    'CREATE POLICY p5 ON public.x TO coralswarm_tenant_reader WITH CHECK (true);')"
  t "policy_roles" "coralswarm_tenant_app coralswarm_tenant_reader neondb_owner" \
    "$(printf '%s\n' "$sql" | policy_roles | tr '\n' ' ' | sed 's/ $//')"
  t "policy_roles skips PUBLIC and role-less policies" "" \
    "$(printf '%s\n' 'CREATE POLICY p ON t TO PUBLIC USING (true);' 'CREATE POLICY q ON t USING (true);' | policy_roles)"

  t "migrations equal" "ok" "$(migration_floor_decision 182 182)"
  t "migrations zero-padded" "ok" "$(migration_floor_decision 0182 182)"
  t "migrations older dump" "older" "$(migration_floor_decision 180 182)"
  t "migrations newer dump" "newer" "$(migration_floor_decision 183 182)"
  t "migrations empty ledger" "refuse" "$(migration_floor_decision '' 182)"
  t "migrations zero" "refuse" "$(migration_floor_decision 0 182)"
  t "migrations garbage" "refuse" "$(migration_floor_decision abc 182)"

  t "restore errors: allow-listed only" "" \
    "$(printf '%s\n' \
      'pg_restore: error: could not execute query: ERROR:  schema "public" already exists' \
      'pg_restore: error: could not execute query: ERROR:  must be owner of extension vector' \
      'pg_restore: warning: errors ignored on restore: 2' | restore_error_lines)"
  t "restore errors: a real error survives" \
    'pg_restore: error: could not execute query: ERROR:  relation "atoms" does not exist' \
    "$(printf '%s\n' \
      'pg_restore: error: could not execute query: ERROR:  schema "public" already exists' \
      'pg_restore: error: could not execute query: ERROR:  relation "atoms" does not exist' | restore_error_lines)"

  t "branch name: drill" "ok" "$(branch_name_decision restore-drill-12345-1)"
  t "branch name: main" "refuse" "$(branch_name_decision main)"
  t "branch name: bare prefix" "refuse" "$(branch_name_decision restore-drill-)"
  t "branch name: injected" "refuse" "$(branch_name_decision 'restore-drill-x;rm')"

  t "repo tenant migration max is numeric" "yes" \
    "$(case "$(repo_tenant_migration_max)" in ''|*[!0-9]*) echo no ;; *) echo yes ;; esac)"

  [ "$FAILED" -eq 0 ] || die "self-test FAILED"
  log "self-test: all assertions passed"
}

# ---------------------------------------------------------------------------
# Neon API (token from NEON_API_KEY; never printed)
# ---------------------------------------------------------------------------

neon() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS --fail-with-body -X "$method" "$NEON_API$path"
    -H "Authorization: Bearer $NEON_API_KEY" -H 'Accept: application/json')
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}"
}

# Wait until no operation on the branch is still running. Neon answers 423
# Locked to a mutation while one is.
neon_wait_ops() {
  local branch="$1" running
  for _ in $(seq 1 90); do
    running="$(neon GET "/projects/$NEON_PROJECT_ID/operations?limit=50" \
      | jq --arg b "$branch" '[.operations[] | select(.branch_id == $b and (.status == "running" or .status == "scheduling"))] | length')"
    [ "$running" = "0" ] && return 0
    sleep 2
  done
  die "Neon operations on branch $branch still running after 180 s"
}

cmd_neon_create() {
  : "${NEON_API_KEY:?NEON_API_KEY must be set}" "${NEON_PROJECT_ID:?NEON_PROJECT_ID must be set}"
  local name="${DRILL_BRANCH_NAME:-$BRANCH_PREFIX${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
  [ "$(branch_name_decision "$name")" = ok ] || die "refusing branch name $name"
  local resp branch dsn
  resp="$(neon POST "/projects/$NEON_PROJECT_ID/branches" \
    "$(jq -nc --arg n "$name" '{branch:{name:$n}, endpoints:[{type:"read_write"}]}')")" \
    || die "branch create failed: $(printf '%s' "$resp" | jq -c '{code,message}' 2>/dev/null)"
  branch="$(printf '%s' "$resp" | jq -r '.branch.id')"
  { [ -n "$branch" ] && [ "$branch" != null ]; } || die "branch create returned no id"
  # Export the id FIRST so the always() delete step finds it even if a later
  # step here fails.
  if [ -n "${GITHUB_ENV:-}" ]; then printf 'DRILL_BRANCH_ID=%s\n' "$branch" >>"$GITHUB_ENV"; fi
  log "created branch $branch ($name) on project $NEON_PROJECT_ID"
  neon_wait_ops "$branch"
  neon POST "/projects/$NEON_PROJECT_ID/branches/$branch/databases" \
    '{"database":{"name":"restore_drill","owner_name":"neondb_owner"}}' >/dev/null \
    || die "database create failed"
  neon_wait_ops "$branch"
  dsn="$(neon GET "/projects/$NEON_PROJECT_ID/connection_uri?branch_id=$branch&database_name=restore_drill&role_name=neondb_owner&pooled=false" | jq -r '.uri')"
  case "$dsn" in postgres*) ;; *) die "no connection URI returned" ;; esac
  case "$dsn" in *-pooler*) die "Neon returned a pooled URI; refusing" ;; esac
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf '::add-mask::%s\n' "$dsn"
    printf 'TARGET_DSN=%s\n' "$dsn" >>"$GITHUB_ENV"
  fi
  log "empty database restore_drill is ready on branch $branch (direct endpoint)"
}

cmd_neon_delete() {
  : "${NEON_API_KEY:?NEON_API_KEY must be set}" "${NEON_PROJECT_ID:?NEON_PROJECT_ID must be set}"
  local branch="${DRILL_BRANCH_ID:-}" name
  if [ -z "$branch" ]; then log "no drill branch was created; nothing to delete"; return 0; fi
  name="$(neon GET "/projects/$NEON_PROJECT_ID/branches/$branch" | jq -r '.branch.name')"
  [ "$(branch_name_decision "$name")" = ok ] \
    || die "refusing to delete branch $branch: its name '$name' is not a restore-drill branch"
  neon_wait_ops "$branch" || true
  neon DELETE "/projects/$NEON_PROJECT_ID/branches/$branch" >/dev/null || die "branch delete failed for $branch"

  for _ in $(seq 1 30); do
    if ! neon GET "/projects/$NEON_PROJECT_ID/branches/$branch" >/dev/null 2>&1; then
      log "deleted branch $branch ($name); confirmed gone"
      return 0
    fi
    sleep 2
  done
  die "branch $branch still present 60 s after delete — delete it by hand: neonctl branches delete $branch --project-id $NEON_PROJECT_ID"
}

# ---------------------------------------------------------------------------
# Scratch roles, sanity, hold-check
# ---------------------------------------------------------------------------

cmd_scratch_roles() {
  : "${TARGET_DSN:?TARGET_DSN must be set}" "${DRILL_DUMP:?DRILL_DUMP must be set}"
  local role n=0
  while IFS= read -r role; do
    [ -n "$role" ] || continue
    "$PSQL" "$TARGET_DSN" -v ON_ERROR_STOP=1 -q -v r="$role" <<'SQL'
SELECT format('CREATE ROLE %I NOLOGIN', :'r')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'r') \gexec
SQL
    n=$((n + 1))
    log "role present in scratch: $role"
  done < <("$PG_RESTORE" --schema-only -f - "$DRILL_DUMP" | policy_roles)
  log "$n policy role(s) ensured"
}

q() { "$PSQL" "$TARGET_DSN" -X -v ON_ERROR_STOP=1 -Atc "$1"; }

cmd_sanity() {
  : "${TARGET_DSN:?TARGET_DSN must be set}" "${DRILL_DUMP:?DRILL_DUMP must be set}"
  local min="${DRILL_MIN_TABLES:-30}" problems=0

  # 1. pg_restore errors beyond the allow-list.
  if [ -n "${DRILL_RESTORE_LOG:-}" ] && [ -f "$DRILL_RESTORE_LOG" ]; then
    local bad
    bad="$(restore_error_lines <"$DRILL_RESTORE_LOG")"
    if [ -n "$bad" ]; then
      log "FAIL: pg_restore reported errors outside the allow-list:"
      printf '%s\n' "$bad" | head -40 >&2
      problems=$((problems + 1))
    else
      log "ok: pg_restore reported no errors outside the allow-list"
    fi
  else
    log "FAIL: DRILL_RESTORE_LOG missing; cannot prove pg_restore succeeded"
    problems=$((problems + 1))
  fi

  # 2. Every table in the dump exists in the target.
  local want got missing
  want="$("$PG_RESTORE" --list "$DRILL_DUMP" | awk '$4 == "TABLE" && $5 != "DATA" {print $5"."$6}' | sort -u)"
  got="$(q "SELECT n.nspname||'.'||c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'" | sort -u)"
  missing="$(comm -23 <(printf '%s\n' "$want") <(printf '%s\n' "$got"))"
  local nwant ngot
  nwant="$(printf '%s\n' "$want" | grep -c . || true)"
  ngot="$(printf '%s\n' "$got" | grep -c . || true)"
  if [ -n "$missing" ]; then
    log "FAIL: tables in the dump but not in the restore: $(printf '%s' "$missing" | tr '\n' ' ')"
    problems=$((problems + 1))
  else
    log "ok: all $nwant dump tables restored ($ngot tables in target)"
  fi
  if [ "$nwant" -lt "$min" ]; then
    log "FAIL: only $nwant tables in the dump, below the floor of $min"
    problems=$((problems + 1))
  fi

  # 2b. Every index in the dump exists in the target and is VALID. A failed
  # CREATE INDEX (e.g. out of shared memory during a parallel HNSW build)
  # leaves the table restored and the index missing, which a table check
  # alone would pass.
  local iwant igot imissing
  iwant="$("$PG_RESTORE" --list "$DRILL_DUMP" | awk '$4 == "INDEX" {print $5"."$6}' | sort -u)"
  igot="$(q "SELECT n.nspname||'.'||c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE i.indisvalid AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'" | sort -u)"
  imissing="$(comm -23 <(printf '%s\n' "$iwant") <(printf '%s\n' "$igot"))"
  if [ -n "$imissing" ]; then
    log "FAIL: indexes in the dump but missing or invalid in the restore: $(printf '%s' "$imissing" | tr '\n' ' ')"
    problems=$((problems + 1))
  else
    log "ok: all $(printf '%s\n' "$iwant" | grep -c . || true) dump indexes restored and valid"
  fi

  # 3. Migration ledger.
  local mmax mfailed mcount repo decision
  mcount="$(q "SELECT count(*) FROM _sqlx_migrations" 2>/dev/null || true)"
  mmax="$(q "SELECT max(version) FROM _sqlx_migrations" 2>/dev/null || true)"
  mfailed="$(q "SELECT count(*) FROM _sqlx_migrations WHERE NOT success" 2>/dev/null || true)"
  repo="$(repo_tenant_migration_max)"
  decision="$(migration_floor_decision "$mmax" "$repo")"
  log "migration ledger: ${mcount:-?} rows, max version ${mmax:-?}, ${mfailed:-?} failed; repo's latest tenant migration is $repo"
  case "$decision" in
    ok) log "ok: restored schema is at the repo's latest tenant migration" ;;
    older) log "note: dump predates the repo's latest tenant migration ($mmax < $repo); expected for an old dump" ;;
    newer) log "note: dump is AHEAD of this checkout ($mmax > $repo); run the drill from main" ;;
    *) log "FAIL: migration ledger missing or empty"; problems=$((problems + 1)) ;;
  esac
  if [ "${mfailed:-x}" != "0" ]; then
    log "FAIL: migration ledger has failed rows (or could not be read)"
    problems=$((problems + 1))
  fi

  # 4. Rows came back. Exact counts, every table, largest first.
  log "--- exact row counts (top 25) ---"
  local counts total
  counts="$(q "SELECT format('%s %s', s.cnt, s.t) FROM (
      SELECT c.oid::regclass::text AS t,
             (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %s', c.oid::regclass), false, true, '')))[1]::text::bigint AS cnt
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%') s
    ORDER BY s.cnt DESC, s.t")"
  printf '%s\n' "$counts" | head -25 >&2
  total="$(printf '%s\n' "$counts" | awk '{s += $1} END {print s + 0}')"
  log "total rows across $(printf '%s\n' "$counts" | grep -c .) tables: $total"
  if [ "$total" -le 0 ]; then
    log "FAIL: the restore holds no rows"
    problems=$((problems + 1))
  fi

  # 5. Extension and HNSW index.
  local ext hnsw
  ext="$(q "SELECT extversion FROM pg_extension WHERE extname = 'vector'" || true)"
  hnsw="$(q "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam WHERE a.amname='hnsw' AND i.indisvalid" || true)"
  if [ -n "$ext" ]; then log "ok: vector extension $ext"; else log "FAIL: vector extension missing"; problems=$((problems + 1)); fi
  if [ "${hnsw:-0}" -ge 1 ]; then log "ok: $hnsw valid HNSW index(es)"; else log "FAIL: no valid HNSW index"; problems=$((problems + 1)); fi

  if [ "$problems" -gt 0 ]; then die "sanity: $problems check(s) failed"; fi
  log "sanity: all checks passed"
}

cmd_hold_check() {
  : "${DRILL_HOLD_KEY:?DRILL_HOLD_KEY must be set}"
  export AWS_ACCESS_KEY_ID="$BACKUP_S3_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$BACKUP_S3_SECRET_ACCESS_KEY"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}"
  s3size() { aws --endpoint-url "$BACKUP_S3_ENDPOINT" s3api head-object --bucket "$BACKUP_S3_BUCKET" --key "$1" --query ContentLength --output text 2>/dev/null || true; }
  s3sha() { aws --endpoint-url "$BACKUP_S3_ENDPOINT" s3 cp "s3://$BACKUP_S3_BUCKET/$1.sha256" - 2>/dev/null | awk '{print $1}' || true; }
  local hsize hsha ssize ssha
  hsize="$(s3size "$DRILL_HOLD_KEY")"; hsha="$(s3sha "$DRILL_HOLD_KEY")"
  { [ -n "$hsize" ] && [ "$hsize" != None ]; } || die "held object $DRILL_HOLD_KEY not found"
  [ -n "$hsha" ] || die "held checksum $DRILL_HOLD_KEY.sha256 not found"
  log "held:   $DRILL_HOLD_KEY  $hsize bytes  sha256 $hsha"
  if [ -n "${DRILL_SOURCE_KEY:-}" ]; then
    ssize="$(s3size "$DRILL_SOURCE_KEY")"; ssha="$(s3sha "$DRILL_SOURCE_KEY")"
    if [ -z "$ssize" ] || [ "$ssize" = None ]; then
      log "source: $DRILL_SOURCE_KEY is gone (pruned) — the held copy is now the only one"
    else
      log "source: $DRILL_SOURCE_KEY  $ssize bytes  sha256 $ssha"
      [ "$ssize" = "$hsize" ] || die "size mismatch between source and held copy"
      [ "$ssha" = "$hsha" ] || die "recorded SHA-256 differs between source and held copy"
      log "ok: held copy matches its source (size and recorded SHA-256)"
    fi
  fi
  log "the fetch step re-hashes the downloaded held bytes against that SHA-256"

  # Retention pruning only lists tenant/<slug>/. The other thing that could
  # expire a held copy is a bucket lifecycle rule. The bucket-scoped backup
  # token cannot read those (AccessDenied), so ask the Cloudflare API when a
  # token is available. Warn-only: absence of a token is not a failure.
  if [ -n "${CLOUDFLARE_API_TOKEN:-}" ] && [ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ]; then
    local rules
    if rules="$(curl -sS --fail-with-body \
        -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
        "https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/r2/buckets/$BACKUP_S3_BUCKET/lifecycle" 2>&1)"; then
      log "bucket lifecycle rules:"
      printf '%s' "$rules" | jq -c '.result.rules[]? | {id, enabled, conditions, deleteObjectsTransition, storageClassTransitions}' >&2 || true
      local hit
      hit="$(printf '%s' "$rules" | jq -r --arg k "$DRILL_HOLD_KEY" \
        '[.result.rules[]? | select(.enabled and (.deleteObjectsTransition != null) and ((.conditions.prefix // "") as $p | $k | startswith($p)))] | length')"
      if [ "${hit:-0}" != "0" ]; then
        log "WARNING: $hit enabled lifecycle rule(s) can DELETE $DRILL_HOLD_KEY. Exclude restore-hold/ from them."
      else
        log "ok: no enabled lifecycle rule deletes objects under ${DRILL_HOLD_KEY%/*}/"
      fi
    else
      log "WARN: could not read bucket lifecycle rules via the Cloudflare API: $(printf '%s' "$rules" | jq -c '.errors' 2>/dev/null || printf '%s' "$rules" | tail -c 200)"
    fi
  else
    log "WARN: CLOUDFLARE_API_TOKEN/CLOUDFLARE_ACCOUNT_ID unset; bucket lifecycle rules not checked"
  fi
}

case "${1:-}" in
  --self-test) self_test ;;
  scratch-roles) cmd_scratch_roles ;;
  neon-create) cmd_neon_create ;;
  neon-delete) cmd_neon_delete ;;
  sanity) cmd_sanity ;;
  hold-check) cmd_hold_check ;;
  *) die "usage: tenant_restore_drill.sh --self-test|scratch-roles|neon-create|neon-delete|sanity|hold-check" ;;
esac
