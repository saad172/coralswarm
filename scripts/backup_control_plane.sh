#!/usr/bin/env bash
#
# backup_control_plane.sh — nightly logical backup of the CONTROL-PLANE
# database (Fly Postgres app `coralswarm-db`, database `coralswarm_backend`
# — routing rows, accounts, sessions, membership; see
# docs/plans/org-tenancy-implementation-plan.md's "Control-plane backup/DR"
# gap and docs/runbooks/control-plane-restore.md for the restore side of
# this).
#
# WHY THIS EXISTS: the control plane had no backup at all — losing it takes every org offline even though their own data
# (in their own tenant DB) survives, because nothing would know how to route
# to it. This script is the backup half; the restore half is the runbook.
#
# WHAT IT DOES, IN ORDER (fail closed at every step — a step that cannot be
# trusted must abort, not skip silently):
#   1. Preflight: every required env var is set, every required binary is on
#      PATH. Missing anything aborts BEFORE touching the database.
#   2. pg_dump --format=custom (a single dump, not per-table) of
#      BACKUP_DATABASE_URL into a private temp dir.
#   3. Integrity check: `pg_restore --list` must parse the artifact and find
#      at least BACKUP_MIN_TABLES `TABLE DATA` entries — catches a truncated
#      or connection-dropped dump that still exits 0.
#   4. Encrypt with openssl (AES-256-CBC, PBKDF2, 200000 iterations) under
#      BACKUP_ENCRYPTION_KEY. See "ENCRYPTION CHOICE" below for why openssl
#      over age.
#   5. SHA-256 checksum of the ENCRYPTED artifact, written alongside it.
#   6. Upload both to an S3-compatible target (BACKUP_S3_ENDPOINT/BUCKET,
#      parameterized so R2/Tigris/S3/MinIO all work identically).
#   7. Retention: keep the newest BACKUP_RETAIN_DAILY objects unconditionally,
#      then for anything older keep one object per calendar month for
#      BACKUP_RETAIN_MONTHLY months and delete the rest. See "RETENTION
#      ALGORITHM" below.
#
# ENCRYPTION CHOICE — openssl, not age. Justification: openssl is
# preinstalled on every environment this script runs in (macOS/LibreSSL for
# local rehearsal, Ubuntu GitHub Actions runners for the real nightly run) —
# `age` is not, and adding it as a new CI dependency for a nightly cron is a
# new thing to keep working for no capability this script needs (no
# recipient/identity-file key management story is required here — this is a
# single operator with one shared secret, not a multi-recipient scheme).
# openssl's PBKDF2 KDF + AES-256-CBC is adequate for a symmetric,
# operator-held secret; if this ever needs public-key recipients (e.g. a
# second operator without shared-secret access) revisit age then.
#
# RETENTION ALGORITHM. Objects are named
#   <prefix>/control-plane-<UTC timestamp>.dump.enc
# so lexical sort == chronological sort. Given all objects under the prefix,
# newest-first:
#   - the newest BACKUP_RETAIN_DAILY objects are kept unconditionally.
#   - every OLDER object is bucketed by its UTC year-month. Within each of
#     the BACKUP_RETAIN_MONTHLY most recent distinct months represented,
#     the EARLIEST object of that month is kept and every other object in
#     that month is deleted (i.e. the pruned tail approximates "one snapshot
#     per month" rather than "one nightly snapshot forever").
#   - any object whose month falls outside the most recent
#     BACKUP_RETAIN_MONTHLY distinct months is deleted.
#   - the paired .sha256 checksum object is deleted alongside its .dump.enc.
# This runs on the OBJECT LIST from the bucket, not on wall-clock elapsed
# time, so a period with no successful backups is retained correctly rather
# than assumed-present.
#
# ENV VARS (required unless noted):
#   BACKUP_DATABASE_URL     Postgres DSN with rights to dump EVERY control
#                           table, INCLUDING `_sqlx_migrations`. This is the
#                           elevated/owner role (the same one
#                           MIGRATOR_DATABASE_URL uses in prod), NOT the
#                           least-privilege `coralswarm_app` serving role —
#                           migration 0083 REVOKEs `_sqlx_migrations` from
#                           `coralswarm_app`, and pg_dump LOCKs every table in
#                           one query, so dumping as the app role fails
#                           outright with "permission denied for table
#                           _sqlx_migrations" (verified against prod; see the
#                           runbook). The host:port in this DSN must already
#                           be reachable from wherever this script runs (the
#                           caller is responsible for e.g. a `flyctl proxy`
#                           tunnel and rewriting the DSN's host to the local
#                           proxy port — this script does not manage
#                           connectivity).
#   BACKUP_ENCRYPTION_KEY   Symmetric passphrase/key for openssl. Generate
#                           with `openssl rand -base64 48`. Never derived
#                           from anything guessable.
#   BACKUP_S3_ENDPOINT      e.g. https://<account>.r2.cloudflarestorage.com
#   BACKUP_S3_BUCKET        bucket name
#   BACKUP_S3_ACCESS_KEY_ID
#   BACKUP_S3_SECRET_ACCESS_KEY
#   BACKUP_S3_REGION        optional, default "auto" (R2 convention)
#   BACKUP_S3_PREFIX        optional, default "control-plane"
#   BACKUP_RETAIN_DAILY     optional, default 30
#   BACKUP_RETAIN_MONTHLY   optional, default 12
#   BACKUP_MIN_TABLES       optional, default 20 — sanity floor on
#                           `TABLE DATA` entries found in the dump; the
#                           control tree has ~70+ tables today, 20 is a
#                           generous floor that still catches a
#                           near-empty/truncated dump.
#
# Usage:
#   scripts/backup_control_plane.sh              # real run: dump, verify,
#                                                 # encrypt, upload, prune
#   scripts/backup_control_plane.sh --dry-run    # dump, verify, encrypt
#                                                 # locally; SKIPS upload and
#                                                 # pruning (prints what
#                                                 # would happen). Does not
#                                                 # require the BACKUP_S3_*
#                                                 # vars.
#
# NEVER prints BACKUP_DATABASE_URL, BACKUP_ENCRYPTION_KEY, or any dump
# contents. Only filenames, sizes, counts, and checksums are logged.

set -euo pipefail

# Associative arrays (`local -A`, retention bucketing) need bash 4+. macOS
# ships 3.2 — fail with a real message instead of `local: -A: invalid option`.
if ((BASH_VERSINFO[0] < 4)); then
  echo "ERROR: this script requires bash >= 4 (found $BASH_VERSION); on macOS: brew install bash" >&2
  exit 1
fi

DRY_RUN=0
SELF_TEST=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    # Pure-bash unit tests for key_month and compute_retention (the
    # retention algorithm) — no database, no network, no env vars required.
    # Same convention as scripts/check_silent_drop_is_alertable.sh
    # --self-test. Wired into CI as its own step in the migration-guard job
    # (.github/workflows/ci.yml).
    --self-test) SELF_TEST=1 ;;
    -h|--help)
      sed -n '2,90p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "backup_control_plane: unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

log() { printf '[backup_control_plane] %s\n' "$*" >&2; }
die() { printf '[backup_control_plane] ERROR: %s\n' "$*" >&2; exit 1; }

# parse_dsn <postgres-uri> -> sets DSN_USER/DSN_PASS/DSN_HOST/DSN_PORT/DSN_DB/
# DSN_QUERY globals. Never logs its argument — callers must not either.
parse_dsn() {
  local dsn="$1"
  # postgres://[user[:pass]@]host[:port]/db[?query] — userinfo is optional
  # (a local dev DSN commonly omits it entirely), so detect '@' explicitly
  # rather than assuming it's always present.
  local rest="${dsn#*://}"
  local hostrest userpass
  if [[ "$rest" == *@* ]]; then
    userpass="${rest%%@*}"
    hostrest="${rest#*@}"
    DSN_USER="${userpass%%:*}"
    if [[ "$userpass" == *:* ]]; then
      DSN_PASS="${userpass#*:}"
    else
      DSN_PASS=""
    fi
  else
    DSN_USER=""
    DSN_PASS=""
    hostrest="$rest"
  fi
  local hostport="${hostrest%%/*}"
  DSN_HOST="${hostport%%:*}"
  if [[ "$hostport" == *:* ]]; then
    DSN_PORT="${hostport#*:}"
  else
    DSN_PORT="5432"
  fi
  local dbrest="${hostrest#*/}"
  DSN_DB="${dbrest%%\?*}"
  if [ "$dbrest" != "$DSN_DB" ]; then
    DSN_QUERY="${dbrest#*\?}"
  else
    DSN_QUERY=""
  fi
}

# key_month <key> -> "YYYY-MM" parsed from <artifact>-YYYYMMDDTHHMMSSZ.dump.enc
# The artifact basename is BACKUP_ARTIFACT_NAME (default "control-plane"), so
# this parses tenant objects (tenant-acme-2026...) identically.
key_month() {
  local base="${1##*/}"
  local stamp="${base#"${BACKUP_ARTIFACT_NAME:-control-plane}"-}"
  stamp="${stamp%%T*}"
  printf '%s-%s' "${stamp:0:4}" "${stamp:4:2}"
}

# compute_retention — reads the global ALL_KEYS array (newest-first,
# .dump.enc object keys) and the global BACKUP_RETAIN_DAILY /
# BACKUP_RETAIN_MONTHLY, and sets the global DELETE_KEYS array and
# KEPT_DAILY count. Pure function of its inputs — no I/O — so it is exercised
# directly by --self-test below with synthetic data, and by the real run
# with a real S3 listing. See the header comment's "RETENTION ALGORITHM"
# section for what this implements and why.
compute_retention() {
  DELETE_KEYS=()
  KEPT_DAILY=0
  local -A month_kept=() # month -> space-joined keys seen for that month
  local max_months="$BACKUP_RETAIN_MONTHLY"

  local i key month
  for i in "${!ALL_KEYS[@]}"; do
    key="${ALL_KEYS[$i]}"
    if [ "$i" -lt "$BACKUP_RETAIN_DAILY" ]; then
      KEPT_DAILY=$((KEPT_DAILY + 1))
      continue
    fi
    # Collect every key (beyond the daily window) into its month's bucket,
    # newest-first (inherited from ALL_KEYS' order). Which one to actually
    # keep per month is decided below, once each bucket is complete.
    month="$(key_month "$key")"
    month_kept[$month]="${month_kept[$month]:-}${month_kept[$month]:+ }$key"
  done

  # For each month bucket beyond the daily window, keep only the OLDEST key
  # (last in our newest-first list for that month) and queue the rest for
  # deletion. Then cap to the most recent BACKUP_RETAIN_MONTHLY distinct
  # months; any month older than that is deleted entirely.
  local months_sorted
  mapfile -t months_sorted < <(printf '%s\n' "${!month_kept[@]}" | sort -r)
  local month_index=0 keys_in_month n k
  for month in "${months_sorted[@]:-}"; do
    [ -z "$month" ] && continue
    month_index=$((month_index + 1))
    # shellcheck disable=SC2206 # word-splitting on space-joined keys is intentional here
    keys_in_month=(${month_kept[$month]})
    if [ "$month_index" -gt "$max_months" ]; then
      # Whole month is past the retention horizon — delete every key in it.
      DELETE_KEYS+=("${keys_in_month[@]}")
      continue
    fi
    # keys_in_month is already newest-first (inherited from ALL_KEYS order);
    # the OLDEST is the last element — keep it, delete the rest.
    n=${#keys_in_month[@]}
    if [ "$n" -gt 1 ]; then
      for ((k = 0; k < n - 1; k++)); do
        DELETE_KEYS+=("${keys_in_month[$k]}")
      done
    fi
  done
}

if [ "$SELF_TEST" -eq 1 ]; then
  fail=0
  assert_eq() {
    local desc="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
      log "PASS: $desc"
    else
      log "FAIL: $desc — want [$want] got [$got]"
      fail=1
    fi
  }

  # --- parse_dsn ---
  parse_dsn "postgres://coralswarm_backend:s3cr3t@coralswarm-db.flycast:5432/coralswarm_backend?sslmode=disable"
  assert_eq "parse_dsn: user" "coralswarm_backend" "$DSN_USER"
  assert_eq "parse_dsn: pass" "s3cr3t" "$DSN_PASS"
  assert_eq "parse_dsn: host" "coralswarm-db.flycast" "$DSN_HOST"
  assert_eq "parse_dsn: port" "5432" "$DSN_PORT"
  assert_eq "parse_dsn: db" "coralswarm_backend" "$DSN_DB"
  assert_eq "parse_dsn: query" "sslmode=disable" "$DSN_QUERY"

  parse_dsn "postgres://localhost:5432/cp_backup_script_test"
  assert_eq "parse_dsn: no userinfo -> empty user" "" "$DSN_USER"
  assert_eq "parse_dsn: no userinfo -> empty pass" "" "$DSN_PASS"
  assert_eq "parse_dsn: no userinfo -> host" "localhost" "$DSN_HOST"
  assert_eq "parse_dsn: no userinfo -> db" "cp_backup_script_test" "$DSN_DB"

  parse_dsn "postgres://user@dbhost/mydb"
  assert_eq "parse_dsn: user with no password" "user" "$DSN_USER"
  assert_eq "parse_dsn: user with no password -> empty pass" "" "$DSN_PASS"
  assert_eq "parse_dsn: default port when omitted" "5432" "$DSN_PORT"

  # --- key_month ---
  assert_eq "key_month basic" "2026-08" "$(key_month "control-plane-20260803T220000Z.dump.enc")"
  assert_eq "key_month with prefix path" "2026-01" "$(key_month "control-plane/control-plane-20260115T010203Z.dump.enc")"

  # key_month under a tenant artifact name. backup_tenants.sh reuses this whole
  # script with BACKUP_ARTIFACT_NAME=tenant-<slug>, and retention pruning
  # depends on this parse: a name that did not strip cleanly would yield a
  # garbage month and delete the wrong objects. Slugs contain hyphens
  # (tenant_databases' charset CHECK allows internal hyphens), so the
  # multi-hyphen case is the one that matters.
  # Set/restore in the current shell rather than a ( ) subshell: key_month
  # reads BACKUP_ARTIFACT_NAME, and a subshell assignment would be invisible
  # to shellcheck's flow analysis (SC2030/SC2031) as well as to any later
  # assertion here.
  _saved_artifact_name="${BACKUP_ARTIFACT_NAME:-}"
  BACKUP_ARTIFACT_NAME="tenant-acme"
  assert_eq "key_month tenant" "2026-08" "$(key_month "tenant-acme-20260803T220000Z.dump.enc")"
  assert_eq "key_month tenant with prefix path" "2026-02" "$(key_month "tenant/acme/tenant-acme-20260210T010203Z.dump.enc")"
  BACKUP_ARTIFACT_NAME="tenant-big-co-2"
  assert_eq "key_month tenant hyphenated slug" "2026-12" "$(key_month "tenant-big-co-2-20261225T000000Z.dump.enc")"
  BACKUP_ARTIFACT_NAME="$_saved_artifact_name"
  unset _saved_artifact_name
  # Unset must still behave exactly as the control plane always did.
  assert_eq "key_month default when unset" "2026-08" "$(key_month "control-plane-20260803T220000Z.dump.enc")"

  # --- compute_retention: 3 backups/day for 40 days (5 daily + old tail) ---
  BACKUP_RETAIN_DAILY=5
  BACKUP_RETAIN_MONTHLY=2
  ALL_KEYS=()
  # Newest 5: keep unconditionally regardless of month boundaries.
  ALL_KEYS+=("control-plane-20260803T090000Z.dump.enc") # day 1 (newest)
  ALL_KEYS+=("control-plane-20260802T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260801T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260731T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260730T090000Z.dump.enc") # 5th newest, still daily window
  # Beyond the daily window, month 2026-07 (3 more keys, newest-first):
  ALL_KEYS+=("control-plane-20260729T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260715T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260701T090000Z.dump.enc") # oldest in 2026-07
  # Month 2026-06 (2 keys) — 2nd distinct month beyond daily, within the cap of 2:
  ALL_KEYS+=("control-plane-20260615T090000Z.dump.enc")
  ALL_KEYS+=("control-plane-20260601T090000Z.dump.enc") # oldest in 2026-06
  # Month 2026-05 (1 key) — 3rd distinct month beyond daily, EXCEEDS the cap
  # of BACKUP_RETAIN_MONTHLY=2, so this whole month must be deleted.
  ALL_KEYS+=("control-plane-20260515T090000Z.dump.enc")

  compute_retention
  assert_eq "compute_retention: KEPT_DAILY" "5" "$KEPT_DAILY"
  DELETE_STR="$(printf '%s\n' "${DELETE_KEYS[@]}" | sort)"
  WANT_STR="$(printf '%s\n' \
    "control-plane-20260729T090000Z.dump.enc" \
    "control-plane-20260715T090000Z.dump.enc" \
    "control-plane-20260615T090000Z.dump.enc" \
    "control-plane-20260515T090000Z.dump.enc" \
    | sort)"
  assert_eq "compute_retention: deletes correct set" "$WANT_STR" "$DELETE_STR"
  assert_eq "compute_retention: total delete count" "4" "${#DELETE_KEYS[@]}"

  # --- edge case: fewer objects than the daily retention window ---
  BACKUP_RETAIN_DAILY=30
  BACKUP_RETAIN_MONTHLY=12
  ALL_KEYS=("control-plane-20260803T090000Z.dump.enc" "control-plane-20260802T090000Z.dump.enc")
  compute_retention
  assert_eq "compute_retention: nothing to prune when under daily window" "0" "${#DELETE_KEYS[@]}"
  assert_eq "compute_retention: KEPT_DAILY caps at actual count" "2" "$KEPT_DAILY"

  if [ "$fail" -eq 1 ]; then
    die "self-test FAILED"
  fi
  log "self-test: all assertions passed"
  exit 0
fi

# ---- 1. Preflight -----------------------------------------------------

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    die "required env var $name is not set. See this script's header comment for the full list."
  fi
}

require_env BACKUP_DATABASE_URL
require_env BACKUP_ENCRYPTION_KEY
if [ "$DRY_RUN" -eq 0 ]; then
  require_env BACKUP_S3_ENDPOINT
  require_env BACKUP_S3_BUCKET
  require_env BACKUP_S3_ACCESS_KEY_ID
  require_env BACKUP_S3_SECRET_ACCESS_KEY
fi

BACKUP_S3_REGION="${BACKUP_S3_REGION:-auto}"
BACKUP_S3_PREFIX="${BACKUP_S3_PREFIX:-control-plane}"
# Artifact basename. Parameterized so backup_tenants.sh can reuse this whole
# script per tenant (same dump/verify/encrypt/upload/retention logic, a
# different object name and prefix) instead of forking a second copy of it.
# The default preserves the control-plane naming exactly, so the existing
# nightly job and every object already in the bucket are unaffected.
# Constrained to the charset that keeps `key_month` parseable: the retention
# algorithm strips this prefix off an object name to find the timestamp, so a
# name containing a `-` followed by digits would corrupt that parse.
BACKUP_ARTIFACT_NAME="${BACKUP_ARTIFACT_NAME:-control-plane}"
case "$BACKUP_ARTIFACT_NAME" in
  *[!a-z0-9-]* | "" | -* | *- )
    die "BACKUP_ARTIFACT_NAME must be lowercase alnum with internal hyphens (got: $BACKUP_ARTIFACT_NAME)" ;;
esac
BACKUP_RETAIN_DAILY="${BACKUP_RETAIN_DAILY:-30}"
BACKUP_RETAIN_MONTHLY="${BACKUP_RETAIN_MONTHLY:-12}"
BACKUP_MIN_TABLES="${BACKUP_MIN_TABLES:-20}"

# Locate Postgres client binaries. Prefer an explicit override (the Homebrew
# postgresql@17 tools on a local mac — the Homebrew DEFAULT is v16 and
# mismatches the prod server, which is 17.x), then PATH.
PG_BIN_DIR="${PG_BIN_DIR:-}"
if [ -z "$PG_BIN_DIR" ] && [ -d /opt/homebrew/opt/postgresql@17/bin ]; then
  PG_BIN_DIR=/opt/homebrew/opt/postgresql@17/bin
fi
PG_DUMP="pg_dump"
PG_RESTORE="pg_restore"
if [ -n "$PG_BIN_DIR" ]; then
  PG_DUMP="$PG_BIN_DIR/pg_dump"
  PG_RESTORE="$PG_BIN_DIR/pg_restore"
fi

for bin in "$PG_DUMP" "$PG_RESTORE" openssl sha256sum; do
  # sha256sum isn't on macOS by default (shasum -a 256 is) — resolved below,
  # this loop only checks the Postgres/openssl binaries strictly.
  if [ "$bin" = sha256sum ]; then continue; fi
  command -v "$bin" >/dev/null 2>&1 || die "required binary not found: $bin (set PG_BIN_DIR if pg_dump/pg_restore need an override)"
done

if command -v sha256sum >/dev/null 2>&1; then
  SHA256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  SHA256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  die "no sha256 tool found (need sha256sum or shasum)"
fi

if [ "$DRY_RUN" -eq 0 ]; then
  command -v aws >/dev/null 2>&1 || die "required binary not found: aws (aws-cli v2, used against the S3-compatible endpoint)"
  command -v jq >/dev/null 2>&1 || die "required binary not found: jq (used to parse S3 object listings for retention pruning)"
fi

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/cp-backup.XXXXXX")"
cleanup() {
  # Best-effort; never let cleanup itself fail the run after real work is done.
  rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DUMP_FILE="$WORKDIR/$BACKUP_ARTIFACT_NAME-$TIMESTAMP.dump"
ENC_FILE="$DUMP_FILE.enc"
SHA_FILE="$ENC_FILE.sha256"

log "starting run (dry-run=$DRY_RUN, timestamp=$TIMESTAMP)"

# ---- 2. pg_dump ---------------------------------------------------------

# parse_dsn is defined near the top of this file (alongside key_month /
# compute_retention) so --self-test can exercise it too. Splits the DSN into
# a passwordless connection string + PGPASSWORD so the password never
# appears as a process argument (visible via `ps aux` to any other user on a
# shared host). BACKUP_DATABASE_URL is never echoed.
parse_dsn "$BACKUP_DATABASE_URL"
if [ -z "$DSN_HOST" ] || [ -z "$DSN_DB" ]; then
  die "BACKUP_DATABASE_URL did not parse as a postgres:// URI"
fi

# Never pg_dump through a transaction-mode pooler (COR-92). pg_dump sets an
# empty search_path for its session; through Neon's `-pooler` PgBouncer that
# setting can stay on a server connection the app later reuses (the COR-82
# outage class). backup_tenants.sh rewrites tenant DSNs to the direct host
# before calling this script; this refusal keeps any other caller honest.
case "$DSN_HOST" in
  *-pooler*) die "BACKUP_DATABASE_URL names a pooler host (-pooler). Dump through the DIRECT endpoint: drop '-pooler' from the hostname." ;;
esac

if [ -n "$DSN_USER" ]; then
  SAFE_DSN="postgresql://${DSN_USER}@${DSN_HOST}:${DSN_PORT}/${DSN_DB}"
else
  SAFE_DSN="postgresql://${DSN_HOST}:${DSN_PORT}/${DSN_DB}"
fi
[ -n "$DSN_QUERY" ] && SAFE_DSN="$SAFE_DSN?$DSN_QUERY"

# ---- 2a. client >= server preflight -------------------------------------
#
# pg_dump refuses to dump a server newer than itself, and the only signal is a
# late "server version mismatch" one dump into the run. That is how the tenant
# job (Neon, 18.x) failed silently for 12 nights behind a green-looking
# `testorg` (issue #745). Check it up front, name both versions, and say what
# to install — a version skew must never look like a generic dump failure.
# `pg_dump (PostgreSQL) 18.1` -> 18. Take the last field then its first
# dot-component; a greedy regex on the whole line would capture the MINOR.
PG_DUMP_MAJOR="$("$PG_DUMP" --version | awk '{print $NF}' | cut -d. -f1)"
# A non-numeric or empty parse must not silently disable the guard: inside an
# `if` condition, `[ "" -lt 18 ]` prints "integer expression expected" and the
# script sails on to pg_dump with the check skipped — the exact absence-renders-
# as-success shape this file exists to prevent.
case "$PG_DUMP_MAJOR" in
  ''|*[!0-9]*) die "could not parse a major version from \`$PG_DUMP --version\` (got: '$PG_DUMP_MAJOR') — refusing to run without a client/server version check" ;;
esac
PSQL="psql"
[ -n "$PG_BIN_DIR" ] && PSQL="$PG_BIN_DIR/psql"
if command -v "$PSQL" >/dev/null 2>&1; then
  # The trailing `|| true` is load-bearing. Under `set -e` + `pipefail` a bare
  # VAR="$(psql ... | tr ...)" assignment INHERITS the pipeline's exit status,
  # so a psql that cannot connect (Neon endpoint asleep, auth, network blip)
  # kills the whole script right here — before the WARNING branch written to
  # handle exactly that, and with no message at all. Measured, not theorised:
  # a stubbed failing psql exited 1 after only the "starting run" line. This
  # check must degrade to a warning, never become a new silent failure mode.
  SERVER_VERSION_NUM="$(PGPASSWORD="$DSN_PASS" "$PSQL" "$SAFE_DSN" -tAc 'SHOW server_version_num' 2>/dev/null | tr -d '[:space:]' || true)"
  # Numeric, not merely non-empty: psql chatter in $(( )) aborts the run.
  if [ -n "$SERVER_VERSION_NUM" ] && [ -z "${SERVER_VERSION_NUM//[0-9]/}" ]; then
    SERVER_MAJOR=$((SERVER_VERSION_NUM / 10000))
    if [ "$PG_DUMP_MAJOR" -lt "$SERVER_MAJOR" ]; then
      die "pg_dump is v$PG_DUMP_MAJOR but $BACKUP_ARTIFACT_NAME server ($DSN_HOST) is v$SERVER_MAJOR — pg_dump cannot dump a newer server. Install postgresql-client-$SERVER_MAJOR and point PG_BIN_DIR at /usr/lib/postgresql/$SERVER_MAJOR/bin."
    fi
    log "client v$PG_DUMP_MAJOR >= server v$SERVER_MAJOR"
  else
    log "WARNING: could not read server_version_num; skipping client/server version assertion"
  fi
else
  log "WARNING: psql not found next to pg_dump; skipping client/server version assertion"
fi

# Names the artifact, not "the control plane" — this script is also the
# per-tenant backup engine (see backup_tenants.sh), and a tenant run logging
# "dumping control plane" would misdirect anyone reading the job output.
log "dumping $BACKUP_ARTIFACT_NAME (host=$DSN_HOST db=$DSN_DB user=$DSN_USER) -> $(basename "$DUMP_FILE")"
if ! PGPASSWORD="$DSN_PASS" "$PG_DUMP" "$SAFE_DSN" \
      --format=custom \
      --no-owner --no-privileges \
      --file="$DUMP_FILE" \
      2>"$WORKDIR/pg_dump.stderr"; then
  log "pg_dump stderr (last 20 lines):"
  tail -20 "$WORKDIR/pg_dump.stderr" >&2 || true
  die "pg_dump failed — see stderr above. If the failure names _sqlx_migrations, BACKUP_DATABASE_URL is pointed at the least-privilege coralswarm_app role, not the elevated owner role — fix the DSN, don't work around the permission."
fi
unset DSN_PASS

DUMP_SIZE="$(wc -c <"$DUMP_FILE" | tr -d ' ')"
log "pg_dump complete: $DUMP_SIZE bytes"

# ---- 3. Integrity check --------------------------------------------------

TOC_FILE="$WORKDIR/toc.txt"
if ! "$PG_RESTORE" --list "$DUMP_FILE" >"$TOC_FILE" 2>"$WORKDIR/pg_restore_list.stderr"; then
  log "pg_restore --list stderr:"
  cat "$WORKDIR/pg_restore_list.stderr" >&2 || true
  die "pg_restore --list could not parse the dump archive — treat as a failed backup, not a partial one."
fi
TABLE_COUNT="$(grep -c "TABLE DATA" "$TOC_FILE" || true)"
log "integrity check: $TABLE_COUNT TABLE DATA entries in archive"
if [ "$TABLE_COUNT" -lt "$BACKUP_MIN_TABLES" ]; then
  die "integrity check failed: only $TABLE_COUNT TABLE DATA entries found (floor: $BACKUP_MIN_TABLES). Dump is likely truncated or connected to the wrong database — refusing to encrypt/upload it."
fi

# ---- 4. Encrypt -----------------------------------------------------------

log "encrypting -> $(basename "$ENC_FILE")"
if ! openssl enc -aes-256-cbc -pbkdf2 -iter 200000 -salt \
      -pass env:BACKUP_ENCRYPTION_KEY \
      -in "$DUMP_FILE" -out "$ENC_FILE" 2>"$WORKDIR/openssl.stderr"; then
  cat "$WORKDIR/openssl.stderr" >&2 || true
  die "openssl encryption failed"
fi
rm -f "$DUMP_FILE" # never let the plaintext dump linger past this point

# ---- 5. Checksum ----------------------------------------------------------

ENC_SHA256="$(SHA256 "$ENC_FILE")"
printf '%s  %s\n' "$ENC_SHA256" "$(basename "$ENC_FILE")" >"$SHA_FILE"
log "checksum: $ENC_SHA256"

ENC_SIZE="$(wc -c <"$ENC_FILE" | tr -d ' ')"
log "encrypted artifact: $ENC_SIZE bytes"

OBJECT_KEY="$BACKUP_S3_PREFIX/$(basename "$ENC_FILE")"
SHA_KEY="$BACKUP_S3_PREFIX/$(basename "$SHA_FILE")"

if [ "$DRY_RUN" -eq 1 ]; then
  log "DRY RUN — would upload s3://${BACKUP_S3_BUCKET:-<unset, not required for --dry-run>}/$OBJECT_KEY ($ENC_SIZE bytes) and $SHA_KEY"
  log "DRY RUN — would then run retention pruning under prefix $BACKUP_S3_PREFIX/ (keep $BACKUP_RETAIN_DAILY daily + $BACKUP_RETAIN_MONTHLY monthly)"
  log "dry run complete — dump, integrity check, and encryption all succeeded; upload and pruning were skipped"
  exit 0
fi

# ---- 6. Upload --------------------------------------------------------

export AWS_ACCESS_KEY_ID="$BACKUP_S3_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$BACKUP_S3_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="$BACKUP_S3_REGION"
export AWS_REGION="$BACKUP_S3_REGION"

s3() {
  aws --endpoint-url "$BACKUP_S3_ENDPOINT" s3 "$@"
}
s3api() {
  aws --endpoint-url "$BACKUP_S3_ENDPOINT" s3api "$@"
}

log "uploading $OBJECT_KEY"
s3 cp "$ENC_FILE" "s3://$BACKUP_S3_BUCKET/$OBJECT_KEY" --only-show-errors
log "uploading $SHA_KEY"
s3 cp "$SHA_FILE" "s3://$BACKUP_S3_BUCKET/$SHA_KEY" --only-show-errors

# Verify the object actually landed with the expected size before trusting it
# and moving on to pruning — an upload that silently truncated must not lead
# to deleting older, still-good backups.
REMOTE_SIZE="$(s3api head-object --bucket "$BACKUP_S3_BUCKET" --key "$OBJECT_KEY" | jq -r '.ContentLength')"
if [ "$REMOTE_SIZE" != "$ENC_SIZE" ]; then
  die "uploaded object size mismatch: local $ENC_SIZE bytes, remote reports $REMOTE_SIZE bytes — NOT pruning older backups this run."
fi
log "upload verified: $OBJECT_KEY ($REMOTE_SIZE bytes)"

# ---- 7. Retention pruning -------------------------------------------------

# List every .dump.enc object under the prefix, newest first, and hand it to
# the same compute_retention() exercised by --self-test above.
mapfile -t ALL_KEYS < <(
  s3api list-objects-v2 --bucket "$BACKUP_S3_BUCKET" --prefix "$BACKUP_S3_PREFIX/" \
    | jq -r '.Contents[]?.Key' \
    | grep -E '\.dump\.enc$' \
    | sort -r
)
TOTAL="${#ALL_KEYS[@]}"
log "retention: $TOTAL backup object(s) under $BACKUP_S3_PREFIX/"

compute_retention

log "retention: keeping $KEPT_DAILY daily + up to $BACKUP_RETAIN_MONTHLY monthly buckets; ${#DELETE_KEYS[@]} object(s) to prune"
for key in "${DELETE_KEYS[@]:-}"; do
  [ -z "$key" ] && continue
  log "pruning $key"
  s3 rm "s3://$BACKUP_S3_BUCKET/$key" --only-show-errors
  sha_key="${key%.dump.enc}.dump.enc.sha256"
  s3 rm "s3://$BACKUP_S3_BUCKET/$sha_key" --only-show-errors 2>/dev/null || true
done

log "run complete: uploaded $OBJECT_KEY, pruned ${#DELETE_KEYS[@]} object(s)"
