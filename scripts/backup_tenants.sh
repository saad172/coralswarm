#!/usr/bin/env bash
#
# backup_tenants.sh — nightly logical backup of EVERY per-org TENANT database.
#
# WHY THIS EXISTS. Three places in this repo assert that tenant DBs are covered
# by provider PITR and therefore need no backup of their own:
#   - scripts/backup_control_plane.sh's own "WHY THIS EXISTS" comment
#   - docs/runbooks/control-plane-restore.md ("Not in scope: per-org tenant
#     databases (Neon). Those get PITR")
#   - docs/plans/org-tenancy-implementation-plan.md
# Measured 2026-08-24, that PITR window was SIX HOURS (`history_retention_
# seconds = 21600` on every Neon project, the free-plan cap at the time). The
# org moved to the paid `launch` plan, and on 2026-10-21 (COR-92) retention
# was raised to 7 days (604800 s) on all three Neon projects. PITR is also not
# a backup in the sense that matters here: it does
# not survive project deletion, credential compromise, or a provider-side
# incident, because it lives inside the very project it protects.
#
# So the recovery story for all customer content was a 6h window with nothing
# behind it. This script is the "behind it" — an encrypted, off-provider copy
# in R2, on the same cadence and retention as the control plane's.
#
# RELATIONSHIP TO backup_control_plane.sh — THIS SCRIPT DOES NOT REIMPLEMENT IT.
# The dump / integrity-check / encrypt / checksum / upload / retention-prune
# pipeline is a single implementation living in backup_control_plane.sh, which
# is parameterized by DSN (BACKUP_DATABASE_URL), object prefix
# (BACKUP_S3_PREFIX) and artifact basename (BACKUP_ARTIFACT_NAME). This script
# ENUMERATES tenants and invokes that one per tenant. Any fix to the backup
# mechanics lands in one place and both jobs get it.
#
# WHAT IT DOES, IN ORDER:
#   1. Preflight: control-plane DSN reachable, required binaries present, the
#      DSN resolver is configured. Aborts before touching anything.
#   2. Enumerate tenants from the control plane's `tenant_databases` routing
#      table: every row with status IN ('active','suspended').
#      'suspended' IS INCLUDED DELIBERATELY — that status is the offboarding
#      write-freeze (see offboard_tenant.sh step 1), i.e. precisely the window
#      in which the data still exists and is about to be deleted forever. A
#      backup that skipped it would go missing exactly when it is most needed.
#      'provisioning' and 'offboarded' are excluded: the first has no data yet,
#      the second's database is already gone.
#   3. Resolve each tenant's DSN (see DSN RESOLUTION below).
#   4. Invoke backup_control_plane.sh per tenant with that DSN, an object
#      prefix of <BACKUP_S3_PREFIX_ROOT>/<slug>, and artifact name
#      tenant-<slug>.
#   5. Aggregate: one tenant's failure does not abort the others (a single bad
#      tenant must not cost every other org its nightly backup), but ANY
#      failure fails the run, loudly, naming which tenants failed.
#
# FAILS CLOSED ON AN EMPTY TENANT LIST. Finding zero tenants is treated as an
# ERROR, not a successful no-op. A routing query that returns nothing because
# of a permissions change, a typo, or a wrong DSN would otherwise render as a
# green "backed up 0 tenants" run — the exact failure shape catalogued in this
# codebase as absence-renders-as-success. Set BACKUP_ALLOW_NO_TENANTS=1 to
# permit it deliberately (a genuinely tenant-less deployment).
#
# DSN RESOLUTION. Tenant DSNs are NOT in the control-plane routing table by
# design — `tenant_databases.db_secret_ref` holds the NAME of a Fly secret
# (TENANT_DB_<SLUG>), never the credential (migration 0078's own comment:
# "credentials never live in this table"). Two resolvers, in precedence order:
#
#   1. ENV — if TENANT_DB_<SLUG> is already exported, it is used directly.
#      This is the first link of the chain every one-shot backfill binary
#      resolves through (backend/src/bin/*.rs, OperatorDsnResolver), and it is what
#      happens when this script runs ON a Fly machine.
#   2. BACKUP_DSN_RESOLVER_CMD — a command receiving the secret name as $1 and
#      printing the DSN on stdout. In CI this is a `flyctl ssh console` call
#      against the running backend, which already holds every tenant secret as
#      an env var. This needs no new credential: FLY_API_TOKEN is already a
#      repo secret used by deploy.yml and the control-plane backup job.
#
#      Self-serve tenants (control 0381) have no Fly secret: their DSN is
#      KEK-encrypted in `tenant_db_credentials`. The CI resolver therefore
#      falls back to `/app/migrate_tenants --resolve-dsn <NAME>` on the
#      machine, which resolves env first, then that table — the same chain
#      the server uses. A local operator run without that resolver cannot
#      reach a self-serve tenant and fails it loudly (never skips it).
#
#      ⚠️ The resolver PRINTS A LIVE CREDENTIAL on stdout. Its output is
#      captured into a variable and passed to the child as an env var; it is
#      never logged, never written to disk, and never echoed. Keep it that way.
#
# DUMPS GO THROUGH THE DIRECT ENDPOINT, NEVER THE POOLER (COR-92). The app's
# TENANT_DB_<SLUG> secrets name Neon's `-pooler` host (PgBouncer, transaction
# mode). pg_dump opens its session with `SELECT pg_catalog.set_config(
# 'search_path', '', false)`; through a transaction-mode pooler that session
# setting stays on the server connection, which PgBouncer then hands to the
# app, whose unqualified queries then fail. That is the COR-82 failure class
# (a 100 s write outage) and the likely cause of the 2026-09-19 empty
# search_path outage.
# So resolve_dsn() rewrites every DSN to the direct host (`direct_dsn`: drop
# `-pooler` from the endpoint label) and the run REFUSES a tenant whose DSN
# still names a pooler afterwards. backup_control_plane.sh refuses a pooler
# host too, so no caller can bypass this.
#
# NEW ORGS ARE COVERED AUTOMATICALLY. Because the tenant list comes from the
# routing table rather than from a hand-maintained list of secrets, an org
# onboarded tomorrow is backed up tomorrow night with no change here. That is
# the property that makes this scale, and the reason enumeration is not simply
# a static list of GitHub secrets.
#
# ENV VARS (required unless noted):
#   DATABASE_URL              Control-plane Postgres DSN — read-only here; the
#                             only query is a SELECT against tenant_databases.
#   BACKUP_ENCRYPTION_KEY     Passed through to backup_control_plane.sh.
#   BACKUP_S3_ENDPOINT        }
#   BACKUP_S3_BUCKET          } passed through unchanged
#   BACKUP_S3_ACCESS_KEY_ID   }
#   BACKUP_S3_SECRET_ACCESS_KEY }
#   BACKUP_DSN_RESOLVER_CMD   optional — see DSN RESOLUTION. Not needed when
#                             every TENANT_DB_<SLUG> is already in the env.
#   BACKUP_S3_PREFIX_ROOT     optional, default "tenant". Objects land at
#                             <root>/<slug>/tenant-<slug>-<ts>.dump.enc.
#   BACKUP_TENANT_MIN_TABLES  optional, default 30 — the per-tenant sanity
#                             floor on `TABLE DATA` entries, passed to the
#                             child as BACKUP_MIN_TABLES. The tenant tree is a
#                             different schema from the control tree, so it
#                             gets its own floor rather than inheriting 20.
#   BACKUP_ONLY_SLUG          optional — back up just this one tenant. For
#                             rehearsal and for re-running a single failure.
#   BACKUP_ALLOW_NO_TENANTS   optional — see FAILS CLOSED above.
#   BACKUP_RETAIN_DAILY       } optional, passed through (same defaults as the
#   BACKUP_RETAIN_MONTHLY     } control plane: 30 daily + 12 monthly)
#
# FLAGS:
#   --dry-run     dump + verify + encrypt locally per tenant, skip upload and
#                 pruning. Passed through to the child.
#   --self-test   run this script's own unit assertions and exit. Exercises
#                 the pure functions (slug->secret-name, object prefix, the
#                 tenant-list parser) with synthetic input, no I/O.
#
# USAGE:
#   scripts/backup_tenants.sh [--dry-run]
#   BACKUP_ONLY_SLUG=acme scripts/backup_tenants.sh --dry-run

set -euo pipefail

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  printf 'ERROR: this script requires bash >= 4 (found %s); on macOS: brew install bash\n' \
    "${BASH_VERSION:-unknown}" >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '[backup_tenants] %s\n' "$*" >&2; }
warn() { printf '[backup_tenants] WARN: %s\n' "$*" >&2; }
die()  { printf '[backup_tenants] ERROR: %s\n' "$*" >&2; exit 1; }

DRY_RUN=0
SELF_TEST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --self-test) SELF_TEST=1 ;;
    -h|--help) sed -n '2,126p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

# --- pure functions (unit-tested by --self-test; no I/O) ---

# secret_name <slug> -> TENANT_DB_<SLUG>. Mirrors orgt_secret_ref() in
# scripts/lib/orgt_tenant_lib.sh and the backend's secret refs. Kept as
# its own function here so --self-test can assert the mapping without sourcing
# the whole tenant lib (which requires psql at source time).
secret_name() {
  printf 'TENANT_DB_%s' "$(printf '%s' "$1" | tr 'a-z-' 'A-Z_')"
}

# mask_secret <value>: register <value> with GitHub Actions' log masking
# (`::add-mask::`), so a resolved DSN is redacted from the job log if any
# later line prints it. The runner consumes the command line itself; it is
# never shown. Outside Actions (a local operator run) it prints NOTHING: on a
# terminal that line would be the secret in clear.
mask_secret() {
  [ "${GITHUB_ACTIONS:-}" = "true" ] || return 0
  [ -n "$1" ] || return 0
  printf '::add-mask::%s\n' "$1"
}

# dsn_password <dsn>: the password component of a DSN on its own, so it can
# be masked separately: a tool that prints a parsed DSN (or just the
# password, e.g. in a connection error) would otherwise slip past a mask
# registered only for the whole URL. Prints the userinfo password as written
# (percent-encoded, if it is) and, when it differs, its percent-decoded form
# on a second line; then any `password=` query parameter the same way.
# Prints nothing for a DSN without a password.
#
# The userinfo ends at the FIRST `@` after the scheme's `//` (a later `@`,
# say in a query parameter, is not part of it), and it never spans a `/`,
# `?` or `#` (then that `@` is past the authority, and there is no userinfo).
dsn_password() {
  local dsn="$1" rest userinfo pw query kv
  local -a found=()
  rest="${dsn#*://}"
  if [[ "$rest" == *@* ]]; then
    userinfo="${rest%%@*}"
    if [[ "$userinfo" == *:* && "$userinfo" != *[/?#]* ]]; then
      pw="${userinfo#*:}"
      [ -n "$pw" ] && found+=("$pw")
    fi
  fi
  if [[ "$dsn" == *\?* ]]; then
    query="${dsn#*\?}"
    local IFS='&'
    for kv in $query; do
      case "$kv" in password=?*) found+=("${kv#password=}") ;; esac
    done
    unset IFS
  fi
  for pw in ${found[@]+"${found[@]}"}; do
    printf '%s\n' "$pw"
    local decoded
    decoded="$(pct_decode "$pw")"
    [ "$decoded" != "$pw" ] && printf '%s\n' "$decoded"
  done
  return 0
}

# pct_decode <s>: decode each `%XX` (two hex digits) to its byte and leave
# everything else exactly as written. Unlike `printf '%b'`, a backslash in
# <s> is an ordinary character, never an escape. `%00` is left encoded (a
# shell string cannot hold a NUL), as is a `%` not followed by two hex
# digits.
pct_decode() {
  local LC_ALL=C s="$1" out="" i=0 c hex ch
  while [ "$i" -lt "${#s}" ]; do
    c="${s:i:1}"
    hex="${s:i+1:2}"
    if [ "$c" = "%" ] && [[ "$hex" =~ ^[0-9A-Fa-f]{2}$ ]] && [ "$hex" != "00" ]; then
      # The format is built from two validated hex digits only.
      # shellcheck disable=SC2059
      printf -v ch "\\x$hex"
      out+="$ch"
      i=$((i + 3))
    else
      out+="$c"
      i=$((i + 1))
    fi
  done
  printf '%s' "$out"
}

# Passwords shorter than this are not masked on their own: Actions would then
# redact every occurrence of a short, common string across the whole log.
# The whole DSN, which contains the password, is still masked.
MIN_MASKED_PASSWORD=8

# mask_dsn <dsn>: mask the whole DSN and each password form on its own.
mask_dsn() {
  local pw
  mask_secret "$1"
  while IFS= read -r pw; do
    [ "${#pw}" -ge "$MIN_MASKED_PASSWORD" ] || continue
    mask_secret "$pw"
  done < <(dsn_password "$1")
}

# artifact_name <slug> -> tenant-<slug>. Must satisfy backup_control_plane.sh's
# BACKUP_ARTIFACT_NAME charset check (lowercase alnum + internal hyphens),
# which the tenant_databases slug CHECK already guarantees.
artifact_name() {
  printf 'tenant-%s' "$1"
}

# object_prefix <root> <slug> -> <root>/<slug>
object_prefix() {
  printf '%s/%s' "$1" "$2"
}

# dsn_userinfo <rest>: <rest> is a DSN with its `scheme://` removed. Prints
# the userinfo INCLUDING its trailing `@` (nothing when there is none). Same
# rule as
# dsn_password: the userinfo ends at the FIRST `@`, and only if that part
# has no `/`, `?` or `#` (else the `@` is past the authority, e.g. in a query
# parameter, and there is no userinfo).
dsn_userinfo() {
  local rest="$1" userinfo
  if [[ "$rest" == *@* ]]; then
    userinfo="${rest%%@*}"
    if [[ "$userinfo" != *[/?#]* ]]; then
      printf '%s@' "$userinfo"
    fi
  fi
}

# direct_dsn <dsn> -> the same DSN on Neon's DIRECT endpoint. Neon names the
# pooled host `<endpoint>-pooler.<region>.aws.neon.tech` and the direct one
# `<endpoint>.<region>.aws.neon.tech`; user, password, database and query are
# identical. Only the host part is touched, so a `-pooler.` that happened to
# appear in a password, database name or query is left alone. The userinfo
# is split off at the first `@` after `//` (dsn_userinfo, the same rule as
# dsn_password), so an `@` in the query cannot hide the host. A DSN with no
# pooler host is returned unchanged.
direct_dsn() {
  local dsn="$1" scheme rest userinfo hostrest host tail
  case "$dsn" in
    *://*) scheme="${dsn%%://*}://"; rest="${dsn#*://}" ;;
    *) printf '%s' "$dsn"; return 0 ;;
  esac
  userinfo="$(dsn_userinfo "$rest")"
  hostrest="${rest#"$userinfo"}"
  host="${hostrest%%[/:?#]*}"
  tail="${hostrest#"$host"}"
  host="${host/-pooler./.}"
  printf '%s%s%s%s' "$scheme" "$userinfo" "$host" "$tail"
}

# dsn_names_pooler <dsn> -> exit 0 if the HOST of the DSN is a pooler host.
# The host is found exactly as direct_dsn finds it.
dsn_names_pooler() {
  local rest="${1#*://}" userinfo host
  userinfo="$(dsn_userinfo "$rest")"
  rest="${rest#"$userinfo"}"
  host="${rest%%[/:?#]*}"
  case "$host" in *-pooler*) return 0 ;; *) return 1 ;; esac
}

# parse_tenant_rows — reads psql -At output (one "slug|status" per line) on
# stdin and prints "slug status" per line, skipping blanks. Isolated so the
# self-test can feed it synthetic rows.
parse_tenant_rows() {
  local line slug status
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    slug="${line%%|*}"
    status="${line#*|}"
    [ -n "$slug" ] || continue
    printf '%s %s\n' "$slug" "$status"
  done
}

# --- self-test ---

if [ "$SELF_TEST" -eq 1 ]; then
  FAILED=0
  assert_eq() {
    local what="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
      log "PASS: $what"
    else
      printf '[backup_tenants] FAIL: %s\n  want: %s\n  got:  %s\n' "$what" "$want" "$got" >&2
      FAILED=1
    fi
  }

  assert_eq "secret_name simple" "TENANT_DB_ACME" "$(secret_name acme)"
  assert_eq "secret_name hyphenated" "TENANT_DB_BIG_CO" "$(secret_name big-co)"
  assert_eq "secret_name digits" "TENANT_DB_ACME2" "$(secret_name acme2)"

  assert_eq "mask_secret registers the value under Actions" "::add-mask::postgres://u:p@h/d" \
    "$(GITHUB_ACTIONS=true mask_secret 'postgres://u:p@h/d')"
  assert_eq "mask_secret prints nothing outside Actions" "" \
    "$(GITHUB_ACTIONS='' mask_secret 'postgres://u:p@h/d')"
  assert_eq "mask_secret ignores an empty value" "" "$(GITHUB_ACTIONS=true mask_secret '')"
  assert_eq "dsn_password: userinfo" "s3cret" "$(dsn_password 'postgres://u:s3cret@h:5432/d?sslmode=require')"
  assert_eq "dsn_password: percent-encoded, both forms" "p%40ss%2Fw"$'\n'"p@ss/w" \
    "$(dsn_password 'postgresql://u:p%40ss%2Fw@ep-a-pooler.us-east-1.aws.neon.tech/db')"
  assert_eq "dsn_password: query parameter" "qpw" "$(dsn_password 'postgres://h/d?sslmode=require&password=qpw')"
  assert_eq "dsn_password: none" "" "$(dsn_password 'postgres://u@h/d')"
  assert_eq "dsn_password: user only, no userinfo" "" "$(dsn_password 'postgres://h/d')"
  assert_eq "mask_dsn masks the URL and the password on its own" \
    "::add-mask::postgres://u:s3cretpw@h/d"$'\n'"::add-mask::s3cretpw" \
    "$(GITHUB_ACTIONS=true mask_dsn 'postgres://u:s3cretpw@h/d')"
  assert_eq "mask_dsn does not mask a short password on its own (the DSN still is)" \
    "::add-mask::postgres://u:s3cret@h/d" \
    "$(GITHUB_ACTIONS=true mask_dsn 'postgres://u:s3cret@h/d')"
  assert_eq "dsn_password: userinfo ends at the first @, not one in the query" "longpassword1" \
    "$(dsn_password 'postgres://u:longpassword1@h/d?application_name=a@b')"
  assert_eq "dsn_password: an @ past the authority is not userinfo" "" \
    "$(dsn_password 'postgres://h:5432/d?application_name=a:b@c')"
  assert_eq "dsn_password: a backslash is literal, not an escape" 'ab\ncdefgh%21'$'\n''ab\ncdefgh!' \
    "$(dsn_password 'postgres://u:ab\ncdefgh%21@h/d')"
  assert_eq "pct_decode leaves %00, a lone % and bad hex as written" 'a%00b%zz%4' "$(pct_decode 'a%00b%zz%4')"
  assert_eq "pct_decode decodes upper- and lower-case hex" 'a/b@c' "$(pct_decode 'a%2fb%40c')"
  assert_eq "mask_dsn prints nothing outside Actions" "" "$(GITHUB_ACTIONS='' mask_dsn 'postgres://u:s3cret@h/d')"
  assert_eq "artifact_name" "tenant-acme" "$(artifact_name acme)"
  assert_eq "artifact_name hyphenated" "tenant-big-co" "$(artifact_name big-co)"

  assert_eq "object_prefix" "tenant/acme" "$(object_prefix tenant acme)"

  # The artifact name must round-trip through the child's key_month parser, or
  # retention pruning deletes the wrong objects. Asserted there too; asserted
  # here so a change to artifact_name() fails in THIS script's tests.
  assert_eq "artifact_name is child-charset-safe" "" \
    "$(printf '%s' "$(artifact_name big-co-2)" | tr -d 'a-z0-9-')"

  # direct_dsn: synthetic DSNs only; no real host or credential.
  assert_eq "direct_dsn strips -pooler from the host" \
    "postgresql://u:p@ep-cool-1.us-east-1.aws.neon.tech/neondb?sslmode=require" \
    "$(direct_dsn "postgresql://u:p@ep-cool-1-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require")"
  assert_eq "direct_dsn keeps an explicit port" \
    "postgres://u:p@ep-a.c-2.us-east-1.aws.neon.tech:5432/db" \
    "$(direct_dsn "postgres://u:p@ep-a-pooler.c-2.us-east-1.aws.neon.tech:5432/db")"
  assert_eq "direct_dsn leaves a direct DSN unchanged" \
    "postgresql://u:p@ep-a.us-east-1.aws.neon.tech/db" \
    "$(direct_dsn "postgresql://u:p@ep-a.us-east-1.aws.neon.tech/db")"
  assert_eq "direct_dsn never touches the password" \
    "postgresql://u:x-pooler.y@ep-a.us-east-1.aws.neon.tech/db" \
    "$(direct_dsn "postgresql://u:x-pooler.y@ep-a-pooler.us-east-1.aws.neon.tech/db")"
  assert_eq "direct_dsn handles a DSN without userinfo" \
    "postgresql://ep-a.us-east-1.aws.neon.tech/db" \
    "$(direct_dsn "postgresql://ep-a-pooler.us-east-1.aws.neon.tech/db")"
  assert_eq "direct_dsn splits userinfo at the first @, not one in the query" \
    "postgresql://u:p@ep-a.us-east-1.aws.neon.tech/db?options=x@ep-b-pooler.y" \
    "$(direct_dsn "postgresql://u:p@ep-a-pooler.us-east-1.aws.neon.tech/db?options=x@ep-b-pooler.y")"
  assert_eq "direct_dsn: an @ past the authority is not userinfo" \
    "postgresql://ep-a.us-east-1.aws.neon.tech/db?application_name=a@b" \
    "$(direct_dsn "postgresql://ep-a-pooler.us-east-1.aws.neon.tech/db?application_name=a@b")"
  assert_eq "dsn_names_pooler: an @ in the query does not hide a pooled host" "yes" \
    "$(dsn_names_pooler "postgresql://u:p@ep-a-pooler.us-east-1.aws.neon.tech/db?options=x@y" && echo yes || echo no)"
  assert_eq "dsn_names_pooler: pooled host" "yes" \
    "$(dsn_names_pooler "postgresql://u:p@ep-a-pooler.us-east-1.aws.neon.tech/db" && echo yes || echo no)"
  assert_eq "dsn_names_pooler: direct host" "no" \
    "$(dsn_names_pooler "postgresql://u:p@ep-a.us-east-1.aws.neon.tech/db" && echo yes || echo no)"
  assert_eq "dsn_names_pooler: pooler only in the password" "no" \
    "$(dsn_names_pooler "postgresql://u:a-pooler@ep-a.us-east-1.aws.neon.tech/db" && echo yes || echo no)"
  assert_eq "dsn_names_pooler: after direct_dsn" "no" \
    "$(dsn_names_pooler "$(direct_dsn "postgresql://u:p@ep-a-pooler.us-east-1.aws.neon.tech/db")" && echo yes || echo no)"

  ROWS="$(printf 'acme|active\nbig-co|suspended\n\n' | parse_tenant_rows)"
  assert_eq "parse_tenant_rows count" "2" "$(printf '%s\n' "$ROWS" | grep -c .)"
  assert_eq "parse_tenant_rows first" "acme active" "$(printf '%s\n' "$ROWS" | sed -n 1p)"
  assert_eq "parse_tenant_rows second" "big-co suspended" "$(printf '%s\n' "$ROWS" | sed -n 2p)"
  assert_eq "parse_tenant_rows ignores blanks" "2" \
    "$(printf 'a|active\n\n\nb|active\n' | parse_tenant_rows | grep -c .)"

  # End to end: run THIS script against a stub child and a stub psql, with a
  # POOLED tenant DSN in the env, and assert the child receives the DIRECT
  # host. This is the regression test for COR-92: on the pre-fix script the
  # child received the pooler host. Synthetic values only.
  E2E="$(mktemp -d)"
  cp "${BASH_SOURCE[0]}" "$E2E/backup_tenants.sh"
  cat >"$E2E/backup_control_plane.sh" <<'STUB'
#!/usr/bin/env bash
rest="${BACKUP_DATABASE_URL#*://}"; rest="${rest##*@}"
printf '%s\n' "${rest%%[/:?]*}" >>"$E2E_OUT"
STUB
  mkdir -p "$E2E/bin"
  printf '#!/usr/bin/env bash\nprintf "acme|active\\n"\n' >"$E2E/bin/psql"
  chmod +x "$E2E/bin/psql" "$E2E/backup_control_plane.sh"
  E2E_RC=0
  env -u BACKUP_DSN_RESOLVER_CMD -u BACKUP_ONLY_SLUG \
    PATH="$E2E/bin:$PATH" E2E_OUT="$E2E/hosts" \
    DATABASE_URL="postgresql://ctl@127.0.0.1:1/ctl" \
    BACKUP_ENCRYPTION_KEY=synthetic \
    TENANT_DB_ACME="postgresql://u:p@ep-e2e-1-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require" \
    "${BASH:-bash}" "$E2E/backup_tenants.sh" --dry-run >/dev/null 2>&1 || E2E_RC=$?
  assert_eq "e2e: run exits 0" "0" "$E2E_RC"
  assert_eq "e2e: child receives the DIRECT host" "ep-e2e-1.us-east-1.aws.neon.tech" \
    "$(cat "$E2E/hosts" 2>/dev/null)"
  rm -rf "$E2E"

  [ "$FAILED" -eq 0 ] || die "self-test FAILED"
  log "self-test: all assertions passed"
  exit 0
fi

# --- preflight ---

CHILD="$HERE/backup_control_plane.sh"
[ -x "$CHILD" ] || [ -f "$CHILD" ] || die "missing sibling script: $CHILD"

[ -n "${DATABASE_URL:-}" ] || die "DATABASE_URL (control-plane DSN) must be set — it is how tenants are enumerated"
command -v psql >/dev/null 2>&1 || die "required binary not found: psql"

[ -n "${BACKUP_ENCRYPTION_KEY:-}" ] || die "BACKUP_ENCRYPTION_KEY must be set"
if [ "$DRY_RUN" -eq 0 ]; then
  for v in BACKUP_S3_ENDPOINT BACKUP_S3_BUCKET BACKUP_S3_ACCESS_KEY_ID BACKUP_S3_SECRET_ACCESS_KEY; do
    [ -n "${!v:-}" ] || die "$v must be set (or pass --dry-run)"
  done
fi

BACKUP_S3_PREFIX_ROOT="${BACKUP_S3_PREFIX_ROOT:-tenant}"
BACKUP_TENANT_MIN_TABLES="${BACKUP_TENANT_MIN_TABLES:-30}"

# --- enumerate ---

# Read-only. `status IN ('active','suspended')` — see step 2 in the header for
# why 'suspended' is included and the other two are not.
SQL="SELECT slug || '|' || status FROM tenant_databases WHERE status IN ('active','suspended') ORDER BY slug"
if ! RAW_ROWS="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -Atc "$SQL" 2>&1)"; then
  die "failed to enumerate tenants from the control plane: $RAW_ROWS"
fi

mapfile -t TENANTS < <(printf '%s\n' "$RAW_ROWS" | parse_tenant_rows)

if [ -n "${BACKUP_ONLY_SLUG:-}" ]; then
  FILTERED=()
  for row in "${TENANTS[@]:-}"; do
    [ -n "$row" ] || continue
    if [ "${row%% *}" = "$BACKUP_ONLY_SLUG" ]; then FILTERED+=("$row"); fi
  done
  [ "${#FILTERED[@]}" -gt 0 ] || die "BACKUP_ONLY_SLUG=$BACKUP_ONLY_SLUG matched no active/suspended tenant"
  TENANTS=("${FILTERED[@]}")
  log "restricted to a single tenant by BACKUP_ONLY_SLUG=$BACKUP_ONLY_SLUG"
fi

TENANT_COUNT="${#TENANTS[@]}"
if [ "$TENANT_COUNT" -eq 0 ]; then
  if [ "${BACKUP_ALLOW_NO_TENANTS:-0}" = "1" ]; then
    warn "no active/suspended tenants found; BACKUP_ALLOW_NO_TENANTS=1 so exiting 0"
    exit 0
  fi
  die "no active or suspended tenants found in tenant_databases. This is treated as a FAILURE, not an empty success — a routing query returning nothing because of a permissions change or a wrong DSN would otherwise look like a green backup run. Set BACKUP_ALLOW_NO_TENANTS=1 if this deployment genuinely has none."
fi

log "found $TENANT_COUNT tenant(s) to back up (dry-run=$DRY_RUN)"

# --- resolve one tenant's DSN ---

# Prints the DSN on stdout. Never logs it.
resolve_dsn() {
  local slug="$1" name
  name="$(secret_name "$slug")"

  # 1. already in the env (running on a Fly machine, or a local operator run)
  if [ -n "${!name:-}" ]; then
    printf '%s' "${!name}"
    return 0
  fi

  # 2. external resolver
  if [ -n "${BACKUP_DSN_RESOLVER_CMD:-}" ]; then
    local out
    if ! out="$($BACKUP_DSN_RESOLVER_CMD "$name" 2>/dev/null)"; then
      return 1
    fi
    # Strip whitespace/CR the transport may add (flyctl ssh emits CRLF).
    out="$(printf '%s' "$out" | tr -d '\r\n')"
    [ -n "$out" ] || return 1
    printf '%s' "$out"
    return 0
  fi

  return 1
}

# --- run ---

SUCCEEDED=() FAILED_TENANTS=()

for row in "${TENANTS[@]}"; do
  slug="${row%% *}"
  status="${row#* }"

  log "--- tenant: $slug (status=$status)"

  if ! dsn="$(resolve_dsn "$slug")"; then
    warn "could not resolve $(secret_name "$slug") for tenant '$slug' (env unset and no working BACKUP_DSN_RESOLVER_CMD)"
    FAILED_TENANTS+=("$slug")
    continue
  fi
  # Mask before ANY use, including the rewrite below: the whole DSN and,
  # on its own, its password (a tool may print either).
  mask_dsn "$dsn"

  # Dump through the DIRECT endpoint (see the header's COR-92 section). The
  # resolved secret is the app's pooled DSN; rewrite it, then refuse if a
  # pooler host somehow survives. Never echo either value.
  dsn="$(direct_dsn "$dsn")"
  mask_dsn "$dsn"
  if dsn_names_pooler "$dsn"; then
    warn "tenant '$slug': DSN still names a -pooler host after rewriting to the direct endpoint; refusing to pg_dump through a pooler"
    FAILED_TENANTS+=("$slug")
    continue
  fi

  child_args=()
  [ "$DRY_RUN" -eq 1 ] && child_args+=(--dry-run)

  # The DSN is passed as an env var to the child process only. It is never
  # echoed, logged, or written to disk anywhere in this script.
  #
  # Invoked with "$BASH" — the interpreter running THIS script — not a bare
  # `bash`. The child requires bash >= 4 and a bare `bash` resolves to
  # /bin/bash, which is 3.2 on macOS: a local rehearsal would fail while CI
  # (Ubuntu, bash 5) passed, hiding the break on exactly the machine an
  # operator rehearses a restore from.
  if BACKUP_DATABASE_URL="$dsn" \
     BACKUP_S3_PREFIX="$(object_prefix "$BACKUP_S3_PREFIX_ROOT" "$slug")" \
     BACKUP_ARTIFACT_NAME="$(artifact_name "$slug")" \
     BACKUP_MIN_TABLES="$BACKUP_TENANT_MIN_TABLES" \
     "${BASH:-bash}" "$CHILD" "${child_args[@]}"; then
    SUCCEEDED+=("$slug")
    log "--- tenant: $slug OK"
  else
    warn "backup FAILED for tenant '$slug'"
    FAILED_TENANTS+=("$slug")
  fi
done

log "done: ${#SUCCEEDED[@]} succeeded, ${#FAILED_TENANTS[@]} failed, of $TENANT_COUNT"

if [ "${#FAILED_TENANTS[@]}" -gt 0 ]; then
  die "tenant backup failed for: ${FAILED_TENANTS[*]}"
fi

log "all $TENANT_COUNT tenant backup(s) completed successfully"
