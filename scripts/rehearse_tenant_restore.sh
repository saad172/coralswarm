#!/usr/bin/env bash
#
# rehearse_tenant_restore.sh — operator-run drill for restoring ONE tenant
# database from its nightly encrypted backup into a FRESH, EMPTY target.
#
# READ docs/runbooks/tenant-restore.md FIRST. This script is the mechanical
# half of that runbook. The runbook carries the judgement.
#
# WHAT THIS DOES NOT RECOVER. Coral text is not in the dump. `atoms` stores
# only `content_blob_key`, a pointer into Cloudflare R2. No script and no
# workflow backs that bucket up. A restore therefore returns metadata,
# embeddings, `oauth_tokens` and `account_keys` — and no words. The
# `sample-decrypt` phase makes that visible instead of leaving it implied: its
# in-dump proof reads from the restored database alone, and its coral proof
# reads from LIVE R2 and says so.
#
# WHY A SCRIPT AND NOT ONLY PROSE. Three separate failure shapes here are
# mechanical, and each one has already cost this repo real time in a sibling
# form:
#
#   1. RESTORING OVER A LIVE DATABASE. Prose cannot enforce "fresh project".
#      The `restore` phase refuses any target that is not empty, refuses when
#      the emptiness query fails or returns nothing, and requires the operator
#      to type the target database name back.
#   2. THE WRONG pg_restore. Tenant artifacts are written by pg_dump 18
#      (.github/workflows/backup-tenants.yml installs postgresql-client-18).
#      A macOS default is older — measured 2026-09-09 on the founder's Mac,
#      `pg_restore --version` was 16.14 — and scripts/backup_control_plane.sh
#      still defaults PG_BIN_DIR to postgresql@17. Both are silently wrong.
#   3. THE THREE ENVELOPES. This codebase stores AES-256-GCM in three
#      DIFFERENT byte layouts, and two of them differ only in field ORDER:
#
#        account_keys.dek_wrapped   [nonce 12][tag 16][ciphertext 32] = 60 B
#                                   (packages/core/src/kek.rs blob_to_bytes)
#        oauth_tokens.access_token  [nonce 12][ciphertext][tag 16]
#                                   (packages/core/src/oauth_token.rs, its OWN
#                                    blob_to_bytes — NOT kek.rs's)
#        R2 coral blob              JSON {"ciphertext":[..],"nonce":[..],
#                                   "tag":[..]}  (packages/core/src/r2.rs)
#
#      Getting that wrong under incident pressure looks like "the backup is
#      corrupt". `--self-test` pins all three, positive and negative, in CI.
#
# NOTHING HERE RUNS IN CI EXCEPT `--self-test`. The drill is operator-run.
#
# PHASES — each is explicit. There is no default phase and no "do everything"
# switch, so no invocation can restore by accident.
#
#   --self-test        pure assertions. No network, no database, no secrets.
#   preflight          check local tooling and env. Touches nothing.
#   fetch              download + decrypt + integrity-check ONE artifact.
#                      Read-only against R2. No database.
#   restore            pg_restore into TARGET_DSN. Refuses a non-empty target.
#   verify             row counts + HNSW index integrity on TARGET_DSN.
#   sample-decrypt     unwrap the DEK from the RESTORED account_keys, then
#                      decrypt (a) an oauth_tokens row from the dump, and
#                      (b) optionally one coral blob from LIVE R2.
#   abandon            print the abandon checklist. Deletes nothing itself.
#
# ENV VARS:
#   TARGET_DSN                 the FRESH Neon database. Required by restore /
#                              verify / sample-decrypt. Must be empty at
#                              restore time. Must be the DIRECT (non-pooled)
#                              connection string — a hostname containing
#                              "-pooler" is refused before pg_restore runs.
#   MASTER_KEK_HEX             64 hex chars. Required by sample-decrypt only.
#                              In no backup; it exists only as a Fly secret on
#                              coralswarm-backend, and every tenant's
#                              account_keys DEK is wrapped with it. Read it out
#                              and store it in a password manager BEFORE the
#                              drill — see docs/runbooks/tenant-restore.md §1.2
#                              for the command and its handling warning.
#                              NOTHING IN THIS SCRIPT EVER PRINTS THIS VALUE.
#                              It is read from the environment, checked for
#                              length, and passed to node via the environment.
#                              It is never echoed, logged or written to disk.
#   BACKUP_ENCRYPTION_KEY      required by fetch (decrypts the artifact).
#   BACKUP_S3_ENDPOINT         }
#   BACKUP_S3_BUCKET           } required by fetch
#   BACKUP_S3_ACCESS_KEY_ID    }
#   BACKUP_S3_SECRET_ACCESS_KEY }
#   R2_ACCOUNT_ID              } required ONLY by `sample-decrypt --coral`,
#   R2_BUCKET_NAME             } which reads the LIVE content bucket.
#   R2_ACCESS_KEY_ID           } Leave unset to skip the coral proof.
#   R2_SECRET_ACCESS_KEY       }
#   PG_BIN_DIR                 optional override for the pg_restore/psql dir.
#                              On macOS: /opt/homebrew/opt/postgresql@18/bin
#   DRILL_WORKDIR              optional. If unset, a mktemp dir is created and
#                              removed on exit. If set, the directory is
#                              created (if missing) but is NEVER removed by
#                              this script — it may hold plaintext customer
#                              data and the operator owns cleaning it up.
#   DRILL_PRINT_CORAL          optional; sample-decrypt --coral withholds the
#                              decrypted plaintext by default. Set to 1 to
#                              print its first 80 chars to stderr.
#
# USAGE:
#   scripts/rehearse_tenant_restore.sh --self-test
#   scripts/rehearse_tenant_restore.sh preflight
#   scripts/rehearse_tenant_restore.sh fetch --slug testorg
#   scripts/rehearse_tenant_restore.sh fetch --slug coralswarm \
#       --key restore-hold/coralswarm/tenant-coralswarm-<ts>.dump.enc
#   scripts/rehearse_tenant_restore.sh restore --dump ./tenant-testorg-<ts>.dump \
#       --confirm <target-db-name>
#   scripts/rehearse_tenant_restore.sh verify
#   scripts/rehearse_tenant_restore.sh sample-decrypt [--coral]
#   scripts/rehearse_tenant_restore.sh abandon

set -euo pipefail

# Everything this script writes under $WORKDIR or the current directory can be
# plaintext customer data (the decrypted dump, the R2 coral blob). Deny group
# and other access from the moment any such file is created, regardless of
# the operator's ambient umask.
umask 077

if [ -z "${BASH_VERSINFO:-}" ] || [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  printf 'ERROR: this script requires bash >= 4 (found %s); on macOS: brew install bash\n' \
    "${BASH_VERSION:-unknown}" >&2
  exit 1
fi

log() { printf '[drill] %s\n' "$*" >&2; }
die() { printf '[drill] ERROR: %s\n' "$*" >&2; exit 1; }

# The client floor. Tenant artifacts are pg_dump 18 as of 2026-09-06.
MIN_PG_MAJOR=18

# ---------------------------------------------------------------------------
# Pure functions. Unit-tested by --self-test. No I/O, no globals mutated.
# ---------------------------------------------------------------------------

# pg_major_from_version_line <line> -> major number, or "" if unparseable.
# Handles "pg_restore (PostgreSQL) 18.6", "... 16.14 (Homebrew)", "... 17.7".
pg_major_from_version_line() {
  local line="$1" ver
  ver="$(printf '%s\n' "$line" | tr ' ' '\n' | grep -E '^[0-9]+(\.[0-9]+)*$' | head -1)"
  [ -n "$ver" ] || return 0
  printf '%s' "${ver%%.*}"
}

# client_version_decision <major> -> "ok" | "refuse"
# Decision, not position: an unparseable or absent major is a REFUSAL, because
# "we could not tell" must never render as "new enough".
client_version_decision() {
  local major="${1:-}"
  case "$major" in
    ''|*[!0-9]*) printf 'refuse'; return 0 ;;
  esac
  if [ "$major" -ge "$MIN_PG_MAJOR" ]; then printf 'ok'; else printf 'refuse'; fi
}

# target_empty_decision <count-string> -> "ok" | "refuse"
# `ok` ONLY for a literal "0". An empty string (query failed, psql printed
# nothing) is a refusal: absence must not render as success.
target_empty_decision() {
  local n="${1:-}"
  case "$n" in
    0) printf 'ok' ;;
    *) printf 'refuse' ;;
  esac
}

# dsn_db_name <dsn> -> the database name, with any ?query stripped.
dsn_db_name() {
  local dsn="$1" tail
  tail="${dsn##*/}"
  printf '%s' "${tail%%\?*}"
}

# dsn_is_pooled <dsn> -> "yes" | "no"
# Neon's pooled endpoint (PgBouncer, transaction mode) puts "-pooler" in the
# hostname. pg_restore needs the DIRECT, session-mode endpoint — transaction
# pooling does not preserve the session state pg_restore depends on.
dsn_is_pooled() {
  case "$1" in
    *-pooler*) printf 'yes' ;;
    *) printf 'no' ;;
  esac
}

# object_prefix <slug> -> tenant/<slug>
# MUST match scripts/backup_tenants.sh's object_prefix() with the default
# BACKUP_S3_PREFIX_ROOT of "tenant". A divergence shows up here as "no
# artifacts found", not as a wrong restore.
object_prefix() { printf 'tenant/%s' "$1"; }

# artifact_stem <slug> -> tenant-<slug>
# MUST match scripts/backup_tenants.sh's artifact_name().
artifact_stem() { printf 'tenant-%s' "$1"; }

# newest_artifact_key — reads `aws s3 ls` style lines on stdin and prints the
# lexicographically greatest *.dump.enc name. The timestamp is
# YYYYMMDDTHHMMSSZ, so lexicographic order IS chronological order.
newest_artifact_key() {
  local line name best=""
  while IFS= read -r line; do
    name="${line##* }"
    case "$name" in
      *.dump.enc) ;;
      *) continue ;;
    esac
    if [ -z "$best" ] || [ "$name" \> "$best" ]; then best="$name"; fi
  done
  printf '%s' "$best"
}

# artifact_key_decision <slug> <key> -> "ok" | "refuse"
# A chosen artifact (fetch --key) must be THIS tenant's dump: under
# tenant/<slug>/ (the nightly prefix) or restore-hold/<slug>/ (copies held
# outside retention pruning), named tenant-<slug>-YYYYMMDDTHHMMSSZ.dump.enc.
# Anything else is refused, so a typo cannot restore another tenant's data or
# a checksum sidecar.
artifact_key_decision() {
  local slug="$1" key="$2" dir name
  dir="${key%/*}"
  name="${key##*/}"
  case "$dir" in
    "tenant/$slug"|"restore-hold/$slug") ;;
    *) printf 'refuse'; return 0 ;;
  esac
  if [[ "$name" =~ ^tenant-${slug}-[0-9]{8}T[0-9]{6}Z\.dump\.enc$ ]]; then
    printf 'ok'
  else
    printf 'refuse'
  fi
}

# ---------------------------------------------------------------------------
# Crypto helpers. Node's built-in `crypto` only — no npm install, no Rust
# build. Each one implements exactly ONE of the three envelope layouts.
# ---------------------------------------------------------------------------

require_node() {
  command -v node >/dev/null 2>&1 \
    || die "node is required for the decrypt steps (built-in crypto only, no packages). Install Node 18+."
}

# unwrap_dek_hex <kek_hex> <wrapped_hex> -> dek hex, or exits non-zero.
# Layout: [nonce 12][tag 16][ciphertext 32] = 60 bytes. AES-256-GCM, no AAD.
# Source of truth: packages/core/src/kek.rs blob_to_bytes/bytes_to_blob.
unwrap_dek_hex() {
  require_node
  KEK_HEX="$1" WRAPPED_HEX="$2" node -e '
    const crypto = require("crypto");
    const kek = Buffer.from(process.env.KEK_HEX.trim(), "hex");
    const w   = Buffer.from(process.env.WRAPPED_HEX.trim(), "hex");
    if (kek.length !== 32) { console.error("MASTER_KEK_HEX must decode to 32 bytes"); process.exit(2); }
    if (w.length !== 60)   { console.error("dek_wrapped must be exactly 60 bytes, got " + w.length); process.exit(2); }
    const nonce = w.subarray(0, 12), tag = w.subarray(12, 28), ct = w.subarray(28, 60);
    const d = crypto.createDecipheriv("aes-256-gcm", kek, nonce);
    d.setAuthTag(tag);
    let out;
    try { out = Buffer.concat([d.update(ct), d.final()]); }
    catch (e) { console.error("DEK unwrap failed: wrong KEK or corrupt bytes"); process.exit(3); }
    if (out.length !== 32) { console.error("unwrapped DEK is not 32 bytes"); process.exit(3); }
    process.stdout.write(out.toString("hex"));
  '
}

# decrypt_oauth_hex <dek_hex> <bytea_hex> -> plaintext on stdout.
# Layout: [nonce 12][ciphertext][tag 16]. NOTE the order differs from the DEK
# envelope above. Source of truth: packages/core/src/oauth_token.rs.
decrypt_oauth_hex() {
  require_node
  DEK_HEX="$1" TOKEN_HEX="$2" node -e '
    const crypto = require("crypto");
    const dek = Buffer.from(process.env.DEK_HEX.trim(), "hex");
    const b   = Buffer.from(process.env.TOKEN_HEX.trim(), "hex");
    if (b.length < 28) { console.error("token blob shorter than 28 bytes"); process.exit(2); }
    const nonce = b.subarray(0, 12);
    const tag   = b.subarray(b.length - 16);
    const ct    = b.subarray(12, b.length - 16);
    const d = crypto.createDecipheriv("aes-256-gcm", dek, nonce);
    d.setAuthTag(tag);
    try { process.stdout.write(Buffer.concat([d.update(ct), d.final()]).toString("utf8")); }
    catch (e) { console.error("token decrypt failed: wrong DEK or corrupt bytes"); process.exit(3); }
  '
}

# decrypt_r2_blob <dek_hex> <json_file> -> plaintext on stdout.
# Wire format: JSON EncryptedBlob. Source of truth: packages/core/src/r2.rs.
decrypt_r2_blob() {
  require_node
  DEK_HEX="$1" BLOB_FILE="$2" node -e '
    const crypto = require("crypto"), fs = require("fs");
    const dek = Buffer.from(process.env.DEK_HEX.trim(), "hex");
    let j;
    try { j = JSON.parse(fs.readFileSync(process.env.BLOB_FILE, "utf8")); }
    catch (e) { console.error("R2 object is not JSON — wrong key or wrong bucket"); process.exit(2); }
    for (const f of ["ciphertext", "nonce", "tag"]) {
      if (!Array.isArray(j[f])) { console.error("R2 envelope missing field: " + f); process.exit(2); }
    }
    const d = crypto.createDecipheriv("aes-256-gcm", dek, Buffer.from(j.nonce));
    d.setAuthTag(Buffer.from(j.tag));
    try {
      const pt = Buffer.concat([d.update(Buffer.from(j.ciphertext)), d.final()]);
      process.stdout.write(pt.toString("utf8"));
    } catch (e) { console.error("coral decrypt failed: wrong DEK or corrupt blob"); process.exit(3); }
  '
}

# ---------------------------------------------------------------------------
# --self-test
# ---------------------------------------------------------------------------

self_test() {
  local FAILED=0
  assert_eq() {
    local what="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
      log "PASS: $what"
    else
      printf '[drill] FAIL: %s\n  want: %s\n  got:  %s\n' "$what" "$want" "$got" >&2
      FAILED=1
    fi
  }

  # --- version parsing -----------------------------------------------------
  assert_eq "pg major from an 18 line" "18" \
    "$(pg_major_from_version_line 'pg_restore (PostgreSQL) 18.6')"
  # The exact string measured on the founder's Mac, 2026-09-09.
  assert_eq "pg major from a Homebrew 16 line" "16" \
    "$(pg_major_from_version_line 'pg_restore (PostgreSQL) 16.14 (Homebrew)')"
  assert_eq "pg major from a 17 line" "17" \
    "$(pg_major_from_version_line 'pg_restore (PostgreSQL) 17.7')"
  assert_eq "pg major from junk is empty" "" \
    "$(pg_major_from_version_line 'command not found')"

  # --- version DECISION, not position -------------------------------------
  # Asserted as the ruling the guard hands down, so moving the floor cannot
  # leave a test that still passes while the guard admits the wrong client.
  assert_eq "client 16 is refused" "refuse" "$(client_version_decision 16)"
  assert_eq "client 17 is refused" "refuse" "$(client_version_decision 17)"
  assert_eq "client 18 is accepted" "ok" "$(client_version_decision 18)"
  assert_eq "client 19 is accepted" "ok" "$(client_version_decision 19)"
  assert_eq "unknown client is refused" "refuse" "$(client_version_decision '')"
  assert_eq "non-numeric client is refused" "refuse" "$(client_version_decision 'x')"

  # --- fail closed on the emptiness check ----------------------------------
  # The negative control that matters: a query that printed NOTHING must not
  # read as "zero rows, go ahead".
  assert_eq "empty target is accepted" "ok" "$(target_empty_decision 0)"
  assert_eq "one table refuses" "refuse" "$(target_empty_decision 1)"
  assert_eq "many tables refuse" "refuse" "$(target_empty_decision 74)"
  assert_eq "no answer refuses" "refuse" "$(target_empty_decision '')"
  assert_eq "psql error text refuses" "refuse" "$(target_empty_decision 'FATAL: no such db')"

  # --- DSN parsing ---------------------------------------------------------
  assert_eq "dsn db name plain" "restoredb" \
    "$(dsn_db_name 'postgresql://u:p@host.neon.tech/restoredb')"
  assert_eq "dsn db name with query" "restoredb" \
    "$(dsn_db_name 'postgresql://u:p@host.neon.tech/restoredb?sslmode=require')"
  assert_eq "pooled neon host is detected" "yes" \
    "$(dsn_is_pooled 'postgresql://u:p@ep-cool-thing-123456-pooler.us-east-2.aws.neon.tech/restoredb')"
  assert_eq "direct neon host is not pooled" "no" \
    "$(dsn_is_pooled 'postgresql://u:p@ep-cool-thing-123456.us-east-2.aws.neon.tech/restoredb')"

  # --- object naming parity with backup_tenants.sh -------------------------
  # These literals mirror backup_tenants.sh's object_prefix()/artifact_name().
  # That script is the source of truth; a divergence surfaces at `fetch` as
  # "no artifacts found", never as a restore of the wrong tenant.
  assert_eq "object prefix" "tenant/acme" "$(object_prefix acme)"
  assert_eq "object prefix hyphenated" "tenant/big-co" "$(object_prefix big-co)"
  assert_eq "artifact stem" "tenant-acme" "$(artifact_stem acme)"

  # --- newest-artifact selection -------------------------------------------
  local LISTING
  LISTING="$(printf '%s\n' \
    '2026-09-06 13:20:20  394112 tenant-testorg-20260906T132020Z.dump.enc' \
    '2026-09-06 13:20:21     101 tenant-testorg-20260906T132020Z.dump.enc.sha256' \
    '2026-09-08 14:03:03  394240 tenant-testorg-20260908T140303Z.dump.enc' \
    '2026-09-07 15:26:37  394200 tenant-testorg-20260907T152637Z.dump.enc')"
  assert_eq "newest artifact wins" "tenant-testorg-20260908T140303Z.dump.enc" \
    "$(printf '%s\n' "$LISTING" | newest_artifact_key)"
  assert_eq "checksum sidecar is not an artifact" "" \
    "$(printf '%s\n' '2026-09-06 13:20:21 101 tenant-testorg-20260906T132020Z.dump.enc.sha256' | newest_artifact_key)"

  # --- chosen-artifact validation (fetch --key) ----------------------------
  assert_eq "key: nightly prefix" "ok" \
    "$(artifact_key_decision coralswarm tenant/coralswarm/tenant-coralswarm-20261013T101010Z.dump.enc)"
  assert_eq "key: restore-hold prefix" "ok" \
    "$(artifact_key_decision coralswarm restore-hold/coralswarm/tenant-coralswarm-20261013T101010Z.dump.enc)"
  assert_eq "key: another tenant's dump" "refuse" \
    "$(artifact_key_decision coralswarm tenant/testorg/tenant-testorg-20261013T101010Z.dump.enc)"
  assert_eq "key: right prefix, other tenant's name" "refuse" \
    "$(artifact_key_decision coralswarm tenant/coralswarm/tenant-testorg-20261013T101010Z.dump.enc)"
  assert_eq "key: checksum sidecar" "refuse" \
    "$(artifact_key_decision coralswarm tenant/coralswarm/tenant-coralswarm-20261013T101010Z.dump.enc.sha256)"
  assert_eq "key: slug prefix of a longer slug" "refuse" \
    "$(artifact_key_decision big tenant/big/tenant-big-co-20261013T101010Z.dump.enc)"
  assert_eq "key: control-plane prefix" "refuse" \
    "$(artifact_key_decision coralswarm control-plane/control-plane-20261013T101010Z.dump.enc)"

  # --- the three envelopes, positive AND negative --------------------------
  # This is the part worth having in CI. It proves the layouts this script
  # decrypts are the layouts the Rust actually writes, using a synthetic key
  # and synthetic plaintext, so the founder is not debugging envelope byte
  # order during the drill.
  require_node
  local KEK DEK FIXTURES
  KEK="$(printf 'ab%.0s' {1..32})"   # 64 hex chars
  DEK="$(printf 'cd%.0s' {1..32})"

  FIXTURES="$(KEK_HEX="$KEK" DEK_HEX="$DEK" node -e '
    const crypto = require("crypto");
    const kek = Buffer.from(process.env.KEK_HEX, "hex");
    const dek = Buffer.from(process.env.DEK_HEX, "hex");

    // account_keys.dek_wrapped: [nonce 12][tag 16][ct 32]  (kek.rs)
    let n = crypto.randomBytes(12);
    let c = crypto.createCipheriv("aes-256-gcm", kek, n);
    const wct = Buffer.concat([c.update(dek), c.final()]);
    const wrapped = Buffer.concat([n, c.getAuthTag(), wct]);

    // oauth_tokens.access_token: [nonce 12][ct][tag 16]  (oauth_token.rs)
    n = crypto.randomBytes(12);
    c = crypto.createCipheriv("aes-256-gcm", dek, n);
    const tct = Buffer.concat([c.update(Buffer.from("ya29.a0-DRILL", "utf8")), c.final()]);
    const token = Buffer.concat([n, tct, c.getAuthTag()]);

    // R2 coral blob: JSON EncryptedBlob  (r2.rs)
    n = crypto.randomBytes(12);
    c = crypto.createCipheriv("aes-256-gcm", dek, n);
    const bct = Buffer.concat([c.update(Buffer.from("the coral text", "utf8")), c.final()]);
    const blob = JSON.stringify({
      ciphertext: Array.from(bct), nonce: Array.from(n), tag: Array.from(c.getAuthTag()),
    });

    process.stdout.write([wrapped.toString("hex"), token.toString("hex"), blob].join("\n"));
  ')"

  local WRAPPED_HEX TOKEN_HEX BLOB_JSON BLOB_FILE
  WRAPPED_HEX="$(printf '%s\n' "$FIXTURES" | sed -n 1p)"
  TOKEN_HEX="$(printf '%s\n' "$FIXTURES" | sed -n 2p)"
  BLOB_JSON="$(printf '%s\n' "$FIXTURES" | sed -n 3p)"

  assert_eq "dek_wrapped fixture is 60 bytes" "120" "${#WRAPPED_HEX}"
  assert_eq "DEK unwraps from the 60-byte envelope" "$DEK" \
    "$(unwrap_dek_hex "$KEK" "$WRAPPED_HEX")"
  assert_eq "oauth token decrypts under the DEK" "ya29.a0-DRILL" \
    "$(decrypt_oauth_hex "$DEK" "$TOKEN_HEX")"

  BLOB_FILE="$(mktemp)"
  printf '%s' "$BLOB_JSON" >"$BLOB_FILE"
  assert_eq "coral blob decrypts under the DEK" "the coral text" \
    "$(decrypt_r2_blob "$DEK" "$BLOB_FILE")"

  # Negative controls. A wrong key must REFUSE, never return plausible bytes.
  local BAD_KEK BAD_DEK
  BAD_KEK="$(printf 'ba%.0s' {1..32})"
  BAD_DEK="$(printf 'dc%.0s' {1..32})"
  assert_eq "wrong KEK refuses the unwrap" "refused" \
    "$(unwrap_dek_hex "$BAD_KEK" "$WRAPPED_HEX" 2>/dev/null || printf 'refused')"
  assert_eq "wrong DEK refuses the token" "refused" \
    "$(decrypt_oauth_hex "$BAD_DEK" "$TOKEN_HEX" 2>/dev/null || printf 'refused')"
  assert_eq "wrong DEK refuses the coral blob" "refused" \
    "$(decrypt_r2_blob "$BAD_DEK" "$BLOB_FILE" 2>/dev/null || printf 'refused')"

  # The two BYTEA layouts differ only in field order. Reading one with the
  # other's parser must fail loudly, or a future edit could swap them and
  # every assertion above would still pass.
  assert_eq "the DEK envelope is not the token envelope" "refused" \
    "$(decrypt_oauth_hex "$KEK" "$WRAPPED_HEX" 2>/dev/null || printf 'refused')"

  rm -f "$BLOB_FILE"

  [ "$FAILED" -eq 0 ] || die "self-test FAILED"
  log "self-test: all assertions passed"
}

# ---------------------------------------------------------------------------
# Tooling resolution
# ---------------------------------------------------------------------------

resolve_pg_bins() {
  PG_BIN_DIR="${PG_BIN_DIR:-}"
  if [ -z "$PG_BIN_DIR" ] && [ -d /opt/homebrew/opt/postgresql@18/bin ]; then
    PG_BIN_DIR=/opt/homebrew/opt/postgresql@18/bin
  fi
  PG_RESTORE="pg_restore"
  PSQL="psql"
  if [ -n "$PG_BIN_DIR" ]; then
    PG_RESTORE="$PG_BIN_DIR/pg_restore"
    PSQL="$PG_BIN_DIR/psql"
  fi
}

assert_client_new_enough() {
  local line major decision
  line="$("$PG_RESTORE" --version 2>/dev/null || true)"
  major="$(pg_major_from_version_line "$line")"
  decision="$(client_version_decision "$major")"
  if [ "$decision" != "ok" ]; then
    die "pg_restore is '${line:-not found}' — this drill needs >= $MIN_PG_MAJOR. Tenant artifacts are written by pg_dump 18. On macOS: brew install postgresql@18, then PG_BIN_DIR=/opt/homebrew/opt/postgresql@18/bin"
  fi
  log "pg_restore client: $line (>= $MIN_PG_MAJOR, ok)"
}

require_env() {
  local v
  for v in "$@"; do
    [ -n "${!v:-}" ] || die "$v must be set for this phase — see this script's header"
  done
}

aws_r2() {
  aws --endpoint-url "$BACKUP_S3_ENDPOINT" "$@"
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

phase_preflight() {
  # Preflight reports EVERY problem in one pass and exits non-zero at the end.
  # Dying on the first one would send an operator round the loop once per
  # missing tool, which is exactly the wrong shape for a pre-drill check.
  resolve_pg_bins
  log "phase: preflight (reads nothing, writes nothing)"
  local problems=0 b v line major

  for b in aws openssl node; do
    if command -v "$b" >/dev/null 2>&1; then
      log "ok:      $b"
    else
      log "MISSING: $b"
      problems=$((problems + 1))
    fi
  done

  line="$("$PG_RESTORE" --version 2>/dev/null || true)"
  major="$(pg_major_from_version_line "$line")"
  if [ "$(client_version_decision "$major")" = "ok" ]; then
    log "ok:      pg_restore ${line}"
  else
    log "WRONG:   pg_restore is '${line:-not found}' — need >= $MIN_PG_MAJOR"
    log "         macOS: brew install postgresql@18 && export PG_BIN_DIR=/opt/homebrew/opt/postgresql@18/bin"
    problems=$((problems + 1))
  fi

  for v in TARGET_DSN MASTER_KEK_HEX BACKUP_ENCRYPTION_KEY BACKUP_S3_ENDPOINT \
           BACKUP_S3_BUCKET BACKUP_S3_ACCESS_KEY_ID BACKUP_S3_SECRET_ACCESS_KEY; do
    if [ -n "${!v:-}" ]; then
      log "set:     $v"
    else
      log "UNSET:   $v"
      problems=$((problems + 1))
    fi
  done

  # MASTER_KEK_HEX exists in exactly one place: a Fly secret on
  # coralswarm-backend. No backup contains it, and every tenant's account_keys
  # DEK is wrapped with it. Check the SHAPE of whatever the operator exported
  # now, so a truncated paste is caught here and not an hour later at
  # sample-decrypt time.
  #
  # LENGTH ONLY. This branch must never echo, log or store the value — see the
  # handling warning in docs/runbooks/tenant-restore.md §1.2.
  if [ -n "${MASTER_KEK_HEX:-}" ] && [ "${#MASTER_KEK_HEX}" -ne 64 ]; then
    log "WRONG:   MASTER_KEK_HEX is ${#MASTER_KEK_HEX} characters — expected 64 hex"
    log "         Re-read it into your password manager, then export it here."
    log "         The command is in docs/runbooks/tenant-restore.md §1.2."
    problems=$((problems + 1))
  fi
  if [ -z "${MASTER_KEK_HEX:-}" ]; then
    log "         MASTER_KEK_HEX is a Fly secret on coralswarm-backend and is in"
    log "         NO backup. Copy it to a password manager before the drill —"
    log "         docs/runbooks/tenant-restore.md §1.2 has the command and the"
    log "         warning about where you must never paste it."
  fi

  for v in R2_ACCOUNT_ID R2_BUCKET_NAME R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY; do
    if [ -n "${!v:-}" ]; then
      log "set:     $v (coral proof available)"
    else
      log "unset:   $v (coral proof will be skipped — this is allowed)"
    fi
  done

  if [ "$problems" -gt 0 ]; then
    die "preflight found $problems problem(s). Fix them before starting the drill. Nothing was read and nothing was written."
  fi
  log "preflight clean. Nothing was read and nothing was written."
}

phase_fetch() {
  local slug="${1:-}" chosen="${2:-}"
  [ -n "$slug" ] || die "fetch needs --slug <tenant-slug>"
  if [ -n "$chosen" ] && [ "$(artifact_key_decision "$slug" "$chosen")" != "ok" ]; then
    die "refusing --key $chosen: not a tenant-$slug artifact under tenant/$slug/ or restore-hold/$slug/"
  fi
  resolve_pg_bins
  assert_client_new_enough
  require_env BACKUP_ENCRYPTION_KEY BACKUP_S3_ENDPOINT BACKUP_S3_BUCKET \
    BACKUP_S3_ACCESS_KEY_ID BACKUP_S3_SECRET_ACCESS_KEY

  export AWS_ACCESS_KEY_ID="$BACKUP_S3_ACCESS_KEY_ID"
  export AWS_SECRET_ACCESS_KEY="$BACKUP_S3_SECRET_ACCESS_KEY"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}"

  local prefix listing name
  if [ -n "$chosen" ]; then
    prefix="${chosen%/*}"
    name="${chosen##*/}"
    log "chosen artifact: $prefix/$name"
  else
    prefix="$(object_prefix "$slug")"
    log "listing s3://$BACKUP_S3_BUCKET/$prefix/"
    listing="$(aws_r2 s3 ls "s3://$BACKUP_S3_BUCKET/$prefix/" || true)"
    name="$(printf '%s\n' "$listing" | newest_artifact_key)"
    [ -n "$name" ] \
      || die "no *.dump.enc under $prefix/ — wrong slug, wrong bucket, or the nightly job has never succeeded for this tenant"
    log "newest artifact: $name"
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf 'artifact_key=%s/%s\ndump_file=%s\n' "$prefix" "$name" "${name%.enc}" >>"$GITHUB_OUTPUT"
  fi

  aws_r2 s3 cp "s3://$BACKUP_S3_BUCKET/$prefix/$name" "$WORKDIR/$name"
  aws_r2 s3 cp "s3://$BACKUP_S3_BUCKET/$prefix/$name.sha256" "$WORKDIR/$name.sha256"

  ( cd "$WORKDIR" && { shasum -a 256 -c "$name.sha256" || sha256sum -c "$name.sha256"; } ) \
    || die "checksum mismatch on $name — treat as a failed backup, take the previous day"

  local dump="${name%.enc}"
  openssl enc -d -aes-256-cbc -pbkdf2 -iter 200000 \
    -pass env:BACKUP_ENCRYPTION_KEY \
    -in "$WORKDIR/$name" -out "$WORKDIR/$dump" \
    || die "decryption failed — BACKUP_ENCRYPTION_KEY is not the key this artifact was encrypted with"

  local tables
  tables="$("$PG_RESTORE" --list "$WORKDIR/$dump" 2>/dev/null | grep -c 'TABLE DATA' || true)"
  log "integrity: $tables TABLE DATA entries"
  [ "${tables:-0}" -ge 30 ] \
    || die "only ${tables:-0} TABLE DATA entries — below the nightly job's own floor of 30. Refusing to call this a usable artifact."

  # Move the plaintext dump out of the auto-removed workdir so `restore` can
  # use it, and so the operator deletes it deliberately (see `abandon`).
  # Refuse to clobber an existing file at the destination — a stale dump left
  # over from a previous run must never be silently overwritten and mistaken
  # for a fresh one.
  [ -e "./$dump" ] && die "refusing to overwrite an existing ./$dump — delete or move it first"
  chmod 600 "$WORKDIR/$dump"
  mv "$WORKDIR/$dump" "./$dump"
  log "decrypted dump ready: ./$dump (mode 600)"
  log "THIS FILE IS PLAINTEXT CUSTOMER DATA. Delete it when the drill ends."
}

phase_restore() {
  local dump="${1:-}" confirm="${2:-}"
  [ -n "$dump" ] || die "restore needs --dump <file>"
  [ -f "$dump" ] || die "no such dump file: $dump"
  require_env TARGET_DSN
  if [ "$(dsn_is_pooled "$TARGET_DSN")" = "yes" ]; then
    die "TARGET_DSN looks like a Neon POOLED endpoint (hostname contains '-pooler'). pg_restore needs the DIRECT, non-pooled connection string — pooled endpoints are transaction-mode PgBouncer and do not preserve the session state pg_restore depends on. Take the direct DSN from the Neon console instead."
  fi
  resolve_pg_bins
  assert_client_new_enough

  local dbname
  dbname="$(dsn_db_name "$TARGET_DSN")"
  [ -n "$dbname" ] || die "could not parse a database name out of TARGET_DSN"
  [ "$confirm" = "$dbname" ] \
    || die "refusing: pass --confirm $dbname to acknowledge which database this writes to (got '${confirm:-<none>}')"

  # FAIL CLOSED. A restore over a live tenant is unrecoverable, so the target
  # must prove it is empty. `|| true` keeps a psql failure from aborting the
  # script before the decision is made — the empty string it leaves behind is
  # itself a refusal (see target_empty_decision).
  # Count every kind of user relation, not just plain tables ('r'). A target
  # holding only partitioned tables ('p'), views ('v'), materialized views
  # ('m'), foreign tables ('f') or sequences ('S') must not read as empty.
  local count decision
  count="$("$PSQL" "$TARGET_DSN" -Atc \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p','m','f','v','S') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'" \
    2>/dev/null || true)"
  decision="$(target_empty_decision "$count")"
  if [ "$decision" != "ok" ]; then
    die "refusing to restore: target reports '${count:-<no answer>}' user relations. This drill only ever writes into a FRESH, EMPTY Neon project. If the number is right and the database is wrong, you are one command from an unrecoverable overwrite."
  fi
  log "target is empty (0 user relations) — proceeding"

  "$PG_RESTORE" --dbname="$TARGET_DSN" --no-owner --no-privileges --jobs=4 "$dump" \
    || log "pg_restore exited non-zero. Extension/ownership notices are common and often benign — read them, then run \`verify\` before believing either outcome."
  log "restore attempted. Nothing is proven until \`verify\` and \`sample-decrypt\` pass."
}

phase_verify() {
  require_env TARGET_DSN
  resolve_pg_bins
  log "phase: verify (read-only against TARGET_DSN)"

  log "--- row counts, largest tables first ---"
  "$PSQL" "$TARGET_DSN" -c "
    SELECT relname AS table, n_live_tup AS approx_rows
    FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 25;"

  log "--- exact counts on the tables a restore exists to return ---"
  "$PSQL" "$TARGET_DSN" -c "
    SELECT 'atoms' t, count(*) FROM atoms
    UNION ALL SELECT 'atoms_with_embedding', count(*) FROM atoms WHERE embedding IS NOT NULL
    UNION ALL SELECT 'atoms_with_inline_text', count(*) FROM atoms WHERE content_inline IS NOT NULL
    UNION ALL SELECT 'account_keys', count(*) FROM account_keys
    UNION ALL SELECT 'oauth_tokens', count(*) FROM oauth_tokens
    UNION ALL SELECT 'tenant_meta', count(*) FROM tenant_meta;"

  log "--- vector column and HNSW index ---"
  # Existence is not integrity. Three separate things must hold: the extension
  # is installed, the index exists with the right method and opclass, and the
  # planner will actually USE it for a nearest-neighbour scan.
  "$PSQL" "$TARGET_DSN" -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector','pg_trgm');"
  "$PSQL" "$TARGET_DSN" -c "SELECT indexdef FROM pg_indexes WHERE indexname = 'atoms_embedding_hnsw_idx';"
  "$PSQL" "$TARGET_DSN" -c "
    SELECT i.relname AS index,
           am.amname AS method,
           pg_relation_size(i.oid) AS bytes,
           idx.indisvalid AS valid,
           idx.indisready AS ready
    FROM pg_index idx
    JOIN pg_class i ON i.oid = idx.indexrelid
    JOIN pg_am am ON am.oid = i.relam
    WHERE i.relname = 'atoms_embedding_hnsw_idx';"

  log "--- does the planner USE the index? Expect an Index Scan on atoms_embedding_hnsw_idx ---"
  # A rebuilt-but-unusable index still shows up in pg_indexes. Only a plan
  # proves it is live. `SET LOCAL enable_seqscan = off` removes the "the table
  # is small so a seq scan is cheaper" confound.
  "$PSQL" "$TARGET_DSN" -c "
    BEGIN;
    SET LOCAL enable_seqscan = off;
    EXPLAIN SELECT atom_id FROM atoms WHERE embedding IS NOT NULL
      ORDER BY embedding <=> (SELECT embedding FROM atoms WHERE embedding IS NOT NULL LIMIT 1)
      LIMIT 5;
    ROLLBACK;"

  cat >&2 <<'NOTE'
[drill]
[drill] READ THE atoms_with_inline_text COUNT ABOVE.
[drill] In production R2 is on, so ordinary ingest leaves content_inline NULL.
[drill] A low number there is not a restore failure. It is the coral-text gap:
[drill] the words live in R2, and R2 is in no backup.
NOTE
}

phase_sample_decrypt() {
  local want_coral="${1:-0}"
  require_env TARGET_DSN MASTER_KEK_HEX
  resolve_pg_bins
  require_node
  log "phase: sample-decrypt"

  # 1. The DEK comes from the RESTORED tenant's OWN account_keys. This is the
  #    step that separates a tested restore from an untested one: it proves the
  #    restored bytes are usable, not merely present.
  local wrapped_hex dek_hex
  wrapped_hex="$("$PSQL" "$TARGET_DSN" -Atc \
    "SELECT encode(dek_wrapped,'hex') FROM account_keys ORDER BY created_at LIMIT 1" 2>/dev/null || true)"
  [ -n "$wrapped_hex" ] \
    || die "no account_keys row in the restored database. Either the dump predates the DEK relocation, or the restore did not carry account_keys. Nothing below can pass — stop here and report it."
  log "account_keys.dek_wrapped: ${#wrapped_hex} hex chars (expect 120)"

  dek_hex="$(unwrap_dek_hex "$MASTER_KEK_HEX" "$wrapped_hex")" \
    || die "DEK unwrap FAILED. MASTER_KEK_HEX does not match the key this DEK was wrapped under, so every byte of content in this tenant is unreadable with the copy you hold. Either your stored copy is wrong or truncated, or the key on coralswarm-backend changed since this dump was taken. Re-read it (docs/runbooks/tenant-restore.md 1.2) and try again."
  log "PASS: DEK unwrapped from the restored account_keys (32 bytes)"

  # 2. In-dump proof. Needs nothing but the artifact and the KEK — so it tests
  #    the restore as a SELF-SUFFICIENT thing.
  local token_hex plain
  token_hex="$("$PSQL" "$TARGET_DSN" -Atc \
    "SELECT encode(access_token,'hex') FROM oauth_tokens ORDER BY created_at DESC LIMIT 1" 2>/dev/null || true)"
  if [ -n "$token_hex" ]; then
    plain="$(decrypt_oauth_hex "$dek_hex" "$token_hex")" \
      || die "oauth_tokens decrypt FAILED under a DEK that unwrapped cleanly. That is a data-integrity problem in the dump, not a key problem. Report it."
    log "PASS: an oauth_tokens row decrypted to ${#plain} plaintext characters"
    log "      (the value is a live credential and is deliberately not printed)"
  else
    log "SKIP: no oauth_tokens rows in this tenant — the in-dump decrypt proof did not run."
    log "      Do not record the drill as fully passed on the strength of the DEK unwrap alone."
  fi

  # 3. Coral proof. Reads LIVE R2. This is the honest part: it can only pass
  #    because the bucket still exists. It proves nothing about recovery.
  if [ "$want_coral" != "1" ]; then
    log "coral proof skipped (pass --coral to run it)."
    return 0
  fi
  require_env R2_ACCOUNT_ID R2_BUCKET_NAME R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY

  local key blobfile
  key="$("$PSQL" "$TARGET_DSN" -Atc \
    "SELECT content_blob_key FROM atoms WHERE content_blob_key <> '' ORDER BY created_at DESC LIMIT 1" 2>/dev/null || true)"
  [ -n "$key" ] || die "no atom with a content_blob_key — cannot run the coral proof"
  log "coral blob key: $key"

  blobfile="$WORKDIR/coral.json"
  AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
  AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}" \
    aws --endpoint-url "https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com" \
        s3 cp "s3://$R2_BUCKET_NAME/$key" "$blobfile" \
    || die "could not read the coral blob from LIVE R2. Note what this means: the dump alone cannot produce this text, and there is no backup of this bucket."

  plain="$(decrypt_r2_blob "$dek_hex" "$blobfile")" \
    || die "coral decrypt failed under a DEK that unwrapped cleanly — report it"
  log "PASS: one coral decrypted to ${#plain} characters of text"
  if [ "${DRILL_PRINT_CORAL:-0}" = "1" ]; then
    printf '[drill] first 80 chars: %s\n' "$(printf '%s' "$plain" | head -c 80)" >&2
  else
    log "plaintext withheld from the log (customer coral text). The PASS line above already proves the decrypt. Set DRILL_PRINT_CORAL=1 to opt in and print the first 80 chars."
  fi
  rm -f "$blobfile"
  cat >&2 <<'NOTE'
[drill]
[drill] THIS PROOF READ LIVE R2, NOT THE BACKUP.
[drill] It shows the restored DEK still decrypts coral text. It does NOT show
[drill] that coral text is recoverable. If the bucket were gone, this step
[drill] would have failed and there would be no second place to get the words.
NOTE
}

phase_abandon() {
  cat >&2 <<'NOTE'
[drill] ABANDON / ROLLBACK CHECKLIST
[drill]
[drill] Nothing in this drill touches production, so there is nothing to roll
[drill] back there. Abandoning means destroying what the drill created and the
[drill] plaintext it put on disk. This script deletes none of it for you — each
[drill] line below is a deliberate act.
[drill]
[drill]   1. Delete the decrypted dump:      rm -P ./tenant-<slug>-<ts>.dump
[drill]      It is plaintext customer data. It was never encrypted at rest.
[drill]   2. Delete the restored Neon project:
[drill]        neonctl projects delete <project-id>
[drill]      Delete the PROJECT, not the branch. A deleted branch leaves the
[drill]      project, its endpoint and its bill behind.
[drill]   3. Clear the shell: unset MASTER_KEK_HEX TARGET_DSN BACKUP_ENCRYPTION_KEY
[drill]      and close the terminal. Check shell history for a pasted DSN.
[drill]   4. Confirm you changed NOTHING in production: no Fly secret was set,
[drill]      no tenant_databases row was written, no live DSN was repointed.
[drill]      If any of those did happen, that is an incident, not a drill.
[drill]
[drill] If you are abandoning MID-RESTORE: stop. A half-restored FRESH project
[drill] is harmless — delete it and start again. Never "fix it forward" by
[drill] pointing the backend at it.
NOTE
}

# ---------------------------------------------------------------------------
# Argument handling
# ---------------------------------------------------------------------------

PHASE=""
ARG_SLUG=""
ARG_KEY=""
ARG_DUMP=""
ARG_CONFIRM=""
ARG_CORAL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --self-test) PHASE="self-test" ;;
    preflight|fetch|restore|verify|sample-decrypt|abandon) PHASE="$1" ;;
    --slug) [ $# -ge 2 ] || die "$1 needs a value"; ARG_SLUG="$2"; shift ;;
    --key) [ $# -ge 2 ] || die "$1 needs a value"; ARG_KEY="$2"; shift ;;
    --dump) [ $# -ge 2 ] || die "$1 needs a value"; ARG_DUMP="$2"; shift ;;
    --confirm) [ $# -ge 2 ] || die "$1 needs a value"; ARG_CONFIRM="$2"; shift ;;
    --coral) ARG_CORAL=1 ;;
    -h|--help) sed -n '1,107p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (run with --help)" ;;
  esac
  shift
done

[ -n "$PHASE" ] || die "no phase given. There is deliberately no default — run with --help."

if [ "$PHASE" = "self-test" ]; then
  self_test
  exit 0
fi

# If the operator supplies DRILL_WORKDIR, this script only creates it if
# missing and NEVER removes it on exit — it may be "." or "~/drill" or
# anything else outside our control, and `rm -rf` on an operator-supplied
# path is exactly the kind of mistake this script exists to prevent. Only a
# directory THIS script created via mktemp is ours to delete.
WORKDIR_IS_OURS=1
if [ -n "${DRILL_WORKDIR:-}" ]; then
  WORKDIR="$DRILL_WORKDIR"
  mkdir -p "$WORKDIR"
  WORKDIR_IS_OURS=0
else
  WORKDIR="$(mktemp -d)"
fi
cleanup() {
  if [ "$WORKDIR_IS_OURS" = "1" ]; then
    [ -n "${WORKDIR:-}" ] && [ -d "$WORKDIR" ] && rm -rf "$WORKDIR"
  else
    log "DRILL_WORKDIR was operator-supplied — leaving it in place: $WORKDIR"
    log "It may still hold plaintext customer data (a decrypted dump, an R2 coral blob). Clean it up yourself."
  fi
}
trap cleanup EXIT

case "$PHASE" in
  preflight)      phase_preflight ;;
  fetch)          phase_fetch "$ARG_SLUG" "$ARG_KEY" ;;
  restore)        phase_restore "$ARG_DUMP" "$ARG_CONFIRM" ;;
  verify)         phase_verify ;;
  sample-decrypt) phase_sample_decrypt "$ARG_CORAL" ;;
  abandon)        phase_abandon ;;
  *)              die "unhandled phase: $PHASE" ;;
esac
