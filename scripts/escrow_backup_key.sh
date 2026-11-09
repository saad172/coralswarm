#!/usr/bin/env bash
#
# escrow_backup_key.sh — get a password-manager copy of BACKUP_ENCRYPTION_KEY
# out of GitHub without the plaintext ever appearing anywhere but the
# founder's own machine. COR-92.
#
# WHY. Every control-plane and tenant dump in R2 is encrypted under
# BACKUP_ENCRYPTION_KEY, and that value exists as a GitHub Actions secret,
# which GitHub never shows again. Lose the repository, or overwrite the
# secret, and every backup becomes noise. So the key must also live in a
# password manager.
#
# HOW. .github/workflows/backup-key-escrow.yml runs this with the secret in
# the env and the founder's PUBLIC key as input. This script encrypts the
# secret to that public key with `age` and prints the armored ciphertext. Only
# the matching private key, on the founder's machine, can open it. The run log
# therefore holds ciphertext and a fingerprint, never the key.
#
# The fingerprint is the first 16 hex characters of SHA-256 over the key's
# exact bytes. It identifies which key a password-manager entry holds without
# revealing it (the key is 48 random bytes; a hash prefix leaks nothing
# usable). docs/runbooks/tenant-restore.md §1.4 has the founder's steps.
#
# ENV:
#   BACKUP_ENCRYPTION_KEY   the secret (from the workflow). Never printed.
#   ESCROW_RECIPIENT        an age recipient (age1...) or an SSH public key
#                           line (ssh-ed25519 ... / ssh-rsa ...).
#
# USAGE:
#   scripts/escrow_backup_key.sh            ciphertext on stdout, fingerprint on stderr
#   scripts/escrow_backup_key.sh --self-test   round trip with a synthetic key; needs age

set -euo pipefail
umask 077

log() { printf '[escrow] %s\n' "$*" >&2; }
die() { printf '[escrow] ERROR: %s\n' "$*" >&2; exit 1; }

# recipient_decision <line> -> ok | refuse
recipient_decision() {
  local r="$1"
  if [[ "$r" =~ ^age1[02-9ac-hj-np-z]{58}$ ]]; then printf 'ok'; return 0; fi
  if [[ "$r" =~ ^(ssh-ed25519|ssh-rsa)\ [A-Za-z0-9+/]+={0,3}(\ .*)?$ ]]; then printf 'ok'; return 0; fi
  printf 'refuse'
}

# fingerprint: SHA-256 of stdin's exact bytes, first 16 hex.
fingerprint() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -c1-16; else shasum -a 256 | cut -c1-16; fi
}

encrypt() {
  local rcpt="$1" f
  f="$(mktemp)"
  printf '%s\n' "$rcpt" >"$f"
  printf '%s' "$BACKUP_ENCRYPTION_KEY" | age --armor -R "$f"
  rm -f "$f"
}

if [ "${1:-}" = "--self-test" ]; then
  command -v age >/dev/null 2>&1 || die "age not installed"
  FAILED=0
  t() { if [ "$2" = "$3" ]; then log "PASS: $1"; else log "FAIL: $1 (want '$2' got '$3')"; FAILED=1; fi; }
  t "age recipient" ok "$(recipient_decision age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p)"
  t "ssh-ed25519 recipient" ok "$(recipient_decision 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample founder@laptop')"
  t "private key refused" refuse "$(recipient_decision 'AGE-SECRET-KEY-1QQQQ')"
  t "empty refused" refuse "$(recipient_decision '')"
  t "two lines refused" refuse "$(recipient_decision $'age1x\nage1y')"
  D="$(mktemp -d)"
  age-keygen -o "$D/id" 2>/dev/null
  SYNTH="synthetic-$(date +%s)-key"
  BACKUP_ENCRYPTION_KEY="$SYNTH" encrypt "$(age-keygen -y "$D/id")" >"$D/ct"
  t "decrypts to the exact bytes (no newline added or lost)" \
    "$(printf '%s' "$SYNTH" | fingerprint)" "$(age -d -i "$D/id" "$D/ct" | fingerprint)"
  if grep -q 'BEGIN AGE ENCRYPTED FILE' "$D/ct"; then log "PASS: armored"; else log "FAIL: not armored"; FAILED=1; fi
  if grep -q "$SYNTH" "$D/ct"; then log "FAIL: plaintext in output"; FAILED=1; else log "PASS: no plaintext in output"; fi
  rm -rf "$D"
  [ "$FAILED" -eq 0 ] || die "self-test FAILED"
  log "self-test: all assertions passed"
  exit 0
fi

[ -n "${BACKUP_ENCRYPTION_KEY:-}" ] || die "BACKUP_ENCRYPTION_KEY is not set"
RCPT="$(printf '%s' "${ESCROW_RECIPIENT:-}" | tr -d '\r' | sed -e 's/^ *//' -e 's/ *$//')"
[ "$(recipient_decision "$RCPT")" = ok ] \
  || die "ESCROW_RECIPIENT must be ONE public key: an age recipient (age1...) or an ssh-ed25519/ssh-rsa public key line. Never paste a private key."
command -v age >/dev/null 2>&1 || die "age not installed"

log "key fingerprint (sha256, first 16 hex): $(printf '%s' "$BACKUP_ENCRYPTION_KEY" | fingerprint)"
log "key length: ${#BACKUP_ENCRYPTION_KEY} characters"
encrypt "$RCPT"
