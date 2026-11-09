#!/usr/bin/env bash
#
# CoralSwarm — one-command dev bootstrap for a fresh macOS machine.
#
#   bash scripts/dev-setup.sh
#
# Safe to re-run: every step checks before it acts, so a machine that is already
# set up ends up a no-op. The script NEVER:
#   • runs sudo, or edits any file outside this repo (incl. your ~/.zshrc),
#   • overwrites an existing .env value you already filled in,
#   • touches the production database (it hardcodes the LOCAL dev DB only),
#   • runs `flyctl proxy` or points anything at prod Postgres.
#
# It automates everything that CAN be automated. Two credentials genuinely
# cannot be scripted (Clerk publishable key, R2 API token) — the final summary
# explains why and exactly what to do by hand.
set -euo pipefail

# ── Locate repo root (works no matter where the script is invoked from) ──────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="$REPO_ROOT/.env"
ENV_EXAMPLE="$REPO_ROOT/.env.example"
MODELS_DIR="$REPO_ROOT/models"
ONNX_PATH="$MODELS_DIR/bge-small-en-v1.5.onnx"
TOKENIZER_PATH="$MODELS_DIR/bge-small-en-v1.5-tokenizer.json"

# Canonical LOCAL dev DB. Hardcoded on purpose — never the Fly prod string.
DB_NAME="coralswarm_dev"
LOCAL_DATABASE_URL="postgres://localhost:5432/${DB_NAME}"
RUSTUP_BIN="/opt/homebrew/opt/rustup/bin"
FLY_APP="coralswarm-backend"   # public app name only; nothing secret embedded

# ── Pretty logging (degrades to plain text when not a TTY) ───────────────────
if [ -t 1 ] && command -v tput >/dev/null 2>&1 && [ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]; then
  C_BLUE="$(tput setaf 4)"; C_GREEN="$(tput setaf 2)"; C_YELLOW="$(tput setaf 3)"
  C_RED="$(tput setaf 1)"; C_BOLD="$(tput bold)"; C_RESET="$(tput sgr0)"
else
  C_BLUE=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_BOLD=""; C_RESET=""
fi
step()  { printf '\n%s==>%s %s%s%s\n' "$C_BLUE" "$C_RESET" "$C_BOLD" "$*" "$C_RESET"; }
ok()    { printf '  %s✓%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
info()  { printf '  %s•%s %s\n' "$C_BLUE" "$C_RESET" "$*"; }
warn()  { printf '  %s!%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
err()   { printf '  %s✗%s %s\n' "$C_RED" "$C_RESET" "$*"; }
have()  { command -v "$1" >/dev/null 2>&1; }

# Accumulate manual follow-ups to print at the very end.
MANUAL_NOTES=()
note_manual() { MANUAL_NOTES+=("$1"); }

# ── .env helpers ─────────────────────────────────────────────────────────────
# Upsert KEY=VALUE in .env, preserving position and other lines. VALUE is passed
# via the environment (not awk -v) so backslashes/slashes are never reinterpreted.
set_env_var() {
  local key="$1" val="$2" file="${3:-$ENV_FILE}" tmp
  tmp="$(mktemp)"
  _K="$key" _V="$val" awk '
    BEGIN { k=ENVIRON["_K"]; v=ENVIRON["_V"]; done=0 }
    !done && $0 ~ "^"k"=" { print k"="v; done=1; next }
    { print }
    END { if (!done) print k"="v }
  ' "$file" >"$tmp" && mv "$tmp" "$file"
}

# Current raw value of KEY in .env (empty string if absent).
get_env_var() {
  local key="$1" file="${2:-$ENV_FILE}"
  [ -f "$file" ] || { printf ''; return; }
  grep -E "^${key}=" "$file" 2>/dev/null | head -1 | cut -d= -f2- || true
}

# True when a value is empty or a well-known placeholder → safe to auto-fill.
needs_fill() {
  local v="$1"
  # strip surrounding quotes for the emptiness test
  v="${v%\"}"; v="${v#\"}"
  case "$v" in
    ""|sk_test_|pk_test_|whsec_|CHANGEME|changeme|your-*|xxx*) return 0 ;;
    *) return 1 ;;
  esac
}

# ════════════════════════════════════════════════════════════════════════════
step "CoralSwarm dev bootstrap — repo: $REPO_ROOT"

# ── 0. Platform / Homebrew ───────────────────────────────────────────────────
step "Checking Homebrew"
if [ "$(uname -s)" != "Darwin" ]; then
  warn "This script targets macOS + Homebrew. On Linux, install node>=20, pnpm@9.15.0,"
  warn "rustup, postgresql-17, pgvector, and the ONNX model by hand (see ONBOARDING.md)."
fi
if ! have brew; then
  err "Homebrew is not installed. Install it first, then re-run this script:"
  echo '     /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
  exit 1
fi
ok "brew present ($(brew --version | head -1))"

# ── 1. Node (>=20) ───────────────────────────────────────────────────────────
step "Checking Node.js (>=20)"
if have node; then
  NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
  if [ "${NODE_MAJOR:-0}" -ge 20 ]; then
    ok "node $(node -v)"
  else
    warn "node $(node -v) is too old (need >=20). Upgrade with: brew install node"
    note_manual "Upgrade Node to >=20 (brew install node)."
  fi
else
  info "Installing node via Homebrew…"
  brew install node
  ok "node $(node -v)"
fi

# ── 2. pnpm (pinned to the repo's packageManager) ────────────────────────────
step "Checking pnpm (pinned pnpm@9.15.0)"
if have pnpm; then
  ok "pnpm $(pnpm -v)"
elif have corepack; then
  info "Activating pnpm@9.15.0 via corepack…"
  corepack enable >/dev/null 2>&1 || true
  corepack prepare pnpm@9.15.0 --activate
  ok "pnpm $(pnpm -v 2>/dev/null || echo 'activated (open a new shell if not on PATH)')"
else
  warn "corepack not found (ships with Node >=16). Install pnpm manually: npm i -g pnpm@9.15.0"
  note_manual "Install pnpm@9.15.0 (corepack prepare pnpm@9.15.0 --activate)."
fi

# ── 3. Rust toolchain (rustup) ───────────────────────────────────────────────
step "Checking Rust toolchain"
if have cargo && have rustc; then
  ok "cargo $(cargo --version | awk '{print $2}'), rustc $(rustc --version | awk '{print $2}')"
elif [ -x "$RUSTUP_BIN/cargo" ]; then
  ok "cargo found at $RUSTUP_BIN (not yet on PATH — see final notes)"
else
  info "Installing rustup via Homebrew (formula is 'rustup', not 'rustup-init')…"
  brew list rustup >/dev/null 2>&1 || brew install rustup
  if have rustup-init; then
    info "Running non-interactive stable install…"
    rustup-init -y --no-modify-path --default-toolchain stable
  elif have rustup; then
    rustup toolchain install stable
    rustup default stable
  fi
  if have cargo || [ -x "$RUSTUP_BIN/cargo" ]; then
    ok "Rust toolchain installed"
  else
    warn "Rust install ran but cargo is not detectable yet — check the PATH note below."
  fi
fi
# PATH guidance — we NEVER edit shell rc files ourselves (that corrupts files
# lacking a trailing newline). Detect and instruct instead.
if ! have cargo && [ -x "$RUSTUP_BIN/cargo" ]; then
  case ":$PATH:" in
    *":$RUSTUP_BIN:"*) : ;;
    *)
      note_manual "Add cargo to PATH. The leading \\n guards against clobbering a file with no trailing newline:
       printf '\\n%s\\n' 'export PATH=\"$RUSTUP_BIN:\$PATH\"' >> ~/.zshrc
     (Verify ~/.zshrc ends in a newline first, or just use the printf form above.)
     For THIS shell right now:  export PATH=\"$RUSTUP_BIN:\$PATH\"" ;;
  esac
fi

# ── 4. Optional toolchains (report only, never force-install) ────────────────
step "Optional toolchains"
if have flyctl; then ok "flyctl present (prod/staging access + optional secret fetch below)"
else info "flyctl not installed — optional, only for prod/staging + fetching dev secrets (brew install flyctl)"; fi

# ── 5. PostgreSQL 17 + pgvector ──────────────────────────────────────────────
# 17 specifically: Homebrew's pgvector bottle is built against postgresql@17/@18,
# NOT @16 — on @16, `CREATE EXTENSION vector` fails with "extension not available".
step "PostgreSQL 17 + pgvector"
if brew list postgresql@17 >/dev/null 2>&1; then
  ok "postgresql@17 installed"
else
  info "Installing postgresql@17…"
  brew install postgresql@17
  ok "postgresql@17 installed"
fi
PG_PREFIX="$(brew --prefix postgresql@17 2>/dev/null || echo /opt/homebrew/opt/postgresql@17)"
PSQL="$PG_PREFIX/bin/psql"
CREATEDB="$PG_PREFIX/bin/createdb"
PG_ISREADY="$PG_PREFIX/bin/pg_isready"

if brew list pgvector >/dev/null 2>&1; then
  ok "pgvector installed"
else
  info "Installing pgvector (bottled against postgresql@17)…"
  brew install pgvector
  ok "pgvector installed"
fi

info "Ensuring the postgresql@17 service is running…"
brew services start postgresql@17 >/dev/null 2>&1 || true
# Wait for the server to accept connections (up to ~30s).
pg_ready=0
for _ in $(seq 1 30); do
  if [ -x "$PG_ISREADY" ] && "$PG_ISREADY" -q -h localhost 2>/dev/null; then pg_ready=1; break; fi
  sleep 1
done
if [ "$pg_ready" -eq 1 ]; then
  ok "Postgres is accepting connections on localhost:5432"

  # Create the LOCAL dev DB if absent (idempotent).
  if "$PSQL" -h localhost -lqt 2>/dev/null | cut -d '|' -f1 | tr -d ' ' | grep -qx "$DB_NAME"; then
    ok "database '$DB_NAME' already exists"
  else
    info "Creating database '$DB_NAME'…"
    "$CREATEDB" -h localhost "$DB_NAME"
    ok "database '$DB_NAME' created"
  fi

  # Required extensions (0003_atoms.sql → vector, 0046_pg_trgm_blocking.sql → pg_trgm).
  info "Ensuring extensions vector + pg_trgm on '$DB_NAME'…"
  if "$PSQL" -h localhost -d "$DB_NAME" -v ON_ERROR_STOP=1 -q \
       -c 'CREATE EXTENSION IF NOT EXISTS vector;' \
       -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm;'; then
    ok "extensions vector + pg_trgm ready"
  else
    err "Failed to create extensions. If vector was 'not available', you are likely on"
    err "postgresql@16 — pgvector isn't bottled for @16. Ensure @17 is the running server."
    note_manual "Resolve pgvector on postgresql@17 (see error above), then re-run this script."
  fi
else
  err "Postgres did not become ready. Start it manually: brew services start postgresql@17"
  note_manual "Start postgresql@17, then re-run to create '$DB_NAME' + extensions."
fi

# ── 6. ONNX embedding model ──────────────────────────────────────────────────
step "ONNX embedding model (bge-small-en-v1.5)"
# models/*.onnx and *-tokenizer.json are gitignored — safe to materialize locally.
if [ -f "$ONNX_PATH" ] && [ "$(wc -c <"$ONNX_PATH" 2>/dev/null || echo 0)" -gt 1000000 ]; then
  ok "model already present ($(du -h "$ONNX_PATH" | awk '{print $1}'))"
else
  info "Downloading model + tokenizer via scripts/download_model.sh…"
  MODELS_DIR="$MODELS_DIR" bash "$SCRIPT_DIR/download_model.sh"
  ok "model downloaded"
fi

# ── 7. JS workspace dependencies ─────────────────────────────────────────────
step "Installing JS workspace dependencies (pnpm)"
if have pnpm; then
  if pnpm install --prefer-offline; then
    ok "pnpm workspace installed"
  else
    warn "pnpm install failed (network?) — continuing so the summary still prints."
    note_manual "Re-run 'pnpm install' at the repo root once resolved."
  fi
else
  warn "pnpm unavailable — skipping. Run 'pnpm install' once pnpm is on PATH."
  note_manual "Run 'pnpm install' at the repo root."
fi

# ── 8. .env: create (only if missing) and repair local-only values ───────────
step "Configuring .env"
if [ -f "$ENV_FILE" ]; then
  ok ".env already exists — keeping it (never overwritten)"
else
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  ok ".env created from .env.example"
fi

# DATABASE_URL: force LOCAL only when blank or pointing at prod. If it already
# targets coralswarm_dev, respect the user's working value.
CUR_DB_URL="$(get_env_var DATABASE_URL)"
if [ -z "$CUR_DB_URL" ] || printf '%s' "$CUR_DB_URL" | grep -q 'coralswarm_backend'; then
  set_env_var DATABASE_URL "$LOCAL_DATABASE_URL"
  if [ -n "$CUR_DB_URL" ]; then
    warn "DATABASE_URL looked like prod — reset to LOCAL dev ($LOCAL_DATABASE_URL)"
  else
    ok "DATABASE_URL set to LOCAL dev ($LOCAL_DATABASE_URL)"
  fi
elif printf '%s' "$CUR_DB_URL" | grep -q "$DB_NAME"; then
  ok "DATABASE_URL already targets local $DB_NAME — left as-is"
else
  warn "DATABASE_URL is set to a non-standard value; not modifying. Confirm it is NOT prod."
fi

# EMBED paths: always rewrite to the repo-local absolute paths (idempotent).
set_env_var EMBED_MODEL_PATH "$ONNX_PATH"
set_env_var EMBED_TOKENIZER_PATH "$TOKENIZER_PATH"
ok "EMBED_MODEL_PATH / EMBED_TOKENIZER_PATH point at repo-local models/"

# Next.js only ever reads .env files from its own project dir (web/), never a
# parent directory, so `next dev` / `next build` in web/ silently see NO
# NEXT_PUBLIC_* vars unless something bridges them from the root .env. A
# symlink means every invocation path (a bare shell, an npm script, a
# background-launched dev server) gets them for free — no one has to remember
# to `source .env` first. Missing this is exactly what caused the app to hang
# forever on the "surfacing…" screen (Clerk's publishableKey silently empty).
WEB_ENV_LOCAL="$REPO_ROOT/web/.env.local"
if [ -L "$WEB_ENV_LOCAL" ] && [ "$(readlink "$WEB_ENV_LOCAL")" = "../.env" ]; then
  ok "web/.env.local already symlinked to ../.env"
elif [ -e "$WEB_ENV_LOCAL" ]; then
  warn "web/.env.local exists and is not our symlink — leaving it alone. If web env vars"
  warn "aren't loading, replace it with: ln -sf ../.env web/.env.local"
else
  ln -s ../.env "$WEB_ENV_LOCAL"
  ok "Symlinked web/.env.local -> ../.env"
fi

# ── 9. Control-tree migrations (out-of-band) ─────────────────────────────────
# The backend used to apply migrations at boot; it no longer does (the elevated
# owner DSN that requires must not live in the serving process — see
# backend/src/bin/migrate_control.rs). In prod this is the Fly deploy
# `release_command`; locally it is one idempotent cargo invocation, run here so
# `cargo run` still Just Works on a fresh machine. SKIP_MIGRATE=1 opts out.
step "Applying control-tree migrations"
MIGRATE_DB_URL="$(get_env_var DATABASE_URL)"
# `|| true` on both lookups: under `set -e` a bare failing test as the last
# command of an `||` list would abort the whole bootstrap silently.
CARGO_BIN="$(command -v cargo 2>/dev/null || true)"
if [ -z "$CARGO_BIN" ] && [ -x "$RUSTUP_BIN/cargo" ]; then
  CARGO_BIN="$RUSTUP_BIN/cargo"
fi
MIGRATE_HINT="DATABASE_URL='${MIGRATE_DB_URL:-$LOCAL_DATABASE_URL}' cargo run -p coralswarm-backend --bin migrate_control"
MIGRATED=0
if [ "${SKIP_MIGRATE:-0}" = "1" ]; then
  info "SKIP_MIGRATE=1 set — skipping."
  note_manual "Apply migrations before starting the backend: $MIGRATE_HINT"
elif [ "${pg_ready:-0}" -ne 1 ]; then
  warn "Postgres never became ready — skipping migrations."
  note_manual "Once Postgres is up: $MIGRATE_HINT"
elif [ -z "$MIGRATE_DB_URL" ]; then
  warn "DATABASE_URL is empty in .env — skipping migrations."
  note_manual "Set DATABASE_URL in .env, then: $MIGRATE_HINT"
elif [ -z "$CARGO_BIN" ]; then
  warn "cargo is not on PATH yet — skipping migrations."
  note_manual "Once cargo is on PATH: $MIGRATE_HINT"
else
  info "Running migrate_control (the first build can take a few minutes)…"
  # Idempotent: a fully-migrated database just prints "up-to-date". Never fatal
  # here — a compile error shouldn't abort the rest of the bootstrap.
  if DATABASE_URL="$MIGRATE_DB_URL" "$CARGO_BIN" run -q -p coralswarm-backend --bin migrate_control; then
    ok "control-tree migrations applied to '$DB_NAME'"
    MIGRATED=1
  else
    err "migrate_control failed (see the output above)."
    note_manual "Re-run once fixed: $MIGRATE_HINT"
  fi
fi

# ── 10. Optional: fetch dev secrets from Fly (best-effort, opt-in) ───────────
step "Dev secrets from Fly (optional)"
FLY_SECRET_KEYS=(
  ANTHROPIC_API_KEY CLERK_SECRET_KEY CLERK_ISSUER CLERK_WEBHOOK_SECRET
  ACCOUNT_MASTER_KEK OAUTH_STATE_SECRET
  MICROSOFT_CLIENT_ID MICROSOFT_CLIENT_SECRET MICROSOFT_TENANT MICROSOFT_SCOPES
  SLACK_CLIENT_ID SLACK_CLIENT_SECRET SLACK_SIGNING_SECRET SLACK_SCOPES SLACK_USER_SCOPES
  INTERNAL_TOKEN MASTER_KEK_HEX RESEND_API_KEY EXTRACT_ESCALATE_MODEL
)
# Values that contain spaces MUST be written quoted (sourcing gotcha).
is_quoted_key() { case "$1" in MICROSOFT_SCOPES|SLACK_SCOPES) return 0 ;; *) return 1 ;; esac; }

do_fly=0
if [ "${SKIP_FLY:-0}" = "1" ]; then
  info "SKIP_FLY=1 set — skipping Fly secret fetch."
elif ! have flyctl; then
  info "flyctl not installed — skipping. Later: brew install flyctl && flyctl auth login, then re-run."
elif ! flyctl auth whoami >/dev/null 2>&1; then
  info "flyctl not authenticated — skipping. Run 'flyctl auth login' then re-run to backfill blanks."
else
  if [ -t 0 ]; then
    printf '  %s?%s Fetch missing dev secrets from Fly app %s into blank .env slots? [y/N] ' \
      "$C_YELLOW" "$C_RESET" "$FLY_APP"
    read -r reply || reply=""
    case "$reply" in y|Y|yes|YES) do_fly=1 ;; *) info "Skipped by user." ;; esac
  else
    info "Non-interactive shell — skipping Fly fetch (re-run in a terminal, or SKIP_FLY handling)."
  fi
fi

if [ "$do_fly" -eq 1 ]; then
  info "Reading env from $FLY_APP via SSH (this may take a few seconds)…"
  FLY_DUMP="$(flyctl ssh console -a "$FLY_APP" -C 'printenv' 2>/dev/null || true)"
  if [ -z "$FLY_DUMP" ]; then
    warn "Could not read secrets (machine stopped or SSH unavailable). Skipping."
  else
    filled=0
    for key in "${FLY_SECRET_KEYS[@]}"; do
      cur="$(get_env_var "$key")"
      needs_fill "$cur" || continue
      remote="$(printf '%s\n' "$FLY_DUMP" | grep -E "^${key}=" | head -1 | cut -d= -f2- || true)"
      [ -n "$remote" ] || continue
      if is_quoted_key "$key"; then
        remote="${remote%\"}"; remote="${remote#\"}"   # normalize, then re-quote
        set_env_var "$key" "\"$remote\""
      else
        set_env_var "$key" "$remote"
      fi
      ok "filled $key from Fly"
      filled=$((filled + 1))
    done
    [ "$filled" -eq 0 ] && info "Nothing to fill — all targeted vars already set locally."
  fi
fi

# ════════════════════════════════════════════════════════════════════════════
# ── Final summary ────────────────────────────────────────────────────────────
step "Summary"
echo "Automated:"
echo "  • Verified/installed: Homebrew deps (node, pnpm, rustup, postgresql@17, pgvector)"
echo "  • Created local DB '$DB_NAME' + extensions vector & pg_trgm"
echo "  • Downloaded the bge-small-en-v1.5 ONNX model into models/"
echo "  • Installed the pnpm workspace"
echo "  • Pointed .env at the LOCAL dev DB and repo-local model paths (never prod)"
if [ "$MIGRATED" -eq 1 ]; then
  echo "  • Applied the control-tree migrations (the server no longer migrates at boot)"
fi

# Scan .env for still-blank keys.
BLANKS=()
if [ -f "$ENV_FILE" ]; then
  while IFS= read -r line; do
    case "$line" in \#*|"") continue ;; esac
    case "$line" in *=) BLANKS+=("${line%=}") ;; esac
  done < <(grep -E '^[A-Za-z_][A-Za-z0-9_]*=[[:space:]]*$' "$ENV_FILE" || true)
fi
if [ "${#BLANKS[@]}" -gt 0 ]; then
  printf '\n%sStill blank in .env%s (fill before those features work):\n' "$C_YELLOW" "$C_RESET"
  for k in "${BLANKS[@]}"; do echo "  • $k"; done
else
  printf '\n%sAll .env keys have values.%s\n' "$C_GREEN" "$C_RESET"
fi

# The two credentials that CANNOT be scripted, with the reasoning.
printf '\n%sTwo credentials must be obtained by hand — no API can script them:%s\n' "$C_BOLD" "$C_RESET"
cat <<EOF
  1) NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY (Clerk)
     Clerk's CLI needs an interactive browser OAuth login that can't run
     non-interactively. Get it with:
         npx clerk@latest auth login      # opens a browser to sign in
         npx clerk@latest apps list       # shows the "CoralSwarm" app's publishable key
     Then set NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_... in .env.

  2) R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY (Cloudflare R2)
     Cloudflare shows an R2 API token's secret ONLY once, at creation time, in
     the dashboard — there is no API to read an existing token's secret, and the
     temp-credentials API caps at a 7-day TTL (unsuitable for a persistent .env).
     Create one manually:
         Cloudflare dashboard → R2 → Manage R2 API Tokens → Create API Token
         → scope to the 'coralswarm' bucket, Object Read & Write → copy all three
         values into .env (the secret is shown once — copy it now).
EOF

# Any deferred manual notes (PATH, upgrades, etc.).
if [ "${#MANUAL_NOTES[@]}" -gt 0 ]; then
  printf '\n%sOther manual follow-ups:%s\n' "$C_BOLD" "$C_RESET"
  for n in "${MANUAL_NOTES[@]}"; do printf '  • %s\n' "$n"; done
fi

# How to run the stack.
printf '\n%sRun the stack:%s\n' "$C_BOLD" "$C_RESET"
cat <<'EOF'
  # Migrations (this script already ran them; re-run after pulling new ones —
  # the backend does NOT migrate at boot, it only refuses to start if the
  # schema is missing or behind the binary)
  set -a && source .env && set +a && cargo run -p coralswarm-backend --bin migrate_control
  # Backend (loads .env, serves :8080 + :8081)
  cd backend && set -a && source ../.env && set +a && cargo run
  # Web (Clerk sign-in at http://localhost:3000/login)
  pnpm --filter web dev
EOF

printf '\n%s✓ dev-setup complete.%s Re-run any time — every step is idempotent.\n' "$C_GREEN" "$C_RESET"
