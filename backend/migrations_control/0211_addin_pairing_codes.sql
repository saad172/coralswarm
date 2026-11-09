-- 0211_addin_pairing_codes.sql — the one-time handshake that mints an
-- `addin_tokens` (0210) row without ever asking a human to copy-paste a
-- secret between two machines.
--
-- THE PROBLEM THIS SOLVES.
--   Apps Script has no way to run clerk-js or hold a live Clerk session
--   (0210's header explains why), so the Gmail Add-on cannot sign the user
--   in itself. But it also must not ask the human to manually copy a bearer
--   token out of a browser dev-tools panel into a script property — that is
--   exactly the "no devtools, no copy-pasting tokens" requirement this
--   feature exists to satisfy. This table is the short-lived, single-use
--   relay between the two machines that ARE capable of an HTTP round trip
--   (Apps Script's `UrlFetchApp`, and the signed-in browser tab that
--   approves it) with nothing secret ever typed by a person.
--
-- THE TWO-SECRET SHAPE, AND WHY ONE ALONE IS NOT ENOUGH.
--   `code` is short (8 chars) BECAUSE a human glances at it — it is shown on
--   the Gmail card and (implicitly) on the approval page's URL, so it has to
--   be short enough to be an unremarkable part of a UI, not a security
--   boundary. `poll_secret` is the actual authorization to retrieve the
--   result: long, high-entropy, known only to the ONE Apps Script instance
--   that called `pair/start`, and required by `pair/poll` alongside `code`.
--   Splitting these means an 8-character code alone — leaked via a shared
--   screen, a support screenshot, a browser history entry on a shared
--   machine — cannot be used to steal a paired session; see
--   `routes::addin::pair_poll`'s own doc comment for how a wrong `code`,
--   a wrong `poll_secret`, and a genuinely expired/consumed row are all
--   made to look IDENTICAL on the wire (`{"status": "expired"}`), so there
--   is no oracle that tells an attacker which of the three they hit.
--
--   `poll_secret_hash` stores only the SHA-256 hex digest of `poll_secret`
--   (same non-negotiable as `addin_tokens.token_hash` — the plaintext is
--   never persisted), and `code` is the plaintext because it is not the
--   secret; it exists to be looked up by, not to be guarded.
--
-- `token_plaintext_once` — THE SINGLE-READ FIELD.
--   `pair/approve` (an authenticated route) mints the real
--   `addin_tokens` PAT and writes its plaintext here, ONE time. `pair/poll`
--   (unauthenticated, gated on `poll_secret` alone) is the only reader, and
--   it deletes the row in the SAME transaction it reads
--   `token_plaintext_once` from (`FOR UPDATE`, so two concurrent polls can't
--   both observe it before either commits) — see `routes::addin::pair_poll`.
--   After that read, the plaintext exists nowhere but the Apps Script
--   instance's own `PropertiesService` and whatever the human's `csat_...`
--   token turns out to be; this table never holds it a second time.
--
-- `addin_token_id` — the orphaned-credential fix (CodeRabbit review on #820).
--   `pair/approve` mints the REAL `addin_tokens` row (an ACTIVE credential,
--   `revoked_at IS NULL`) before this row's `token_plaintext_once` is ever
--   read. If the pairing then expires — or is swept up by `pair/start`'s
--   opportunistic cleanup — before `pair/poll` ever retrieves it, deleting
--   THIS row alone would leave that `addin_tokens` row permanently active
--   with no pairing record left to point at it and no plaintext anyone can
--   ever present again: a live, unusable-by-its-intended-owner, unrevoked
--   credential. Persisting the minted token's id here lets both cleanup
--   paths (`pair_poll`'s own expiry check, `pair_start`'s day-later sweep)
--   revoke that SPECIFIC token before deleting the pairing row that named
--   it — see `routes::addin::pair_poll`/`pair_start`'s own comments.
--   Nullable (unset until approval, same as `account_id`/`owner_user_id`
--   above), and `ON DELETE SET NULL` rather than `CASCADE`: deleting a
--   PAIRING row must never cascade into deleting the actual credential row
--   it names — the two are cleaned up by explicit, ordered statements
--   (revoke, then delete), never by a foreign-key side effect.
--
-- `account_id` / `owner_user_id` — NULL until approved.
--   Set by `pair/approve` from the AUTHENTICATED caller's own session
--   (`UserContext`/`Tenant`, never from client input — the security-critical
--   binding this whole flow exists to get right). Before approval there is
--   no account to name; the row is purely "an Apps Script instance is
--   waiting for someone to approve code X".
--
-- WHY THIS IS A SEPARATE TABLE FROM `addin_tokens`, NOT A "PENDING" ROW ON
-- IT. The two have almost disjoint lifecycles and disjoint auth models
-- (this table is reachable UNAUTHENTICATED by design — `pair/start` runs
-- before the caller has any CoralSwarm identity at all — `addin_tokens`
-- never is) and disjoint lifetimes (`expires_at` here is ~10 minutes;
-- `addin_tokens` rows are meant to outlive a browser session by months).
-- Folding them into one table would need a `NULL`-heavy state machine on
-- the very table `require_auth` hits on every add-on request, for the sake
-- of a handshake used once per install.
--
-- CONTROL-ONLY, NO TENANT TWIN, NO RLS — identical justification to 0210
-- (`addin_tokens`)'s own header: resolved entirely before any tenant pool
-- exists, by routes that (for `pair/start`/`pair/poll`) run with NO
-- `Tenant`/`TenancyDecision` at all. Registered in
-- `scripts/check_schema_parity.sh`'s CONTROL_TABLES in the same PR.
--
-- EXPIRY / CLEANUP.
--   `expires_at` (~10 minutes from `pair/start`) is checked on every
--   `pair/approve` and `pair/poll` call, and a row found expired at poll
--   time is deleted then and there — REVOKING its linked `addin_tokens` row
--   first (via `addin_token_id`) whenever the pairing was approved but never
--   retrieved. `pair/start` also opportunistically sweeps rows more than a
--   day past `expires_at` (belt-and-braces for a pairing attempt that is
--   started but never polled again), and does the identical revoke-then-
--   delete for anything that sweep catches. There is deliberately NO
--   dedicated sweep worker yet (contrast `workers::oauth_state_jti_sweep`
--   for `oauth_state_jti`, 0173): this table's rows are already short-lived
--   and self-deleting on the success path, and a background worker for a
--   handful of abandoned 10-minute rows per install is not worth the
--   operational surface yet. Flagged as a deferred follow-up, not an
--   oversight, if this table's row count ever becomes worth watching.
--
-- Depends on: 0210_addin_tokens (the row this table's approved outcome
--             feeds).
--
-- Down:
--   DROP TABLE IF EXISTS addin_pairing_codes;

CREATE TABLE IF NOT EXISTS addin_pairing_codes (
    -- Short, human-glanceable, NOT the security boundary — see this
    -- migration's header. 8 chars from `routes::addin::generate_pairing_code`.
    code                  TEXT        PRIMARY KEY,
    -- SHA-256 hex digest of the long, high-entropy secret only the
    -- requesting Apps Script instance knows. The REAL authorization to
    -- retrieve `token_plaintext_once`.
    poll_secret_hash      TEXT        NOT NULL,
    -- NULL until `pair/approve` binds this pairing attempt to a real
    -- CoralSwarm identity, from the AUTHENTICATED caller's own session.
    account_id            UUID        REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id         UUID        REFERENCES users(id) ON DELETE CASCADE,
    -- The minted `addin_tokens` PAT, in the clear, exactly once. Written by
    -- `pair/approve`; read and then deleted (row-and-all) by the next
    -- successful `pair/poll` — see this migration's header.
    token_plaintext_once  TEXT,
    -- The id of the `addin_tokens` row `pair/approve` minted for this
    -- pairing attempt — see this migration's header on why this exists (the
    -- orphaned-credential fix) and why `ON DELETE SET NULL`, not `CASCADE`.
    addin_token_id        UUID        REFERENCES addin_tokens(id) ON DELETE SET NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at            TIMESTAMPTZ NOT NULL,
    approved_at           TIMESTAMPTZ
);

-- `pair/start`'s own opportunistic sweep (this migration's header) and any
-- future dedicated worker's access pattern: everything past its expiry.
CREATE INDEX IF NOT EXISTS idx_addin_pairing_codes_expires_at
    ON addin_pairing_codes (expires_at);
