-- Migration: 0196_retraction_audit_log
-- Description: FM-5 (docs/plans/capture/02-b2-retract-fixes.md, B2 unit 02,
--   D5/D7/D8) — the append-only substrate that makes retract's per-(ocean,
--   surface) outcome a durable, user-facing, legally-relevant record instead
--   of tracing-only logging on failure
--   (`backend/src/routes/deletion.rs:167,238,310` at transcription time).
--   Without this table `activity_events`' CHECK
--   (`0114_promote_event_type.sql:21-31`, eight event types, none
--   deletion-related) REJECTS a retraction event outright — this is a new
--   table, not a new event type on that one.
--
--   ── Why the CONTROL plane (founder decision, 2026-08-25) ──
--   D7's own precedent, `member_ocean_lifecycle_log`
--   (`0092_member_ocean_control_tables.sql:137-167`), is control-plane while
--   describing tenant data, for the identical reason this table needs:
--   *"the author's cross-ocean view (D8) is one query instead of a
--   tenant-DB fan-out, and FM-4's `not_a_member` outcome row must be
--   writable for an ex-member — on the tenant plane that write targets a DB
--   the actor just lost, which is exactly the blocked path."* Unlike
--   `query_audit_log`/`connector_audit_log` (migrations 0191/0069,
--   0193/0071), which are PAIRED control+tenant tables because an org
--   tenant's own audit trail has to survive independently of the control
--   plane, this table is deliberately CONTROL-ONLY — no tenant-tree twin,
--   ever (the same posture as `member_ocean_lifecycle_log`, and for the
--   same "a row about tenant data must be readable even when the actor's
--   membership — or the tenant DB itself — is gone" reason). Retracts
--   against an ORG TENANT database still write here, over the SAME
--   control-plane pool `AppState.db`/`state.db` every request already holds
--   (`backend/src/tenancy/extractor.rs`'s own note that content-plane and
--   control-plane pools are resolved independently) — see
--   `services::retraction_audit`'s module doc for the write path.
--
--   ── Identity (D7) ──
--   `retraction_id` groups every row ONE retract call produced (one call can
--   touch several `(ocean, surface)` pairs via propagation or FM-2/FM-6
--   reach) — the D6 UI's "reached and unreached copies in one list" renders
--   one `retraction_id` at a time. `atom_id`/`source_id` are D7's stable
--   `(source_type, source_id, atom_id)` triple, at whatever granularity the
--   surface addresses: `"content"` rows carry a real `atom_id`;
--   `"meeting_transcript"`/`"coding_session"`/`"journal_rollup"` rows
--   address a GROUP of atoms via `source_id` alone (`atom_id IS NULL`) — see
--   `services::deletion::SurfaceOutcome`'s own doc comment.
--
--   ── Outcome vocabulary (D6/D7) ──
--   Exactly three values, `CHECK`-enforced: `removed`, `out_of_window`
--   (FM-3's accepted limitation — the journal/query-history self-heal
--   window), `not_a_member` (FM-4's accepted limitation — the caller lost
--   membership in that ocean). FM-3 and FM-4 are NOT fixed by this table;
--   this table only lets the app tell the truth about them instead of
--   staying silent.
--
--   ── `is_personal_ocean` (D8) ──
--   `graphs.kind IN ('personal', 'member')` is a single-viewer ocean (a
--   personal account's own ocean, or an org member's private ocean —
--   `0090_member_ocean_columns.sql` widened `graphs_kind_check` to admit
--   `'member'`); `'team'`/`'org'` are shared. Resolved once per `graph_id`
--   by the WRITER (`services::deletion`, against the CONTENT pool — the
--   only pool that can see the `graphs` row, whichever plane it lives on)
--   and carried on the row rather than re-derived by a cross-database join
--   at read time, because no such join is possible when the ocean is an ORG
--   TENANT graph the control plane cannot see at all.
--
--   ── Metadata-only `manifest` (D7, copying `member_ocean_lifecycle_log`
--   verbatim) ──
--   *"A bare `JSONB` column happily accepts an atom body, so the contract
--   has to be a constraint or it is just a comment."* The allowlist here is
--   deliberately narrow — `survivors` only (D7's `survivors[]`, ids of
--   derived artifacts left standing) — because D7 itself is the reason a
--   digest was refused: *"FM-1 is that retract writes an extra plaintext
--   copy of what is being retracted into the `op_log` tombstone. A content
--   digest in the audit row is the weaker form of the identical mistake,
--   and it would have been introduced by the very row that fixes FM-1."*
--   NO content, no hash, no digest — ever. Enforced TWO ways, not one
--   (added on review, finding 8): `retraction_audit_log_manifest_metadata_
--   only` constrains which KEYS `manifest` may carry (`survivors` only);
--   `retraction_audit_log_manifest_survivors_shape` constrains the VALUE
--   under that key to a JSON array of bare UUID strings — a key-name-only
--   check alone would have accepted an atom body under the allowlisted key
--   itself (`{"survivors": "<content>"}`), which is exactly the hole a
--   digest was refused to avoid re-opening a different way.
--
--   ── Append-only (D7: REVOKE, not a CHECK) ──
--   `0083_consumer_rls.sql:382` grants `coralswarm_app` `DELETE`/`UPDATE` on
--   every table by default (`ALTER DEFAULT PRIVILEGES`), so a brand-new
--   table is NOT append-only until told otherwise — the explicit `REVOKE`
--   below is that instruction, same class as the three narrowings 0083
--   itself carries (`tenant_databases`/`escrow_shares`/`_sqlx_migrations`).
--   Deliberately a REVOKE, not merely an absent RLS policy for
--   UPDATE/DELETE (the shape `0193_connector_audit_log.sql` uses): RLS's
--   default-deny for an unmatched command silently affects ZERO rows — a
--   caller sees success, having deleted nothing. A privilege REVOKE instead
--   raises a hard `permission denied` error, which is the honest response
--   to "delete this audit row" and the literal negative control D5/FM-5
--   names: *"attempt a DELETE on the new table as the app role and have the
--   REVOKE refuse it."*
--
--   ── RLS (D8) ──
--   `author-all-oceans + admin-of-that-ocean, personal rows author-only`.
--   `actor_user_id = app.user_id` (author, every ocean — including
--   `is_personal_ocean` rows, since the author IS the only visible party
--   there anyway) OR (`NOT is_personal_ocean` AND the caller is
--   `owner`/`admin` on `account_members` for this row's `account_id`). A
--   `system`/`webhook` row (`actor_user_id IS NULL`, mirroring
--   `connector_audit_log`'s own `actor_kind`/`actor_user_id` pairing) is
--   NEVER author-visible (nobody authored it) and falls through to the
--   admin clause only — correct, since nobody's personal act needs hiding
--   from an admin here. `app.user_id` is the SAME control-plane session GUC
--   `0191_query_audit_log.sql`/`0193_connector_audit_log.sql` already
--   established (not the tenant-plane `app.user_id` `tenancy::scope`'s
--   module doc describes — same NAME, disjoint databases, no collision
--   possible).
--
-- Depends on: 0001_init (accounts, users, account_members),
--   0092_member_ocean_control_tables (the `manifest` CHECK / control-only
--   placement precedent), 0090_member_ocean_columns (`graphs_kind_check`
--   admitting `'member'`), 0191_query_audit_log /
--   0193_connector_audit_log (the `app.account_id` + `app.user_id`
--   author-plus-admin RLS shape and the actor-pairing CHECK this migration
--   follows).
-- Down:
--   REVOKE ALL ON retraction_audit_log FROM coralswarm_app;
--   DROP POLICY IF EXISTS retraction_audit_select ON retraction_audit_log;
--   DROP POLICY IF EXISTS retraction_audit_insert ON retraction_audit_log;
--   DROP TABLE IF EXISTS retraction_audit_log;
--   DROP FUNCTION IF EXISTS retraction_audit_log_survivors_are_uuids(JSONB);

-- See the `retraction_audit_log_manifest_survivors_shape` CONSTRAINT's own
-- comment below (on the table this function's only caller belongs to) for
-- why this exists as a function rather than an inline CHECK expression.
-- `IMMUTABLE`: depends only on its input.
CREATE OR REPLACE FUNCTION retraction_audit_log_survivors_are_uuids(m JSONB)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $func$
    SELECT NOT (m ? 'survivors')
        OR (
            jsonb_typeof(m -> 'survivors') = 'array'
            AND NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements_text(m -> 'survivors') AS elem(v)
                WHERE v !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
        )
$func$;

CREATE TABLE IF NOT EXISTS retraction_audit_log (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Groups every row ONE retract call produced (propagation / FM-2 / FM-6
    -- can each add rows beyond the one atom the caller named).
    retraction_id     UUID        NOT NULL,
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    actor_kind        TEXT        NOT NULL CHECK (actor_kind IN ('user', 'system', 'webhook')),
    actor_user_id     UUID,
    retracted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- D7 identity triple, part 1/3.
    source_type       TEXT        NOT NULL CHECK (source_type <> ''),
    -- D7 identity triple, part 2/3 — the finest-grained id THIS surface
    -- addresses (an atom's own source_id, a meeting's base source_id, or a
    -- coding_sessions.session_id).
    source_id         TEXT        NOT NULL CHECK (source_id <> ''),
    -- D7 identity triple, part 3/3 — a real atom_id for `"content"` rows;
    -- NULL for group-addressed surfaces (meeting_transcript/coding_session/
    -- journal_rollup), which have no single atom to name.
    atom_id           UUID,
    -- The ocean. Bare uuid, no FK — same reasoning as
    -- `member_ocean_lifecycle_log.graph_id` (0092): this ocean may live in
    -- an ORG TENANT database this control-plane row cannot reference, and
    -- Postgres has no cross-database foreign key regardless of intent.
    graph_id          UUID        NOT NULL,
    -- D8's author-only trigger — see this migration's header.
    is_personal_ocean BOOLEAN     NOT NULL,
    -- Free text (not a fixed enum): new surfaces are expected over time.
    -- This PR writes exactly four: content, meeting_transcript,
    -- coding_session, journal_rollup.
    surface           TEXT        NOT NULL CHECK (surface <> ''),
    -- D6/D7's fixed three-value vocabulary — see `services::deletion::outcome`.
    outcome           TEXT        NOT NULL CHECK (outcome IN ('removed', 'out_of_window', 'not_a_member')),
    -- Metadata-only, D7's `survivors[]` the only allowlisted key — see this
    -- migration's header.
    manifest          JSONB       NOT NULL DEFAULT '{}'::jsonb,

    -- `actor_kind = 'user'` iff a real `actor_user_id` is present — same
    -- pairing CHECK `0193_connector_audit_log.sql` uses, same reason: a
    -- write bypassing the `services::connector_audit::Actor`-shaped caller
    -- could otherwise misattribute or de-attribute a row.
    CONSTRAINT retraction_audit_log_actor_pairing
        CHECK ((actor_kind = 'user') = (actor_user_id IS NOT NULL)),

    -- The metadata-only contract, ENFORCED rather than merely documented —
    -- copying `member_ocean_lifecycle_log_manifest_metadata_only`
    -- (0092:137-167) verbatim in shape, narrowed to this table's own
    -- allowlist. `manifest - ARRAY['survivors']` deletes the one allowlisted
    -- key; anything left over means an unexpected key was supplied (e.g. an
    -- atom body) and the row is rejected.
    --
    -- **KEY NAMES ONLY — added on review (finding 8, CRITICAL).** The CHECK
    -- above constrains which KEYS `manifest` may carry; it says nothing
    -- about the SHAPE of the values under them.
    -- `manifest - ARRAY['survivors'] = '{}'::jsonb` happily accepts
    -- `{"survivors": "the fee is $48,500 — the retracted body"}` — an atom
    -- BODY sitting in the one column this table's own header says must
    -- "NEVER" carry content, which is precisely the weaker-form-of-FM-1
    -- mistake D7 refused a content digest to avoid. The second constraint
    -- below closes that: when present, `survivors` must be a JSON ARRAY,
    -- and every element must be a bare UUID string (the shape every
    -- `SurfaceOutcome::survivors` producer in `services::deletion` actually
    -- populates it with — derived-artifact ROW IDS, never prose) — so a
    -- content string, a nested object, or any other shape is rejected at
    -- the same hard boundary the key-name check already enforces.
    CONSTRAINT retraction_audit_log_manifest_metadata_only
        CHECK (manifest - ARRAY['survivors'] = '{}'::jsonb),
    -- `retraction_audit_log_survivors_are_uuids(jsonb)` below — CHECK
    -- constraints cannot contain a subquery directly (Postgres: "cannot use
    -- subquery in check constraint", the exact limitation 0092's own header
    -- names for why ITS manifest contract is a key-subtract expression, not
    -- a per-element check). Wrapping the per-element validation in an
    -- IMMUTABLE SQL function moves the subquery out of the CHECK expression
    -- itself (legal: only the constraint's own top-level expression is
    -- restricted) while keeping the semantics identical.
    CONSTRAINT retraction_audit_log_manifest_survivors_shape
        CHECK (retraction_audit_log_survivors_are_uuids(manifest))
);


-- Author's cross-ocean view (D8: "author -> own rows, every ocean") and the
-- admin's per-account listing both filter by `actor_user_id`/`account_id`
-- first, newest first.
CREATE INDEX IF NOT EXISTS idx_retraction_audit_log_actor
    ON retraction_audit_log (actor_user_id, retracted_at DESC);
CREATE INDEX IF NOT EXISTS idx_retraction_audit_log_account
    ON retraction_audit_log (account_id, retracted_at DESC);
-- The D6 UI's own read pattern: "every row from ONE retract call".
CREATE INDEX IF NOT EXISTS idx_retraction_audit_log_retraction_id
    ON retraction_audit_log (retraction_id);

-- ── RLS (D8) ──────────────────────────────────────────────────────────────
ALTER TABLE retraction_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS retraction_audit_select ON retraction_audit_log;
DROP POLICY IF EXISTS retraction_audit_insert ON retraction_audit_log;

CREATE POLICY retraction_audit_select ON retraction_audit_log
    FOR SELECT
    USING (
        actor_user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
        OR (
            NOT is_personal_ocean
            AND EXISTS (
                SELECT 1 FROM account_members am
                WHERE am.account_id = retraction_audit_log.account_id
                  AND am.user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
                  AND (am.role = 'owner' OR am.role = 'admin')
            )
        )
    );

CREATE POLICY retraction_audit_insert ON retraction_audit_log
    FOR INSERT
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            actor_user_id IS NULL
            OR actor_user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
        )
    );

-- ── Append-only (D7: REVOKE, not a CHECK) ──────────────────────────────────
-- `0083_consumer_rls.sql:382`'s `ALTER DEFAULT PRIVILEGES` granted
-- `coralswarm_app` UPDATE/DELETE on this table the moment it was CREATEd
-- above — this narrows it back off, the same class of exception 0083
-- itself carries for `tenant_databases`/`escrow_shares`/`_sqlx_migrations`.
REVOKE UPDATE, DELETE ON retraction_audit_log FROM coralswarm_app;
