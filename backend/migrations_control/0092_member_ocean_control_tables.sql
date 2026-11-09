-- Migration: 0092_member_ocean_control_tables
-- Description: member-private-oceans PR 3
--              (docs/plans/member-private-oceans.md §2.1/§2.6/§7) — the two
--              CONTROL-ONLY tables the lifecycle work needs. No tenant-tree
--              counterpart exists (and none should ever be added): these
--              tables are deliberately NOT in the shared `graphs`-style
--              paired-migration convention, which is exactly why this PR can
--              land independently of the sibling predicate PR (0091, control-
--              only-numbering-wise this PR claims 0092 and NO tenant number at
--              all).
--
--              ── member_ocean_desired_state ──
--              The provisioning/lifecycle target for one (org, member) pair.
--              `clerk_org_id TEXT NOT NULL` — deliberately NO foreign key, the
--              same convention as `org_revocations`/`org_membership_tombstones`
--              (migrations 0081/0082): an org's `accounts` row can churn
--              (offboarding, re-provisioning) independently of this row's
--              lifecycle, and a bare TEXT column can't even be declared
--              against a tenant-tree id from the control tree the other
--              direction (there is no tenant-tree FK target here regardless).
--              `user_id` DOES carry a real FK (`users(id) ON DELETE CASCADE`)
--              — unlike the org id, a deleted USER genuinely should not leave
--              orphaned desired-state rows behind; there is nothing left to
--              reconcile toward once the user row itself is gone.
--              `offboarded_at` is set ONCE and never reset by a webhook
--              replay (plan §2.6): an `organizationMembership.updated` event
--              re-delivered after a member already left must not push the
--              60-day retention clock forward, or effective retention could
--              silently exceed the stated window indefinitely. Enforcing
--              "set once" is an application-level contract in this migration
--              (the future webhook/reconciler upsert path uses
--              `ON CONFLICT ... DO UPDATE SET offboarded_at = COALESCE(
--              member_ocean_desired_state.offboarded_at, EXCLUDED.offboarded_at)`
--              or equivalent) — not a CHECK constraint, since Postgres has no
--              clean way to express "this column, once non-null, must never
--              change" without a trigger, and a trigger is more machinery than
--              this table needs on day one.
--              `graph_id` is nullable: desired state can exist (e.g. a webhook
--              upserted "this member should have a present ocean") before any
--              worker has actually created the `graphs` row.
--
--              ── member_ocean_lifecycle_log ──
--              The persistent, cross-plane-durable audit trail (plan §2.6,
--              decisions table: "Audit | Persistent rows in the control DB,
--              not logs only"). Precedent: `escrow_recovery_log`
--              (0030_org_escrow.sql) is control-side for the identical
--              reason — a row describing tenant data must be readable AFTER
--              the tenant is gone. Org offboarding runs `neonctl projects
--              delete`, which destroys the ENTIRE tenant database — a
--              tenant-tree audit table would vanish at exactly the moment
--              "did we export member X before deleting their data?" becomes
--              the question an operator asks. `clerk_org_id TEXT NOT NULL`
--              carries the same no-FK reasoning as above (and the same as
--              0081/0082). `subject_user_id` is nullable and carries NO FK —
--              deliberately: a `users` row can be hard-deleted (e.g. a GDPR
--              erasure request) while its purge/export audit trail must
--              survive that deletion untouched; an `ON DELETE CASCADE` (or
--              even `ON DELETE SET NULL` racing a live INSERT) FK here would
--              risk exactly the audit loss this table exists to prevent.
--              `graph_id` is NOT NULL and also carries no FK to the tenant
--              tree for the structural reason above (no cross-database FK is
--              possible in Postgres regardless of intent) — it is a plain
--              value carried for cross-referencing against tenant-side
--              `graphs.id` while the tenant DB still exists.
--              `manifest JSONB` carries ONLY identifiers, counts, and digests
--              — counts of rows/blobs, byte sizes, a bundle sha256 — NEVER
--              atom/node content. This is what keeps the control (shared,
--              multi-tenant) plane from gaining any of a single org's tenant
--              data: an audit row about a purge is safe to store next to
--              every OTHER org's control rows precisely because it carries no
--              content, only metadata about the operation.
--              Two-phase (`started` -> `completed`, with `failed` as the third
--              terminal state) because there is no cross-plane transaction
--              linking a control-DB audit write to a tenant-DB blob delete or
--              row delete — plan §2.6: "A `started` row with no `finished_at`
--              past the job timeout is the operator's 'verify by hand'
--              signal."
--
--              ── Indexing ──
--              `member_ocean_lifecycle_log_org_idx (clerk_org_id, started_at
--              DESC)` is the operator/runbook access pattern: "show me this
--              org's lifecycle history, newest first" (e.g. verifying an
--              export happened before answering a departed member's data
--              request, or checking for a stuck `started` row before an
--              offboard). `member_ocean_desired_state`'s primary key
--              `(clerk_org_id, user_id)` already serves the reconciler's
--              natural access pattern ("every desired-state row for this
--              org"), so no additional index is added for it in this PR.
--
--              ── Not yet written by any code path in this PR ──
--              PR 3 (this one) adds the schema and the provisioning-only
--              reconciler skeleton (`workers::member_ocean_reconciler`),
--              which reads `account_members` directly (the plan's "retroactive
--              backfill is trigger 3 with no extra code" — desired state IS
--              every `account_members` row for the org, so the reconciler's
--              provision arm does not need to read OR write
--              `member_ocean_desired_state` to do its job) and writes only to
--              tenant-side `graphs`. Neither table here is written by any
--              Rust in this PR: `member_ocean_desired_state` gets its first
--              writer in PR 4 (the webhook trigger's upsert) and
--              `member_ocean_lifecycle_log` gets its first writer in PR 7
--              (archive/restore) and PR 8 (purge). Both tables are additive
--              and behind `MEMBER_OCEANS_ENABLED` (default off) at the
--              call-site level once those PRs land — this migration alone is
--              schema-only and changes no runtime behavior.
--
--              ── CI note ──
--              Both table names MUST be added to `CONTROL_TABLES` in
--              `scripts/check_schema_parity.sh` in this SAME PR (done) — that
--              script diffs a schema-only `pg_dump` of the control tree
--              (minus `CONTROL_TABLES`) against the tenant baseline, and these
--              two tables have no tenant-tree counterpart at all.
--
-- Depends on: 0001_init (users), 0081_org_membership_tombstones /
--             0082 (the clerk_org_id-as-plain-TEXT precedent),
--             0030_org_escrow (escrow_recovery_log control-side-audit
--             precedent), 0090_member_ocean_columns (the graphs.kind='member'
--             / owner_user_id / archived_at columns this lifecycle data
--             describes).
--
-- Down:
--   DROP INDEX IF EXISTS member_ocean_lifecycle_log_org_idx;
--   DROP TABLE IF EXISTS member_ocean_lifecycle_log;
--   DROP TABLE IF EXISTS member_ocean_desired_state;

CREATE TABLE member_ocean_desired_state (
    clerk_org_id  TEXT NOT NULL,          -- plain TEXT, no FK (0081/0082 precedent)
    user_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    state         TEXT NOT NULL CHECK (state IN ('present','archived','purged')),
    offboarded_at TIMESTAMPTZ,            -- set ONCE, never reset by webhook replay
    restored_at   TIMESTAMPTZ,
    graph_id      UUID,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (clerk_org_id, user_id)
);

CREATE TABLE member_ocean_lifecycle_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action          TEXT NOT NULL CHECK (action IN ('archive','restore','export','purge')),
    status          TEXT NOT NULL CHECK (status IN ('started','completed','failed')),
    clerk_org_id    TEXT NOT NULL,
    subject_user_id UUID,                 -- nullable: survives user deletion
    graph_id        UUID NOT NULL,
    actor           TEXT NOT NULL,        -- 'webhook' | 'reconciler' | 'operator:<label>'
    manifest        JSONB NOT NULL DEFAULT '{}',   -- counts, bytes, bundle sha256
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,

    -- ── The metadata-only contract, ENFORCED rather than merely documented ──
    -- This table lives on the SHARED control plane but describes TENANT data.
    -- The whole reason that placement is acceptable is that a row carries
    -- identifiers, counts and digests -- never content. A bare `JSONB` column
    -- happily accepts an atom body, so the contract has to be a constraint or
    -- it is just a comment that the first careless caller breaks.
    --
    -- `manifest - ARRAY[...]` deletes the allowlisted keys; if anything is
    -- left over, an unexpected key was supplied and the row is rejected. A
    -- subquery over jsonb_object_keys() would be the obvious way to express
    -- this, but CHECK constraints cannot contain subqueries -- the key-subtract
    -- operator is the form that works here.
    CONSTRAINT member_ocean_lifecycle_log_manifest_metadata_only CHECK (
        manifest - ARRAY[
            'atoms', 'nodes', 'node_atoms', 'blobs', 'bytes',
            'relationship_edges', 'transcripts', 'conversations',
            'manifest_sha256', 'export_id', 'schema_version',
            'offboarded_at', 'archived_at', 'drift_seconds', 'error'
        ] = '{}'::jsonb
    ),

    -- Terminal rows must record when they finished; in-flight rows must not
    -- claim to have. Without this, a `started` row with a `finished_at` and a
    -- `completed` row without one are both representable, and the two-phase
    -- audit's central signal -- "a `started` row with no `finished_at` past the
    -- job timeout means verify this by hand" -- stops being trustworthy.
    CONSTRAINT member_ocean_lifecycle_log_finished_at_matches_status CHECK (
        (status = 'started' AND finished_at IS NULL)
        OR (status IN ('completed', 'failed') AND finished_at IS NOT NULL)
    )
);
CREATE INDEX member_ocean_lifecycle_log_org_idx ON member_ocean_lifecycle_log (clerk_org_id, started_at DESC);
