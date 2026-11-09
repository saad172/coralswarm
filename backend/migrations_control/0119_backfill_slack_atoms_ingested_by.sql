-- Migration: 0119_backfill_slack_atoms_ingested_by
-- Description: one-shot repair for the Slack author-attribution gap
--   (ticket .scratch/today-zoomable-journal/issues/08-task-slack-huddle-
--   attribution.md; audit gap 1, .scratch/today-zoomable-journal/research/
--   hourly-completeness.md). `routes::slack_events::ingest_slack_message` now
--   stamps `ingested_by` from the `connector_identities` directory it already
--   reads for the canonical author name (see that function's "Author-mapped
--   `ingested_by` attribution" comment) — but every Slack atom inserted
--   BEFORE that fix carries `ingested_by = NULL` forever, since nothing
--   re-derives it after the fact, and `store::journal_hour_inputs` /
--   `journal_dirty_hours` both filter on `ingested_by`
--   (`backend/src/store/mod.rs` — `WHERE ingested_by = $user` /
--   `ingested_by IS NOT NULL`), so a NULL-authored atom is invisible to every
--   user's journal.
--
--   For every historical Slack atom whose author NOW resolves to a confirmed
--   CoralSwarm user (a link that may well not have existed yet AT INGEST
--   TIME — the whole reason this is a backfill and not just "wait for new
--   messages"), stamp `ingested_by`. `author_id`/`workspace_id` are the SAME
--   structured provenance columns (migrations 0034/0058) the live insert
--   already carries per atom, so this is a straight equality join, never a
--   display-name guess.
--
--   Bounded to `source_type = 'slack' AND ingested_by IS NULL`, so it can
--   never touch a row a human (or an earlier run of this very migration) has
--   already resolved — idempotent by construction, safe to leave permanently
--   in `migrations_control` rather than run-and-delete. Re-running it after
--   more `connector_identities` links land is intentionally a no-op here
--   (this is a ONE-SHOT repair for the pre-fix backlog, not a standing sweep)
--   — a later author who links their account only benefits going forward,
--   same as the existing `should_persist_identity` name-repair path already
--   accepts for `author_name`.
--
--   ── Why huddle transcripts (source_type = 'transcript') are NOT backfilled
--      here ──
--   Huddle atoms only ever persisted resolved DISPLAY NAMES in
--   `participants` (`routes::slack::resolve_user_name`, which — unlike Slack
--   message ingest's `canonical_author_name` — never consults
--   `connector_identities`), never the raw Slack attendee ids
--   `workers::huddle::capture_huddle` actually saw. There is no durable
--   per-atom column on a historical huddle atom to equality-join against
--   `connector_identities.external_user_id`, and matching by display-name
--   string instead would be exactly the "never guess from display names"
--   the connector's own contract (docs/connectors/CONNECTOR_CONTRACT.md §4,
--   C5 row) forbids everywhere else in this pipeline. Historical huddle
--   atoms are therefore left `ingested_by = NULL` — the same accepted
--   "invisible to journals" degradation an unmappable Slack message author
--   already has — only NEWLY captured huddles get attribution, via
--   `workers::huddle::capture_huddle`'s new `resolve_huddle_attribution`
--   lookup. Flagged in the PR for founder awareness, not silently dropped.
--
--   ── Plane ──
--   Consumer/control-plane only, matching every other piece of this
--   pipeline: Slack ingest reads/writes `atoms` on the SAME single
--   RLS-scoped database `routes::slack_events::ingest_slack_message` already
--   uses (`begin_account_scoped`, org-tenancy B.3), never a per-org tenant
--   database — same posture migration 0102's header documents for the
--   sibling `slack_huddle_captures` ledger. Runs as the migration owner
--   (BYPASSRLS), which is how every other cross-account backfill migration
--   in this tree already operates (e.g. 0051, 0102's own backfill step) —
--   `connector_identities` itself carries no RLS at all (migration 0104).
--
-- Depends on: 0034_atom_metadata (author_id), 0049_connector_identity_self_reef
--             (connector_identities), 0058_atoms_workspace_provenance
--             (workspace_id), 0104_connector_identity_verified_link (user_id)
-- Down: not reversible — recording which rows this UPDATE touched (to null
--   them back out) is not worth the added complexity for a backfill whose
--   effect is strictly "make an already-existing, otherwise-invisible atom
--   attributable to its real author." Re-running with the underlying fix
--   reverted has no further effect regardless (every already-set row is
--   excluded by `ingested_by IS NULL`).

UPDATE atoms a
SET    ingested_by = ci.user_id
FROM   connector_identities ci
WHERE  a.source_type = 'slack'
  AND  a.ingested_by IS NULL
  AND  a.author_id IS NOT NULL
  AND  a.workspace_id IS NOT NULL
  AND  ci.provider = 'slack'
  AND  ci.workspace_id = a.workspace_id
  AND  ci.external_user_id = a.author_id
  AND  ci.user_id IS NOT NULL;
