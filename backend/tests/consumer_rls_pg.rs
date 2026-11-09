//! Integration test for the consumer-plane RLS backstop (org-tenancy PR 3.4,
//! migration `0083_consumer_rls`).
//!
//! Proves TWO things, both required by the plan:
//!
//! 1. **Enforcement is real under the `coralswarm_app` role.** A connection
//!    authenticated AS `coralswarm_app` (a real, separate login — not
//!    `SET ROLE` inside the owner's session, so this exercises the exact
//!    connection path OPS-4 will point the app at) with
//!    `SET LOCAL app.account_id` set to account A can read account A's rows
//!    on every RLS'd table and NONE of account B's — and, per the plan's
//!    deliberately strict predicate (`current_setting('app.account_id')`,
//!    no `missing_ok` default), a session that never sets it gets a hard
//!    Postgres error, not a quiet empty result.
//! 2. **It is inert today.** A connection as the current owner/superuser role
//!    (`BYPASSRLS`, same as prod's app role right now) sees every account's
//!    rows regardless of `app.account_id`, proving migration 0083 changes
//!    zero query results for the app as currently deployed.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights AND permission to `ALTER ROLE`/`CREATE ROLE`). Follows the
//! same scratch-DB pattern as `tenant_migrate_pg.rs` / `dek_relocation_pg.rs`.
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test consumer_rls_pg

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

use std::str::FromStr;

use chrono::Utc;
use coralswarm_backend::retrieval::RetrievalFilter;
use coralswarm_backend::store::{PostgresStore, Store};

use coralswarm_backend::tenancy::scope::begin_account_scoped;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

/// The exact table roster carrying a consumer-plane `account_isolation`-
/// shaped policy. Migration 0083 covers every content table it shipped
/// against except the three deliberately uncovered ones (`jobs`,
/// `connector_identities`, `sync_cursors` — see that migration's header);
/// later migrations (e.g. `0101_slack_watched_channels`,
/// docs/connectors/slack.md §2/§4) add their own `account_isolation`-shaped
/// policy on top — kept in lockstep with every migration that adds such a
/// policy. Also kept in lockstep with `scripts/check_schema_parity.sh`'s
/// `CONTROL_ONLY_RLS_TABLES` — but NOT as an identical set (CodeRabbit
/// round-1 fix on the gsuite G2.1 PR: an earlier version of this comment
/// implied equality, which is wrong): this roster is a STRICT SUPERSET of
/// that one. It also holds the control-plane-ONLY tables that have no
/// tenant-tree twin at all (e.g. `credit_balances`, `grants`,
/// `stripe_subscriptions`), and the shared tables whose policy is identical on
/// both planes and so is never stripped (e.g. `teams_meeting_captures`; see
/// that script's `CONTROL_ONLY_FORCE_RLS_TABLES`). See that script's own
/// `CONTROL_ONLY_RLS_TABLES` comment for the full reasoning.
/// Asserted as a set (not just spot-checked) so a future edit to a
/// migration that silently drops (or forgets to add) a policy fails this
/// test, not just a manual review. Deliberately NOT stated as a table count
/// here: that count has drifted (gone stale) multiple times already as new
/// tables were added — the roster below is the only thing this test
/// actually enforces.
const RLS_TABLES: &[&str] = &[
    // Shared read-model rows and system-only fences, plus control authority revision.
    "read_models",
    "read_model_triggers",
    "read_model_graph_fences",
    "read_model_user_fences",
    "read_model_control_revisions",
    // Account billing, deletion, and library listing tables added by later migrations.
    "account_deletions",
    "billing_notices",
    "extra_usage_balances",
    "extra_usage_ledger",
    "extra_usage_periods",
    "extra_usage_reloads",
    "extra_usage_settings",
    "library_listing_changes",
    "library_listing_generations",
    "library_listing_outcomes",
    "library_listing_source_heads",
    "library_listing_staged_outcomes",
    "library_listing_staged_sources",
    "library_listing_state",
    "org_deletion_policy",
    "org_deletions",
    "org_unpaid_watch",

    // Immutable meeting source versions and evidence.
    "source_items",
    "source_version_chunks",
    "source_versions",
    "semantic_atoms",
    "semantic_atom_passages",
    "node_semantic_atoms",
    "semantic_atom_windows",
    "source_retraction_outbox",
    "assertion_disagreements",
    "corpus_enrichment_runs",
    "corpus_enrichment_units",
    // P10 0307/0142 noncontent managed capture admission receipts.
    "corpus_capture_admissions",
    "managed_budget_periods",
    "managed_attempt_receipts",
    "managed_credit_reservations",
    // 0345: funded receipts billing recovery has not confirmed on control.
    "managed_receipt_recovery_queue",
    "managed_embedding_inputs",
    // PR 3 scaffold: all-input receipts for managed_enrichment.
    "managed_enrichment_inputs",
    // Before-floor overrides: operator-written; account-scoped SELECT only.
    "standing_floor_overrides",
    // P7 0303/0138 scoped answer receipts; P11 0309/0144 recorded temporal state.
    "answer_support_receipts",
    "source_version_observations",
    "temporal_evidence_records",
    "fact_supersession_edges",
    // 0351 / tenant 0182: statement vectors for scoped Ask (managed cloud_embedding).
    "semantic_atom_embeddings",
    // 0353 / tenant 0184: stored workspace card counts, one row per graph.
    "graph_workspace_counts",
    // 0354 / tenant 0185: graph-keyed Library evidence storage.
    "library_evidence_changes",
    "library_evidence_state",
    "library_node_evidence",
    "library_node_item_evidence",
    // 0364: control-only, operator-account-scoped rollout proof ledgers.
    "library_rollout_runs",
    "library_rollout_samples",
    "library_rollout_windows",
    "answer_temporal_receipts",
    // P8 0308: purge outbox/dependencies/reviews, selected-evidence consent
    // and known-new upload cleanup.
    "source_purge_events",
    "source_purge_dependencies",
    "source_purge_reviews",
    "selected_evidence_publications",
    "unattached_capture_uploads",
    // 0325 / tenant 0157: supported human reef identity records. The three
    // read/insert/retire policies require the graph's account and reachable
    // actor; reads/inserts also require exact current permitted support.
    // Non-FORCE RLS preserves owner bypass. This entry checks inventory and
    // policy presence; actual supported writes, actor/foreign-account denials,
    // immutable events and support-loss delivery are exercised by
    // reef_identity_pg::reef_identity_consumer_security_and_lifecycle.
    "reef_identity_corrections",

    "activity_events",
    "atoms",
    "coding_session_shares",
    "coding_sessions",
    // Composio 0301 (account-keyed); the roster had drifted since its merge.
    "composio_capture_scopes",
    "composio_connections",
    // 0401 (connectors redesign B1): per-member connector source state, account-keyed.
    "connector_sources",
    // 0112 — §3.4 model-verdict marker. Anchored on `account_id` via the
    // owning graph, same shape as every other content table here.
    "conflict_dismissal",
    // 0193 (m365 enterprise train PR 4.2, plan decision #11 part 1): the
    // connector compliance audit log. Anchored DIRECTLY on `account_id` (no
    // ocean anchor — `subject`/`detail` are bare ids/counts, not a
    // `graph_id`), same shape as `journal_rollups`/`extraction_cache`.
    // Append-only via a SELECT/INSERT command split (no UPDATE/DELETE
    // policy at all) — see `connector_audit_log_is_append_only_and_isolated_pg`
    // for the dedicated proof, mirroring `grant_seat_tiers`'s own dedicated
    // test below.
    "connector_audit_log",
    // 0243 (CS-446, org-onboarding plan §3 item 16, D16) — the durable,
    // admin-facing connector-health record: today, "the member whose Google
    // grant backed this org's Workspace directory has left, so D16's rung 1
    // is gone." Anchored DIRECTLY on `account_id`, control-only, no
    // tenant-tree twin — same posture as `org_domains`/`org_join_requests`/
    // `org_join_request_digests` further down, so it is in
    // `check_schema_parity.sh`'s `CONTROL_TABLES` and deliberately not in
    // its `CONTROL_ONLY_RLS_TABLES`.
    //
    // Registered HERE, in the same PR as the migration, deliberately: 0238
    // and 0240 each enabled RLS without touching this roster and left this
    // assertion red on `main` for a day (see the `org_join_request_digests`
    // note below). There are THREE registration sites for an
    // `account_isolation` policy added by a later migration — this one,
    // `EXTRA_RLS_TABLES` in `scripts/check_rls_coverage.sh`, and
    // `scripts/rls_coverage.manifest` — and this is the one that gets
    // forgotten, because it is the only one that is a Rust test rather than
    // a shell list.
    "connector_health",
    "conversation_messages",
    "conversation_oceans",
    "conversation_surfaced_edges",
    "conversations",
    // 0150 — usage-metering credit ledger. `credit_balances`, `credit_ledger`,
    // `enforcement_events` and `grants` all anchor DIRECTLY on `account_id`
    // (the billing subject), same shape as `journal_rollups`/`extraction_cache`
    // rather than the ocean-anchored content tables. All four are control-only
    // — the ledger lives on the control plane by design — so they carry no
    // tenant-plane twin policy. `plans` is deliberately ABSENT from this
    // roster: it is global reference data with no `account_id` at all, the
    // same precedent as `model_rates`/`credit_conversion`.
    "credit_balances",
    "credit_ledger",
    "query_billing_state",
    "enforcement_events",
    // 0121 — content-hash extraction dedup cache (Billing Plan v2 lever six).
    // Anchored directly on `account_id`, same shape as `journal_rollups` — no
    // ocean anchor at all, so unlike most tables here it carries NO tenant-
    // plane twin policy (see `check_schema_parity.sh`'s CONTROL_ONLY_RLS_TABLES
    // comment): the tenant plane is deliberately DB-wide for this table.
    // 0126 — blank-verdict anchor for high-signal review's write gate.
    // Graph-keyed policy, same shape as relationship_edge's.
    "evaluation_ledger",
    "extraction_cache",
    // 0168 — billing self-serve launch PR B, per-grant seat-tier breakdown.
    // Anchored via a one-hop subquery policy back to its parent `grants` row
    // (`grant_id IN (SELECT id FROM grants WHERE account_id = ...)`) — the
    // same shape this migration's own header (0083) documents for every
    // table reached only through a foreign key rather than a direct
    // `account_id` column; see 0168's own header for the full reasoning
    // (an earlier version of that migration denormalized `account_id` onto
    // this table on the mistaken belief that a `USING` policy cannot join to
    // another table — corrected). Same control-only-no-tenant-twin shape as
    // the `credit_ledger` block below.
    "grant_seat_tiers",
    // 0150 — see the `credit_ledger` block above; sorts here, not there.
    "grants",
    "graph_subscriptions",
    "graphs",
    // (`google_watch_channels`'s roster entry lives further down this list,
    // next to `team_seat_grants` — see the comment there. CodeRabbit round 1
    // finding #6 on PR #686: this SAME entry used to also be declared HERE,
    // duplicated by two different PRs — gsuite G2.1 and m365 PR 4.1 — each
    // independently "fixing" the same 0180 schema-parity debt without
    // knowing the other already had. A duplicate string in this const Vec
    // makes `expected_tables` (after `.sort()`) one element longer than the
    // real, deduplicated `rls_tables` read back from `pg_class`, so the
    // roster's own `assert_eq!` failed before ever reaching a later
    // assertion — including, for this PR, the one this PR's own
    // `gmail_watched_threads` addition needed to prove.)
    // 0186 (gsuite Train G2 PR 3, renumbered from the originally-announced
    // 0185 after #683 merged holding that number first) — per-member Gmail
    // opt-in thread tracking, structural copy of
    // `outlook_watched_conversations`. Anchored DIRECTLY
    // on `account_id`, same shape as `outlook_watched_conversations`'s own
    // entry here.
    "gmail_watched_threads",
    // 0198 (gsuite Train G4 PR 3) — per-space Chat destination table.
    // Anchored on `graph_id`, same explicit-WITH-CHECK shape as
    // `slack_watched_channels`' own entry below (see that migration's
    // header — #692's USING-only finding is deliberately NOT repeated here).
    "google_chat_watched_spaces",
    // 0197 (gsuite enterprise train PR G2.4) — per-(account, owner) OPT-IN
    // Gmail everything-mode enablement, the SECOND implementation of the
    // carve-out `outlook_everything_mode` (0192) shipped first. Anchored
    // DIRECTLY on `account_id`, same shape.
    "gmail_everything_mode",
    // 0204/0205 (gsuite Train G3 PR 2) — Meet's pull-only sweep cursor
    // (`google_meet_sync_state`) and its per-conference dedup/observability
    // ledger (`google_meet_ingested_conferences`). Both anchored DIRECTLY on
    // `account_id`, same shape as `gmail_everything_mode`/
    // `google_watch_channels` above — neither carries a `graph_id`, since
    // the fan-out destination is resolved fresh per attendee at ingest time
    // rather than stored (see `connectors::google_meet`'s own module doc).
    "google_meet_sync_state",
    "google_meet_ingested_conferences",
    // 0269 (Meet per-meeting opt-in, #1137) — FORCE RLS, owner-keyed
    // (`account_id` AND `owner_user_id` from `app.user_id`), same shape as
    // `teams_meeting_captures` below. Tenant twin: tenant 0111.
    "google_meet_captures",
    // 0277 — Gmail trash/spam retraction ledger. Direct `account_id`
    // account_isolation. Tenant twin: tenant 0116.
    "gmail_hidden_messages",
    // 0258 — Google Chat in-space cold-start notice ledger. Direct
    // `account_id` account_isolation, control-only (no tenant twin).
    "chat_cold_start_notices",
    // 0113 — per-(user, ocean, hour) journal rollup. Anchored on `account_id`
    // directly, same shape as `activity_events`.
    "journal_rollups",
    // 0404 — the Today gist. Anchored on `account_id` directly, same shape
    // as `journal_rollups`. Tenant twin: tenant 0218.
    "journal_gists",
    "llm_usage",
    // 0149 — usage-metering PR4's reserve/finalize choke point. Anchored
    // DIRECTLY on `account_id`, same shape as `llm_usage`'s own policy on
    // this plane — a pending row is scoped identically to the real row it
    // either finalizes into or is swept away without becoming.
    "llm_usage_pending",
    // 0140 — MCP spec 2026-07-28 PR 2's task-based `ask_ocean`. Anchored
    // DIRECTLY on `account_id` (the caller's active account, not the ocean's
    // owning account — this table is id-anchored, not ocean-anchored), same
    // shape as `journal_rollups`/`extraction_cache`.
    "mcp_tasks",
    "mcp_tool_calls",
    // 0189 (m365 enterprise train PR 3.8) — per-member Microsoft Calendar
    // sync bookkeeping (the default-on enablement + per-member off switch).
    // Anchored DIRECTLY on `account_id`, same shape as `gmail_watched_threads`'s
    // own entry above.
    "microsoft_calendar_sync_state",
    // 0278 (Microsoft Calendar personal sweep): the sweep's rotation clock —
    // one `last_attempt_at` per account, stamped before every pass.
    // Anchored DIRECTLY on `account_id`, control-only, no tenant-tree twin
    // (so it is in `check_schema_parity.sh`'s `CONTROL_TABLES`, not its
    // `CONTROL_ONLY_RLS_TABLES`) — same posture as `connector_health`.
    "microsoft_calendar_sync_attempts",
    "meeting_shares",
    "meeting_transcripts",
    // 0125_membership_events — the team-roster audit trail. Carries an
    // `account_isolation` policy on this plane for the same reason
    // `team_members`/`team_seat_grants` do: it is team_id-anchored, and a
    // roster's history is exactly as account-private as the roster itself.
    // 0161 — billing Phase 2b per-member allowance buckets. Anchored directly
    // on `account_id` (the billing subject), exactly like its `credit_ledger`/
    // `credit_balances`/`grants` siblings above rather than the ocean-anchored
    // content tables, and control-only for the same reason — so it carries no
    // tenant-plane twin policy. `user_id` is a plain column here, not a policy
    // anchor: the policy isolates by ACCOUNT, and which member a row belongs
    // to is data the account is entitled to read.
    "member_allowance_periods",
    "membership_events",
    "message_feedback",
    "non_supersession_pair",
    "node_atoms",
    "node_merges",
    "node_promotions",
    "nodes",
    "oauth_tokens",
    "op_log",
    // 0242 (CS-443, org-onboarding plan §3 item 37, D11) — the org's claimed
    // and verified email domains. Anchored DIRECTLY on `account_id`, same
    // shape as `slot_reservations`/`org_join_requests` below it here.
    // Control-only, no tenant-tree twin (a claimed domain is bookkeeping
    // about an ORG, never ocean content, and the guest-rule/auto-add gates
    // that read it resolve before any tenant pool is chosen), so it is in
    // `check_schema_parity.sh`'s `CONTROL_TABLES` and deliberately NOT in
    // its `CONTROL_ONLY_RLS_TABLES` — see that list's own "which of these
    // two questions does it answer" note.
    "org_domains",
    // 0247 (CS-447, org-onboarding plan §3 item 9, D1/D11/D16) — the
    // off-by-default auto-add toggle and the tier it is allowed to spend.
    // Direct `account_id` (it IS the primary key here — one settings row
    // per org), control-only, same posture as `org_domains` above.
    "org_auto_add_settings",
    // 0247 (CS-447, D13 as amended 2026-09-22) — the ledger of members
    // added with no human in the loop, and the rung that admitted each.
    // An auto-added member never creates an `org_join_requests` row, so
    // this is the ONLY record of the add that the daily digest can report
    // from; an RLS gap here would leak "who did Acme auto-add" to any
    // other account. Direct `account_id`, control-only.
    "org_auto_adds",
    // 0240 (CS-438, D13) — the per-(account, admin) daily join-request
    // digest send ledger. Direct `account_id`, control-only, same posture.
    //
    // **Added by CS-443, late.** 0240 enabled RLS on this table without
    // adding it here, and 0238 did the same for `org_join_requests` below,
    // so this assertion — "exactly the documented roster, no more, no
    // less" — has been RED on `main` since CS-437 merged. It failed on the
    // real, live-Postgres roster, which is the gate working exactly as
    // designed; nobody ran it. Both are registered here now, alongside
    // this PR's own table, rather than leaving a gate red that CS-443's own
    // table would otherwise be the third entry missing from.
    "org_join_request_digests",
    // 0238 (CS-437, D3/D9/D10/D19) — the join-request queue. Direct
    // `account_id`, control-only; see the note on the line above.
    "org_join_requests",
    // 0382 (self-serve signup PR C′) — a team org's free-credit trial
    // lifecycle. Direct `account_id`, strict USING + WITH CHECK, control-only.
    // (Its sibling `org_trial_claims` is NOT RLS-enabled: the app role holds
    // no privilege on it at all and reaches it only through 0382's definers.)
    "org_trials",
    // 0257 (Phase G item 48) — the fleet-wide singleton "connect emails
    // began" clock. Deliberately NOT account-scoped: one row, no account
    // content, `FOR SELECT USING (true)` so the app role can read it (see
    // that migration's header). Control-only.
    "org_install_capture_grace",
    // 0192 (m365 enterprise train PR 3.4) — per-(account, owner) OPT-IN
    // everything-mode enablement (never default-on, the opposite carve-out
    // shape from `microsoft_calendar_sync_state` above). Anchored DIRECTLY on
    // `account_id`, same shape.
    "outlook_everything_mode",
    "outlook_watched_conversations",
    "outlook_workflows",
    "projects",
    // 0263 / 0266 (Outlook "save while scheduling", #1120) — per-intent
    // receipts and schedule intents. FORCE RLS, owner-keyed (`account_id`
    // AND `user_id` from `app.user_id`). Tenant twins: tenant 0107 / 0109.
    "outlook_capture_receipts",
    "outlook_schedule_intents",
    "queries",
    "query_history",
    // 0138 — slice 0 T2 identifier awareness (email/slack_user/git_author/
    // clerk_user identity signals for entity-resolution). Anchored on its one
    // `node_id` column via the owning graph, same shape as
    // `conflict_dismissal`'s `node_a_id` anchor.
    "reef_identifiers",
    // 0139 — judgement-graph resolver slice PR1 (schema + Store plumbing
    // only, no worker/route caller yet). Anchored DIRECTLY on `graph_id`
    // (not via a node-endpoint join): `node_a_id`/`node_b_id` are bare UUID
    // with no FK to `nodes` (see that migration's header), so a node-anchored
    // policy would hide a row the moment either endpoint is deleted.
    "reef_judgement",
    "relationship_edge",
    // 0196 (B2 unit 02, FM-5, docs/plans/capture/02-b2-retract-fixes.md):
    // the retraction audit log. Anchored on `account_id` (control-only, no
    // tenant twin — the deciding reason is D8's cross-ocean author view and
    // FM-4's `not_a_member` row needing to be writable for an ex-member, see
    // that migration's own header), but with a RICHER policy than the
    // plain `account_id = app.account_id` shape most of this roster uses:
    // author-across-every-account (`actor_user_id = app.user_id`) OR
    // admin-of-that-account via a one-hop `account_members` subquery, same
    // subquery-policy class as `grant_seat_tiers`/0168 above. The dedicated
    // filter proof (both branches, plus the REVOKE-not-CHECK append-only
    // half) lives in `backend/tests/b2_retract_fixes_pg.rs`'s
    // `fm5_probe_c_delete_as_app_role_is_refused_by_the_revoke` — this
    // roster entry only proves the policy EXISTS.
    "retraction_audit_log",
    "slack_huddle_captures",
    "slack_huddle_consents",
    "slack_integrations",
    "slack_watched_channels",
    // 0227 (chat-per-user plan PR 6, "email the unmatched") — the persisted,
    // per-recipient email cooldown that replaces the in-process per-container
    // one for this one send path. Anchored DIRECTLY on `account_id`, no
    // graph/ocean concept at all — same shape as `connector_audit_log`/
    // `extraction_cache` above, not the `graph_id`-anchored content tables.
    "slack_unmatched_email_notices",
    // 0233 (CS-435, slot pool, #1017) — per-account slot reservations with
    // their own later account_isolation policy. Anchored directly on
    // `account_id`, same shape as `slack_unmatched_email_notices` above. It
    // shipped RLS-enabled and was not added here, which is what this roster
    // assertion is for; registered while merging the GitHub summary tier.
    "slot_reservations",
    // 0235 (GitHub connector PR A) — per-repo watch/destination table,
    // same account_isolation-with-graph_id-WITH-CHECK shape as
    // slack_watched_channels above, NULL-friendly for its own
    // not-yet-routed pending-row state (see that migration's header).
    "github_watched_repos",
    // 0237 (GitHub connector PR B part 2, the summary tier) — per-group
    // package/directory/repo-rollup summary state. Same account_isolation-
    // with-graph_id-WITH-CHECK shape as github_watched_repos above, but
    // WITHOUT the NULL-friendly branch: this table's graph_id is NOT NULL
    // (see that migration's header).
    "github_code_groups",
    // 0213 (#871) — the standing publication CONSENT row and the per-atom
    // publication RECEIPTS underneath it. Both were shipped RLS-enabled with
    // a policy and neither was added to this roster, which is what made this
    // assertion the thing that caught them (#874's own CI, this fix's PR).
    //
    // `source_connections` is the direct-`account_id` shape, plus a second
    // conjunct the plain `account_isolation` tables do not have:
    // `source_graph_id IN (SELECT id FROM graphs) AND target_graph_id IN
    // (...)`, so on the tenant plane the same policy also inherits `graphs`'
    // `member_ocean_isolation`. The row names which source item in which
    // ocean flows into which shared ocean — one account's publication
    // topology — so this is a content table, not bookkeeping.
    //
    // `source_connection_atoms` carries no `account_id` of its own and
    // anchors through a one-hop subquery to its parent connection
    // (`connection_id IN (SELECT id FROM source_connections)`), the same
    // shape as `grant_seat_tiers`/0168 above.
    "source_connection_atoms",
    "source_connections",
    // 0111 — whole-source publication consent. Two-anchor (source OR target
    // graph's account) shape, same as `node_promotions`; see that migration's
    // header for the full RLS rationale.
    "source_publications",
    // 0162 — Stripe linkage + the webhook idempotency ledger (Phase 4a).
    // Both anchored directly on `account_id` like their billing siblings,
    // control-only, no tenant twin. `stripe_webhook_events.account_id` is
    // NOT NULL specifically so this policy can also serve as its WITH CHECK:
    // a nullable anchor could not be INSERTed under `coralswarm_app` at all.
    "stripe_subscriptions",
    "stripe_webhook_events",
    "supersession_edge",
    "team_keys",
    "team_member_key_envelopes",
    "team_members",
    // 0106 — auto-granted seat provenance. Anchored on `account_id`, with the
    // WITH CHECK additionally proving `team_id` belongs to that same account
    // (same cross-account-pointer class as 0083's `node_promotions` and
    // 0101's `slack_watched_channels`).
    // 0180/0181 (gsuite train G1.2, merged #674) enabled RLS +
    // `account_isolation` on `google_watch_channels` but never added it to
    // this roster — this suite has failed on main since that merge. Fixed
    // independently, in passing, by BOTH m365 PR 4.1 (this entry) and
    // gsuite G2.1 (a second entry that used to live up near
    // `graph_subscriptions`); the schema-parity strip-list half of the same
    // gap is fixed by gsuite G3.1/#677. The resulting DUPLICATE entry was
    // itself a live bug (CodeRabbit round 1 finding #6 on PR #686 — see the
    // note where the other one used to be) — deduplicated here, this one
    // entry kept.
    "google_watch_channels",
    // 0274 (Google Calendar personal sweep): the sweep's rotation clock —
    // one `last_attempt_at` per account, stamped before every pass.
    // Anchored DIRECTLY on `account_id`, control-only, no tenant-tree twin
    // (so it is in `check_schema_parity.sh`'s `CONTROL_TABLES`, not its
    // `CONTROL_ONLY_RLS_TABLES`) — same posture as `connector_health`.
    "google_calendar_sync_attempts",
    "team_seat_grants",
    // 0184 (m365 enterprise train PR 4.1): the durable founding marker —
    // same account_isolation shape (and team-ownership WITH CHECK) as
    // team_seat_grants/0106.
    "seat_grant_sources",
    "teams",
    "teams_watched_resources",
    "transcript_corrections",
    // 0260 (Teams per-meeting opt-in) — all three FORCE RLS.
    // `teams_meeting_installations`: account-keyed. `teams_meeting_captures`:
    // owner-keyed (`account_id` AND `user_id`), WITH CHECK pins `graph_id` to
    // the member's own unarchived ocean. `teams_meeting_contexts`:
    // system-actor only (`app.tenant_actor = 'system'`), control-only.
    // Tenant twins (installations, captures): tenant 0105.
    "teams_meeting_captures",
    "teams_meeting_contexts",
    "teams_meeting_installations",
    // 0191 (m365 enterprise train PR 4.3): the query-audit disclosure log.
    // Append-only (`account_isolation_select`/`account_isolation_insert`
    // only — see that migration's header) rather than the usual bare
    // `account_isolation` FOR ALL policy every other single-anchor table in
    // this roster carries; the roster/has_policy checks below don't care
    // about the split, only that SOME policy exists. See
    // `query_audit_log_rls_isolates_and_is_append_only` for the dedicated
    // filter + append-only proof this table's own PR adds (same precedent
    // as `grant_seat_tiers_rls_isolates_via_the_one_hop_grants_subquery`
    // below).
    "query_audit_log",
];

/// A password set ONLY on this test's ephemeral scratch database's cluster,
/// for the duration of the test — `coralswarm_app` is created with NO
/// password by the migration itself (see 0083's header), and this is not a
/// real credential (never used against a real deploy).
const APP_ROLE_TEST_PASSWORD: &str = "orgt_consumer_rls_test_only_pw";

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    retained_database::recreate(conn, db).await.unwrap();
}

/// A pool connected AS `coralswarm_app` (real login, not `SET ROLE`) against
/// `db_url`.
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app")
}

/// Seed a (user, account) pair and one row on `graphs`/`queries`/
/// `activity_events` (direct-`account_id` tables) PLUS `atoms` (a
/// `graph_id`-anchored table, the P1 fix's central case — atoms are read
/// directly by `graph_id`/`atom_id` all over the codebase, never joining
/// `graphs`) — enough breadth to prove the backstop isn't a `graphs`-only
/// fluke, without needing bespoke fixtures for every table in [`RLS_TABLES`].
async fn seed_account(owner: &PgPool, email: &str) -> (Uuid, Uuid, Uuid, Uuid) {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(owner)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(owner)
            .await
            .expect("insert account");
    let graph_id: Uuid =
        sqlx::query_scalar("INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id")
            .bind(account_id)
            .fetch_one(owner)
            .await
            .expect("insert graph");
    sqlx::query("INSERT INTO queries(account_id, user_id, text) VALUES ($1, $2, 'probe')")
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("insert query");
    sqlx::query("INSERT INTO activity_events(account_id, event_type) VALUES ($1, 'atom_created')")
        .bind(account_id)
        .execute(owner)
        .await
        .expect("insert activity event");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'probe-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .fetch_one(owner)
    .await
    .expect("insert atom");
    (user_id, account_id, graph_id, atom_id)
}

/// Must match `tests/support/rls_probe.rs`'s key — the lock is only useful if
/// every writer of the shared `coralswarm_app` role agrees on it.
const SETUP_ADVISORY_LOCK_KEY: i64 = 0x0083_5C09;

/// Create a fresh scratch DB, apply the control tree, and set the test-only
/// password on `coralswarm_app`, serialized cluster-wide.
///
/// `coralswarm_app` is a CLUSTER-level role shared by every scratch DB in this
/// file (and by every other `*_pg` test binary, which cargo runs in PARALLEL
/// processes), so both the migrate that creates it and the `ALTER ROLE` that
/// sets its password are shared-catalog DDL. Two running concurrently fail with
/// `XX000 tuple concurrently updated`. Same advisory lock, and same reasoning,
/// as `tests/support/rls_probe.rs`.
async fn scratch_db_with_app_role(base: &str, db: &str) -> PgPool {
    let mut admin_conn = admin(base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(base, db);

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("take setup advisory lock");

    let owner: PgPool = PgPoolOptions::new()
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(SETUP_ADVISORY_LOCK_KEY)
        .execute(&mut admin_conn)
        .await
        .expect("release setup advisory lock");

    owner
}

#[tokio::test]
async fn consumer_rls_enforces_under_app_role_and_stays_inert_under_owner_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping consumer_rls_enforces_under_app_role_and_stays_inert_under_owner_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls";
    let db_url = with_db(&base, db);

    // Applies the control tree (creates `coralswarm_app` + policies) and sets a
    // test-only password so this test can open a REAL separate connection AS
    // coralswarm_app (the same connection path OPS-4 will point the app at),
    // rather than `SET ROLE` inside the owner's own session.
    let owner = scratch_db_with_app_role(&base, db).await;

    // ── Seed two accounts with content on graphs/queries/activity_events/atoms ─
    let (_user_a, account_a, graph_a, atom_a) = seed_account(&owner, "rls-a@x.co").await;
    let (_user_b, account_b, graph_b, atom_b) = seed_account(&owner, "rls-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;

    // ── 1. Enforced under coralswarm_app: A sees only A, never B ───────────
    for (scope_account, expect_graph, other_graph, expect_atom, other_atom) in [
        (account_a, graph_a, graph_b, atom_a, atom_b),
        (account_b, graph_b, graph_a, atom_b, atom_a),
    ] {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(scope_account.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");

        let graph_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs ORDER BY id")
            .fetch_all(&mut *tx)
            .await
            .expect("select graphs as app role");
        assert_eq!(
            graph_ids,
            vec![expect_graph],
            "coralswarm_app scoped to {scope_account} must see exactly its own graph, \
             never {other_graph}"
        );

        let query_texts: Vec<String> = sqlx::query_scalar("SELECT text FROM queries")
            .fetch_all(&mut *tx)
            .await
            .expect("select queries as app role");
        assert_eq!(
            query_texts,
            vec!["probe".to_string()],
            "queries must be scoped to this account alone (one row, not both accounts')"
        );

        let event_count: i64 = sqlx::query_scalar("SELECT count(*) FROM activity_events")
            .fetch_one(&mut *tx)
            .await
            .expect("count activity_events as app role");
        assert_eq!(event_count, 1, "activity_events must be scoped too");

        // ── THE P1 PROOF: a direct `atoms` read by `atom_id`, never joining
        // `graphs` at all — exactly the classic IDOR pivot the security
        // review flagged (`SELECT ... FROM atoms WHERE atom_id = ANY($1)` in
        // `store/mod.rs`, which never mentions `graphs`). Both atom ids are
        // queried at once; only this account's own atom must come back.
        let visible_atoms: Vec<Uuid> = sqlx::query_scalar(
            "SELECT atom_id FROM atoms WHERE atom_id = ANY($1) ORDER BY atom_id",
        )
        .bind(vec![atom_a, atom_b])
        .fetch_all(&mut *tx)
        .await
        .expect("select atoms by atom_id as app role");
        assert_eq!(
            visible_atoms,
            vec![expect_atom],
            "a direct atoms-by-atom_id read (no join to graphs) must return only this \
             account's atom, never {other_atom}'s"
        );

        // ── Same proof, but the OTHER classic direct-access shape: filtering
        // by `graph_id` directly (`SELECT ... FROM atoms WHERE graph_id =
        // $1`), never mentioning `graphs` either. Querying the OTHER
        // account's graph_id directly must return ZERO rows.
        let cross_account_by_graph: Vec<Uuid> =
            sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id = $1")
                .bind(other_graph)
                .fetch_all(&mut *tx)
                .await
                .expect("select atoms by the OTHER account's graph_id as app role");
        assert!(
            cross_account_by_graph.is_empty(),
            "coralswarm_app scoped to {scope_account} querying atoms by {other_graph}'s \
             graph_id directly must return ZERO rows — got {cross_account_by_graph:?}"
        );

        tx.rollback().await.ok();
    }

    // ── 2. Strict predicate: unset app.account_id -> hard error, not empty ─
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        let result: Result<Vec<Uuid>, sqlx::Error> =
            sqlx::query_scalar("SELECT id FROM graphs").fetch_all(&mut *tx).await;
        assert!(
            result.is_err(),
            "querying an RLS'd table as coralswarm_app with app.account_id never set must \
             error (strict current_setting, no missing_ok default) — got {result:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 3. Out-of-scope control table (users) is NOT policy-restricted ─────
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let user_count: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
            .fetch_one(&mut *tx)
            .await
            .expect("users has no RLS policy — must be readable regardless of app.account_id");
        assert_eq!(user_count, 2, "users is out of this backstop's scope entirely");
        tx.rollback().await.ok();
    }

    // ── 4. Inert under the owner/BYPASSRLS role: sees both accounts always ──
    {
        let graph_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs ORDER BY id")
            .fetch_all(&owner)
            .await
            .expect("select graphs as owner");
        let mut expected = vec![graph_a, graph_b];
        expected.sort();
        let mut actual = graph_ids;
        actual.sort();
        assert_eq!(
            actual, expected,
            "the current owner/BYPASSRLS role must still see every account's rows — \
             migration 0083 must be a no-op for the app as deployed today"
        );

        let query_count: i64 = sqlx::query_scalar("SELECT count(*) FROM queries")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(query_count, 2, "owner role sees both accounts' queries, unaffected by RLS");

        // The atoms IDOR-pivot query, unaffected: the owner/BYPASSRLS role
        // reading cross-account by graph_id directly still sees the OTHER
        // account's atom — proving the RLS policy on `atoms` (2b) is equally
        // inert for the app's current connection.
        let cross_account_atoms: Vec<Uuid> =
            sqlx::query_scalar("SELECT atom_id FROM atoms WHERE graph_id = $1")
                .bind(graph_b)
                .fetch_all(&owner)
                .await
                .unwrap();
        assert_eq!(
            cross_account_atoms,
            vec![atom_b],
            "owner/BYPASSRLS role must still read atoms by graph_id across accounts, unaffected \
             by the new atoms policy"
        );
    }

    // ── 4b. node_promotions: WRITE proof (CodeRabbit MAJOR fix) ────────────
    // A bare `USING`-only policy is `FOR ALL` and reuses `USING` for
    // `WITH CHECK` too — for every table in this roster except
    // `node_promotions` that's correct (single-account predicate), but
    // `node_promotions`' original OR-across-two-
    // accounts predicate, reused as `WITH CHECK`, let account A insert a row
    // whose `target_graph_id` belonged to account B merely because A owned
    // the source side. Proves: A can insert/upsert a promotion referencing
    // ONLY its own graphs; A cannot insert one whose target belongs to B.
    {
        let (node_src_a, node_tgt_a): (Uuid, Uuid) = {
            let src: Uuid = sqlx::query_scalar(
                "INSERT INTO nodes(graph_id, type, title, body) \
                 VALUES ($1, 'entity', 'Source A', '{}') RETURNING node_id",
            )
            .bind(graph_a)
            .fetch_one(&owner)
            .await
            .expect("insert source node for A");
            let tgt: Uuid = sqlx::query_scalar(
                "INSERT INTO nodes(graph_id, type, title, body) \
                 VALUES ($1, 'entity', 'Target A', '{}') RETURNING node_id",
            )
            .bind(graph_a)
            .fetch_one(&owner)
            .await
            .expect("insert target node for A");
            (src, tgt)
        };

        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // Legit same-account promotion: A owns both the source reef's graph
        // and the target graph. Must succeed.
        let insert_own = sqlx::query(
            "INSERT INTO node_promotions(source_node_id, target_graph_id, target_node_id, from_version) \
             VALUES ($1, $2, $3, 1)",
        )
        .bind(node_src_a)
        .bind(graph_a)
        .bind(node_tgt_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a node_promotions row \
             referencing only A's own graphs — got {insert_own:?}"
        );

        // Cross-account write: A owns the source, but claims graph_b (B's
        // graph) as the target. Must be REJECTED — this is the exact bug
        // the review found (previously succeeded under the OR-reused-as-
        // WITH-CHECK bare policy).
        let insert_cross = sqlx::query(
            "INSERT INTO node_promotions(source_node_id, target_graph_id, target_node_id, from_version) \
             VALUES ($1, $2, $3, 1)",
        )
        .bind(node_src_a)
        .bind(graph_b)
        .bind(node_tgt_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a node_promotions row \
             whose target_graph_id belongs to account B, even though A owns the source \
             side — got {insert_cross:?}"
        );

        tx.rollback().await.ok();

        // node_promotions carries exactly 4 policies (select/insert/update/
        // delete), not one bare ALL policy — locks in the split design.
        let policy_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_policies WHERE schemaname='public' AND tablename='node_promotions'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            policy_count, 4,
            "node_promotions must carry 4 separate policies (select/insert/update/delete), \
             not a single bare ALL policy"
        );
    }

    // ── 4c. slack_watched_channels: cross-account graph_id WRITE proof ─────
    // Migration 0101's original policy was `USING (account_id = ...)` with NO
    // explicit `WITH CHECK` — a bare `USING`-only policy is `FOR ALL` and
    // reuses `USING` for `WITH CHECK` too, which checks `account_id` but says
    // nothing about `graph_id`'s ownership. `graph_id`'s FK
    // (`REFERENCES graphs(id) ON DELETE CASCADE`) carries no account
    // correlation at all, so account A could INSERT a row with its own
    // `account_id` but a `graph_id` belonging to account B — a real
    // cross-account write (this repo's SECOND occurrence of the same bug
    // class 0083's `node_promotions` fix closed — see this migration's own
    // header). Proves: A can insert a row referencing only its OWN graph; A
    // cannot insert one whose `graph_id` belongs to B.
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // Legit same-account row: A owns both account_id and graph_id.
        let insert_own = sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, $3, 'T-own', 'C-own')",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a slack_watched_channels row \
             referencing only A's own graph — got {insert_own:?}"
        );

        // Cross-account write: A's own account_id, but B's graph_id as the
        // destination. Must be REJECTED — this is the exact bug the review
        // found (previously succeeded under the bare USING-reused-as-
        // WITH-CHECK policy, and would have let `slack_watched_destination`
        // hand account B's graph back to A's message-ingest funnel).
        let insert_cross = sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, $3, 'T-cross', 'C-cross')",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(graph_b)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a slack_watched_channels row \
             whose graph_id belongs to account B, even though the row's own account_id is A's \
             — got {insert_cross:?}"
        );

        tx.rollback().await.ok();

        // Exactly one policy — this fix does NOT need node_promotions' 4-way
        // command split (there is only one READ anchor, account_id, direct
        // and unambiguous); a single FOR-ALL policy with a stricter
        // WITH CHECK than its USING is sufficient. Locks in that design so a
        // future edit doesn't silently "fix" this by copying the 4-policy
        // shape (which would also require updating
        // `scripts/check_schema_parity.sh`'s generic per-table DROP POLICY
        // handling).
        let policy_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_policies WHERE schemaname='public' AND tablename='slack_watched_channels'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            policy_count, 1,
            "slack_watched_channels must carry exactly 1 policy (account_isolation, FOR ALL \
             with an explicit WITH CHECK), not a node_promotions-style 4-way split"
        );
    }

    // ── 4d. slack_watched_channels: a 'fanout' row (NULL graph_id) is still
    // account-isolated — the companion to §4c above. `mode`/nullable
    // `graph_id` were added (migration `0101_slack_watched_channels`'s
    // "AMENDED BEFORE SHIPPING" header) specifically so a legitimate
    // 'fanout' pick — "everyone's individual oceans", docs/connectors/
    // slack.md §2 — is not rejected by the SAME `WITH CHECK` that blocks a
    // cross-account `graph_id` in §4c: a bare `graph_id IN (...)` is
    // NULL-unfriendly (`NULL IN (...)` is NULL, not TRUE), so without the
    // `graph_id IS NULL OR ...` arm every legitimate fanout insert would
    // have been rejected right alongside the illegitimate cross-account
    // ones §4c tests. Proves BOTH directions at once: A can write its own
    // 'fanout' row, and account isolation still holds when `graph_id` is
    // NULL — it is not merely the `graph_id` ownership check that was
    // carrying the whole isolation guarantee.
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // A legitimate 'fanout' pick: NULL graph_id, mode='fanout'. Must be
        // ACCEPTED under A's own account_id.
        let insert_fanout = sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'fanout', NULL, 'T-fanout', 'C-fanout')",
        )
        .bind(account_a)
        .bind(_user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_fanout.is_ok(),
            "coralswarm_app scoped to A must be able to write a legitimate 'fanout' row (NULL \
             graph_id) for its OWN account — got {insert_fanout:?}"
        );

        tx.commit().await.expect("commit A's fanout row");
    }
    {
        // Cross-account isolation still holds for that fanout row: B, scoped
        // to itself, must never see A's row.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        let visible: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM slack_watched_channels \
             WHERE workspace_id = 'T-fanout' AND channel_id = 'C-fanout'",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            visible, 0,
            "account B must NEVER see account A's 'fanout' row, NULL graph_id or not"
        );

        // B (scoped to itself) attempting to claim account A's account_id on
        // a NEW fanout row must still be refused — the NULL-friendly
        // `graph_id IS NULL` arm must not have loosened the account_id half
        // of the predicate.
        let insert_cross = sqlx::query(
            "INSERT INTO slack_watched_channels(account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'fanout', NULL, 'T-fanout-cross', 'C-fanout-cross')",
        )
        .bind(account_a)
        .bind(_user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross.is_err(),
            "coralswarm_app scoped to B must NOT be able to write a 'fanout' row claiming \
             account A's account_id — got {insert_cross:?}"
        );

        tx.rollback().await.ok();
    }

    // ── 4c2/4d2. google_chat_watched_spaces: the same cross-account graph_id
    // WRITE proof as slack_watched_channels' §4c/§4d, shipped from THIS
    // table's FIRST migration rather than as a later fix (gsuite Train G4 PR
    // 3, design doc §1.8: `slack_watched_channels`' stricter-WITH-CHECK-
    // than-USING shape is the model, not `gmail_watched_threads`' USING-only
    // gap). Also proves the THIRD ("unrouted", NULL `mode`/`graph_id`) state
    // this table adds beyond slack_watched_channels' two — see migration
    // 0198's header — is equally NULL-friendly under the WITH CHECK.
    // Each insert runs in its OWN transaction — a Postgres transaction is
    // poisoned (every subsequent statement errors "current transaction is
    // aborted") the moment one statement inside it fails, and the
    // `insert_cross` case below is EXPECTED to fail, so it cannot share a
    // transaction with the assertions that follow it.
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // Legit same-account row: A owns both account_id and graph_id.
        let insert_own = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, graph_id, space_id, space_type, mode) \
             VALUES ($1, $2, 'spaces/own', 'SPACE', 'single')",
        )
        .bind(account_a)
        .bind(graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a google_chat_watched_spaces row \
             referencing only A's own graph — got {insert_own:?}"
        );
        tx.commit().await.expect("commit A's own-graph row");
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // Cross-account write: A's own account_id, but B's graph_id as the
        // destination. Must be REJECTED.
        let insert_cross = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, graph_id, space_id, space_type, mode) \
             VALUES ($1, $2, 'spaces/cross', 'SPACE', 'single')",
        )
        .bind(account_a)
        .bind(graph_b)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a google_chat_watched_spaces \
             row whose graph_id belongs to account B — got {insert_cross:?}"
        );
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // The "unrouted" state (NULL mode, NULL graph_id — a space
        // ADDED_TO_SPACE has created a row for but nobody has picked a
        // destination for yet) must be accepted under A's own account_id:
        // the NULL-friendly WITH CHECK arm must cover this state too, not
        // only 'fanout'.
        let insert_unrouted = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, graph_id, space_id, space_type, mode) \
             VALUES ($1, NULL, 'spaces/unrouted', 'SPACE', NULL)",
        )
        .bind(account_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_unrouted.is_ok(),
            "coralswarm_app scoped to A must be able to write an 'unrouted' \
             google_chat_watched_spaces row (NULL mode, NULL graph_id) for its OWN account \
             — got {insert_unrouted:?}"
        );
        tx.rollback().await.ok();
    }

    // Exactly one policy — same single-FOR-ALL-with-stricter-WITH-CHECK
    // shape as slack_watched_channels, not node_promotions' 4-way split.
    {
        let policy_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_policies WHERE schemaname='public' AND tablename='google_chat_watched_spaces'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            policy_count, 1,
            "google_chat_watched_spaces must carry exactly 1 policy (account_isolation, FOR ALL \
             with an explicit WITH CHECK)"
        );
    }

    // ── 4c3/4d3. google_chat_watched_spaces: the MAILBOX archetype (G4.5) —
    // owner-scoped opt-in rows, extending §4c2/§4d2's proof to the shape
    // those rows never covered (every row inserted above left `owner_user_id`
    // NULL — the container archetype). `docs/connectors/google-workspace.md`
    // §9: a mailbox row is written by `connectors::google_chat::
    // record_mailbox_optin`, always `mode = 'single'`, `owner_user_id IS NOT
    // NULL`, `graph_id` the opting-in member's own private ocean. Consumer-
    // plane RLS here is ACCOUNT-scoped only (0198's `account_isolation`
    // policy has no `owner_user_id` predicate at all — true per-owner
    // isolation is the TENANT-plane's `member_ocean_isolation` job, proven
    // separately in `tenant_rls_pg.rs`'s
    // `google_chat_mailbox_optin_is_visible_only_to_its_own_owner_pg`), so
    // what this block proves is the property THIS plane is actually
    // responsible for: cross-ACCOUNT isolation of a mailbox row, and the
    // archetype invariant holding under the real `coralswarm_app` role (not
    // merely the schema-owner role every other pg test in this tree runs
    // as by default).
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // A's own mailbox opt-in: her own account, her own graph, her own
        // owner_user_id. The legal shape.
        let insert_own_mailbox = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/mailbox-own', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(account_a)
        .bind(graph_a)
        .bind(_user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own_mailbox.is_ok(),
            "coralswarm_app scoped to A must be able to write A's own mailbox opt-in — got \
             {insert_own_mailbox:?}"
        );
        tx.commit().await.expect("commit A's own mailbox row");
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        // Cross-account write: A's own account_id, but B's graph as the
        // destination — must be REJECTED by the SAME graph_id-ownership
        // WITH CHECK the container archetype's cross-account test already
        // covers, now exercised with owner_user_id populated too.
        let insert_cross_mailbox = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/mailbox-cross', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(account_a)
        .bind(graph_b)
        .bind(_user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_mailbox.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a mailbox row whose graph_id \
             belongs to account B — got {insert_cross_mailbox:?}"
        );
        tx.rollback().await.ok();
    }
    {
        // Cross-account READ: B's session must never see A's mailbox opt-in
        // — same account-scoped `USING` clause the container archetype's
        // rows already rely on, proven here against a row that actually
        // carries an owner_user_id.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-own'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(visible, 0, "account B must never see account A's mailbox opt-in");
        tx.rollback().await.ok();
    }
    {
        // "No fan-out under any circumstance" — the archetype CHECK
        // (migration 0198) makes a mailbox row's `mode = 'fanout'`
        // unwritable REGARDLESS of caller, but pinned here under the real,
        // least-privilege `coralswarm_app` role rather than only the
        // schema-owner role, so a future RLS/grant change cannot
        // accidentally route around the constraint for the role that
        // actually serves production traffic.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let fanout_attempt = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/mailbox-no-fanout', 'GROUP_CHAT', 'fanout', NULL, $2, $2)",
        )
        .bind(account_a)
        .bind(_user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            fanout_attempt.is_err(),
            "a mailbox row must be unwritable as 'fanout' even under the real coralswarm_app \
             role — got {fanout_attempt:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 4e. membership_events: isolation on REAL rows, not just metadata ───
    // CodeRabbit gap: §5 below (and the roster loop it drives) only proves
    // RLS is ENABLED and a policy EXISTS on `membership_events` — it never
    // proves the policy actually isolates a real row. Seed one team +
    // `membership_events` row per account and prove `coralswarm_app`, scoped
    // to each account in turn via the same role-switching convention as §1
    // above, sees only its own account's audit row — never the other
    // account's — both by a plain scoped SELECT and by the direct-by-id shape
    // that mirrors this file's own atoms IDOR-pivot proof.
    {
        let team_a: Uuid = sqlx::query_scalar(
            "INSERT INTO teams(account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'Team A', $3) RETURNING id",
        )
        .bind(account_a)
        .bind(graph_a)
        .bind(_user_a)
        .fetch_one(&owner)
        .await
        .expect("insert team for account A");
        let team_b: Uuid = sqlx::query_scalar(
            "INSERT INTO teams(account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'Team B', $3) RETURNING id",
        )
        .bind(account_b)
        .bind(graph_b)
        .bind(_user_b)
        .fetch_one(&owner)
        .await
        .expect("insert team for account B");

        let event_a: Uuid = sqlx::query_scalar(
            "INSERT INTO membership_events(team_id, actor_user_id, target_user_id, action, new_role) \
             VALUES ($1, $2, $2, 'created', 'admin') RETURNING id",
        )
        .bind(team_a)
        .bind(_user_a)
        .fetch_one(&owner)
        .await
        .expect("insert membership_events row for account A's team");
        let event_b: Uuid = sqlx::query_scalar(
            "INSERT INTO membership_events(team_id, actor_user_id, target_user_id, action, new_role) \
             VALUES ($1, $2, $2, 'created', 'admin') RETURNING id",
        )
        .bind(team_b)
        .bind(_user_b)
        .fetch_one(&owner)
        .await
        .expect("insert membership_events row for account B's team");

        for (scope_account, expect_event, other_event) in
            [(account_a, event_a, event_b), (account_b, event_b, event_a)]
        {
            let mut tx = app_pool.begin().await.expect("begin as app role");
            sqlx::query("SELECT set_config('app.account_id', $1, true)")
                .bind(scope_account.to_string())
                .execute(&mut *tx)
                .await
                .expect("set_config app.account_id");

            let visible: Vec<Uuid> =
                sqlx::query_scalar("SELECT id FROM membership_events ORDER BY id")
                    .fetch_all(&mut *tx)
                    .await
                    .expect("select membership_events as app role");
            assert_eq!(
                visible,
                vec![expect_event],
                "coralswarm_app scoped to {scope_account} must see exactly its own account's \
                 membership_events row, never {other_event}'s"
            );

            // The direct-by-id shape too, mirroring the atoms IDOR-pivot proof
            // in §1: querying BOTH ids at once must return only this
            // account's own row.
            let by_id: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM membership_events WHERE id = ANY($1) ORDER BY id",
            )
            .bind(vec![event_a, event_b])
            .fetch_all(&mut *tx)
            .await
            .expect("select membership_events by id as app role");
            assert_eq!(
                by_id,
                vec![expect_event],
                "a direct membership_events-by-id read must return only this account's own row, \
                 never {other_event}'s"
            );

            tx.rollback().await.ok();
        }
    }

    // ── 4f. reef_judgement: isolation on a REAL row (review round 1, finding
    // 7 — the roster-only entry §5's loop adds is a set-membership/policy-
    // existence proof, same gap CodeRabbit's finding for `membership_events`
    // above closed: it never proves the policy isolates a real row). Seed one
    // judgement row per account, anchored DIRECTLY on `graph_a`/`graph_b`
    // (this table's endpoints carry no FK — see migration 0139's header — so,
    // unlike `membership_events`, no `teams`/node fixture is needed first;
    // any two `Uuid::new_v4()` node ids satisfy `reef_judgement_ordered_pair`).
    {
        let (rj_node_a1, rj_node_a2) = {
            let mut ids = [Uuid::new_v4(), Uuid::new_v4()];
            ids.sort();
            (ids[0], ids[1])
        };
        let (rj_node_b1, rj_node_b2) = {
            let mut ids = [Uuid::new_v4(), Uuid::new_v4()];
            ids.sort();
            (ids[0], ids[1])
        };
        let judgement_a: Uuid = sqlx::query_scalar(
            "INSERT INTO reef_judgement(graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'positive', 'test') RETURNING judgement_id",
        )
        .bind(graph_a)
        .bind(rj_node_a1)
        .bind(rj_node_a2)
        .fetch_one(&owner)
        .await
        .expect("insert reef_judgement row for account A's graph");
        let judgement_b: Uuid = sqlx::query_scalar(
            "INSERT INTO reef_judgement(graph_id, node_a_id, node_b_id, verdict, source) \
             VALUES ($1, $2, $3, 'positive', 'test') RETURNING judgement_id",
        )
        .bind(graph_b)
        .bind(rj_node_b1)
        .bind(rj_node_b2)
        .fetch_one(&owner)
        .await
        .expect("insert reef_judgement row for account B's graph");

        for (scope_account, expect_judgement, other_judgement) in
            [(account_a, judgement_a, judgement_b), (account_b, judgement_b, judgement_a)]
        {
            let mut tx = app_pool.begin().await.expect("begin as app role");
            sqlx::query("SELECT set_config('app.account_id', $1, true)")
                .bind(scope_account.to_string())
                .execute(&mut *tx)
                .await
                .expect("set_config app.account_id");

            let visible: Vec<Uuid> =
                sqlx::query_scalar("SELECT judgement_id FROM reef_judgement ORDER BY judgement_id")
                    .fetch_all(&mut *tx)
                    .await
                    .expect("select reef_judgement as app role");
            assert_eq!(
                visible,
                vec![expect_judgement],
                "coralswarm_app scoped to {scope_account} must see exactly its own account's \
                 reef_judgement row, never {other_judgement}'s"
            );

            // The direct-by-id shape too, mirroring the atoms IDOR-pivot proof
            // in §1 and membership_events' own proof in §4e: querying BOTH ids
            // at once must return only this account's own row.
            let by_id: Vec<Uuid> = sqlx::query_scalar(
                "SELECT judgement_id FROM reef_judgement WHERE judgement_id = ANY($1) \
                 ORDER BY judgement_id",
            )
            .bind(vec![judgement_a, judgement_b])
            .fetch_all(&mut *tx)
            .await
            .expect("select reef_judgement by id as app role");
            assert_eq!(
                by_id,
                vec![expect_judgement],
                "a direct reef_judgement-by-id read must return only this account's own row, \
                 never {other_judgement}'s"
            );

            tx.rollback().await.ok();
        }
    }

    // ── 4g. microsoft_calendar_sync_state: (account_id, owner_user_id,
    // graph_id) WRITE proof (CodeRabbit round 1, finding 1 on PR #692). Same
    // bug class §4b/§4c above already closed for `node_promotions`/
    // `slack_watched_channels`: the original policy carried only
    // `USING (account_id = ...)`, and a bare USING-only policy is FOR ALL
    // and reuses USING as WITH CHECK too — which checks `account_id` but
    // says nothing about whether `graph_id` is genuinely `owner_user_id`'s
    // own member ocean. Proves: A can insert a row pairing its own member
    // ocean with its own real owner; A cannot insert a row naming a
    // DIFFERENT account's `graph_id` (cross-account), and cannot insert a
    // row naming ITS OWN `graph_id` but a DIFFERENT `owner_user_id`
    // (cross-owner, within the SAME account).
    {
        let member_graph_a: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'A private reef', 'member', $2) RETURNING id",
        )
        .bind(account_a)
        .bind(_user_a)
        .fetch_one(&owner)
        .await
        .expect("insert A's member ocean");
        let member_graph_b: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'B private reef', 'member', $2) RETURNING id",
        )
        .bind(account_b)
        .bind(_user_b)
        .fetch_one(&owner)
        .await
        .expect("insert B's member ocean");
        // A second member on account A, to prove the same-account
        // cross-owner case is caught, not just the cross-account case.
        let user_a2: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind("rls-a2@x.co")
            .fetch_one(&owner)
            .await
            .expect("insert second user for account A");

        // Legit: A's own member ocean paired with that ocean's real owner.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a microsoft_calendar_sync_state \
             row pairing its own owner_user_id with that owner's real member ocean — \
             got {insert_own:?}"
        );
        tx.rollback().await.ok();

        // Cross-account: A's own account_id, but B's graph_id. Must be
        // REJECTED — this is the exact bug class the review found.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_account = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_b)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_account.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a microsoft_calendar_sync_state \
             row whose graph_id belongs to account B — got {insert_cross_account:?}"
        );
        tx.rollback().await.ok();

        // Cross-owner within the SAME account: account_id and graph_id are
        // both genuinely A's own, but owner_user_id names a DIFFERENT user
        // than that ocean's real owner. Must be REJECTED.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(user_a2)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.is_err(),
            "coralswarm_app scoped to A must NOT be able to pair a DIFFERENT owner_user_id with \
             A's own member ocean graph_id — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();

        // Personal-plane arm (migration 0272, fix/cal-deletions): the
        // account-wide Microsoft Calendar connection keeps its
        // `calendarView/delta` cursor on this same table, keyed on the
        // ACCOUNT OWNER and the account's primary personal ocean. Proves:
        // A's owner + A's primary personal ocean is accepted; the same
        // ocean paired with any other user (even a real user) is rejected
        // — the cross-owner guarantee above holds on this arm too.
        let personal_graph_a: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'A primary', 'personal', true) RETURNING id",
        )
        .bind(account_a)
        .fetch_one(&owner)
        .await
        .expect("insert A's primary personal ocean");

        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_personal_owner = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id, delta_link) \
             VALUES ($1, $2, $3, 'https://graph.microsoft.com/v1.0/me/calendarView/delta?$deltatoken=x')",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(personal_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_personal_owner.is_ok(),
            "coralswarm_app scoped to A must be able to write the personal-plane cursor row pairing \
             A's owner with A's primary personal ocean — got {insert_personal_owner:?}"
        );
        tx.rollback().await.ok();

        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_personal_other_user = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(user_a2)
        .bind(personal_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_personal_other_user.is_err(),
            "coralswarm_app scoped to A must NOT be able to pair A's primary personal ocean with a \
             user who is not the account owner — got {insert_personal_other_user:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 4h. outlook_everything_mode: (account_id, owner_user_id, graph_id)
    // WRITE proof (migration 0192, m365 enterprise train PR 3.4) — the SAME
    // bug class §4g above closes for `microsoft_calendar_sync_state`, on
    // this table's own `WITH CHECK`. Proves: A can insert a row pairing its
    // own member ocean with its own real owner; A cannot insert a row naming
    // a DIFFERENT account's `graph_id` (cross-account), and cannot insert a
    // row naming ITS OWN `graph_id` but a DIFFERENT `owner_user_id`
    // (cross-owner, within the SAME account) — own fresh graphs/user, since
    // §4g's own `member_graph_a`/`member_graph_b`/`user_a2` locals are scoped
    // to that block and already out of scope here.
    {
        let member_graph_a: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'A private reef (4h)', 'member', $2) RETURNING id",
        )
        .bind(account_a)
        .bind(_user_a)
        .fetch_one(&owner)
        .await
        .expect("insert A's member ocean");
        let member_graph_b: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'B private reef (4h)', 'member', $2) RETURNING id",
        )
        .bind(account_b)
        .bind(_user_b)
        .fetch_one(&owner)
        .await
        .expect("insert B's member ocean");
        let user_a2: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind("rls-a2-4h@x.co")
            .fetch_one(&owner)
            .await
            .expect("insert second user for account A");

        // Legit: A's own member ocean paired with that ocean's real owner.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write an outlook_everything_mode row \
             pairing its own owner_user_id with that owner's real member ocean — got {insert_own:?}"
        );
        tx.rollback().await.ok();

        // Cross-account: A's own account_id, but B's graph_id. Must be
        // REJECTED.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_account = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_b)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_account.is_err(),
            "coralswarm_app scoped to A must NOT be able to write an outlook_everything_mode row \
             whose graph_id belongs to account B — got {insert_cross_account:?}"
        );
        tx.rollback().await.ok();

        // Cross-owner within the SAME account: account_id and graph_id are
        // both genuinely A's own, but owner_user_id names a DIFFERENT user
        // than that ocean's real owner. Must be REJECTED.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_a)
        .bind(user_a2)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.is_err(),
            "coralswarm_app scoped to A must NOT be able to pair a DIFFERENT owner_user_id with \
             A's own member ocean graph_id — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 4i. gmail_everything_mode: (account_id, owner_user_id, graph_id)
    // WRITE proof (migration 0197, gsuite enterprise train PR G2.4) — the
    // SAME bug class §4h above closes for `outlook_everything_mode`, on this
    // table's own `WITH CHECK` (this is the SECOND implementation of the
    // same carve-out; §4h's own precedent is what this block mirrors). Own
    // fresh graphs/user, since §4h's own locals are scoped to that block and
    // already out of scope here.
    //
    // Every probe below explicitly INSERTs `enabled = TRUE` (unlike §4h's
    // own, which relies on the column default) — 0197's `WITH CHECK` (fixed
    // during adversarial review, see that migration's own header, "WITH
    // CHECK") gates its graph-ownership validation on `NOT enabled OR
    // EXISTS (...)`, specifically so a DISABLE never needs to re-validate a
    // graph it isn't touching. Omitting `enabled` here would default to
    // FALSE and every one of these negative probes would trivially succeed
    // (the check never even runs), proving nothing.
    {
        let member_graph_a: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'A private reef (4i)', 'member', $2) RETURNING id",
        )
        .bind(account_a)
        .bind(_user_a)
        .fetch_one(&owner)
        .await
        .expect("insert A's member ocean");
        let member_graph_b: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'B private reef (4i)', 'member', $2) RETURNING id",
        )
        .bind(account_b)
        .bind(_user_b)
        .fetch_one(&owner)
        .await
        .expect("insert B's member ocean");
        let user_a2: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind("rls-a2-4i@x.co")
            .fetch_one(&owner)
            .await
            .expect("insert second user for account A");

        // Legit: A's own member ocean paired with that ocean's real owner.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a gmail_everything_mode row \
             pairing its own owner_user_id with that owner's real member ocean — got {insert_own:?}"
        );
        tx.rollback().await.ok();

        // Cross-account: A's own account_id, but B's graph_id. Must be
        // REJECTED.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_account = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_a)
        .bind(_user_a)
        .bind(member_graph_b)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_account.is_err(),
            "coralswarm_app scoped to A must NOT be able to write a gmail_everything_mode row \
             whose graph_id belongs to account B — got {insert_cross_account:?}"
        );
        tx.rollback().await.ok();

        // Cross-owner within the SAME account: account_id and graph_id are
        // both genuinely A's own, but owner_user_id names a DIFFERENT user
        // than that ocean's real owner. Must be REJECTED.
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_a)
        .bind(user_a2)
        .bind(member_graph_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.is_err(),
            "coralswarm_app scoped to A must NOT be able to pair a DIFFERENT owner_user_id with \
             A's own member ocean graph_id — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 5. Exactly the documented RLS table roster carries RLS + a policy ──
    let rls_tables: Vec<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class \
         WHERE relnamespace = 'public'::regnamespace AND relrowsecurity ORDER BY relname",
    )
    .fetch_all(&owner)
    .await
    .expect("list RLS-enabled tables");
    let mut expected_tables: Vec<String> = RLS_TABLES.iter().map(|s| s.to_string()).collect();
    expected_tables.sort();
    assert_eq!(
        rls_tables, expected_tables,
        "the control tree (0083 plus every later account_isolation-adding migration, e.g. 0101) \
         must enable RLS on exactly the documented table roster — no more, no less"
    );
    for t in RLS_TABLES {
        // 0387 deliberately denies all app access to the operator-only singleton.
        if *t == "org_deletion_policy" {
            let accessible: bool = sqlx::query_scalar("SELECT has_table_privilege('coralswarm_app', 'org_deletion_policy', 'SELECT,INSERT,UPDATE,DELETE')")
                .fetch_one(&owner).await.unwrap();
            assert!(!accessible, "operator deletion policy must deny app access");
            continue;
        }
        let has_policy: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=$1)",
        )
        .bind(t)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(has_policy, "{t} has RLS enabled but no policy — every RLS'd table needs one");
    }
    // The control twins of the five tenant tables 0119_tenant_no_force_rls
    // un-FORCEs must keep FORCE here. `check_schema_parity.sh` strips FORCE
    // from exactly these five on the control side
    // (`CONTROL_ONLY_FORCE_RLS_TABLES`), so a control-side drop would be
    // invisible to that diff. This assertion is what catches it.
    for t in [
        "google_meet_captures",
        "outlook_capture_receipts",
        "outlook_schedule_intents",
        "teams_meeting_captures",
        "teams_meeting_installations",
    ] {
        let forced: bool = sqlx::query_scalar(
            "SELECT relforcerowsecurity FROM pg_class \
             WHERE relnamespace = 'public'::regnamespace AND relname = $1",
        )
        .bind(t)
        .fetch_one(&owner)
        .await
        .unwrap_or_else(|e| panic!("read relforcerowsecurity for {t}: {e}"));
        assert!(forced, "{t} must keep FORCE ROW LEVEL SECURITY on the control plane");
    }

    app_pool.close().await;
    owner.close().await;
    // Fresh admin connection: setup's own was consumed by
    // `scratch_db_with_app_role` (it holds the advisory lock for the duration).
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// **Billing self-serve launch PR B, migration `0168_grant_seat_tiers.sql`.**
/// A dedicated cross-account isolation proof for `grant_seat_tiers`, the one
/// new RLS-covered table this PR's schema remodel adds. The generic roster
/// check in [`consumer_rls_enforces_under_app_role_and_stays_inert_under_owner_role`]
/// (its step 5, "exactly the documented RLS table roster carries RLS + a
/// policy") already proves this table's policy EXISTS; this test proves the
/// policy actually FILTERS — the same bar every bespoke table in that big
/// test clears for its own table, applied here to the one this PR adds.
///
/// `grant_seat_tiers` has no `account_id` column of its own in the sense
/// every OTHER table in that big test does — its policy reaches `grants
/// .account_id` through a one-hop subquery instead (see `0168`'s header) —
/// this test is also the proof that the subquery policy actually FILTERS
/// through that join, not merely parses.
///
/// Own scratch DB (`orgt_consumer_rls_grant_seat_tiers`), separate from the
/// shared `orgt_consumer_rls` db the big test above uses, so the two never
/// contend for the same database name if a future run parallelizes within
/// this binary.
#[tokio::test]
async fn grant_seat_tiers_rls_isolates_via_the_one_hop_grants_subquery() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping grant_seat_tiers_rls_isolates_via_the_one_hop_grants_subquery: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_grant_seat_tiers";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    // ── Seed two team-org accounts, each with a `team` grant and a
    // `grant_seat_tiers` row ─────────────────────────────────────────────────
    async fn seed_team_grant(owner: &PgPool, email: &str) -> (Uuid, Uuid) {
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
                .bind(email)
                .fetch_one(owner)
                .await
                .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(owner)
        .await
        .expect("insert org account");
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(owner)
            .await
            .expect("team plan must be seeded (migration 0167)");
        let mut financial = begin_account_scoped(owner, account_id).await.unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        let grant_id: Uuid = sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
             VALUES ($1, $2, true, 3) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&mut *financial)
        .await
        .expect("insert team grant");
        let seat_tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
            .fetch_one(owner)
            .await
            .expect("standard seat tier must be seeded (migration 0165)");
        sqlx::query(
            "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
             VALUES ($1, $2, 3)",
        )
        .bind(grant_id)
        .bind(seat_tier_id)
        .execute(&mut *financial)
        .await
        .expect("insert grant_seat_tiers row");
        financial.commit().await.unwrap();
        (account_id, grant_id)
    }

    let (account_a, grant_a) = seed_team_grant(&owner, "gst-rls-a@x.co").await;
    let (account_b, grant_b) = seed_team_grant(&owner, "gst-rls-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;

    // ── 1. Enforced under coralswarm_app: A sees only its own row ──────────
    for (scope_account, expect_grant, other_grant) in
        [(account_a, grant_a, grant_b), (account_b, grant_b, grant_a)]
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(scope_account.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");

        let visible_grant_ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT grant_id FROM grant_seat_tiers ORDER BY grant_id")
                .fetch_all(&mut *tx)
                .await
                .expect("select grant_seat_tiers as app role");
        assert_eq!(
            visible_grant_ids,
            vec![expect_grant],
            "coralswarm_app scoped to {scope_account} must see exactly its own \
             grant_seat_tiers row, never {other_grant}'s"
        );

        // Direct-by-grant_id read of the OTHER account's row, never touching
        // `grants` — the same IDOR-pivot shape the big RLS test's `atoms`
        // proof exercises, applied to this table.
        let cross_account: Vec<Uuid> =
            sqlx::query_scalar("SELECT grant_id FROM grant_seat_tiers WHERE grant_id = $1")
                .bind(other_grant)
                .fetch_all(&mut *tx)
                .await
                .expect("select grant_seat_tiers by the OTHER account's grant_id as app role");
        assert!(
            cross_account.is_empty(),
            "coralswarm_app scoped to {scope_account} querying grant_seat_tiers by \
             {other_grant}'s grant_id directly must return ZERO rows — got {cross_account:?}"
        );

        tx.rollback().await.ok();
    }

    // ── 2. Strict predicate: unset app.account_id -> hard error ────────────
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        let result: Result<Vec<Uuid>, sqlx::Error> =
            sqlx::query_scalar("SELECT grant_id FROM grant_seat_tiers").fetch_all(&mut *tx).await;
        assert!(
            result.is_err(),
            "querying grant_seat_tiers as coralswarm_app with app.account_id never set must \
             error, not return rows or an empty result — got {result:?}"
        );
        tx.rollback().await.ok();
    }

    {
        let premium_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium'")
                .fetch_one(&owner)
                .await
                .expect("premium seat tier must be seeded (migration 0165)");

        let seats_before: Vec<(Uuid, Uuid, i32)> = sqlx::query_as(
            "SELECT grant_id, seat_tier_id, seats FROM grant_seat_tiers ORDER BY grant_id, seat_tier_id",
        )
        .fetch_all(&owner)
        .await
        .expect("snapshot grant seat rows before the foreign write");

        let mut tx = begin_account_scoped(&app_pool, account_a).await.expect("begin as app role");
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_a).await.unwrap();

        // Legit same-account write: A's own grant, a tier not already
        // claimed by the seed row (grant_a already has a `standard` row
        // from `seed_team_grant` — PRIMARY KEY (grant_id, seat_tier_id)
        // would collide on a repeat of that same tier).
        let insert_own = sqlx::query(
            "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)",
        )
        .bind(grant_a)
        .bind(premium_id)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_app scoped to A must be able to write a grant_seat_tiers row for \
             its OWN grant — got {insert_own:?}"
        );

        let insert_cross = sqlx::query(
            "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)",
        )
        .bind(grant_b)
        .bind(premium_id)
        .execute(&mut *tx)
        .await;
        let error = insert_cross.expect_err("A's financial entry must refuse B's grant seat write");
        let database_error = error.as_database_error().expect("database must refuse the write");
        assert_eq!(database_error.code().as_deref(), Some("P0001"));
        assert_eq!(
            database_error.message(),
            "grant seat requires its actual parent",
            "the financial seat guard must refuse a parent hidden by account scope"
        );

        tx.rollback().await.expect("roll back the refused foreign write");
        let seats_after: Vec<(Uuid, Uuid, i32)> = sqlx::query_as(
            "SELECT grant_id, seat_tier_id, seats FROM grant_seat_tiers ORDER BY grant_id, seat_tier_id",
        )
        .fetch_all(&owner)
        .await
        .expect("read grant seat rows after the foreign write");
        assert_eq!(
            seats_after, seats_before,
            "the refused foreign write must leave all seat rows unchanged"
        );
    }

    // ── 4. Inert under the owner/BYPASSRLS role: sees both accounts' rows ──
    {
        let grant_ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT grant_id FROM grant_seat_tiers ORDER BY grant_id")
                .fetch_all(&owner)
                .await
                .expect("select grant_seat_tiers as owner");
        let mut expected = vec![grant_a, grant_b];
        expected.sort();
        let mut actual = grant_ids;
        actual.sort();
        assert_eq!(
            actual, expected,
            "the owner/BYPASSRLS role must still see every account's grant_seat_tiers rows"
        );
    }

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// **m365 enterprise train PR 4.2, migration `0193_connector_audit_log.sql`.**
/// Dedicated cross-account isolation AND append-only proof for
/// `connector_audit_log`, the one new RLS-covered table this PR's schema
/// adds — same "the generic roster check (step 5 of the big test above)
/// only proves the policy EXISTS; this proves it actually filters, and
/// that append-only is enforced by the ABSENCE of an UPDATE/DELETE policy,
/// not by convention" reasoning as `grant_seat_tiers_rls_isolates_via_the_
/// one_hop_grants_subquery` just above.
#[tokio::test]
async fn connector_audit_log_is_append_only_and_isolated_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping connector_audit_log_is_append_only_and_isolated_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_consumer_rls_connector_audit_log";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    async fn seed(owner: &PgPool, email: &str) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(owner)
            .await
            .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(owner)
                .await
                .expect("insert account");
        sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, \
             provider, subject) VALUES ($1, $2, 'user', 'install', 'slack', 'T-secret')",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(owner)
        .await
        .expect("seed connector_audit_log row");
        (account_id, user_id)
    }

    let (account_a, user_a) = seed(&owner, "cal-rls-a@x.co").await;
    let (account_b, _user_b) = seed(&owner, "cal-rls-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;

    // ── 1. Isolation: A sees only A's row, never B's ────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        let rows: Vec<Uuid> = sqlx::query_scalar("SELECT account_id FROM connector_audit_log")
            .fetch_all(&mut *tx)
            .await
            .expect("select connector_audit_log as app role");
        assert_eq!(rows, vec![account_a], "A must see exactly A's own row, never B's");
        tx.rollback().await.ok();
    }

    // ── 2. Append-only: UPDATE/DELETE affect zero rows even for a session
    // scoped to the row's own account (no UPDATE/DELETE policy exists at all
    // — RLS's default-deny for an unmatched command, not mere convention) ──
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");

        let updated = sqlx::query("UPDATE connector_audit_log SET subject = 'tampered' WHERE account_id = $1")
            .bind(account_a)
            .execute(&mut *tx)
            .await
            .expect("UPDATE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(updated.rows_affected(), 0, "connector_audit_log must be append-only: no UPDATE policy exists");

        let deleted = sqlx::query("DELETE FROM connector_audit_log WHERE account_id = $1")
            .bind(account_a)
            .execute(&mut *tx)
            .await
            .expect("DELETE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(deleted.rows_affected(), 0, "connector_audit_log must be append-only: no DELETE policy exists");

        tx.rollback().await.ok();
    }

    // ── 3. Actor pairing on INSERT: A may write a row naming itself as
    // actor_user_id, but NOT one naming another account's user id ──────────
    // Each sub-check below opens its OWN transaction (rather than sharing
    // one): Postgres aborts an entire transaction after any statement
    // error, so a shared-tx version of this block would have every
    // assertion AFTER the expected `spoofed_actor` failure itself fail with
    // "current transaction is aborted" instead of being evaluated — a test
    // bug caught by this suite's own first run, not a product bug.
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(user_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let own_actor = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, $2, 'user', 'seat_grant', 'slack')",
        )
        .bind(account_a)
        .bind(user_a)
        .execute(&mut *tx)
        .await;
        assert!(own_actor.is_ok(), "a session may write a row naming ITSELF as actor_user_id — got {own_actor:?}");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(user_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let spoofed_actor = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, $2, 'user', 'seat_grant', 'slack')",
        )
        .bind(account_a)
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await;
        assert!(spoofed_actor.is_err(), "a session must NOT be able to write a row naming a DIFFERENT actor_user_id");
        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(user_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let system_actor = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, NULL, 'system', 'seat_grant', 'slack')",
        )
        .bind(account_a)
        .execute(&mut *tx)
        .await;
        assert!(system_actor.is_ok(), "a NULL actor_user_id (system/webhook row) must always be insertable — got {system_actor:?}");
        tx.rollback().await.ok();
    }

    // ── 4. Inert under the owner/BYPASSRLS role ─────────────────────────────
    {
        let rows: Vec<Uuid> = sqlx::query_scalar("SELECT account_id FROM connector_audit_log ORDER BY account_id")
            .fetch_all(&owner)
            .await
            .expect("select connector_audit_log as owner");
        let mut expected = vec![account_a, account_b];
        expected.sort();
        let mut actual = rows;
        actual.sort();
        assert_eq!(actual, expected, "the owner/BYPASSRLS role must still see every account's rows");
    }

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// CodeRabbit round 1 (#1005, Data Integrity): FAILS WITHOUT THE FIX. The
/// generic roster check (this file's step 5) already proves
/// `slack_unmatched_email_notices`'s `account_isolation` policy EXISTS; this
/// proves it actually FILTERS. Simpler than
/// [`connector_audit_log_is_append_only_and_isolated_pg`]'s own proof
/// (there is no append-only question here — this table supports ordinary
/// per-recipient UPDATEs via `claim_notice_slot`'s `ON CONFLICT DO UPDATE`
/// and `suppress_by_token`'s `UPDATE`), so only the isolation half is
/// pinned: account A must see exactly its own row, never account B's.
#[tokio::test]
async fn slack_unmatched_email_notices_rls_isolates_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping slack_unmatched_email_notices_rls_isolates_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_consumer_rls_slack_unmatched_email_notices";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    async fn seed(owner: &PgPool, email: &str, notice_email: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(owner)
            .await
            .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(owner)
                .await
                .expect("insert account");
        sqlx::query(
            "INSERT INTO slack_unmatched_email_notices (account_id, email, workspace_id, channel_id) \
             VALUES ($1, $2, 'T-secret', 'C-secret')",
        )
        .bind(account_id)
        .bind(notice_email)
        .execute(owner)
        .await
        .expect("seed slack_unmatched_email_notices row");
        account_id
    }

    let account_a = seed(&owner, "sue-rls-a@x.co", "unmatched-a@example.com").await;
    let account_b = seed(&owner, "sue-rls-b@x.co", "unmatched-b@example.com").await;

    let app_pool = app_role_pool(&db_url).await;

    let mut tx = app_pool.begin().await.expect("begin as app role");
    sqlx::query("SELECT set_config('app.account_id', $1, true)")
        .bind(account_a.to_string())
        .execute(&mut *tx)
        .await
        .expect("set_config app.account_id");
    let rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT account_id, email FROM slack_unmatched_email_notices")
            .fetch_all(&mut *tx)
            .await
            .expect("select slack_unmatched_email_notices as app role");
    assert_eq!(
        rows,
        vec![(account_a, "unmatched-a@example.com".to_string())],
        "A must see exactly A's own row (and its real email), never B's — a permissive \
         policy here would leak a real person's address across accounts"
    );
    tx.rollback().await.ok();

    // The owner/BYPASSRLS role must still see every account's rows.
    let all_rows: Vec<Uuid> = sqlx::query_scalar(
        "SELECT account_id FROM slack_unmatched_email_notices ORDER BY account_id",
    )
    .fetch_all(&owner)
    .await
    .expect("select slack_unmatched_email_notices as owner");
    let mut expected = vec![account_a, account_b];
    expected.sort();
    let mut actual = all_rows;
    actual.sort();
    assert_eq!(actual, expected, "the owner/BYPASSRLS role must still see every account's rows");

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// **m365 enterprise train PR 4.3, migration `0191_query_audit_log.sql`.**
/// A dedicated cross-account isolation + append-only proof for
/// `query_audit_log`, the one new RLS-covered table this PR adds — same
/// precedent as [`grant_seat_tiers_rls_isolates_via_the_one_hop_grants_subquery`]
/// just above: the generic roster check (this file's step 5) already proves
/// the policy EXISTS; this proves it actually FILTERS, and additionally
/// proves the append-only design (no UPDATE/DELETE policy at all — see the
/// migration's header) is real, not merely documented.
///
/// Own scratch DB (`orgt_consumer_rls_query_audit_log`), same reasoning as
/// `grant_seat_tiers`'s own scratch DB: never contends with the big test's
/// `orgt_consumer_rls` database name.
#[tokio::test]
async fn query_audit_log_rls_isolates_and_is_append_only() {
    let Some(base) = base_url() else {
        eprintln!("skipping query_audit_log_rls_isolates_and_is_append_only: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_consumer_rls_query_audit_log";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    async fn seed_row(owner: &PgPool, email: &str) -> (Uuid, Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(owner)
            .await
            .expect("insert user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(owner)
                .await
                .expect("insert account");
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name) VALUES ($1, 'home') RETURNING id",
        )
        .bind(account_id)
        .fetch_one(owner)
        .await
        .expect("insert graph");
        let digest = "b".repeat(64);
        let row_id: Uuid = sqlx::query_scalar(
            "INSERT INTO query_audit_log (account_id, asked_by_user_id, question_digest, \
                graphs_searched, atom_ids_cited) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(&digest)
        .bind(vec![graph_id])
        .bind(Vec::<Uuid>::new())
        .fetch_one(owner)
        .await
        .expect("insert query_audit_log row");
        (user_id, account_id, row_id)
    }

    let (_user_a, account_a, row_a) = seed_row(&owner, "qad-rls-a@x.co").await;
    let (_user_b, account_b, row_b) = seed_row(&owner, "qad-rls-b@x.co").await;

    let app_pool = app_role_pool(&db_url).await;

    // ── 1. Enforced under coralswarm_app: A sees only its own row ──────────
    for (scope_account, expect_row, other_row) in
        [(account_a, row_a, row_b), (account_b, row_b, row_a)]
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(scope_account.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");

        let visible_ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM query_audit_log ORDER BY id")
                .fetch_all(&mut *tx)
                .await
                .expect("select query_audit_log as app role");
        assert_eq!(
            visible_ids,
            vec![expect_row],
            "coralswarm_app scoped to {scope_account} must see exactly its own \
             query_audit_log row, never {other_row}'s"
        );

        // Direct-by-id read of the OTHER account's row — the same IDOR-pivot
        // shape the big RLS test's `atoms` proof exercises.
        let cross_account: Vec<Uuid> =
            sqlx::query_scalar("SELECT id FROM query_audit_log WHERE id = $1")
                .bind(other_row)
                .fetch_all(&mut *tx)
                .await
                .expect("select query_audit_log by the OTHER account's id as app role");
        assert!(
            cross_account.is_empty(),
            "coralswarm_app scoped to {scope_account} querying query_audit_log by \
             {other_row}'s id directly must return ZERO rows — got {cross_account:?}"
        );

        tx.rollback().await.ok();
    }

    // ── 2. Append-only: UPDATE/DELETE refused for the app role, even on the
    // account's OWN row (no policy exists for either command at all — see
    // the migration header). A mis-shaped policy here would show up as a
    // successful write, not an error: RLS's "no applicable policy" behavior
    // is to filter the row out of the command's own WHERE, which for
    // UPDATE/DELETE means AFFECTS ZERO ROWS, not a permission-denied error
    // (the role DOES hold table-level UPDATE/DELETE grants — 0083 grants
    // them on every table — RLS is what removes the row from view here).
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");

        let updated = sqlx::query("UPDATE query_audit_log SET question_digest = $1 WHERE id = $2")
            .bind("c".repeat(64))
            .bind(row_a)
            .execute(&mut *tx)
            .await
            .expect("UPDATE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            updated.rows_affected(),
            0,
            "query_audit_log must be append-only: an UPDATE of the account's OWN row must \
             affect zero rows (no UPDATE policy exists at all)"
        );

        let deleted = sqlx::query("DELETE FROM query_audit_log WHERE id = $1")
            .bind(row_a)
            .execute(&mut *tx)
            .await
            .expect("DELETE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            deleted.rows_affected(),
            0,
            "query_audit_log must be append-only: a DELETE of the account's OWN row must \
             affect zero rows (no DELETE policy exists at all)"
        );

        tx.rollback().await.ok();
    }

    // ── 3. Inert under the owner/BYPASSRLS role: sees both accounts' rows ──
    {
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM query_audit_log ORDER BY id")
            .fetch_all(&owner)
            .await
            .expect("select query_audit_log as owner");
        let mut expected = vec![row_a, row_b];
        expected.sort();
        let mut actual = ids;
        actual.sort();
        assert_eq!(
            actual, expected,
            "the owner/BYPASSRLS role must still see every account's query_audit_log rows"
        );

        // The owner role, being the table owner, CAN update/delete — proving
        // §2's zero-rows-affected result above is RLS denying the app role,
        // not some other reason (a missing column, a broken WHERE) that
        // would also block the owner.
        let updated = sqlx::query("UPDATE query_audit_log SET question_digest = $1 WHERE id = $2")
            .bind("d".repeat(64))
            .bind(row_a)
            .execute(&owner)
            .await
            .expect("owner UPDATE must succeed");
        assert_eq!(updated.rows_affected(), 1, "owner role is exempt from RLS entirely");
    }

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// **D6 UI (routed separately from B2 unit 02/#718 —
/// `docs/plans/capture/02-b2-retract-fixes.md`'s "D6 UI" section),
/// migration `0196_retraction_audit_log.sql`.** A dedicated D8-visibility +
/// append-only proof for `retraction_audit_log` — same precedent as
/// [`query_audit_log_rls_isolates_and_is_append_only`] just above: the
/// generic roster check (this file's step 5) already proves the policy
/// EXISTS; this proves it actually implements D8's three-way rule (author,
/// every ocean · admin of a shared ocean · personal-ocean rows author-only),
/// which `routes::retractions` (the D6 UI's read endpoints) leans on
/// ENTIRELY — those routes carry no `WHERE account_id = …`/`WHERE
/// actor_user_id = …` of their own, so if this policy is wrong, that route
/// silently leaks. Also proves this table's SELECT policy keys on
/// `app.user_id` ALONE (no `app.account_id` predicate at all, unlike every
/// `account_isolation`-shaped table above) — D8's author clause is
/// deliberately cross-ACCOUNT (an org member's own retractions must be
/// visible to them even scoped to their PERSONAL account), so a test that
/// only varied `app.account_id` would never catch a broken author clause.
///
/// Own scratch DB (`orgt_consumer_rls_retraction_audit_log`), same reasoning
/// as `grant_seat_tiers`/`query_audit_log`'s own scratch DBs.
#[tokio::test]
async fn retraction_audit_log_rls_enforces_d8_visibility_and_is_append_only() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping retraction_audit_log_rls_enforces_d8_visibility_and_is_append_only: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_retraction_audit_log";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    async fn seed_user(owner: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(owner)
            .await
            .expect("insert user")
    }
    async fn seed_account(owner: &PgPool, owner_user: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner_user)
            .fetch_one(owner)
            .await
            .expect("insert account")
    }
    async fn add_member(owner: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(user_id)
            .bind(role)
            .execute(owner)
            .await
            .expect("insert account_members");
    }
    #[allow(clippy::too_many_arguments)]
    async fn seed_row(
        owner: &PgPool,
        account_id: Uuid,
        actor_user_id: Uuid,
        graph_id: Uuid,
        is_personal_ocean: bool,
        surface: &str,
        outcome: &str,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO retraction_audit_log \
                (retraction_id, account_id, actor_kind, actor_user_id, source_type, \
                 source_id, atom_id, graph_id, is_personal_ocean, surface, outcome, manifest) \
             VALUES (gen_random_uuid(), $1, 'user', $2, 'coding_session', 'cc:rls-test:0', \
                     NULL, $3, $4, $5, $6, '{}'::jsonb) \
             RETURNING retraction_id",
        )
        .bind(account_id)
        .bind(actor_user_id)
        .bind(graph_id)
        .bind(is_personal_ocean)
        .bind(surface)
        .bind(outcome)
        .fetch_one(owner)
        .await
        .expect("insert retraction_audit_log row")
    }

    // ── Account A: author + a plain member + an admin, one personal row and
    // one shared row, both authored by `author` ────────────────────────────
    let author = seed_user(&owner, "retract-rls-author@x.co").await;
    let plain_member = seed_user(&owner, "retract-rls-member@x.co").await;
    let admin_user = seed_user(&owner, "retract-rls-admin@x.co").await;
    let account_a = seed_account(&owner, author).await;
    add_member(&owner, account_a, author, "owner").await;
    add_member(&owner, account_a, plain_member, "member").await;
    add_member(&owner, account_a, admin_user, "admin").await;
    let personal_row = seed_row(&owner, account_a, author, Uuid::new_v4(), true, "content", "removed").await;
    let shared_row = seed_row(&owner, account_a, author, Uuid::new_v4(), false, "journal_rollup", "out_of_window").await;

    // ── Account B: a second author with their OWN row — proves the author
    // clause is genuinely per-user, not "everyone in retraction_audit_log" ──
    let author_b = seed_user(&owner, "retract-rls-author-b@x.co").await;
    let account_b = seed_account(&owner, author_b).await;
    add_member(&owner, account_b, author_b, "owner").await;
    let other_account_row =
        seed_row(&owner, account_b, author_b, Uuid::new_v4(), false, "content", "removed").await;

    let app_pool = app_role_pool(&db_url).await;

    async fn scoped_ids(app_pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Vec<Uuid> {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_id.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(user_id.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT retraction_id FROM retraction_audit_log ORDER BY retraction_id")
            .fetch_all(&mut *tx)
            .await
            .expect("select retraction_audit_log as app role");
        tx.rollback().await.ok();
        ids
    }

    // ── 1. Author: sees BOTH their own rows (personal + shared), never the
    // other account's row — D8's "author -> own rows, every ocean" ────────
    let author_ids = scoped_ids(&app_pool, account_a, author).await;
    let mut expected_author: Vec<Uuid> = vec![personal_row, shared_row];
    expected_author.sort();
    let mut actual_author = author_ids;
    actual_author.sort();
    assert_eq!(
        actual_author, expected_author,
        "the author must see exactly their own personal AND shared rows"
    );
    assert!(
        !actual_author.contains(&other_account_row),
        "the author must never see another account's author's row"
    );

    // ── 2. Plain member (not author, not owner/admin): sees NOTHING — the
    // personal row is author-only, and a plain member is not admitted by the
    // shared row's admin clause either ──────────────────────────────────────
    let member_ids = scoped_ids(&app_pool, account_a, plain_member).await;
    assert!(
        member_ids.is_empty(),
        "a non-author, non-admin member must see ZERO retraction_audit_log rows — got {member_ids:?}"
    );

    // ── 3. Admin (not the author): sees ONLY the shared row, never the
    // personal one — D8's "personal-ocean rows -> author only, never visible
    // to an employer" ───────────────────────────────────────────────────────
    let admin_ids = scoped_ids(&app_pool, account_a, admin_user).await;
    assert_eq!(
        admin_ids,
        vec![shared_row],
        "an admin must see exactly the shared-ocean row and NEVER the personal one"
    );

    // ── 4. Append-only: UPDATE/DELETE refused for the app role, even on the
    // author's own row (0196 REVOKEs UPDATE/DELETE from coralswarm_app
    // directly — a stronger mechanism than a missing RLS policy, so this
    // must be a hard permission-denied error, not merely zero rows
    // affected). ─────────────────────────────────────────────────────────────
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(author.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let updated = sqlx::query("UPDATE retraction_audit_log SET outcome = 'removed' WHERE retraction_id = $1")
            .bind(personal_row)
            .execute(&mut *tx)
            .await;
        assert!(
            updated.is_err(),
            "retraction_audit_log must be append-only: an UPDATE by the app role must be a hard \
             permission-denied ERROR (privilege REVOKE, D7) — got {updated:?}"
        );

        tx.rollback().await.ok();
    }
    {
        let mut tx = app_pool.begin().await.expect("begin as app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(account_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(author.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let deleted = sqlx::query("DELETE FROM retraction_audit_log WHERE retraction_id = $1")
            .bind(personal_row)
            .execute(&mut *tx)
            .await;
        assert!(
            deleted.is_err(),
            "retraction_audit_log must be append-only: a DELETE by the app role must be a hard \
             permission-denied ERROR (privilege REVOKE, D7) — got {deleted:?}"
        );

        tx.rollback().await.ok();
    }

    // ── 5. Inert under the owner/BYPASSRLS role: sees every account's rows,
    // and CAN update/delete (proving §4's errors are the REVOKE biting the
    // app role specifically, not some unrelated SQL problem) ────────────────
    {
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT retraction_id FROM retraction_audit_log ORDER BY retraction_id")
            .fetch_all(&owner)
            .await
            .expect("select retraction_audit_log as owner");
        let mut expected = vec![personal_row, shared_row, other_account_row];
        expected.sort();
        let mut actual = ids;
        actual.sort();
        assert_eq!(
            actual, expected,
            "the owner/BYPASSRLS role must still see every account's retraction_audit_log rows"
        );

        let updated = sqlx::query("UPDATE retraction_audit_log SET outcome = 'removed' WHERE retraction_id = $1")
            .bind(shared_row)
            .execute(&owner)
            .await
            .expect("owner UPDATE must succeed — the REVOKE targets coralswarm_app only");
        assert_eq!(updated.rows_affected(), 1, "owner role is exempt from the REVOKE entirely");
    }

    app_pool.close().await;
    owner.close().await;
    retained_database::finish(&mut admin(&base).await, db, true)
        .await
        .ok();
}

/// Unit-level proof for `tenancy::scope::begin_account_scoped` (the Rust-side
/// `SET LOCAL app.account_id` plumbing): the GUC round-trips correctly inside
/// the transaction it was set on, and — critically for pooled-connection
/// safety — does NOT leak onto a later, unrelated transaction that happens to
/// reuse the same physical connection once the scoped transaction ends.
#[tokio::test]
async fn begin_account_scoped_round_trips_and_does_not_leak_across_transactions() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping begin_account_scoped_round_trips_and_does_not_leak_across_transactions: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_scope";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    // A single-connection pool so the SECOND transaction is guaranteed to
    // reuse the exact same physical connection the first one used — the
    // scenario that would expose a `SET SESSION`-style leak.
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect");

    let account_id = Uuid::new_v4();
    let mut tx = begin_account_scoped(&pool, account_id)
        .await
        .expect("begin_account_scoped");
    let seen: Uuid = sqlx::query_scalar("SELECT current_setting('app.account_id')::uuid")
        .fetch_one(&mut *tx)
        .await
        .expect("read back app.account_id inside the scoped transaction");
    assert_eq!(seen, account_id, "the GUC must round-trip to the account_id it was set to");
    let search_path: String = sqlx::query_scalar("SHOW search_path")
        .fetch_one(&mut *tx)
        .await
        .expect("read search_path inside the scoped transaction");
    assert!(
        search_path.contains("public"),
        "begin_account_scoped must pin public on search_path so a pooled \
         backend with an empty path cannot hide public tables, got {search_path:?}"
    );
    tx.commit().await.expect("commit scoped transaction");

    // A fresh transaction on the same (single-connection) pool must NOT see
    // the prior transaction's VALUE — proves `set_config(..., true)` really
    // behaves as SET LOCAL (transaction-scoped) rather than leaking the real
    // account_id onto the next borrower of a reused pooled connection.
    //
    // Postgres quirk worth documenting rather than tripping over: `app.*` is
    // an unregistered "placeholder" GUC class, so once a session has SET it
    // at all, the setting doesn't revert to "undefined" outside that
    // transaction — it reverts to the empty string (`''`), which is the
    // placeholder's session-invented base value. So `current_setting(...,
    // true)` here is `Some("")`, not `None`. That's still safe: the RLS
    // policies use the STRICT, non-`missing_ok` form
    // (`current_setting('app.account_id')::uuid`), and casting `''` to
    // `uuid` is a hard error — so a reused connection that races a new
    // transaction before that transaction's own `SET LOCAL` runs still fails
    // closed (an error, never a query silently scoped to the PRIOR
    // transaction's real account_id).
    let mut tx2 = pool.begin().await.expect("begin unscoped transaction");
    let reverted: Option<String> =
        sqlx::query_scalar("SELECT current_setting('app.account_id', true)")
            .fetch_one(&mut *tx2)
            .await
            .expect("current_setting with missing_ok must not error");
    assert_eq!(
        reverted.as_deref(),
        Some(""),
        "outside the scoped transaction, the placeholder GUC reverts to '' (Postgres's own \
         session-invented base value for an unregistered custom GUC) — NOT the prior \
         transaction's real account_id"
    );
    let strict_result: Result<Uuid, sqlx::Error> =
        sqlx::query_scalar("SELECT current_setting('app.account_id')::uuid")
            .fetch_one(&mut *tx2)
            .await;
    assert!(
        strict_result.is_err(),
        "the RLS policies' STRICT predicate must hard-fail (invalid uuid cast of '') on a \
         reused connection whose new transaction hasn't called SET LOCAL yet — never \
         silently resolve to the previous transaction's account_id, got {strict_result:?}"
    );
    tx2.rollback().await.ok();

    pool.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// The 2026-07-26 production outage, as a regression test.
///
/// `Store::intake_summary` reads `atoms` on the bare pool, so it carries no
/// `app.account_id`. Under the `coralswarm_app` role that is **not** a narrower
/// answer — it is a hard failure (`unrecognized configuration parameter`). The
/// ask path swallowed that failure into `IntakeSummary { total: 0, .. }`, which
/// renders as "Nothing new has come into this ocean in the last 24 hours": an
/// outage wearing the costume of a real answer.
///
/// Both halves are asserted deliberately. (a) alone would be satisfied by a
/// completely broken store, and (b) alone would not catch a regression that
/// re-introduces an unscoped read on a path that happens to have a GUC set by
/// an unrelated caller.
#[tokio::test]
async fn intake_summary_needs_a_scoped_transaction_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping intake_summary_needs_a_scoped_transaction_under_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_intake";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    let (_user_a, account_a, graph_a, _atom_a) = seed_account(&owner, "intake-a@x.co").await;

    // A real separate login AS coralswarm_app — NOT `SET ROLE` in the owner's
    // session. Run as the owner this whole test passes vacuously, because the
    // owner is exempt from ENABLE-only RLS.
    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());
    let filter = RetrievalFilter::default();
    let since = Utc::now() - chrono::Duration::days(1);

    // ── (a) The unscoped form must FAIL, not quietly answer zero ────────────
    let unscoped = store.intake_summary(graph_a, since, 10, &filter).await;
    assert!(
        unscoped.is_err(),
        "Store::intake_summary must FAIL under the app role rather than return a \
         count — if this starts returning Ok, the GUC is being set by something \
         outside the scoped-transaction contract and the swallow bug can silently \
         come back. Got: {unscoped:?}"
    );

    // ── (b) The scoped form must return the real data ───────────────────────
    let mut tx = begin_account_scoped(&app_pool, account_a)
        .await
        .expect("begin_account_scoped as app role");
    let summary = store
        .intake_summary_scoped(&mut tx, graph_a, since, 10, &filter)
        .await
        .expect("intake_summary_scoped must succeed inside a scoped transaction");
    tx.rollback().await.ok();

    assert_eq!(
        summary.total, 1,
        "the seeded atom must be counted under the scoped read — a zero here is \
         the exact false-empty answer this test exists to prevent"
    );
    assert_eq!(
        summary.recent.len(),
        1,
        "the seeded atom must appear in recent highlights"
    );
}

/// The event-time sibling of the test above — the half `#369` did not cover.
///
/// `Store::timestamp_window_atoms` is what makes "what did we work on today?"
/// answerable at all: candidate generation is otherwise always similarity- or
/// lexically-gated against the query text, so a same-day atom with no textual
/// tie to the question is never fetched. It too read `atoms` on the bare pool,
/// so under `coralswarm_app` it raised — and `apply_event_window_merge`
/// swallowed that into `Vec::new()`, read it as "nothing happened today", and
/// dropped the temporal scope entirely. The ask then answered from pure
/// semantic retrieval, i.e. a confident "I have nothing from today" over an
/// ocean full of that day's work.
///
/// Same two-sided shape as the intake test: (a) the unscoped read must FAIL
/// rather than answer an empty window — an `Ok(vec![])` here is precisely the
/// indistinguishable case that caused the bug — and (b) the scoped read must
/// return the real atom.
#[tokio::test]
async fn timestamp_window_atoms_needs_a_scoped_transaction_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping timestamp_window_atoms_needs_a_scoped_transaction_under_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_event_window";
    let owner = scratch_db_with_app_role(&base, db).await;
    let db_url = with_db(&base, db);

    let (_user_a, account_a, graph_a, atom_a) = seed_account(&owner, "eventwin-a@x.co").await;

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());
    let filter = RetrievalFilter::default();
    // `seed_account` stamps the atom at `now()`, so a generous window around
    // now contains it without depending on wall-clock precision.
    let since = Utc::now() - chrono::Duration::days(1);
    let until = Utc::now() + chrono::Duration::days(1);

    // ── (a) The unscoped form must FAIL, not quietly answer "empty window" ───
    let unscoped = store
        .timestamp_window_atoms(graph_a, since, until, Some(50), &filter)
        .await;
    assert!(
        unscoped.is_err(),
        "Store::timestamp_window_atoms must FAIL under the app role rather than return a \
         window — an Ok(empty) here is indistinguishable from 'nothing happened today', \
         which is exactly how the temporal scope silently disappeared. Got: {unscoped:?}"
    );

    // ── (b) The scoped form must return the real data ────────────────────────
    let mut tx = begin_account_scoped(&app_pool, account_a)
        .await
        .expect("begin_account_scoped as app role");
    let atoms = store
        .timestamp_window_atoms_scoped(&mut tx, graph_a, since, until, Some(50), &filter)
        .await
        .expect("timestamp_window_atoms_scoped must succeed inside a scoped transaction");
    tx.rollback().await.ok();

    assert_eq!(
        atoms.len(),
        1,
        "the seeded same-day atom must be found under the scoped read — an empty result \
         here is the false-empty window this test exists to prevent"
    );
    assert_eq!(
        atoms[0].0, atom_a,
        "the scan must return the seeded atom itself"
    );

    // ── (c) `limit: None` really means NO limit on Postgres ──────────────────
    //
    // The unbounded scan is how a date-range recap covers a whole range without
    // silently dropping days, and it is implemented as `LIMIT NULL` (Postgres
    // for "no limit") via a nullable bind rather than a sentinel number. That is
    // exactly the kind of thing that could quietly degrade to "return one row"
    // or "return nothing", so assert it against a real server with more rows
    // than any limit under test.
    for i in 0..5u32 {
        sqlx::query(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'test', $2, 'blob-key', now())",
        )
        .bind(graph_a)
        .bind(format!("probe-window-{i}"))
        .execute(&owner)
        .await
        .expect("seed extra window atoms");
    }

    let mut tx = begin_account_scoped(&app_pool, account_a)
        .await
        .expect("begin_account_scoped as app role");
    let unbounded = store
        .timestamp_window_atoms_scoped(&mut tx, graph_a, since, until, None, &filter)
        .await
        .expect("unbounded scoped scan must succeed");
    let bounded = store
        .timestamp_window_atoms_scoped(&mut tx, graph_a, since, until, Some(2), &filter)
        .await
        .expect("bounded scoped scan must succeed");
    tx.rollback().await.ok();

    assert_eq!(
        unbounded.len(),
        6,
        "limit: None must return the COMPLETE window (1 original + 5 seeded), not a \
         truncated or empty one — `LIMIT NULL` silently behaving as a real limit is the \
         failure mode this asserts against"
    );
    assert_eq!(
        bounded.len(),
        2,
        "limit: Some(n) must still truncate to n"
    );
}

/// The Slack destination picker's three reads — the bug an end-to-end test
/// against real Slack found on 2026-07-27, which every pg test and every CI
/// gate had missed.
///
/// `routes::slack_events::post_destination_prompt` read `graphs` (personal
/// oceans) and `teams` on the bare pool, and
/// `routes::slack_interactivity::dispatch` read `oauth_tokens` the same way.
/// All three carry 0083's `account_isolation`, whose
/// `current_setting('app.account_id')::uuid` cast RAISES on a connection that
/// never set the GUC — it does not filter to zero rows:
///
///     invalid input syntax for type uuid: ""
///
/// Both call sites then swallowed it (`.map_err(...)?` into a logged-and-
/// dropped warning, and `.unwrap_or(None)`), so in production the picker was
/// inert on BOTH halves: the prompt never posted, and the button click
/// returned early as though the workspace had never been installed. Every
/// channel silently fell back to the account default, and the entire org
/// plane was unreachable.
///
/// Why nothing caught it: every other pg test runs as the database OWNER, who
/// is exempt from ENABLE-only RLS, so an unscoped read passes locally and
/// fails only under `coralswarm_app`. `scripts/check_rls_coverage.sh` did not
/// help either — its gate is per-FILE, and both files already contained a
/// `begin_account_scoped` elsewhere.
///
/// Same two-sided shape as the `intake_summary` / `timestamp_window_atoms`
/// tests above, for the same reason: (a) the unscoped read must FAIL rather
/// than answer "no oceans" / "no workspace", because an `Ok(empty)` is
/// exactly the indistinguishable case that caused the bug, and (b) the scoped
/// read must return the real rows.
#[tokio::test]
async fn slack_destination_picker_reads_need_a_scoped_transaction_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping slack_destination_picker_reads_need_a_scoped_transaction_under_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_slack_picker";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    let (_user_a, account_a, _graph_a, _atom_a) = seed_account(&owner, "picker-a@x.co").await;

    // A Slack install for this account — what the click path looks up.
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id) \
         VALUES ($1, 'slack', 'channels:history', '\\x00'::bytea, 'T-PICKER')",
    )
    .bind(account_a)
    .execute(&owner)
    .await
    .expect("seed oauth_tokens");

    let app_pool = app_role_pool(&db_url).await;

    // ── (a) Unscoped, under the app role: every one of the three must ERROR
    // rather than return an empty answer. An `Ok` here means the picker can
    // silently read "this account has no oceans / no teams / no workspace"
    // and disable itself, which is precisely what shipped.
    for (label, sql) in [
        (
            "personal oceans (post_destination_prompt)",
            "SELECT id FROM graphs WHERE account_id = $1 AND kind = 'personal'",
        ),
        ("teams (post_destination_prompt)", "SELECT id FROM teams WHERE account_id = $1"),
        (
            "workspace id (slack_interactivity dispatch)",
            "SELECT id FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
        ),
    ] {
        let unscoped = sqlx::query_scalar::<_, Uuid>(sql)
            .bind(account_a)
            .fetch_optional(&app_pool)
            .await;
        assert!(
            unscoped.is_err(),
            "{label}: an UNSCOPED read under coralswarm_app must FAIL, not answer empty. An \
             Ok(_) here means the RLS GUC is being set outside the scoped-transaction contract, \
             and the swallow bug that made the Slack picker inert in production can silently \
             come back. Got: {unscoped:?}"
        );
    }

    // ── (b) Scoped: the same three reads must return the real rows ──────────
    let mut tx = begin_account_scoped(&app_pool, account_a)
        .await
        .expect("begin_account_scoped as app role");

    let oceans: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE account_id = $1 AND kind = 'personal'")
            .bind(account_a)
            .fetch_all(&mut *tx)
            .await
            .expect("scoped personal-ocean read must succeed");
    assert_eq!(
        oceans.len(),
        1,
        "the seeded personal ocean must be visible under the scoped read — a zero here is the \
         false-empty that made the picker skip its own selector"
    );

    let workspace: Option<String> = sqlx::query_scalar(
        "SELECT provider_workspace_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'slack' LIMIT 1",
    )
    .bind(account_a)
    .fetch_optional(&mut *tx)
    .await
    .expect("scoped workspace read must succeed");
    assert_eq!(
        workspace.as_deref(),
        Some("T-PICKER"),
        "the scoped read must find the install — a None here is the 'no oauth_tokens row' \
         early-return that made every cs_dest_* button click do nothing"
    );

    tx.rollback().await.ok();
}

/// The citation/atlas ENRICHMENT surface — the half `#369`/`#371` did not
/// cover, found by the #561 call-site audit.
///
/// These four reads back the Atlas people layer, Atlas coral labels, and the
/// `citation_details` backfill guard. Every one of them ran on the bare pool
/// with its error swallowed (`unwrap_or_default`, or `_ => None` for
/// `atom_graph_id`), so under the live `coralswarm_app` role they could not
/// succeed and their callers silently rendered an empty people layer, opaque
/// source-id labels, and a backfill that resolved nothing. The failure was
/// invisible precisely because it was swallowed — which is why the assertion
/// that the UNSCOPED form still errors matters as much as the scoped one: if
/// it ever starts returning `Ok`, the GUC is being set outside the scoped
/// contract and the silent-empty bug can come back unnoticed.
#[tokio::test]
async fn enrichment_reads_need_a_scoped_transaction_under_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping enrichment_reads_need_a_scoped_transaction_under_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_consumer_rls_enrichment";
    let db_url = with_db(&base, db);
    let owner = scratch_db_with_app_role(&base, db).await;

    let (_user_a, account_a, graph_a, atom_a) = seed_account(&owner, "enrich-a@x.co").await;

    // Give the seeded atom the provenance + inline content these reads project,
    // so a scoped success is proved by real returned rows rather than by an
    // empty-but-Ok result that would pass vacuously.
    sqlx::query(
        "UPDATE atoms SET content_inline = 'hello world', header_len = 0, \
         author_id = 'U123', author_name = 'Ada' WHERE atom_id = $1",
    )
    .bind(atom_a)
    .execute(&owner)
    .await
    .expect("stamp author + inline content on the seeded atom");

    let app_pool = app_role_pool(&db_url).await;
    let store = PostgresStore::new(app_pool.clone());
    let ids = [atom_a];

    // ── (a) Every unscoped form must FAIL, not quietly answer empty ─────────
    assert!(
        store.atom_authors(&ids).await.is_err(),
        "Store::atom_authors must FAIL under the app role — it used to be \
         unwrap_or_default()'d into an empty Atlas people layer (#561)"
    );
    assert!(
        store.atom_inline_content(&ids).await.is_err(),
        "Store::atom_inline_content must FAIL under the app role — it used to be \
         unwrap_or_default()'d into source-type-only coral labels (#561)"
    );
    assert!(
        store.atom_content_refs(&ids).await.is_err(),
        "Store::atom_content_refs must FAIL under the app role (#561)"
    );
    assert!(
        store.atom_graph_id(atom_a).await.is_err(),
        "Store::atom_graph_id must FAIL under the app role — citation_details' \
         tenant guard swallowed this into `_ => None`, dropping every \
         backfilled citation id (#561)"
    );

    // ── (b) Every scoped twin must return the real row ──────────────────────
    let mut tx = begin_account_scoped(&app_pool, account_a)
        .await
        .expect("begin_account_scoped as app role");

    let authors = store
        .atom_authors_scoped(&mut tx, &ids)
        .await
        .expect("atom_authors_scoped must succeed inside a scoped transaction");
    assert_eq!(
        authors.len(),
        1,
        "the seeded atom's author must come back — an empty vec here is the \
         false-empty people layer this test exists to prevent"
    );
    assert_eq!(authors[0].2.as_deref(), Some("Ada"));

    let inline = store
        .atom_inline_content_scoped(&mut tx, &ids)
        .await
        .expect("atom_inline_content_scoped must succeed inside a scoped transaction");
    assert_eq!(inline.len(), 1, "the seeded atom's inline content must come back");
    assert_eq!(inline[0].1.as_deref(), Some("hello world"));

    let refs = store
        .atom_content_refs_scoped(&mut tx, &ids)
        .await
        .expect("atom_content_refs_scoped must succeed inside a scoped transaction");
    assert_eq!(refs.len(), 1, "the seeded atom's content ref must come back");
    assert_eq!(refs[0].3, account_a, "the ref must carry the owning account");

    let gid = store
        .atom_graph_id_scoped(&mut tx, atom_a)
        .await
        .expect("atom_graph_id_scoped must succeed inside a scoped transaction");
    assert_eq!(
        gid,
        Some(graph_a),
        "the tenant guard must resolve the atom's home graph — a None here is \
         what silently emptied the citation backfill"
    );

    tx.rollback().await.ok();
}

/// Source evidence must have real owning-account rows, cross-account denial,
/// and a RLS-off negative control on all seven relations in the inventory.
#[tokio::test]
async fn source_evidence_rls_isolates_all_seven_tables_non_vacuously() {
    let Some(probe) = support::rls_probe::RlsProbe::setup("orgt_source_evidence_consumer_rls").await else {
        eprintln!("skipping source_evidence_rls_isolates_all_seven_tables_non_vacuously: set ORGT_TEST_BASE_URL");
        return;
    };
    let (role, bypass): (String, bool) = sqlx::query_as(
        "SELECT current_user,rolbypassrls FROM pg_roles WHERE rolname=current_user",
    ).fetch_one(&probe.app_pool).await.unwrap();
    assert_eq!(role, "coralswarm_app");
    assert!(!bypass, "the fixture must use the production non-privileged role");
    assert!(sqlx::query_scalar::<_,bool>("SELECT corpus_lifecycle_delete_repair_ready()")
        .fetch_one(&probe.app_pool).await.unwrap());
    assert!(!sqlx::query_scalar::<_,bool>("SELECT has_table_privilege(current_user,'_sqlx_migrations','SELECT')")
        .fetch_one(&probe.app_pool).await.unwrap(), "readiness must not broaden bookkeeping access");
    for fx in [&probe.a, &probe.b] {
        support::source_evidence::seed(&probe.owner, fx.graph_id, fx.atom_id, fx.node_id).await;
        // P11 temporal tables also require member reachability (app.user_id),
        // stricter than account isolation; bind each account's real member.
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
            .bind(fx.account_id).bind(fx.user_id).execute(&probe.owner).await.unwrap();
    }
    let member_tx = |fx: &support::rls_probe::AccountFixture| {
        let (account, user) = (fx.account_id, fx.user_id);
        let probe = &probe;
        async move {
            let mut tx = probe.scoped_tx(account).await;
            sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(user.to_string()).execute(&mut *tx).await.unwrap();
            tx
        }
    };
    for table in support::source_evidence::TABLES {
        assert!(RLS_TABLES.contains(table), "evidence table must remain in the policy inventory");
        let query = format!("SELECT graph_id FROM {table} ORDER BY graph_id");
        let owned: Vec<Uuid> = sqlx::query_scalar(&query).fetch_all(&probe.owner).await.unwrap();
        let mut expected = vec![probe.a.graph_id, probe.b.graph_id];
        expected.sort();
        assert_eq!(owned, expected, "both fixture rows must exist in {table}");
        for (own, other) in [(&probe.a, &probe.b), (&probe.b, &probe.a)] {
            let mut tx = member_tx(own).await;
            let visible: Vec<Uuid> = sqlx::query_scalar(&query).fetch_all(&mut *tx).await.unwrap();
            assert_eq!(visible, vec![own.graph_id], "{table} must allow its owning account alone");
            let forbidden = format!("SELECT count(*) FROM {table} WHERE graph_id=$1");
            let denied: i64 = sqlx::query_scalar(&forbidden).bind(other.graph_id)
                .fetch_one(&mut *tx).await.unwrap();
            assert_eq!(denied, 0, "direct cross-account graph probes must be denied in {table}");
            tx.rollback().await.unwrap();
        }

        // Prove the denial comes from RLS rather than missing rows/grants.
        probe.owner.execute(format!("ALTER TABLE {table} DISABLE ROW LEVEL SECURITY").as_str()).await.unwrap();
        let mut tx = member_tx(&probe.a).await;
        let leaked: Vec<Uuid> = sqlx::query_scalar(&query).fetch_all(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        probe.owner.execute(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY").as_str()).await.unwrap();
        assert_eq!(leaked, expected, "RLS-off control must reveal both real rows in {table}");
        let mut tx = member_tx(&probe.a).await;
        let restored: Vec<Uuid> = sqlx::query_scalar(&query).fetch_all(&mut *tx).await.unwrap();
        assert_eq!(restored, vec![probe.a.graph_id], "restored RLS must isolate {table} again");
        tx.rollback().await.unwrap();
    }
    probe.teardown().await;
}

/// P8 0308 lifecycle tables: one real row per account, owning account reads
/// only its own, and an RLS-off control proves the denial is not vacuous.
#[tokio::test]
async fn p8_lifecycle_tables_isolate_accounts_non_vacuously() {
    let Some(probe) = support::rls_probe::RlsProbe::setup("orgt_p8_lifecycle_consumer_rls").await else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "local PG required");
        eprintln!("skipping p8_lifecycle_tables_isolate_accounts_non_vacuously: set ORGT_TEST_BASE_URL");
        return;
    };
    for fx in [&probe.a, &probe.b] {
        let (acc, user, g, atom, node) = (fx.account_id, fx.user_id, fx.graph_id, fx.atom_id, fx.node_id);
        probe.owner.execute(format!(r#"
WITH ev AS (INSERT INTO source_purge_events(account_id,owner_user_id,graph_id,source_item_id,event_kind)
  VALUES ('{acc}','{user}','{g}',gen_random_uuid(),'deleted') RETURNING event_id)
INSERT INTO source_purge_dependencies(event_id,kind,graph_id,object_id) SELECT event_id,'atom','{g}','{atom}' FROM ev;
INSERT INTO source_purge_reviews(account_id,graph_id,object_id,kind,review_status) VALUES ('{acc}','{g}','{node}','node','confirmed');
ALTER TABLE selected_evidence_publications DISABLE TRIGGER selected_publication_qualified;
INSERT INTO selected_evidence_publications(publication_id,account_id,owner_user_id,request_id,source_graph_id,source_item_id,
  source_version_id,selection,passage_ids,target_graph_id,selected_sha256)
  VALUES (gen_random_uuid(),'{acc}','{user}',gen_random_uuid(),'{g}',gen_random_uuid(),gen_random_uuid(),'[]'::jsonb,
  ARRAY[gen_random_uuid()],gen_random_uuid(),repeat('a',64));
ALTER TABLE selected_evidence_publications ENABLE TRIGGER selected_publication_qualified;
INSERT INTO unattached_capture_uploads(account_id,graph_id,object_id,blob_key,capture_database,capture_xid)
  VALUES ('{acc}','{g}','{atom}','{g}/{atom}',current_database(),'1');
"#).as_str()).await.unwrap();
    }
    let member = |fx: &support::rls_probe::AccountFixture| {
        let (account, user) = (fx.account_id, fx.user_id);
        let probe = &probe;
        async move {
            let mut tx = probe.scoped_tx(account).await;
            sqlx::query("SELECT set_config('app.user_id',$1,true)").bind(user.to_string()).execute(&mut *tx).await.unwrap();
            tx
        }
    };
    for table in ["source_purge_events", "source_purge_dependencies", "source_purge_reviews",
                  "selected_evidence_publications", "unattached_capture_uploads"] {
        assert!(RLS_TABLES.contains(&table), "{table} must be in the policy inventory");
        let q = format!("SELECT count(*) FROM {table}");
        let owned: i64 = sqlx::query_scalar(&q).fetch_one(&probe.owner).await.unwrap();
        assert_eq!(owned, 2, "both fixture rows must exist in {table}");
        let mut tx = member(&probe.a).await;
        let own: i64 = sqlx::query_scalar(&q).fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(own, 1, "{table} must show the owning account its row and nothing else");
        probe.owner.execute(format!("ALTER TABLE {table} DISABLE ROW LEVEL SECURITY").as_str()).await.unwrap();
        let mut tx = member(&probe.a).await;
        let leaked: i64 = sqlx::query_scalar(&q).fetch_one(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        probe.owner.execute(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY").as_str()).await.unwrap();
        assert_eq!(leaked, 2, "RLS-off control must reveal both rows in {table}");
    }
    probe.teardown().await;
}
