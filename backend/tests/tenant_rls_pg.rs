//! Integration test for the TENANT-plane RLS backstop (migration
//! `migrations_tenant/0009_tenant_rls`).
//!
//! A tenant database holds exactly one org, so cross-org isolation is already
//! physical and there is nothing for a policy to prove there. The boundary this
//! suite exercises is the one that exists INSIDE a tenant database: the
//! member-ocean boundary. Member A's private reef must be invisible to member
//! B, and a team ocean must be invisible to a non-member — enforced by
//! Postgres, from a connection authenticated AS the low-privilege
//! `coralswarm_tenant_app` role, not by the application.
//!
//! # Why this suite is built the way it is
//!
//! A policy suite that passes because the connection had `BYPASSRLS` all along
//! — or because the fixture was never written, or because the query hit an
//! empty table — is worse than no suite: it is a green check that certifies
//! nothing. Four structural defenses against that, all of them assertions
//! rather than comments:
//!
//! 1. **The connection is proven non-privileged before anything else runs**:
//!    `rolbypassrls = false` AND `current_user = 'coralswarm_tenant_app'` AND
//!    the role owns no table in `public` (a table owner is exempt from
//!    `ENABLE`-only RLS, which would make all 52 policies no-ops — see 0009 §7).
//!    It is a REAL separate login, not `SET ROLE` inside the owner's session,
//!    so it exercises the exact connection path the Phase-2 cutover creates.
//!
//! 2. **Every denial carries its own negative control.** For each of the 32
//!    covered tables, [`prove_denial_and_negative_control`] asserts B sees zero
//!    forbidden rows, then DISABLEs row-level security on that one table and
//!    asserts the same query now returns them, then re-enables and asserts the
//!    denial is back. Green therefore means "the row exists, this connection
//!    can reach the table, and RLS is the only reason it is hidden" — the
//!    assertion cannot pass vacuously. (Note the control has to be DISABLE, not
//!    `DROP POLICY`: RLS enabled with no policy at all is deny-everything, so
//!    dropping the policy would leave the test green for the wrong reason.
//!    `atoms` and `op_log` additionally get a `USING (true)` swap, which
//!    isolates the PREDICATE as the thing doing the denying.)
//!
//! 3. **The roster is asserted as a set**, not spot-checked: exactly the
//!    tables in `RLS_TABLES` carry RLS, the documented policy count exists, and no table
//!    carries `FORCE` (0009 §7 explains why FORCE is deliberately absent, and
//!    an accidental `FORCE` would break the SECURITY DEFINER predicate).
//!
//! 4. **Inertness is asserted too.** The owner role still sees every ocean,
//!    which is what makes this migration safe to land before the cutover.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights AND permission to `CREATE ROLE`/`ALTER ROLE`), same
//! scratch-DB pattern as `consumer_rls_pg.rs` / `tenant_migrate_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test tenant_rls_pg

mod support;

#[path = "support/retained_database.rs"]
mod retained_database;

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

/// The exact roster carrying a `member_ocean_isolation`-shaped policy.
/// Migration 0009 itself protects 31 of the tenant tree's 42 tables it
/// shipped against; `0013_slack_watched_channels` (added later, paired with
/// control's `0101_slack_watched_channels`, docs/connectors/slack.md §2/§4)
/// makes 32. Keep in lockstep with every migration that adds such a policy
/// and with `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES`.
const RLS_TABLES: &[&str] = &[
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
    // 0177: funded receipts billing recovery has not confirmed; system only.
    "managed_receipt_recovery_queue",
    // 0152: system mutation plus member-visible embedding input metadata.
    "managed_embedding_inputs",
    // managed_enrichment scaffold: system mutation plus member-visible input metadata.
    "managed_enrichment_inputs",
    // Before-floor overrides: operator-written; system or member SELECT only.
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
    "read_models",
    "read_model_triggers",
    "read_model_graph_fences",
    "read_model_user_fences",
    "library_node_evidence",
    "library_node_item_evidence",
    "library_evidence_changes",
    "library_evidence_state",
    // 0196: listing facts, independent events and guarded generation storage.
    "library_listing_state",
    "library_listing_changes",
    "library_listing_generations",
    "library_listing_source_heads",
    "library_listing_staged_sources",
    "library_listing_outcomes",
    "library_listing_staged_outcomes",
    "answer_temporal_receipts",
    // P8 0143: purge outbox/dependencies/reviews, selected-evidence consent
    // and known-new upload cleanup (system-actor-only).
    "source_purge_events",
    "source_purge_dependencies",
    "source_purge_reviews",
    "selected_evidence_publications",
    "unattached_capture_uploads",

    "activity_events",
    "atoms",
    "coding_session_shares",
    "coding_sessions",
    // Composio tenant 0136, anchored on graph_id; the roster had drifted since its merge.
    "composio_capture_scopes",
    // 0023 — the tenant half of control's 0112. Anchored via
    // `tenant_rls_visible_graph_ids()` through the owning graph, same shape
    // as every other content table here.
    "conflict_dismissal",
    "conversation_messages",
    "conversation_oceans",
    "conversation_surfaced_edges",
    "conversations",
    // 0033 — the tenant half of control's 0126 (evaluation ledger; graph-
    // keyed member_ocean_isolation like relationship_edge).
    "evaluation_ledger",
    "graphs",
    // 0024 — the tenant half of control's 0113. Anchored directly on
    // `graph_id` via `tenant_rls_visible_graph_ids()`, same shape as
    // `activity_events`/`atoms`.
    "journal_rollups",
    // 0218 — the tenant half of control's 0404 (Today gist). Anchored
    // directly on `graph_id`, same shape as `journal_rollups`.
    "journal_gists",
    "llm_usage",
    // 0055 — the tenant half of control's 0149 (usage-metering PR4's
    // reserve/finalize choke point). Anchored DIRECTLY on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same predicate as `llm_usage`'s own
    // policy on this plane.
    "llm_usage_pending",
    // 0047 — the tenant half of control's 0140 (MCP spec 2026-07-28 PR 2's
    // task-based `ask_ocean`). Anchored DIRECTLY on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same shape as `reef_judgement`.
    "mcp_tasks",
    "mcp_tool_calls",
    "meeting_shares",
    "meeting_transcripts",
    // 0032_membership_events (paired with control's 0125) — the team-roster
    // audit trail, anchored identically to `team_members`/`team_seat_grants`:
    // `team_id -> teams.graph_id -> tenant_rls_visible_graph_ids()`.
    "membership_events",
    "message_feedback",
    // 0067 (m365 enterprise train PR 3.8, renumbered from the originally-
    // announced tenant 0066 after #686 merged holding that number first for
    // `gmail_watched_threads`) — tenant twin of control's 0189: per-member
    // Microsoft Calendar sync bookkeeping. Anchored on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same shape as `gmail_watched_threads`.
    "microsoft_calendar_sync_state",
    "node_atoms",
    "node_merges",
    "node_promotions",
    "nodes",
    "non_supersession_pair",
    "op_log",
    // 0070 (m365 enterprise train PR 3.4, paired with control's 0192) —
    // tenant twin of `microsoft_calendar_sync_state` above, but OPT-IN
    // (never default-on) everything-mode enablement. Anchored on `graph_id`
    // via `tenant_rls_visible_graph_ids()`, same shape.
    "outlook_everything_mode",
    // 0074 (gsuite enterprise train PR G2.4, paired with control's 0197) —
    // the SECOND implementation of the same opt-in everything-mode
    // carve-out, this time for Gmail. Anchored on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same shape as
    // `outlook_everything_mode` directly above.
    "gmail_everything_mode",
    "outlook_watched_conversations",
    "outlook_workflows",
    "projects",
    "query_history",
    // 0066 (gsuite Train G2 PR 3, renumbered from the originally-announced
    // 0065 after #683 merged holding that number first) — tenant twin of
    // control's 0186: per-member Gmail opt-in thread tracking, structural
    // copy of `outlook_watched_conversations`. Anchored on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same shape.
    "gmail_watched_threads",
    // 0073 (gsuite Train G4 PR 3) — the tenant half of control's 0198:
    // per-space Chat destination table. Anchored on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, with an explicit `WITH CHECK` from
    // the start (design doc §1.8 — #692's USING-only finding is
    // deliberately NOT repeated here).
    "google_chat_watched_spaces",
    // 0045 — the tenant half of control's 0138 (slice 0 T2 identifier
    // awareness). Anchored on its one `node_id` column via
    // `tenant_rls_visible_graph_ids()`, same shape as `conflict_dismissal`.
    "reef_identifiers",
    // 0046 — the tenant half of control's 0139 (judgement-graph resolver
    // slice PR1: schema + Store plumbing only, no worker/route caller yet).
    // Anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()` —
    // `node_a_id`/`node_b_id` are bare UUID with no FK to `nodes` (see that
    // migration's header), unlike every other node-endpoint-anchored table
    // in this roster.
    // 0157: authenticated, same-graph, currently supported human identity corrections.
    "reef_identity_corrections",
    "reef_judgement",
    "relationship_edge",
    "slack_watched_channels",
    "slack_huddle_consents",
    // `slack_unmatched_email_notices` (formerly here, tenant migration 0093,
    // chat-per-user plan PR 6) is DELETED, not merely removed from this
    // roster — see control migration 0227's own "Control-only, no
    // tenant-tree twin" header section (corrected 2026-09-20). The table has
    // no tenant-tree twin at all now, so there is nothing here for this
    // suite to stand up an RLS fixture against.
    // 0095 (paired with control's 0235, GitHub connector PR A) — tenant
    // twin, NULL-friendly member_ocean_isolation for the pending-row state.
    "github_watched_repos",
    // 0096 (paired with control's 0237, GitHub connector PR B part 2, the
    // summary tier) — tenant twin, anchored DIRECTLY on graph_id via
    // tenant_rls_visible_graph_ids(), WITHOUT the NULL-friendly branch
    // github_watched_repos needs (this table's graph_id is NOT NULL).
    "github_code_groups",
    // 0081 (#871) — tenant twin of control's 0213, byte-identical policy
    // text. The standing publication CONSENT row and the per-atom
    // publication RECEIPTS underneath it; both shipped RLS-enabled with a
    // policy and neither was added to this roster.
    //
    // `source_connections`' policy is `account_id = app.account_id AND
    // source_graph_id IN (SELECT id FROM graphs) AND target_graph_id IN
    // (...)`, so on THIS plane it inherits `graphs`' own
    // `member_ocean_isolation` through both graph conjuncts — which is
    // precisely why `services::source_publication::sweep_due`'s Org arm needs
    // `begin_tenant_system_scoped` and not merely `begin_account_scoped`.
    //
    // `source_connection_atoms` anchors one hop up
    // (`connection_id IN (SELECT id FROM source_connections)`).
    //
    // Both have fixture rows below. Their roster probes use the SAME real
    // account for A and B via `count_roster_probe_as`, so a missing account
    // cannot fake member isolation or fail the strict account UUID cast after
    // a pooled SET LOCAL has reset to ''. A must see the row; B must not,
    // except while that table's RLS is deliberately disabled as the control.
    "source_connection_atoms",
    "source_connections",
    "supersession_edge",
    "team_keys",
    "team_member_key_envelopes",
    "team_members",
    // 0017 — the tenant half of control's 0106. Anchored `team_id -> teams ->
    // graph_id` through `tenant_rls_visible_graph_ids()`, exactly like
    // `team_members` above: a seat grant is reachable precisely when the team
    // ocean it seats somebody into is.
    "team_seat_grants",
    // 0064 (m365 enterprise train PR 4.1): the durable founding marker —
    // same tenant policy anchor as team_seat_grants.
    "seat_grant_sources",
    "teams",
    "teams_watched_resources",
    "transcript_corrections",
    // 0069 (m365 enterprise train PR 4.3): the query-audit disclosure log.
    // NOT a member-ocean-anchored table (no single `graph_id` — see its own
    // migration header) — SELECT is `tenant_rls_is_system()`-only, so no
    // ordinary member session (A's or B's) can read a row here at all. The
    // roster's generic probe still proves something real: B's session
    // denied (trivially, along with everyone else's) AND the negative
    // control (RLS disabled) sees the row, so the fixture is proven
    // non-vacuous exactly like every other table here.
    "query_audit_log",
    // 0071 (m365 enterprise train PR 4.2, plan decision #11 part 1): the
    // connector compliance audit log. NOT a member-ocean-anchored table (no
    // `graph_id` — `subject`/`detail` are bare ids/counts) — SELECT is
    // `tenant_rls_is_system()`-only, same shape as `query_audit_log`
    // (0069, sibling PR 4.3). See the "── connector_audit_log-specific"
    // block below for the dedicated append-only + system-only-SELECT proof.
    "connector_audit_log",
    // 0116 — tenant twin of control's 0277: the Gmail trash/spam retraction
    // ledger. `member_ocean_isolation` anchored on `graph_id` via
    // `tenant_rls_visible_graph_ids()`, same shape as `gmail_watched_threads`.
    "gmail_hidden_messages",
    // 0105 / 0107 / 0109 / 0111 — the per-meeting opt-in capture tables
    // (tenant twins of control's 0260 / 0263 / 0266 / 0269). NOT
    // member-ocean-anchored: each policy is keyed on `app.account_id`
    // (`teams_meeting_installations`) or on `app.account_id` AND the owning
    // `app.user_id` (the other four). `count_as` sets only `app.user_id`, so
    // the generic probe's denial is partly trivial here (`account_id = NULL`
    // hides the row from everyone) — the same standing caveat
    // `source_connections` / `query_audit_log` carry. The negative control
    // with RLS disabled still proves each fixture row is real.
    "google_meet_captures",
    "outlook_capture_receipts",
    "outlook_schedule_intents",
    "teams_meeting_captures",
    "teams_meeting_installations",
    // 0216 — tenant twin of control's 0401 (connectors redesign B1): per-member
    // connector source state, keyed on `app.account_id` like
    // `slack_huddle_consents` (`count_as` sets only `app.user_id`, so the
    // generic probe's denial is trivial; the negative control still proves the
    // fixture row is real).
    "connector_sources",
];

/// The tenant tables 0009 deliberately leaves uncovered (§6(f)), minus
/// `sync_cursors`, which tenant 0210 dropped with the desktop app. Asserted
/// explicitly so "we forgot one" and "we decided not to" stay distinguishable:
/// adding a policy to one of these without updating this list fails the roster
/// assertion, and so does silently dropping one from the covered set.
const DELIBERATELY_UNCOVERED: &[&str] = &[
    "account_keys",
    "connector_identities",
    "graph_subscriptions",
    "jobs",
    "oauth_tokens",
    "queries",
    "slack_huddle_captures",
    "slack_integrations",
    // NEW in migration 0008 (PR #349). Uncovered by POLICY on purpose — it gets
    // an outright `REVOKE ALL` instead, which is stronger. Asserted separately
    // by `revoked_forensic_ledger_is_unreadable_by_the_app_role` below.
    "supersession_edge_cross_ocean_deletions",
    "tenant_meta",
    // NEW in migration 0018 (integration/graph-temporal merge). Same shape as
    // `supersession_edge_cross_ocean_deletions` above — a fold record names
    // two ALREADY-COLLAPSED node ids, one of which no longer exists to
    // resolve an ocean from, so it gets the same outright `REVOKE ALL`
    // instead of a policy. Asserted separately by
    // `revoked_node_case_fold_log_is_unreadable_by_the_app_role` below.
    "node_case_fold_log",
];

/// Set only on this test's ephemeral scratch database's cluster, for the
/// duration of the test. `coralswarm_tenant_app` is created with NO password by
/// the migration (0009 §8); this is not a real credential and is never used
/// against any deploy.
const APP_ROLE_TEST_PASSWORD: &str = "orgt_tenant_rls_test_only_pw";

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

/// `ALTER ROLE coralswarm_tenant_app WITH PASSWORD ...`, retried.
///
/// Roles live in `pg_authid`, a CLUSTER-WIDE catalog, and the three tests in
/// this file each create their own scratch database and then set this password
/// — concurrently, by default, since cargo runs test fns in parallel. That is
/// the same shared-catalog race migration 0009 documents and retries in its own
/// role block, and it bites here for the same reason: the advisory lock the
/// migration takes is scoped to the CURRENT DATABASE, so three sessions on
/// three different scratch DBs serialise against nothing.
///
/// Observed as an intermittent `expect("set test-only password")` panic in
/// roughly one run in three. Retrying is safe because the statement is
/// idempotent — it sets the same value every time.
async fn set_app_role_password(owner: &PgPool) {
    let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'");
    let mut last: Option<sqlx::Error> = None;
    for attempt in 0..10u32 {
        match sqlx::query(&stmt).execute(owner).await {
            Ok(_) => return,
            Err(e) => {
                last = Some(e);
                // Jittered, like the migration's own loop: a fixed ramp makes
                // racing sessions retry in lockstep and collide again.
                let base = 50 * u64::from(attempt + 1);
                let jitter = u64::from(attempt * 17 % 40);
                tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
            }
        }
    }
    panic!("set test-only password on coralswarm_tenant_app after 10 attempts: {last:?}");
}

/// A pool connected AS `coralswarm_tenant_app` — a real, separate login, which
/// is what makes this suite exercise the same connection path the Phase-2
/// cutover creates rather than a `SET ROLE` that could differ in subtle ways.
async fn app_role_pool(db_url: &str) -> PgPool {
    let opts = PgConnectOptions::from_str(db_url)
        .expect("parse scratch db url")
        .username("coralswarm_tenant_app")
        .password(APP_ROLE_TEST_PASSWORD);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_tenant_app")
}

/// Every id the fixture creates. One org (`account`), two members, and four
/// oceans: the org commons, A's private reef, B's private reef, and a team
/// ocean A belongs to and B does not.
struct Fx {
    account: Uuid,
    user_a: Uuid,
    user_b: Uuid,
    g_commons: Uuid,
    g_a: Uuid,
    g_b: Uuid,
    g_team: Uuid,
    /// The `teams.id` primary key itself — distinct from `g_team` (that
    /// team's ocean/`graphs.id`). Needed by the org-admin governance test
    /// below, which has to name the exact row for `SELECT ... FOR UPDATE`.
    team: Uuid,
}

/// Seeds exactly ONE forbidden row per covered table, all of them anchored to a
/// reef member B cannot reach (A's private ocean, or the team ocean B is not a
/// member of). That uniformity is what lets the probe for 31 of the 32 tables
/// be the same trivial `SELECT count(*)` — if B can see ANY row in those
/// tables, the boundary leaked.
///
/// Executed as the OWNER, which is exempt from `ENABLE`-only RLS, so the
/// fixture itself is never shaped by the policies under test.
async fn seed(owner: &PgPool) -> Fx {
    let fx = Fx {
        account: Uuid::new_v4(),
        user_a: Uuid::new_v4(),
        user_b: Uuid::new_v4(),
        g_commons: Uuid::new_v4(),
        g_a: Uuid::new_v4(),
        g_b: Uuid::new_v4(),
        g_team: Uuid::new_v4(),
        team: Uuid::new_v4(),
    };
    // Node ids, ordered so `non_supersession_pair`'s `node_a_id < node_b_id`
    // CHECK is satisfiable without sorting at insert time.
    let mut nodes = [Uuid::new_v4(), Uuid::new_v4()];
    nodes.sort();
    let (node_1, node_2) = (nodes[0], nodes[1]);

    let atom = Uuid::new_v4();
    // The published COPY of `atom` living in the team ocean, and the standing
    // connection that put it there — the `source_connections` /
    // `source_connection_atoms` fixture (tenant 0081, #871). Modelled the way
    // a real publication is: source = A's member ocean, target = the team
    // ocean A belongs to and B does not.
    let published_atom = Uuid::new_v4();
    let source_connection = Uuid::new_v4();
    let conversation = Uuid::new_v4();
    let team_key = Uuid::new_v4();
    // A syntactically-valid `question_digest` for the `query_audit_log`
    // fixture row (0069's CHECK constraint: `^[0-9a-f]{64}$`) — content is
    // irrelevant to this suite, only that the CHECK is satisfied.
    let qad = "a".repeat(64);
    // `github_watched_repos`/`github_code_groups` fixture rows (GitHub
    // connector PR A / PR B part 2). `github_repo` is the watch row's own
    // id, needed by `github_code_groups.watched_repo_id`'s FK.
    let github_repo = Uuid::new_v4();

    let Fx {
        account,
        user_a,
        user_b,
        g_commons,
        g_a,
        g_b,
        g_team,
        team,
    } = fx;

    // One script, so a failure points at a line rather than at "some insert".
    // Every interpolated value is a Rust-generated `Uuid`, never external
    // input.
    let sql = format!(
        r#"
-- ── Oceans ────────────────────────────────────────────────────────────────
INSERT INTO graphs(id, account_id, name, kind, is_primary)
  VALUES ('{g_commons}', '{account}', 'commons', 'org', true);
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_a}', '{account}', 'A private reef', 'member', '{user_a}');
INSERT INTO graphs(id, account_id, name, kind, owner_user_id)
  VALUES ('{g_b}', '{account}', 'B private reef', 'member', '{user_b}');
INSERT INTO graphs(id, account_id, name, kind)
  VALUES ('{g_team}', '{account}', 'team ocean', 'team');

-- ── Team ocean: A is a member, B is not ───────────────────────────────────
INSERT INTO teams(id, account_id, graph_id, name, created_by)
  VALUES ('{team}', '{account}', '{g_team}', 'Team One', '{user_a}');
INSERT INTO team_members(team_id, user_id, role) VALUES ('{team}', '{user_a}', 'admin');
-- The roster audit trail (0032_membership_events). Mirrors exactly what
-- `create_team` writes for the seeding admin, so the negative control at the
-- bottom of this suite has a real row to prove `membership_events` isolation
-- is non-vacuous rather than "the probe found nothing either way".
INSERT INTO membership_events(team_id, actor_user_id, target_user_id, action, new_role)
  VALUES ('{team}', '{user_a}', '{user_a}', 'created', 'admin');
INSERT INTO team_seat_grants(account_id, team_id, user_id, source)
  VALUES ('{account}', '{team}', '{user_a}', 'slack:C-FIXTURE');
-- The durable founding marker (0064, m365 PR 4.1) — a real row so the
-- negative control proves `seat_grant_sources` isolation non-vacuously.
INSERT INTO seat_grant_sources(account_id, team_id, source)
  VALUES ('{account}', '{team}', 'slack:C-FIXTURE');
-- Calendar sync state (0067, m365 PR 3.8) — anchored on A's private ocean so
-- the negative control proves `microsoft_calendar_sync_state` isolation
-- non-vacuously (B must see 0 rows; RLS-off must see 1).
INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id)
  VALUES ('{account}', '{user_a}', '{g_a}');
-- Everything-mode opt-in state (0070, m365 enterprise train PR 3.4) —
-- anchored on A's private ocean so the negative control proves
-- `outlook_everything_mode` isolation non-vacuously (B must see 0 rows;
-- RLS-off must see 1).
INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id, enabled)
  VALUES ('{account}', '{user_a}', '{g_a}', true);
-- Gmail everything-mode opt-in state (0074, gsuite enterprise train PR
-- G2.4, the SECOND implementation of the same carve-out) — anchored on A's
-- private ocean so the negative control proves `gmail_everything_mode`
-- isolation non-vacuously (B must see 0 rows; RLS-off must see 1).
INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled)
  VALUES ('{account}', '{user_a}', '{g_a}', true);
INSERT INTO team_keys(id, graph_id, created_by) VALUES ('{team_key}', '{g_team}', '{user_a}');
INSERT INTO team_member_key_envelopes(team_key_id, user_id, wrapped_key)
  VALUES ('{team_key}', '{user_a}', '\x00'::bytea);

-- ── Content, all of it inside A's private reef ────────────────────────────
INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, "timestamp")
  VALUES ('{atom}', '{g_a}', 'test', 'a-secret-1', 'blob', now());
INSERT INTO nodes(node_id, graph_id, type, title, body)
  VALUES ('{node_1}', '{g_a}', 'entity', 'A secret node 1', '{{}}'),
         ('{node_2}', '{g_a}', 'entity', 'A secret node 2', '{{}}');
INSERT INTO node_atoms(node_id, atom_id) VALUES ('{node_1}', '{atom}');
INSERT INTO relationship_edge(graph_id, src_node_id, predicate, dst_node_id)
  VALUES ('{g_a}', '{node_1}', 'relates_to', '{node_2}');
INSERT INTO evaluation_ledger(graph_id, subject_node_id, predicate, confidence)
  VALUES ('{g_a}', '{node_1}', 'expert_in', 0.6);
-- `graph_id` is NOT NULL as of migration 0008 (PR #349); this row is what
-- proves the DIRECT anchor denies, where the pre-#349 fixture proved the
-- `superseded_id -> nodes` hop did.
INSERT INTO supersession_edge(superseded_id, superseder_id, source, graph_id)
  VALUES ('{node_1}', '{node_2}', 'manual', '{g_a}');
INSERT INTO non_supersession_pair(node_a_id, node_b_id) VALUES ('{node_1}', '{node_2}');
INSERT INTO conflict_dismissal(node_a_id, node_b_id, verdict, model, prompt_version)
  VALUES ('{node_1}', '{node_2}', 'different', 'test-model', 'test-prompt-v1');
INSERT INTO node_merges(graph_id, keep_id, dup_id, dup_snapshot)
  VALUES ('{g_a}', '{node_1}', '{node_2}', '{{}}');
INSERT INTO node_promotions(source_node_id, target_graph_id, target_node_id, from_version)
  VALUES ('{node_1}', '{g_a}', '{node_2}', 1);
-- 0045 (T2 identifier awareness) — anchored on its one `node_id` column,
-- same shape as `conflict_dismissal` above.
INSERT INTO reef_identifiers(graph_id, node_id, kind, value_norm, source)
  VALUES ('{g_a}', '{node_1}', 'email', 'a-secret@example.com', 'test');
-- 0046 (judgement-graph resolver slice PR1) — anchored DIRECTLY on
-- `graph_id`, not via node_1/node_2's own FK (this table has none on its
-- endpoints — see that migration's header). `node_1 < node_2` already holds
-- from the sort above, satisfying `reef_judgement_ordered_pair`.
INSERT INTO reef_judgement(graph_id, node_a_id, node_b_id, verdict, source)
  VALUES ('{g_a}', '{node_1}', '{node_2}', 'positive', 'test');
-- 0047 (MCP spec 2026-07-28 PR 2, task-based `ask_ocean`) — anchored
-- DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()`, same shape as
-- `reef_judgement` above. `status` defaults to 'working', no result/error
-- needed for the negative-control probe.
INSERT INTO mcp_tasks(account_id, graph_id, owner_user_id, tool_name)
  VALUES ('{account}', '{g_a}', '{user_a}', 'ask_ocean');

INSERT INTO conversations(id, account_id, graph_id, user_id, title)
  VALUES ('{conversation}', '{account}', '{g_a}', '{user_a}', 'A secret chat');
INSERT INTO conversation_messages(conversation_id, seq, role, text)
  VALUES ('{conversation}', 1, 'user', 'a secret question');
INSERT INTO message_feedback(message_id, user_id, account_id, vote, note)
  SELECT id, '{user_a}', '{account}', -1, 'a secret note' FROM conversation_messages WHERE conversation_id = '{conversation}';
INSERT INTO conversation_oceans(conversation_id, graph_id) VALUES ('{conversation}', '{g_a}');
INSERT INTO conversation_surfaced_edges(conversation_id, edge_id)
  VALUES ('{conversation}', '{node_2}');

INSERT INTO meeting_transcripts(graph_id, source_id) VALUES ('{g_a}', 'a-meeting-1');
INSERT INTO transcript_corrections(graph_id, atom_id, source_id, surface_form, canonical)
  VALUES ('{g_a}', '{atom}', 'a-meeting-1', 'Aay', 'A');
INSERT INTO meeting_shares(graph_id, source_id, token, created_by)
  VALUES ('{g_a}', 'a-meeting-1', 'tok-a-meeting', '{user_a}');
INSERT INTO coding_sessions(graph_id, session_id) VALUES ('{g_a}', 'a-session-1');
INSERT INTO coding_session_shares(graph_id, session_id, token, created_by)
  VALUES ('{g_a}', 'a-session-1', 'tok-a-session', '{user_a}');
INSERT INTO projects(graph_id, project_key) VALUES ('{g_a}', 'a-project');

INSERT INTO activity_events(account_id, graph_id, event_type)
  VALUES ('{account}', '{g_a}', 'atom_created');
INSERT INTO llm_usage(account_id, graph_id, task, model)
  VALUES ('{account}', '{g_a}', 'extract', 'haiku');
-- Sibling of the `llm_usage` row above, and required for the same reason:
-- the roster probe asserts member A's ocean is hidden from member B, and
-- that assertion is VACUOUS without a row to be denied. The suite's own
-- negative control (re-running the probe with RLS disabled and requiring a
-- non-zero count) catches the omission rather than certifying nothing.
INSERT INTO llm_usage_pending(account_id, graph_id, task, provider, model, estimated_input_tokens)
  VALUES ('{account}', '{g_a}', 'extract', 'anthropic', 'haiku', 1);
INSERT INTO mcp_tool_calls(account_id, graph_id, tool, outcome)
  VALUES ('{account}', '{g_a}', 'search_atoms', 'ok');
INSERT INTO query_history(account_id, graph_id, query_text)
  VALUES ('{account}', '{g_a}', 'a secret query');
-- 0069 (m365 enterprise train PR 4.3): no `graph_id` column at all — the
-- ocean it names lives inside the `graphs_searched` array instead. Same
-- vacuity-guard reasoning as every other row here.
INSERT INTO query_audit_log(account_id, asked_by_user_id, question_digest, graphs_searched, atom_ids_cited)
  VALUES ('{account}', '{user_a}', '{qad}', ARRAY['{g_a}']::uuid[], ARRAY['{atom}']::uuid[]);
INSERT INTO op_log(op_id, entity, entity_id, op_type, updated_at, device_id, graph_id)
  VALUES (gen_random_uuid(), 'atom', '{atom}', 'upsert', now(), 'dev-a', '{g_a}');

INSERT INTO outlook_watched_conversations(account_id, graph_id, ingested_by, conversation_id)
  VALUES ('{account}', '{g_a}', '{user_a}', 'AAMk-secret');
-- gsuite Train G2 PR 3 (0066): structural copy of the outlook row just
-- above. Required for the same reason every other roster row here is —
-- CodeRabbit round 1 finding #7 on PR #686: `gmail_watched_threads` was
-- added to RLS_TABLES below with no fixture row, so the negative control
-- (`leaked > 0`) failed instead of proving anything.
INSERT INTO gmail_watched_threads(account_id, graph_id, ingested_by, thread_id)
  VALUES ('{account}', '{g_a}', '{user_a}', 'thread-secret');
-- gsuite Train G4 PR 3 (0073): a routed SPACE row (owner_user_id NULL —
-- the archetype CHECK only requires an owner for GROUP_CHAT/DIRECT_MESSAGE).
-- Required for the same reason every other roster row here is (CodeRabbit
-- round 1 finding #7 on PR #686, carried forward): a table added to
-- RLS_TABLES with no fixture row makes the negative control vacuous.
INSERT INTO google_chat_watched_spaces(account_id, graph_id, space_id, space_type, mode)
  VALUES ('{account}', '{g_a}', 'spaces/secret', 'SPACE', 'single');
INSERT INTO outlook_workflows(account_id, user_id, graph_id, name, filter)
  VALUES ('{account}', '{user_a}', '{g_a}', 'A workflow', '{{}}');
INSERT INTO teams_watched_resources(account_id, user_id, graph_id, kind)
  VALUES ('{account}', '{user_a}', '{g_a}', 'channel');
INSERT INTO slack_watched_channels(account_id, user_id, graph_id, workspace_id, channel_id)
  VALUES ('{account}', '{user_a}', '{g_a}', 'T-secret', 'C-secret');
-- 0071 (m365 enterprise train PR 4.2): no `graph_id` column at all — same
-- shape as `query_audit_log` (0069). Same vacuity-guard reasoning as every
-- other row here; the generic probe still proves something real for this
-- table (B's session denied, same as everyone else's, AND the negative
-- control with RLS disabled sees the row) even though SELECT is actually
-- `tenant_rls_is_system()`-only — see the dedicated
-- connector_audit_log-specific block below for the stronger, table-specific
-- proof (append-only, system-actor-only SELECT).
INSERT INTO connector_audit_log(account_id, actor_user_id, actor_kind, action, provider, subject)
  VALUES ('{account}', '{user_a}', 'user', 'install', 'slack', 'T-secret');
-- 0081 (#871): the standing publication consent + one per-atom receipt.
-- Required for the same reason every other roster row here is — a table in
-- RLS_TABLES with no fixture row makes the negative control vacuous
-- (CodeRabbit round 1 finding #7 on PR #686, carried forward). Source is A's
-- member ocean, target is the team ocean, and the receipt points `atom` (in
-- A's ocean) at `published_atom` (its copy in the team ocean), so BOTH graph
-- conjuncts of `source_connection_scope` name a real, member-isolated ocean.
INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, "timestamp")
  VALUES ('{published_atom}', '{g_team}', 'test', 'a-secret-1', 'blob', now());
INSERT INTO source_connections(id, account_id, owner_id, source_graph_id, target_graph_id,
                               source_type, source_id, label, mode, include_history)
  VALUES ('{source_connection}', '{account}', '{user_a}', '{g_a}', '{g_team}',
          'test', 'a-secret-1', 'a-secret-1', 'once', true);
INSERT INTO source_connection_atoms(connection_id, atom_id, revision, target_atom_id)
  VALUES ('{source_connection}', '{atom}', 1, '{published_atom}');
-- GitHub connector PR A (0095) — required for the same reason every other
-- roster row here is (CodeRabbit round 1 finding #7 on PR #686, carried
-- forward): a table in RLS_TABLES with no fixture row makes the negative
-- control vacuous. Anchored on A's private ocean.
INSERT INTO github_watched_repos(id, account_id, installation_id, repo_id, owner, name,
                                  default_branch, active, graph_id, owner_user_id, destination_kind)
  VALUES ('{github_repo}', '{account}', 9001, 500100200, 'acme', 'secret-repo',
          'main', true, '{g_a}', '{user_a}', 'personal');
-- GitHub connector PR B part 2 (0096) — same vacuity-guard reasoning. A
-- single detected package group on the repo above, in A's private ocean.
INSERT INTO github_code_groups(account_id, watched_repo_id, graph_id, path, kind, file_count, files_digest)
  VALUES ('{account}', '{github_repo}', '{g_a}', 'src', 'package', 3, 'digest-fixture');
-- Tenant 0116 — Gmail trash/spam retraction ledger, anchored on A's private
-- ocean. Same vacuity-guard reasoning as every other row here.
INSERT INTO gmail_hidden_messages(account_id, graph_id, owner_user_id, thread_id, message_id)
  VALUES ('{account}', '{g_a}', '{user_a}', 'thread-secret', 'msg-secret');
-- Tenant 0105 / 0107 / 0109 / 0111 — the per-meeting opt-in capture tables,
-- all owned by member A. Same vacuity-guard reasoning.
INSERT INTO teams_meeting_installations(account_id, tenant_id, installation_id, context_id)
  VALUES ('{account}', 'aad-tenant', 'inst-secret', 'ctx-secret');
INSERT INTO teams_meeting_captures(account_id, user_id, aad_id, tenant_id, meeting_id,
                                   occurrence_start, occurrence_end, graph_id, source)
  VALUES ('{account}', '{user_a}', 'aad-a', 'aad-tenant', 'meeting-secret',
          '2026-07-15T09:00:00Z', '2026-07-15T10:00:00Z', '{g_a}', 'explicit_meeting');
INSERT INTO outlook_capture_receipts(account_id, user_id, nonce)
  VALUES ('{account}', '{user_a}', gen_random_uuid());
INSERT INTO outlook_schedule_intents(account_id, user_id, tenant_id, aad_id, graph_id)
  VALUES ('{account}', '{user_a}', 'aad-tenant', 'aad-a', '{g_a}');
INSERT INTO google_meet_captures(account_id, owner_user_id, meeting_code)
  VALUES ('{account}', '{user_a}', 'abc-defg-hij');
-- Tenant 0124 — Slack huddle disclosure/consent metadata, account-keyed. Same
-- vacuity-guard reasoning as every other row here.
-- Tenant 0136/0146 — Composio workspace capture in A's private ocean. Same vacuity-guard reasoning.
INSERT INTO composio_capture_scopes(graph_id, account_id, owner_user_id, connection_id, service, provider_namespace)
  VALUES ('{g_a}', '{account}', '{user_a}', gen_random_uuid(), 'linear', 'linear:workspace:secret');
INSERT INTO slack_huddle_consents(id, account_id, workspace_id, channel_id, huddle_thread_ts,
                                  fingerprint, non_attendee_count, capture_payload)
  VALUES (gen_random_uuid(), '{account}', 'T-secret', 'C-secret', '1700000000.000100',
          'fp-secret', 1, '{{}}'::jsonb);
-- Tenant 0216 — per-member connector source state, account-keyed. Same
-- vacuity-guard reasoning as every other row here.
INSERT INTO connector_sources(account_id, owner_user_id, provider, source, enabled)
  VALUES ('{account}', '{user_a}', 'slack', 'channels', true);
-- P8 tenant 0143 — purge outbox, review markers, selected-evidence consent and
-- known-new upload cleanup, all owned by A's private ocean. Same vacuity-guard
-- reasoning as every other row here. The consent row skips only the
-- source/target qualification trigger (it needs real published versions,
-- which this policy probe does not exercise); the RLS policy is untouched.
WITH ev AS (
  INSERT INTO source_purge_events(account_id, owner_user_id, graph_id, source_item_id, event_kind)
    VALUES ('{account}', '{user_a}', '{g_a}', gen_random_uuid(), 'deleted') RETURNING event_id)
INSERT INTO source_purge_dependencies(event_id, kind, graph_id, object_id)
  SELECT event_id, 'atom', '{g_a}', '{atom}' FROM ev;
INSERT INTO source_purge_reviews(account_id, graph_id, object_id, kind, review_status)
  VALUES ('{account}', '{g_a}', '{node_1}', 'node', 'confirmed');
ALTER TABLE selected_evidence_publications DISABLE TRIGGER selected_publication_qualified;
INSERT INTO selected_evidence_publications(publication_id, account_id, owner_user_id, request_id,
    source_graph_id, source_item_id, source_version_id, selection, passage_ids, target_graph_id, selected_sha256)
  VALUES (gen_random_uuid(), '{account}', '{user_a}', gen_random_uuid(), '{g_a}', gen_random_uuid(),
    gen_random_uuid(), '[]'::jsonb, ARRAY[gen_random_uuid()], '{g_team}', repeat('a', 64));
ALTER TABLE selected_evidence_publications ENABLE TRIGGER selected_publication_qualified;
INSERT INTO unattached_capture_uploads(account_id, graph_id, object_id, blob_key, capture_database, capture_xid)
  VALUES ('{account}', '{g_a}', '{atom}', '{g_a}/{atom}', current_database(), '1');
"#
    );
    owner.execute(sql.as_str()).await.expect("seed fixture");
    support::source_evidence::seed(owner, g_a, atom, node_1).await;
    owner.execute(format!("INSERT INTO library_node_evidence(graph_id,node_id,source_count,unknown_independence_record_count,atom_count_retained,atom_count_served,unmapped_legacy_count,admitted_unfiltered,has_source_link,title_visible,project_mention,work_eligible,stats_admitted,computed_version) VALUES('{g_a}','{node_1}',0,0,0,0,0,false,false,false,false,false,false,1); INSERT INTO library_node_item_evidence(graph_id,node_id,source_item_id,semantic_link,legacy_link,legacy_independent_version,legacy_synthetic,atom_count_retained,atom_count_served,project_mention) VALUES('{g_a}','{node_1}',gen_random_uuid(),false,false,false,false,0,0,false); INSERT INTO library_evidence_state(graph_id) VALUES('{g_a}')").as_str()).await.expect("seed Library evidence rows");
    // Real rows in BOTH private oceans: own-member visibility and reciprocal
    // denial cannot pass merely because one side has no cached contributions.
    seed_listing_facts(owner, g_a).await;
    seed_listing_facts(owner, g_b).await;
    // Seed compiled outputs after evidence mutations: permission reductions
    // retract prose, and the RLS probes require non-vacuous retained rows.
    owner.execute(format!(r#"INSERT INTO journal_rollups(account_id, user_id, graph_id, hour_start, period_start, summary, input_fingerprint, model)
  VALUES ('{account}', '{user_a}', '{g_a}', '2026-07-15T09:00:00Z', '2026-07-15T09:00:00Z', 'a secret hour', 'fp-fixture', 'test-model');
-- 0218 (Today gist): a member gist in A's ocean, same vacuity guard.
INSERT INTO journal_gists(account_id, graph_id, scope, user_id, granularity, period_start, period_end, tz, gist, input_fingerprint, model)
  VALUES ('{account}', '{g_a}', 'member', '{user_a}', 'day', '2026-07-15T00:00:00Z', '2026-07-16T00:00:00Z', 'UTC', '{{"done": ["a secret gist"]}}', 'fp-fixture', 'test-model');
"#).as_str()).await.expect("seed final journal RLS outputs");
    fx
}

/// Seed a historical listing contribution plus its stage/guard identities.
/// Object FKs are deliberately absent: counted OLD facts must survive raw
/// deletion. These are unready graph rows, not fabricated ready3 certificates.
async fn seed_listing_facts(owner: &PgPool, graph: Uuid) {
    let epoch = Uuid::new_v4();
    let item = Uuid::new_v4();
    let generation = Uuid::new_v4();
    let version = Uuid::new_v4();
    let outcome = Uuid::new_v4();
    let mut tx = owner.begin().await.expect("begin listing RLS fixture");
    sqlx::query("INSERT INTO library_listing_state(graph_id,epoch) VALUES($1,$2)")
        .bind(graph).bind(epoch).execute(&mut *tx).await.unwrap();
    let guard: i64 = sqlx::query_scalar(
        "INSERT INTO library_listing_changes(graph_id,source_item_id) VALUES($1,$2) RETURNING id",
    ).bind(graph).bind(item).fetch_one(&mut *tx).await.unwrap();
    tx.execute(format!(r#"
INSERT INTO library_listing_generations(graph_id,source_item_id,generation,epoch,guard_change_id,captured_change_ids,source_present,retained_count,served_count)
 VALUES('{graph}','{item}','{generation}','{epoch}',{guard},ARRAY[{guard}]::bigint[],true,1,1);
INSERT INTO library_listing_staged_sources(graph_id,source_item_id,epoch,generation,source_type,provider_namespace,source_id,origin,discovery_content_kind,current_version_id,selected_version_id,served_version_id,captured_at,title256,unknown_independence,raw_extraction_status,raw_next_start_char,retained_count,served_count)
 VALUES('{graph}','{item}','{epoch}','{generation}','note','rls:listing','listing-{item}',jsonb_build_object('service_id','fixture','transport','native','content_kind','note','container',jsonb_build_object('kind','project','id','{item}','label','listing fixture')),'note','{version}','{version}','{version}',TIMESTAMPTZ '2026-01-01T00:00:00Z','listing fixture',false,'done',0,1,1);
INSERT INTO library_listing_staged_outcomes(graph_id,semantic_atom_id,source_item_id,source_version_id,generation,captured_at,source_version_is_current)
 VALUES('{graph}','{outcome}','{item}','{version}','{generation}',TIMESTAMPTZ '2026-01-01T00:00:00Z',true);
-- Exercise the actual guarded transition/write path; do not disable triggers.
UPDATE library_listing_generations SET phase='upsert',build_complete=true
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
INSERT INTO library_listing_outcomes(graph_id,semantic_atom_id,source_item_id,source_version_id,generation,captured_at,source_version_is_current)
 SELECT graph_id,semantic_atom_id,source_item_id,source_version_id,generation,captured_at,source_version_is_current
 FROM library_listing_staged_outcomes WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
UPDATE library_listing_generations SET phase='prune',upsert_complete=true
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
UPDATE library_listing_generations SET phase='ack',prune_complete=true
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
UPDATE library_listing_generations SET phase='publish'
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
UPDATE library_listing_generations SET phase='complete',guard_change_id=NULL
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
INSERT INTO library_listing_source_heads SELECT * FROM library_listing_staged_sources
 WHERE graph_id='{graph}' AND source_item_id='{item}' AND generation='{generation}';
UPDATE library_listing_state SET node_count=1,nodes_complete=true WHERE graph_id='{graph}';
"#).as_str()).await.expect("seed guarded listing facts for RLS");
    tx.commit().await.expect("commit listing fixture with deferred parent checks");
}

/// The probe for one table: how many rows member B can see that B must not.
///
/// All but one of the tables (33 as of `mcp_tasks`, migration 0047) hold
/// ONLY forbidden rows (the fixture puts nothing in them that B is entitled
/// to), so a bare `count(*)` is the probe. `graphs` is the exception — B
/// legitimately sees the org commons and B's own reef — so its probe names
/// the two oceans B must never see.
fn probe_sql(table: &str, fx: &Fx) -> String {
    if table == "graphs" {
        format!(
            "SELECT count(*) FROM graphs WHERE id IN ('{}', '{}')",
            fx.g_a, fx.g_team
        )
    } else if table.starts_with("library_listing_") || ["library_node_evidence","library_node_item_evidence","library_evidence_changes","library_evidence_state"].contains(&table) {
        // Captures also append valid B/commons keys; probe only A/team rows.
        format!("SELECT count(*) FROM {table} WHERE graph_id IN ('{}','{}')",fx.g_a,fx.g_team)
    } else {
        format!("SELECT count(*) FROM {table}")
    }
}

/// Run `sql` as `coralswarm_tenant_app` inside a transaction scoped to
/// `user_id` (the `app.user_id` GUC migration 0009's policies read), and return
/// the single count it selects.
async fn count_as(app: &PgPool, user_id: Uuid, sql: &str) -> i64 {
    let mut tx = app.begin().await.expect("begin as tenant app role");
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(user_id.to_string())
        .execute(&mut *tx)
        .await
        .expect("set_config app.user_id");
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&mut *tx)
        .await
        .unwrap_or_else(|e| panic!("probe failed ({sql}): {e}"));
    tx.rollback().await.ok();
    n
}

/// The publication tables require BOTH account and member scope. Keep that
/// scope limited to those two roster probes: account-only tables elsewhere in
/// this inventory intentionally exercise denial without an account grant.
async fn count_roster_probe_as(app: &PgPool, user_id: Uuid, table: &str, fx: &Fx) -> i64 {
    let sql = probe_sql(table, fx);
    if !matches!(table, "source_connections" | "source_connection_atoms" | "read_models") {
        return count_as(app, user_id, &sql).await;
    }
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
        app, fx.account, user_id,
    )
    .await
    .expect("begin publication probe with same-account member scope");
    let n: i64 = sqlx::query_scalar(&sql)
        .fetch_one(&mut *tx)
        .await
        .unwrap_or_else(|e| panic!("publication probe failed ({sql}): {e}"));
    tx.rollback().await.expect("roll back publication probe scope");
    n
}

/// Same as [`count_as`], but additionally sets the org-admin GUC
/// (`app.is_org_admin`, migration 0032's `tenant_rls_is_org_admin()`) inside
/// the SAME transaction before running `sql`. Kept as a separate helper
/// rather than a flag on `count_as` so every OTHER call site in this file —
/// which must never accidentally acquire the org-admin grant — cannot do so
/// by a stray argument.
async fn count_as_org_admin(app: &PgPool, user_id: Uuid, sql: &str) -> i64 {
    let mut tx = app.begin().await.expect("begin as tenant app role");
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(user_id.to_string())
        .execute(&mut *tx)
        .await
        .expect("set_config app.user_id");
    sqlx::query("SELECT set_config('app.is_org_admin', 'true', true)")
        .execute(&mut *tx)
        .await
        .expect("set_config app.is_org_admin");
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&mut *tx)
        .await
        .unwrap_or_else(|e| panic!("probe failed ({sql}): {e}"));
    tx.rollback().await.ok();
    n
}

/// Match a policy refusal by SQLSTATE, never by message text. Same
/// convention as this file's sibling `routes_b5b_rls_pg.rs`'s helper of the
/// same name: a foreign-key violation (`23503`) or any other error class must
/// never be mistaken for a `WITH CHECK` refusal (`42501`).
fn is_rls_denied(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("42501"))
}

/// One row of `pg_policies`, captured verbatim.
///
/// This exists because of a CodeRabbit MAJOR on the first revision of this
/// file, and the finding is worth restating rather than just fixing: the
/// predicate-specific negative control below used to restore the policy by
/// HAND-WRITING a `CREATE POLICY ... USING (...)` statement copied from the
/// migration. That is a test asserting against a policy the test itself
/// authored. It silently drops whatever the migration actually declares —
/// command scope (`FOR ...`), role scope (`TO ...`), `AS RESTRICTIVE`, and any
/// `WITH CHECK` distinct from `USING`. Every assertion that ran afterwards —
/// including §4's `atoms` WRITE assertions — was then exercising the test's
/// policy, not the shipped one, so a wrong `WITH CHECK` in the migration would
/// still have produced a green suite.
///
/// That is the same defect class this whole PR exists to backstop (a check that
/// passes for a reason other than the one it claims), so the fix is structural:
/// capture what the migration actually created, restore FROM that capture, and
/// then assert the capture round-tripped.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicySnapshot {
    name: String,
    permissive: String,
    cmd: String,
    roles: Vec<String>,
    qual: Option<String>,
    with_check: Option<String>,
}

impl PolicySnapshot {
    /// Rebuild the exact `CREATE POLICY` statement this snapshot came from.
    ///
    /// Every clause is emitted from the captured catalog row, never from a
    /// literal in this file. `qual`/`with_check` are `NULL` for the command
    /// scopes that have no such clause (`INSERT` has no `USING`; `SELECT` and
    /// `DELETE` have no `WITH CHECK`), so each is emitted only when present.
    fn create_stmt(&self, table: &str) -> String {
        let mut stmt = format!(
            "CREATE POLICY {} ON {} AS {} FOR {} TO {}",
            self.name,
            table,
            self.permissive,
            self.cmd,
            self.roles.join(", ")
        );
        if let Some(q) = &self.qual {
            stmt.push_str(&format!(" USING ({q})"));
        }
        if let Some(w) = &self.with_check {
            stmt.push_str(&format!(" WITH CHECK ({w})"));
        }
        stmt
    }
}

/// One `pg_policies` row as sqlx hands it back, before it becomes a
/// [`PolicySnapshot`]. Named because the bare 6-tuple trips clippy's
/// `type_complexity`, and because the column ORDER here has to stay in lockstep
/// with the `SELECT` list below — a silent transposition of two adjacent
/// `Option<String>` columns (`qual` / `with_check`) would compile fine and
/// quietly swap a policy's read predicate with its write check.
type PolicyRow = (
    String,
    String,
    String,
    Vec<String>,
    Option<String>,
    Option<String>,
);

/// Every policy on `table`, as the database currently holds it.
async fn snapshot_policies(owner: &PgPool, table: &str) -> Vec<PolicySnapshot> {
    let rows: Vec<PolicyRow> =
        sqlx::query_as(
            "SELECT policyname, permissive, cmd, roles::text[], qual, with_check              FROM pg_policies WHERE schemaname = 'public' AND tablename = $1              ORDER BY policyname",
        )
        .bind(table)
        .fetch_all(owner)
        .await
        .unwrap_or_else(|e| panic!("snapshot policies on {table}: {e}"));
    rows.into_iter()
        .map(
            |(name, permissive, cmd, roles, qual, with_check)| PolicySnapshot {
                name,
                permissive,
                cmd,
                roles,
                qual,
                with_check,
            },
        )
        .collect()
}

/// Drop whatever policies `table` currently has and recreate exactly `snaps`.
///
/// Then re-snapshot and assert equality. That final assert is the point: it
/// makes a lossy restore impossible to miss, no matter how the reconstruction
/// in [`PolicySnapshot::create_stmt`] might drift from what Postgres accepts.
async fn restore_policies(owner: &PgPool, table: &str, snaps: &[PolicySnapshot]) {
    for existing in snapshot_policies(owner, table).await {
        owner
            .execute(format!("DROP POLICY {} ON {table}", existing.name).as_str())
            .await
            .unwrap_or_else(|e| panic!("drop policy {} on {table}: {e}", existing.name));
    }
    for snap in snaps {
        let stmt = snap.create_stmt(table);
        owner
            .execute(stmt.as_str())
            .await
            .unwrap_or_else(|e| panic!("restore policy on {table} via `{stmt}`: {e}"));
    }
    let restored = snapshot_policies(owner, table).await;
    assert_eq!(
        restored, snaps,
        "restoring `{table}`'s policies did not reproduce the migration's own definition. \
         The negative control below is only meaningful if what it restores is byte-identical \
         to what 0009 shipped — a lossy restore would leave every later assertion testing a \
         policy this test invented."
    );
}

/// The heart of the suite: assert the denial, then PROVE the assertion is not
/// vacuous by removing the control and watching the same probe go red, then
/// restore and re-assert.
///
/// The control is `DISABLE ROW LEVEL SECURITY`, not `DROP POLICY`, and that
/// choice is load-bearing: RLS enabled with zero policies denies EVERYTHING, so
/// dropping the policy would leave the probe returning 0 and the test green for
/// precisely the wrong reason. Disabling RLS is the true inverse — it proves
/// the fixture row exists, that this connection has the base SELECT grant on
/// the table, and that row-level security is the only thing hiding it.
async fn prove_denial_and_negative_control(owner: &PgPool, app: &PgPool, table: &str, fx: &Fx) {
    let sql = probe_sql(table, fx);

    if matches!(table, "source_connections" | "source_connection_atoms" | "read_models") {
        assert_eq!(
            count_roster_probe_as(app, fx.user_a, table, fx).await,
            1,
            "member A must read the publication fixture in `{table}` with the same account \
             scope as member B; deny-everyone is not member isolation"
        );
    }
    let denied = count_roster_probe_as(app, fx.user_b, table, fx).await;
    assert_eq!(
        denied, 0,
        "member B must not see any of member A's rows in `{table}` \
         (probe: {sql}) — the member-ocean boundary leaked"
    );

    owner
        .execute(format!("ALTER TABLE {table} DISABLE ROW LEVEL SECURITY").as_str())
        .await
        .unwrap_or_else(|e| panic!("disable RLS on {table}: {e}"));
    let leaked = count_roster_probe_as(app, fx.user_b, table, fx).await;
    owner
        .execute(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY").as_str())
        .await
        .unwrap_or_else(|e| panic!("re-enable RLS on {table}: {e}"));

    assert!(
        leaked > 0,
        "NEGATIVE CONTROL FAILED for `{table}`: with row-level security disabled the same probe \
         ({sql}) still returned {leaked} rows. The `denied == 0` assertion above is therefore \
         vacuous — the fixture row is missing, or this connection cannot read the table at all, \
         and this suite is certifying nothing. Fix the fixture before trusting the green."
    );

    let denied_again = count_roster_probe_as(app, fx.user_b, table, fx).await;
    assert_eq!(
        denied_again, 0,
        "`{table}` did not deny again after row-level security was re-enabled"
    );
}

#[tokio::test]
async fn tenant_rls_hides_each_members_ocean_from_every_other_member() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping tenant_rls_hides_each_members_ocean_from_every_other_member: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009");

    set_app_role_password(&owner).await;

    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;
    assert!(sqlx::query_scalar::<_,bool>("SELECT corpus_lifecycle_delete_repair_ready()")
        .fetch_one(&app).await.unwrap());
    assert!(!sqlx::query_scalar::<_,bool>("SELECT has_table_privilege(current_user,'_sqlx_migrations','SELECT')")
        .fetch_one(&app).await.unwrap(), "readiness must not broaden bookkeeping access");

    // ── 0. The connection is genuinely non-privileged ──────────────────────
    // Everything below is meaningless if this connection could bypass RLS, or
    // if the role owned the tables (an owner is exempt from ENABLE-only RLS —
    // 0009 §7, and the reason FORCE is deliberately absent).
    {
        let who: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(&app)
            .await
            .unwrap();
        assert_eq!(who, "coralswarm_tenant_app");

        let bypass: bool =
            sqlx::query_scalar("SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")
                .fetch_one(&app)
                .await
                .unwrap();
        assert!(
            !bypass,
            "coralswarm_tenant_app has BYPASSRLS — every policy in 0009 is a no-op and this \
             entire suite would pass without enforcing anything"
        );

        let owned: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner \
             WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r','p') \
               AND r.rolname = 'coralswarm_tenant_app'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            owned, 0,
            "coralswarm_tenant_app owns table(s) in public — a table owner is EXEMPT from \
             ENABLE-only RLS, so the policies would not apply to it"
        );
    }

    // L2: content-bearing answers need a real row for the negative control;
    // trigger subscriptions are deliberately public, non-content metadata.
    sqlx::query("INSERT INTO read_models(model,scope_key,version,account_id,graph_ids,viewer_user_id,compute_as) VALUES('workspace_counts','rls-fixture',1,$1,$2,$3,$3)")
        .bind(fx.account).bind(vec![fx.g_a]).bind(fx.user_a).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO read_model_user_fences(account_id,model,user_id,invalidated_xid) VALUES($1,'workspace_counts',$2,pg_current_xact_id())")
        .bind(fx.account).bind(fx.user_a).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO read_model_triggers(table_name,model) VALUES('atoms','workspace_counts') ON CONFLICT DO NOTHING").execute(&owner).await.unwrap();
    assert_eq!(count_roster_probe_as(&app,fx.user_a,"read_models",&fx).await,1);
    // New policy tables need real rows before the generic denial/leak probes.
    // Keep this setup in this proof, not shared seed(), so sibling cases keep
    // their original fixtures. These are metadata controls, never dispatch.
    {
        let (support_atom, supported_node): (Uuid, Uuid) = sqlx::query_as(
            "SELECT a.atom_id,n.node_id FROM nodes n \
             JOIN node_atoms l ON l.node_id=n.node_id \
             JOIN atoms a ON a.atom_id=l.atom_id AND a.graph_id=n.graph_id \
             JOIN source_version_chunks c ON c.atom_id=a.atom_id AND c.graph_id=a.graph_id \
             JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
             WHERE n.graph_id=$1 AND a.source_evidence_bound \
             AND s.current_version_id=v.version_id AND s.access_state='permitted' \
             ORDER BY n.node_id LIMIT 1",
        ).bind(fx.g_a).fetch_one(&owner).await.unwrap();
        let input = "synthetic RLS-only embedding input";
        let inserted = sqlx::query(
            "INSERT INTO managed_embedding_inputs(receipt_id,account_id,graph_id,purpose,atom_id,ordinal,content_revision,content_sha256,input_bytes) \
             SELECT r.receipt_id,r.account_id,r.graph_id,r.purpose,a.atom_id,0,a.content_revision, \
             sha256(convert_to($3,'UTF8')),octet_length($3::text) \
             FROM managed_attempt_receipts r JOIN atoms a ON a.graph_id=r.graph_id \
             WHERE r.graph_id=$1 AND a.atom_id=$2 AND r.purpose='cloud_embedding'",
        ).bind(fx.g_a).bind(support_atom).bind(input).execute(&owner).await.unwrap();
        assert_eq!(inserted.rows_affected(), 1, "one existing same-graph cloud-embedding receipt must own the input fixture");
        // managed_enrichment has no seeded receipt; mint one funded (protocol 1)
        // same-graph receipt and one input so the probes below are not vacuous.
        sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) \
             VALUES($1,'managed_enrichment',current_date,2000000) ON CONFLICT DO NOTHING")
            .bind(fx.account).execute(&owner).await.unwrap();
        let enrichment_receipt: Uuid = sqlx::query_scalar(
            "INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,protocol_version,dispatch_phase,content_plane,request_hash) \
             VALUES($1,'managed_enrichment',current_date,$2,$3,gen_random_uuid(),1,ARRAY['fixture'],1,1,1,1,'prepared','org:fixture',repeat('c',64)) RETURNING receipt_id",
        ).bind(fx.account).bind(fx.g_a).bind(fx.user_a).fetch_one(&owner).await.unwrap();
        sqlx::query(
            "INSERT INTO managed_enrichment_inputs(receipt_id,account_id,graph_id,ordinal,input_kind,input_id,content_revision,content_sha256) \
             VALUES($1,$2,$3,0,'atom',$4,1,sha256('synthetic RLS-only enrichment input'))",
        ).bind(enrichment_receipt).bind(fx.account).bind(fx.g_a).bind(support_atom).execute(&owner).await.unwrap();
        // managed_receipt_recovery_queue (0177): one queued row so the
        // system-only denial probe below is not vacuous. The enqueue trigger
        // only fires for terminal funded source-extraction receipts, which
        // this fixture does not mint, so the row is written directly.
        sqlx::query("INSERT INTO managed_receipt_recovery_queue(receipt_id,account_id) VALUES($1,$2)")
            .bind(enrichment_receipt).bind(fx.account).execute(&owner).await.unwrap();
        // fact_supersession_edges (0167): #1349 added it to RLS_TABLES with no
        // fixture row, so its negative control found 0 rows with RLS disabled
        // and failed. One edge in A's ocean on the seeded semantic atom and
        // the node it is linked to.
        let superseded = sqlx::query(
            "INSERT INTO fact_supersession_edges(graph_id,subject_node_id,state_key,superseder_atom_id, \
               superseded_atom_id,superseder_fact,superseded_fact,rule,status) \
             SELECT n.graph_id,n.node_id,'fixture.status',n.semantic_atom_id,n.semantic_atom_id, \
               repeat('a',32),repeat('b',32),'later_start','active' \
             FROM node_semantic_atoms n WHERE n.graph_id=$1 ORDER BY n.semantic_atom_id LIMIT 1",
        ).bind(fx.g_a).execute(&owner).await.unwrap();
        assert_eq!(superseded.rows_affected(), 1, "one fact_supersession_edges fixture row in A's ocean");
        // semantic_atom_embeddings (0182): one statement vector in A's ocean on
        // the seeded semantic atom, so its negative control is not vacuous.
        let embedded = sqlx::query(
            "INSERT INTO semantic_atom_embeddings(graph_id,semantic_atom_id,embed_model,statement_sha256,embedding) \
             SELECT a.graph_id,a.semantic_atom_id,'openai/text-embedding-3-small@384',sha256(convert_to(a.statement,'UTF8')), \
               array_fill(0.5::real,ARRAY[384])::vector \
             FROM semantic_atoms a WHERE a.graph_id=$1 ORDER BY a.semantic_atom_id LIMIT 1",
        ).bind(fx.g_a).execute(&owner).await.unwrap();
        assert_eq!(embedded.rows_affected(), 1, "one semantic_atom_embeddings fixture row in A's ocean");
        // standing_floor_overrides: one operator override on a version in A's ocean, so
        // the generic denial probe below is not vacuous for this table.
        let overridden = sqlx::query(
            "INSERT INTO standing_floor_overrides(source_version_id,graph_id,extraction_version,reason,granted_by) \
             SELECT version_id,graph_id,'source-atoms-v2','synthetic RLS-only override','fixture' FROM source_versions \
              WHERE graph_id=$1 ORDER BY version_id LIMIT 1",
        ).bind(fx.g_a).execute(&owner).await.unwrap();
        assert_eq!(overridden.rows_affected(), 1, "A's ocean must hold a version for the override fixture");

        // Use the real current source mapping/fingerprint and authenticated
        // member scope. The correction's integrity trigger stays enabled.
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
            &owner, fx.account, fx.user_a,
        ).await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor','member',true)")
            .execute(&mut *tx).await.unwrap();
        let correction: Uuid = sqlx::query_scalar(
            "INSERT INTO reef_identity_corrections(graph_id,node_id,label,recorded_by,support_atom_id,support_kind,source_version_id,source_item_id,content_revision,input_sha256) \
             SELECT n.graph_id,n.node_id,n.title,$2,a.atom_id,'bound',v.version_id,s.source_item_id, \
             a.content_revision,reef_identity_atom_fingerprint(a) \
             FROM nodes n JOIN node_atoms l ON l.node_id=n.node_id \
             JOIN atoms a ON a.atom_id=l.atom_id AND a.graph_id=n.graph_id \
             JOIN source_version_chunks c ON c.atom_id=a.atom_id AND c.graph_id=a.graph_id \
             JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id \
             WHERE n.graph_id=$1 AND n.node_id=$3 AND a.atom_id=$4 \
             AND s.current_version_id=v.version_id AND s.access_state='permitted' RETURNING correction_id",
        ).bind(fx.g_a).bind(fx.user_a).bind(supported_node).bind(support_atom)
            .fetch_one(&mut *tx).await.unwrap();
        let valid: bool = sqlx::query_scalar(
            "SELECT reef_identity_support_valid(c) FROM reef_identity_corrections c WHERE correction_id=$1",
        ).bind(correction).fetch_one(&mut *tx).await.unwrap();
        assert!(valid, "the identity fixture must have genuine current same-graph retained support");
        tx.commit().await.unwrap();

        // The generic probe for these tables sets only user_id. Supply the
        // SAME real account for A and B here too, so absence cannot fake isolation
        // for the correction's conjunctive account/member/support policy.
        for (member, expected) in [(fx.user_a, 1_i64), (fx.user_b, 0_i64)] {
            let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
                &app, fx.account, member,
            ).await.unwrap();
            for table in ["managed_embedding_inputs", "managed_enrichment_inputs", "reef_identity_corrections"] {
                let visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                    .fetch_one(&mut *tx).await.unwrap();
                assert_eq!(visible, expected, "same-account member visibility must be exact for {table}");
            }
            tx.rollback().await.unwrap();
        }
    }

    // ── 1. Every covered table denies, and every denial is non-vacuous ─────
    for table in support::source_evidence::TABLES {
        assert!(RLS_TABLES.contains(table), "evidence table must remain in the policy inventory");
        let query = format!("SELECT count(*) FROM {table}");
        let owner_count: i64 = sqlx::query_scalar(&query).fetch_one(&owner).await.unwrap();
        assert_eq!(owner_count, 1, "owner fixture must exist in {table}");
        assert_eq!(count_as(&app, fx.user_a, &query).await, 1,
            "owning member A must read its evidence in {table}; deny-everyone is not isolation");
    }
    let listing_tables: Vec<_> = RLS_TABLES.iter().copied()
        .filter(|table| table.starts_with("library_listing_")).collect();
    assert_eq!(listing_tables.len(), 7, "all listing grains (heads, outcomes, their staging, state, changes, generations) need real role fixtures");
    for table in listing_tables {
        for (graph, allowed, denied) in [(fx.g_a, fx.user_a, fx.user_b), (fx.g_b, fx.user_b, fx.user_a)] {
            let query = format!("SELECT count(*) FROM {table} WHERE graph_id='{graph}'");
            let owner_count: i64 = sqlx::query_scalar(&query).fetch_one(&owner).await.unwrap();
            assert!(owner_count > 0, "real listing fixture must exist for {table}/{graph}");
            assert_eq!(count_as(&app, allowed, &query).await, owner_count,
                "owning member must read every listing fixture row in {table}");
            assert_eq!(count_as(&app, denied, &query).await, 0,
                "other member must not read the listing fixture in {table}");
        }
    }
    for table in RLS_TABLES {
        if *table=="read_model_triggers" {
            assert_eq!(count_as(&app,fx.user_a,"SELECT count(*) FROM read_model_triggers").await,1);
            assert_eq!(count_as(&app,fx.user_b,"SELECT count(*) FROM read_model_triggers").await,1);
            let columns:i64=sqlx::query_scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='read_model_triggers'").fetch_one(&owner).await.unwrap();
            assert_eq!(columns,2,"permissive subscription metadata must remain table/model names only");
        }else{prove_denial_and_negative_control(&owner, &app, table, &fx).await;}
    }

    // ── 2. Predicate-specific control on the two highest-value tables ──────
    // §1 proves "RLS hides it". This proves the PREDICATE is what hides it, by
    // swapping in a permissive policy and watching the row reappear with RLS
    // still fully enabled — the exact failure a mis-written `USING` clause
    // would produce.
    //
    // `atoms` is the classic IDOR read pivot (read by `atom_id`/`graph_id` all
    // over the codebase, never joining `graphs`). `op_log` is the surface the
    // 2026-07-25 `sync/pull` member-ocean leak actually came through.
    //
    // The restore goes through [`snapshot_policies`]/[`restore_policies`] and
    // NOT through a `CREATE POLICY` literal in this file — see
    // [`PolicySnapshot`] for the CodeRabbit MAJOR that motivated it. Everything
    // after this loop (notably §4's `atoms` WRITE assertions) therefore runs
    // against the policy migration 0009 declared, command scope, role scope and
    // `WITH CHECK` included, rather than one this test wrote.
    for table in ["atoms", "op_log"] {
        let sql = probe_sql(table, &fx);
        assert_eq!(count_as(&app, fx.user_b, &sql).await, 0);

        // Capture what the MIGRATION created, before touching anything.
        let shipped = snapshot_policies(&owner, table).await;
        assert!(
            !shipped.is_empty(),
            "`{table}` must carry at least one policy to snapshot"
        );

        for snap in &shipped {
            owner
                .execute(format!("DROP POLICY {} ON {table}", snap.name).as_str())
                .await
                .unwrap();
        }
        owner
            .execute(format!("CREATE POLICY probe_permissive ON {table} USING (true)").as_str())
            .await
            .unwrap();
        let leaked = count_as(&app, fx.user_b, &sql).await;

        // Restore FROM THE SNAPSHOT (this also drops `probe_permissive`), and
        // assert the round-trip was lossless before continuing.
        restore_policies(&owner, table, &shipped).await;

        assert!(
            leaked > 0,
            "NEGATIVE CONTROL FAILED for `{table}`: replacing the policy predicate with \
             `USING (true)` did not make member A's rows visible to B, so the real predicate \
             is not what is doing the denying"
        );
        assert_eq!(
            count_as(&app, fx.user_b, &sql).await,
            0,
            "`{table}` must deny again once the migration's own policy is restored"
        );
    }

    // Belt-and-braces for the CodeRabbit MAJOR: §4 below asserts `atoms` WRITE
    // behaviour, which is governed by the write policies' `WITH CHECK`/`USING`
    // clauses — the exact clauses the old hand-written restore used to drop.
    // Prove here, against the live catalog, that `atoms`' policy set is
    // precisely what migration `0064_team_seat_grants_history_from` (m365
    // enterprise train PR 4.1) declares.
    //
    // Historical note: before that migration, `atoms` carried a single bare
    // `FOR ALL` policy (0009), and this block asserted exactly that. PR 4.1
    // split it into four command-scoped policies — a bare policy cannot carry
    // a SELECT-only retrieval window without also windowing writes, which
    // would incorrectly refuse ingest/edit of an atom timestamped before some
    // OTHER member's window. This is the same command-split `node_promotions`
    // already established in 0009 §3e; see this suite's node_promotions-
    // adjacent commentary (and `0064`'s own migration header) for that
    // precedent.
    {
        let atoms = snapshot_policies(&owner, "atoms").await;
        assert_eq!(
            atoms.len(),
            4,
            "0064 declares four command-scoped policies on atoms (select/insert/update/delete), \
             got {atoms:?}"
        );
        let by_name: std::collections::HashMap<&str, &PolicySnapshot> =
            atoms.iter().map(|p| (p.name.as_str(), p)).collect();

        // SELECT: the new windowed predicate, no WITH CHECK.
        let select = by_name["member_ocean_isolation_select"];
        assert_eq!(select.cmd, "SELECT");
        assert_eq!(select.permissive, "PERMISSIVE");
        assert_eq!(select.roles, vec!["public".to_string()]);
        assert!(
            select
                .qual
                .as_deref()
                .is_some_and(|q| q.contains("tenant_rls_visible_graph_windows")),
            "atoms' SELECT policy must resolve through the PR 4.1 windowed predicate \
             (tenant_rls_visible_graph_windows), got {:?}",
            select.qual
        );
        assert_eq!(
            select.with_check, None,
            "a SELECT policy has no WITH CHECK clause, got {:?}",
            select.with_check
        );

        // INSERT: no USING, WITH CHECK is the UNWINDOWED predicate — history
        // must never block ingest of a pre-window-timestamped atom.
        let insert = by_name["member_ocean_isolation_insert"];
        assert_eq!(insert.cmd, "INSERT");
        assert_eq!(insert.qual, None, "an INSERT policy has no USING clause, got {:?}", insert.qual);
        assert!(
            insert.with_check.as_deref().is_some_and(|q| q.contains("tenant_rls_visible_graph_ids")
                && !q.contains("tenant_rls_visible_graph_windows")),
            "atoms' INSERT policy must keep the UNWINDOWED write predicate, got {:?}",
            insert.with_check
        );

        // UPDATE: both USING and WITH CHECK are the unwindowed predicate.
        let update = by_name["member_ocean_isolation_update"];
        assert_eq!(update.cmd, "UPDATE");
        for clause in [&update.qual, &update.with_check] {
            assert!(
                clause.as_deref().is_some_and(|q| q.contains("tenant_rls_visible_graph_ids")
                    && !q.contains("tenant_rls_visible_graph_windows")),
                "atoms' UPDATE policy must keep the UNWINDOWED predicate on both USING and \
                 WITH CHECK, got {clause:?}"
            );
        }

        // DELETE: USING only, unwindowed predicate.
        let delete = by_name["member_ocean_isolation_delete"];
        assert_eq!(delete.cmd, "DELETE");
        assert!(
            delete.qual.as_deref().is_some_and(|q| q.contains("tenant_rls_visible_graph_ids")
                && !q.contains("tenant_rls_visible_graph_windows")),
            "atoms' DELETE policy must keep the UNWINDOWED predicate, got {:?}",
            delete.qual
        );
        assert_eq!(
            delete.with_check, None,
            "a DELETE policy has no WITH CHECK clause, got {:?}",
            delete.with_check
        );

        for p in &atoms {
            assert_eq!(p.permissive, "PERMISSIVE");
            assert_eq!(p.roles, vec!["public".to_string()]);
        }
    }

    // The counterpart assertion, on the ONE policy in 0009 that deliberately
    // declares a `WITH CHECK` distinct from its `USING`: `graphs` (§3a — its
    // USING is a self-referencing subquery that must not be reused as the
    // write check). This is what proves the snapshot/restore machinery above
    // is not silently blind to the clause CodeRabbit flagged: if it were, this
    // is the policy where the loss would show up.
    {
        let g = snapshot_policies(&owner, "graphs").await;
        assert_eq!(g.len(), 1, "0009 declares exactly one policy on graphs");
        let p = &g[0];
        assert_eq!(p.cmd, "ALL");
        let qual = p.qual.as_deref().expect("graphs policy has a USING clause");
        let check = p
            .with_check
            .as_deref()
            .expect("graphs policy declares an EXPLICIT WITH CHECK distinct from USING (§3a)");
        assert_ne!(
            qual, check,
            "graphs' WITH CHECK must remain DISTINCT from its USING — reusing the \
             self-referencing `id IN (visible)` subquery as the write check is exactly what \
             §3a avoids"
        );
        assert!(
            check.contains("owner_user_id"),
            "graphs' WITH CHECK must still gate member-ocean creation on owner_user_id, got \
             {check:?}"
        );

        // And prove a snapshot round-trip preserves that distinct clause — the
        // hand-written restore this replaced would have silently dropped it.
        let before = snapshot_policies(&owner, "graphs").await;
        restore_policies(&owner, "graphs", &before).await;
    }

    // ── 3. A sees A's reef and the team ocean; B sees neither ──────────────
    // The mirror image of §1: the boundary must not be "deny everything".
    {
        let a_sees: Vec<String> = {
            let mut tx = app.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.user_id', $1, true)")
                .bind(fx.user_a.to_string())
                .execute(&mut *tx)
                .await
                .unwrap();
            let v = sqlx::query_scalar("SELECT name FROM graphs ORDER BY name")
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            tx.rollback().await.ok();
            v
        };
        assert_eq!(
            a_sees,
            vec![
                "A private reef".to_string(),
                "commons".to_string(),
                "team ocean".to_string()
            ],
            "member A must see their own reef, the org commons, and the team ocean they belong \
             to — and never B's reef"
        );

        let b_sees: Vec<String> = {
            let mut tx = app.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.user_id', $1, true)")
                .bind(fx.user_b.to_string())
                .execute(&mut *tx)
                .await
                .unwrap();
            let v = sqlx::query_scalar("SELECT name FROM graphs ORDER BY name")
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            tx.rollback().await.ok();
            v
        };
        assert_eq!(
            b_sees,
            vec!["B private reef".to_string(), "commons".to_string()],
            "member B must see their own reef and the org commons — never A's reef, and never \
             the team ocean B is not a member of"
        );

        // A's atom read by atom-less direct `graph_id` filter, the shape that
        // never joins `graphs`: B querying A's graph_id explicitly gets zero.
        let cross: i64 = count_as(
            &app,
            fx.user_b,
            &format!("SELECT count(*) FROM atoms WHERE graph_id = '{}'", fx.g_a),
        )
        .await;
        assert_eq!(
            cross, 0,
            "member B querying atoms by member A's graph_id directly (no join to `graphs`) must \
             return zero rows"
        );
    }

    // ── 3b. `membership_events`: the POSITIVE half, not just the denial ────
    // CodeRabbit gap: this suite already proves `membership_events` is on the
    // exact RLS roster (§8), that it carries a policy, that the total policy
    // count is right, and — because it runs through the SAME generic §1 loop
    // as every other table — that member B is denied the fixture's one audit
    // row NON-vacuously (the negative control: disabling RLS makes it
    // reappear). What none of that proves is the mirror image every
    // ocean-scoped table needs: that a user who SHOULD see a row actually
    // DOES. A policy that quietly denied EVERYONE — including a team's own
    // members — would still pass every `count == 0`-for-B assertion above
    // without ever being caught, because §1's loop only ever probes the
    // FORBIDDEN direction. Prove the allowed direction here, on the fixture's
    // own `created` event for the team ocean A belongs to (seeded in `seed`
    // above, right next to that team's `team_members` row), alongside the
    // restated denial for B so both directions live side by side.
    {
        let a_events: Vec<(Uuid, Uuid, String)> = {
            let mut tx = app.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.user_id', $1, true)")
                .bind(fx.user_a.to_string())
                .execute(&mut *tx)
                .await
                .unwrap();
            let rows = sqlx::query_as(
                "SELECT actor_user_id, target_user_id, action FROM membership_events",
            )
            .fetch_all(&mut *tx)
            .await
            .expect(
                "member A, an ACTIVE member of the team this event belongs to, must be able to \
                 read `membership_events` under RLS — a hard error here means the policy denies \
                 even a legitimate reader, not just member B",
            );
            tx.rollback().await.ok();
            rows
        };
        assert_eq!(
            a_events,
            vec![(fx.user_a, fx.user_a, "created".to_string())],
            "member A must see the fixture's seeded `membership_events` row EXACTLY as seeded \
             (not merely a nonzero count) — this is the positive assertion CodeRabbit flagged \
             as missing"
        );

        // The mirror negative, restated explicitly rather than left implicit
        // in §1's generic per-table loop: B, who is not on this team's
        // roster, still sees zero rows of A's team's audit trail.
        let b_events =
            count_as(&app, fx.user_b, "SELECT count(*) FROM membership_events").await;
        assert_eq!(
            b_events, 0,
            "member B, who is not a member of this team, must see zero `membership_events` rows"
        );
    }

    // ── 4. Writes are refused across the boundary too ──────────────────────
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        let own = sqlx::query(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
             VALUES ($1, 'test', 'a-own-write', 'blob', now())",
        )
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            own.is_ok(),
            "A must be able to write into A's own reef: {own:?}"
        );

        let cross = sqlx::query(
            "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
             VALUES ($1, 'test', 'a-writes-into-b', 'blob', now())",
        )
        .bind(fx.g_b)
        .execute(&mut *tx)
        .await;
        assert!(
            cross.is_err(),
            "A must NOT be able to write an atom into B's private reef — got {cross:?}"
        );
        tx.rollback().await.ok();
    }

    // `graphs`' WITH CHECK is written against the NEW row's own columns rather
    // than reusing `USING` (0009 §3a). Proves both halves of that: A may create
    // an ocean A owns, and may not create one owned by B.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();

        let mine = sqlx::query(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'A second reef', 'member', $2)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            mine.is_ok(),
            "A must be able to create an ocean A owns: {mine:?}"
        );

        let theirs = sqlx::query(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id) \
             VALUES ($1, 'stolen reef', 'member', $2)",
        )
        .bind(fx.account)
        .bind(fx.user_b)
        .execute(&mut *tx)
        .await;
        assert!(
            theirs.is_err(),
            "A must NOT be able to create a member ocean owned by B — got {theirs:?}"
        );
        tx.rollback().await.ok();
    }

    // `node_promotions`' four-policy split (0009 §3e / §6(e)): a bare
    // USING-only policy would be FOR ALL and reuse the permissive OR predicate
    // as WITH CHECK, letting A insert a promotion whose target is B's reef —
    // writing into a reef A cannot even read.
    {
        let (src, tgt): (Uuid, Uuid) = (
            sqlx::query_scalar(
                "INSERT INTO nodes(graph_id, type, title, body) \
                 VALUES ($1, 'entity', 'promo src', '{}') RETURNING node_id",
            )
            .bind(fx.g_a)
            .fetch_one(&owner)
            .await
            .unwrap(),
            sqlx::query_scalar(
                "INSERT INTO nodes(graph_id, type, title, body) \
                 VALUES ($1, 'entity', 'promo tgt in B', '{}') RETURNING node_id",
            )
            .bind(fx.g_b)
            .fetch_one(&owner)
            .await
            .unwrap(),
        );

        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let cross = sqlx::query(
            "INSERT INTO node_promotions(source_node_id, target_graph_id, target_node_id, from_version) \
             VALUES ($1, $2, $3, 1)",
        )
        .bind(src)
        .bind(fx.g_b)
        .bind(tgt)
        .execute(&mut *tx)
        .await;
        assert!(
            cross.is_err(),
            "A must NOT be able to write a node_promotions row targeting B's private reef, even \
             though A owns the source node — got {cross:?}"
        );
        tx.rollback().await.ok();

        let policies: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_policies \
             WHERE schemaname='public' AND tablename='node_promotions'",
        )
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(
            policies, 4,
            "node_promotions must carry 4 command-split policies, not one bare ALL policy"
        );
    }

    // ── 5. Unscoped session: fail-CLOSED, not fail-loud (0009 §3) ──────────
    // Unlike the consumer plane's strict predicate, an unscoped tenant session
    // must NOT error — it must simply see no member-private or team ocean. This
    // is asserted rather than left to be discovered, because it is exactly the
    // property that makes the Phase-2 cutover survivable while worker paths are
    // still unconverted.
    {
        let mut tx = app.begin().await.unwrap();
        let names: Vec<String> = sqlx::query_scalar("SELECT name FROM graphs ORDER BY name")
            .fetch_all(&mut *tx)
            .await
            .expect(
                "an unscoped tenant session must NOT hard-error (0009 uses the missing_ok form of \
                 current_setting deliberately)",
            );
        assert_eq!(
            names,
            vec!["commons".to_string()],
            "an unscoped session must see the org commons and NO member/team ocean"
        );
        let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            atoms, 0,
            "an unscoped session must see no member-ocean atoms"
        );
        tx.rollback().await.ok();
    }

    // ── 6. The `system` actor is the one deliberate bypass (0009 §4) ───────
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            n, 4,
            "the tenant service principal must see every ocean in the tenant — this is the \
             documented escape for background workers"
        );
        tx.rollback().await.ok();

        // Any OTHER value is not the system principal.
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor', 'System', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            n, 1,
            "`app.tenant_actor` must match the exact string 'system'; anything else (here \
             'System') must NOT grant the bypass"
        );
        tx.rollback().await.ok();
    }

    // ── 7. Inert for the owner role — the app as deployed today ────────────
    {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(
            n, 4,
            "the owner/BYPASSRLS role must still see every ocean — migration 0009 must change \
             zero query results for the app as deployed today"
        );
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(fx.g_a)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert!(n > 0, "the owner role must still read A's reef unfiltered");
    }

    // ── 8. Exactly the documented roster, and NO table forced ──────────────
    {
        let actual: Vec<String> = sqlx::query_scalar(
            "SELECT relname FROM pg_class \
             WHERE relnamespace = 'public'::regnamespace AND relrowsecurity ORDER BY relname",
        )
        .fetch_all(&owner)
        .await
        .unwrap();
        let mut expected: Vec<String> = RLS_TABLES.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(
            actual, expected,
            // Deliberately carries NO table count. It said "40-table" while
            // the roster held 51, because every migration that adds a table
            // updates the list and nobody updates the prose beside it — a
            // number in an assertion message is a second source of truth
            // that only ever drifts. `RLS_TABLES` is the roster; the diff
            // above already names exactly which table is missing or extra.
            "the tenant tree (0009 plus every later member_ocean_isolation-adding migration, \
             e.g. 0013_slack_watched_channels, 0017_team_seat_grants) must enable RLS on \
             exactly the tables in RLS_TABLES — add the table there, with the reason, \
             when a migration gives it a policy"
        );

        for t in DELIBERATELY_UNCOVERED {
            let covered: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_class \
                 WHERE relnamespace='public'::regnamespace AND relname=$1 AND relrowsecurity)",
            )
            .bind(t)
            .fetch_one(&owner)
            .await
            .unwrap();
            assert!(
                !covered,
                "`{t}` is listed as deliberately uncovered in 0009 §6(f) but now carries RLS — \
                 update the migration header and this list together"
            );
        }

        for t in RLS_TABLES {
            let has_policy: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=$1)",
            )
            .bind(t)
            .fetch_one(&owner)
            .await
            .unwrap();
            assert!(has_policy, "{t} has RLS enabled but no policy");
        }

        let total: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_policies WHERE schemaname='public'")
                .fetch_one(&owner)
                .await
                .unwrap();
        // Bumped 37->38 (Today-journal PR A): 0024 adds `journal_rollups`'
        // own single `member_ocean_isolation` policy, making it 34
        // single-policy tables (was 33) + node_promotions' 4.
        // Bumped 38->39 (org team membership): 0032 adds `membership_events`'
        // own single `member_ocean_isolation` policy, making it 35
        // single-policy tables (was 34) + node_promotions' 4.
        // Bumped 39->40 (high-signal review): 0033 adds `evaluation_ledger`'s
        // single policy — 36 single-policy tables + node_promotions' 4.
        // Bumped 40->52 (team-create bootstrap): 0040 command-splits FOUR
        // tables — `teams`, `team_keys`, `team_member_key_envelopes`,
        // `team_members` — from one bare `FOR ALL` policy each into the
        // `_select`/`_insert`/`_update`/`_delete` set, so 4 policies become 16.
        // 32 single-policy tables + 5 split tables x 4 = 52. The split is what
        // lets the bootstrap arm exist on INSERT and nowhere else; see
        // `migrations_tenant/0040_team_create_bootstrap_rls.sql`.
        // Bumped 52->53 (slice 0, T2 "identifier awareness"): 0045 adds
        // `reef_identifiers`'s own single `member_ocean_isolation` policy —
        // 33 single-policy tables + 5 split tables x 4 = 53.
        // Bumped 53->54 (judgement-graph resolver slice PR1): 0046 adds
        // `reef_judgement`'s own single `member_ocean_isolation` policy —
        // 34 single-policy tables + 5 split tables x 4 = 54.
        // Bumped 54->55 (MCP spec 2026-07-28 PR 2, task-based `ask_ocean`):
        // 0047 adds `mcp_tasks`'s own single `member_ocean_isolation` policy —
        // 35 single-policy tables + 5 split tables x 4 = 55.
        // Bumped 55->56 (usage-metering PR4, the reserve/finalize choke point):
        // 0055 adds `llm_usage_pending`'s own single `member_ocean_isolation`
        // policy, anchored on `graph_id` through `tenant_rls_visible_graph_ids()`
        // exactly like its `llm_usage` sibling — 36 single-policy tables +
        // 5 split tables x 4 = 56.
        // Bumped 56->60 (m365 enterprise train PR 4.1, "history_from"): 0064
        // command-splits `atoms` from one bare `FOR ALL` policy into the
        // `_select`/`_insert`/`_update`/`_delete` set (see that migration's
        // header — a bare policy cannot carry a SELECT-only retrieval window
        // without also windowing writes), turning one single-policy table
        // into one more split table (36 -> 35 single, +1 split), AND adds
        // `seat_grant_sources` with its own single `member_ocean_isolation`
        // policy (35 -> 36 single again): 36 single-policy tables + 6 split
        // tables (node_promotions, teams, team_keys,
        // team_member_key_envelopes, team_members, atoms) x 4 = 36 + 24 = 60.
        // Bumped 60->61 (gsuite Train G2 PR 3): 0066 adds
        // `gmail_watched_threads`'s own single `member_ocean_isolation`
        // policy, anchored on `graph_id` exactly like `outlook_watched_
        // conversations`'s own — 37 single-policy tables + 6 split tables x
        // 4 = 37 + 24 = 61.
// Bumped 61->62 (m365 PR 3.8): tenant 0067 adds
        // `microsoft_calendar_sync_state`'s single `member_ocean_isolation`
        // policy, anchored on `graph_id` — 38 single + 24 split = 62.
        // Bumped 62->64 (m365 PR 4.3, tenant 0069): `query_audit_log`'s TWO
        // policies (`member_ocean_isolation_select`/`_insert` — append-only)
        // as a 2-way-split table: 38 single + 24 + 2 = 64.
        // Bumped 64->65 (m365 PR 3.4, tenant 0070): `outlook_everything_mode`'s
        // single `member_ocean_isolation` policy — 39 single + 24 + 2 = 65.
        // Bumped 65->67 (m365 PR 4.2, tenant 0071, merged after #700): tenant
        // 0071 adds `connector_audit_log`'s two policies, the SECOND 2-way-
        // split table alongside `query_audit_log`:
        // 39 single + 6 x 4 (24) + 2 x 2 (4) = 67.
        // Bumped 67->68 (gsuite Train G4 PR 3, tenant 0073):
        // `google_chat_watched_spaces`'s own single `member_ocean_isolation`
        // policy (one FOR ALL policy carrying an explicit WITH CHECK is
        // still one policy object) — 40 single + 24 + 4 = 68.
        // Bumped 68->69 (gsuite enterprise train PR G2.4, tenant 0074):
        // `gmail_everything_mode`'s single `member_ocean_isolation` policy —
        // the SECOND implementation of `outlook_everything_mode`'s own
        // carve-out — 41 single + 24 + 4 = 69.
        // Bumped 69->71 (source publication, #871, tenant 0081): TWO single
        // policies — `source_connection_scope` and
        // `source_connection_atom_scope`, one `FOR ALL` object each carrying
        // an explicit WITH CHECK — 43 single + 24 + 4 = 71. This count and
        // the roster above went stale in the same commit and for the same
        // reason: 0081 shipped the tables RLS-enabled and updated neither.
        // Bumped 71->72 (GitHub connector PR A, tenant 0095): this bump was
        // never recorded when 0095 landed (found while adding PR B part 2's
        // own entry below) — `github_watched_repos`' single
        // `member_ocean_isolation` policy — 44 single + 24 + 4 = 72.
        // Bumped 72->73 (GitHub connector PR B part 2, tenant 0096):
        // `github_code_groups`' single `member_ocean_isolation` policy —
        // 45 single + 24 + 4 = 73.
        // Bumped 73->79 (roster sync): six single-policy tables that shipped
        // RLS-enabled without updating this count or the roster —
        // `gmail_hidden_messages` (0116), `teams_meeting_installations` and
        // `teams_meeting_captures` (0105), `outlook_capture_receipts` (0107),
        // `outlook_schedule_intents` (0109), `google_meet_captures` (0111) —
        // 51 single + 24 + 4 = 79.
        // Bumped 79->80 (Slack huddle disclosure, tenant 0124):
        // `slack_huddle_consents`' single `account_isolation` policy —
        // 52 single + 24 + 4 = 80.
        // Bumped 80->87 (shared source evidence, tenant 0126/0127): the
        // seven source-evidence tables each carry one member-ocean policy —
        // 59 single + 24 + 4 = 87.
        // P2 lifecycle and P3 corpus ledger add four graph-scoped policies.
        // Bumped 91->96: Composio 0136 `composio_capture_scopes`, P7 0138
        // `answer_support_receipts`, and P11 0144 `source_version_observations`,
        // `temporal_evidence_records`, `answer_temporal_receipts` — one
        // member-ocean policy each.
        // Bumped 96->97: P10 0142 `corpus_capture_admissions` — one
        // member-ocean policy.
        // Bumped 97->102: P8 0143 `source_purge_events`,
        // `source_purge_dependencies`, `source_purge_reviews`,
        // `selected_evidence_publications`, `unattached_capture_uploads` —
        // one policy each.
        // 0152 adds embedding-input system mutation + member SELECT (2);
        // 0157 adds correction SELECT/INSERT/UPDATE (3). Preserve exact total.
        // The managed_enrichment scaffold adds input system mutation + member
        // SELECT (2), the same split as 0152.
        // standing_floor_overrides adds one system/member SELECT policy (1).
        // fact_supersession_edges (0167) adds one graph-scoped policy (1), not
        // counted here when #1349 added it to RLS_TABLES.
        // 0177 managed_receipt_recovery_queue adds one system-only policy (1).
        // 0182 semantic_atom_embeddings adds one graph-scoped policy (1).
        // 0184 graph_workspace_counts adds one graph-scoped policy (1).
        // 0199 adds seven listing-grain policies (state, changes, generations,
        // heads, staged heads, outcomes, staged outcomes); retain the exact
        // inventory.
        // 0218 journal_gists adds one graph-scoped policy (1).
        // 0219 message_feedback adds four policies (4).
        // 0220 connector_sources adds one account-keyed policy (1).
        // 0223 read_models member/system + trigger metadata + system-only fences (5).
        assert_eq!(
            total, 139,
            "tenant policy inventory includes managed receipt/input splits and supported identity corrections"
        );

        // ── The drift guard a hardcoded roster cannot be ───────────────
        // DELIBERATELY_UNCOVERED catches "a listed table gained RLS". It does
        // NOT catch what actually happened during this PR's review: another PR
        // (#349) merged a BRAND NEW tenant table. A new table with an ocean
        // anchor and no policy is a silent hole, and nobody editing that PR
        // would think to update a list in this file.
        //
        // So assert the structural invariant instead — no table without RLS may
        // carry an ocean-adjacent column or a foreign key to `graphs`/`nodes`.
        // That IS the property "every ocean-scoped table is covered", checked
        // against the live catalog rather than against a list someone has to
        // remember to edit.
        //
        // Two allowed exceptions, both protected by an outright REVOKE instead
        // of a policy (see 0009 §6(f) and each's own dedicated test):
        //   * `supersession_edge_cross_ocean_deletions` (migration 0008 / PR
        //     #349) carries `superseded_graph_id`/`superseder_graph_id` —
        //     `revoked_forensic_ledger_is_unreadable_by_the_app_role`.
        //   * `node_case_fold_log` (migration 0018, integration/graph-temporal
        //     merge) carries `graph_id` —
        //     `revoked_node_case_fold_log_is_unreadable_by_the_app_role`.
        let unpoliced_ocean_columns: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT c.relname FROM pg_class c \
             JOIN pg_attribute a ON a.attrelid = c.oid \
             WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' \
               AND NOT c.relrowsecurity \
               AND a.attnum > 0 AND NOT a.attisdropped \
               AND (a.attname LIKE '%graph%' OR a.attname LIKE '%node%') \
               AND c.relname NOT IN ('supersession_edge_cross_ocean_deletions', 'node_case_fold_log') \
             ORDER BY 1",
        )
        .fetch_all(&owner)
        .await
        .unwrap();
        assert!(
            unpoliced_ocean_columns.is_empty(),
            "table(s) {unpoliced_ocean_columns:?} carry an ocean-adjacent column but have NO \
             row-level security. Either add a policy to \
             migrations_tenant/0009_tenant_rls.sql, or add the table to \
             DELIBERATELY_UNCOVERED here WITH a reason in that migration's §6(f) — and if it \
             holds member-ocean identifiers, revoke the app role's access the way the forensic \
             ledger does."
        );

        let unpoliced_ocean_fks: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT con.conrelid::regclass::text \
             FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid \
             WHERE con.contype = 'f' AND con.connamespace = 'public'::regnamespace \
               AND NOT c.relrowsecurity \
               AND con.confrelid::regclass::text IN ('graphs', 'nodes') \
             ORDER BY 1",
        )
        .fetch_all(&owner)
        .await
        .unwrap();
        assert!(
            unpoliced_ocean_fks.is_empty(),
            "table(s) {unpoliced_ocean_fks:?} have a foreign key to graphs/nodes but no \
             row-level security — same decision required as above"
        );

        // FORCE is deliberately absent everywhere (0009 §7). An accidental
        // FORCE would (a) un-inert this migration for the owner-connected app
        // and (b) reintroduce the policy recursion the SECURITY DEFINER
        // predicate exists to break.
        let forced: Vec<String> = sqlx::query_scalar(
            "SELECT relname FROM pg_class \
             WHERE relnamespace='public'::regnamespace AND relforcerowsecurity ORDER BY relname",
        )
        .fetch_all(&owner)
        .await
        .unwrap();
        assert!(
            forced.is_empty(),
            "no tenant table may carry FORCE ROW LEVEL SECURITY (see 0009 §7) — found {forced:?}"
        );
    }

    // ── connector_audit_log-specific: append-only enforced, SELECT is
    // system-scope-only, and INSERT pairs the acting session to its own
    // actor_user_id (0071, m365 enterprise train PR 4.2) ───────────────────
    //
    // The generic §1 loop already proves ordinary sessions (ANY member,
    // including the row's own actor) cannot SELECT a connector_audit_log
    // row — its SELECT policy is `tenant_rls_is_system()`-only, same as
    // `query_audit_log`. This block proves the claims the generic loop
    // cannot: (a) UPDATE/DELETE are refused for EVERY actor, even the
    // system one — no such policy exists at all; (b) the system actor CAN
    // read it — what makes the admin route's `begin_tenant_system_scoped`
    // read work; (c) an ordinary member session may INSERT a row naming
    // itself as `actor_user_id`, but not one naming a different user.
    {
        let mut tx = app.begin().await.expect("begin as tenant app role");
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .expect("set_config app.tenant_actor");

        let updated = sqlx::query("UPDATE connector_audit_log SET subject = $1 WHERE account_id = $2")
            .bind("tampered")
            .bind(fx.account)
            .execute(&mut *tx)
            .await
            .expect("UPDATE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            updated.rows_affected(),
            0,
            "connector_audit_log must be append-only even for the system actor: no UPDATE policy \
             exists at all"
        );

        let deleted = sqlx::query("DELETE FROM connector_audit_log WHERE account_id = $1")
            .bind(fx.account)
            .execute(&mut *tx)
            .await
            .expect("DELETE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            deleted.rows_affected(),
            0,
            "connector_audit_log must be append-only even for the system actor: no DELETE policy \
             exists at all"
        );

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
            .fetch_one(&mut *tx)
            .await
            .expect("system actor SELECT on connector_audit_log");
        assert!(
            n > 0,
            "the tenant service principal must be able to read connector_audit_log — this is \
             what the admin route's begin_tenant_system_scoped read relies on"
        );

        tx.rollback().await.ok();
    }
    {
        let mut tx = app.begin().await.expect("begin as tenant app role");
        sqlx::query("SELECT set_config('app.account_id', $1, true)")
            .bind(fx.account.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.account_id");
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .expect("set_config app.user_id");

        let own_actor = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, $2, 'user', 'seat_grant', 'slack')",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .execute(&mut *tx)
        .await;
        assert!(
            own_actor.is_ok(),
            "a member session may write a connector_audit_log row naming ITSELF as \
             actor_user_id — got {own_actor:?}"
        );

        let spoofed_actor = sqlx::query(
            "INSERT INTO connector_audit_log (account_id, actor_user_id, actor_kind, action, provider) \
             VALUES ($1, $2, 'user', 'seat_grant', 'slack')",
        )
        .bind(fx.account)
        .bind(fx.user_b)
        .execute(&mut *tx)
        .await;
        assert!(
            spoofed_actor.is_err(),
            "a member session must NOT be able to write a connector_audit_log row naming a \
             DIFFERENT member as actor_user_id"
        );

        tx.rollback().await.ok();
    }

    // ── 9. query_audit_log-specific: append-only enforced, and SELECT is
    // system-scope-only (0069, m365 enterprise train PR 4.3) ───────────────
    //
    // The generic §1 loop already proves ordinary sessions (ANY member,
    // including the row's own asker) cannot SELECT a query_audit_log row —
    // its SELECT policy is `tenant_rls_is_system()`-only, stronger than the
    // per-member-ocean boundary every other table's probe exercises (see
    // that migration's header). This block proves the two claims the
    // generic loop cannot: (a) UPDATE/DELETE are refused for EVERY actor,
    // even the system one (no such policy exists at all — append-only by
    // design, not convention); (b) the system actor CAN read it, which is
    // what makes `routes::query_audit::list_query_audit`'s
    // `begin_tenant_system_scoped` read work at all.
    {
        let mut tx = app.begin().await.expect("begin as tenant app role");
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .expect("set_config app.tenant_actor");

        let updated = sqlx::query("UPDATE query_audit_log SET question_digest = $1 WHERE account_id = $2")
            .bind("f".repeat(64))
            .bind(fx.account)
            .execute(&mut *tx)
            .await
            .expect("UPDATE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            updated.rows_affected(),
            0,
            "query_audit_log must be append-only even for the system actor: no UPDATE policy \
             exists at all"
        );

        let deleted = sqlx::query("DELETE FROM query_audit_log WHERE account_id = $1")
            .bind(fx.account)
            .execute(&mut *tx)
            .await
            .expect("DELETE must not error — RLS denies by filtering rows, not by SQL error");
        assert_eq!(
            deleted.rows_affected(),
            0,
            "query_audit_log must be append-only even for the system actor: no DELETE policy \
             exists at all"
        );

        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_audit_log")
            .fetch_one(&mut *tx)
            .await
            .expect("system actor SELECT on query_audit_log");
        assert!(
            n > 0,
            "the tenant service principal must be able to read query_audit_log — this is what \
             the admin route's begin_tenant_system_scoped read relies on"
        );

        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// EXECUTE is an explicit allowlist, not a schema-wide grant.
///
/// CodeRabbit MAJOR on `6c8c63a`: an earlier revision granted
/// `EXECUTE ON ALL FUNCTIONS IN SCHEMA public` plus an `ALTER DEFAULT
/// PRIVILEGES ... GRANT EXECUTE ON FUNCTIONS`. Nearly every function in this
/// schema is `SECURITY DEFINER` — it runs with the OWNER's privileges and is
/// therefore exempt from every policy in 0009 — so a default-privileges grant
/// makes ANY such function added later automatically callable by the
/// low-privilege role. That is an RLS bypass that appears with no migration
/// reviewing it and no test noticing.
///
/// This asserts both halves: the six resolvers the app genuinely calls ARE
/// executable, and a NEWLY created `SECURITY DEFINER` function is NOT. The
/// second half is the one that regresses if someone reinstates the convenience
/// grant.
#[tokio::test]
async fn execute_is_an_allowlist_not_a_schema_wide_grant() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping execute_is_an_allowlist_not_a_schema_wide_grant: set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_execute";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");
    set_app_role_password(&owner).await;

    // ── The allowlist: every resolver a live org request path actually calls ─
    for sig in [
        "resolve_graph_account_id(uuid)",
        "resolve_atoms_account_id(uuid[])",
        "resolve_tenant_reachable_graphs(uuid,uuid)",
        "resolve_tenant_graph_reachable_by_user(uuid,uuid,uuid)",
        "resolve_meeting_share_account_id(text)",
        "resolve_session_share_account_id(text)",
        "tenant_rls_actor_user_id()",
        "tenant_rls_is_system()",
        "tenant_rls_visible_graph_ids()",
        "read_model_visible_windows()",
        "read_model_graph_account(uuid)",
        "read_model_node_account(uuid)",
        "read_model_conversation_account(uuid)",
        "read_model_fence_state(uuid,uuid[],text,uuid,pg_snapshot)",
        "read_models_mark_user_dirty(uuid,uuid,text[])",
        "read_model_org_usage_facts(uuid,timestamptz)",
    ] {
        let ok: bool = sqlx::query_scalar(
            "SELECT has_function_privilege('coralswarm_tenant_app', $1, 'EXECUTE')",
        )
        .bind(sig)
        .fetch_one(&owner)
        .await
        .unwrap_or_else(|e| panic!("has_function_privilege({sig}): {e}"));
        assert!(
            ok,
            "coralswarm_tenant_app must be able to EXECUTE `{sig}` — a live org request path \
             calls it, and without the grant that path 42501s after cutover"
        );
    }

    // ── The thing the allowlist exists to prevent ───────────────────────────
    // A brand-new SECURITY DEFINER function, exactly as a future migration
    // would add one. Under the old schema-wide default-privileges grant this
    // would be immediately callable by the app role; it must not be.
    owner
        .execute(
            "CREATE FUNCTION future_definer_helper() RETURNS int              LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS $$ SELECT 1 $$",
        )
        .await
        .expect("create a future SECURITY DEFINER helper");

    let auto_granted: bool = sqlx::query_scalar(
        "SELECT has_function_privilege('coralswarm_tenant_app', 'future_definer_helper()', 'EXECUTE')",
    )
    .fetch_one(&owner)
    .await
    .unwrap();
    assert!(
        !auto_granted,
        "a NEWLY created SECURITY DEFINER function is automatically EXECUTE-able by \
         coralswarm_tenant_app. That means 0009 has a schema-wide or default-privileges EXECUTE \
         grant again — every future SECURITY DEFINER function is an RLS bypass reachable by the \
         low-privilege role, with no migration reviewing it."
    );

    // Belt-and-braces: no default-privileges ACL entry mentioning the role.
    let default_acls: Vec<String> = sqlx::query_scalar(
        "SELECT coalesce(array_to_string(d.defaclacl, ','), '')          FROM pg_default_acl d WHERE d.defaclobjtype = 'f'",
    )
    .fetch_all(&owner)
    .await
    .unwrap();
    assert!(
        !default_acls
            .iter()
            .any(|a| a.contains("coralswarm_tenant_app")),
        "found a FUNCTION default-privileges entry granting coralswarm_tenant_app: \
         {default_acls:?}"
    );

    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// Migration 0008 (PR #349) added `supersession_edge_cross_ocean_deletions`:
/// the forensic record of the cross-ocean supersession edges that PR deleted.
/// It stores both endpoints' node ids AND both endpoints' graph ids — on this
/// plane, member-private ocean identifiers — and identifier disclosure across
/// exactly this boundary is what #348 had to close.
///
/// 0009 does not give it a policy. It revokes the app role's access outright,
/// which is stronger, and which also makes 0008's own instruction ("if a future
/// change gives it an application reader, it must get a policy in the same PR")
/// self-enforcing: such a reader gets a loud 42501 rather than silently
/// receiving filtered rows.
///
/// This asserts the revoke actually survives 0009's own schema-wide
/// `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public` —
/// which runs in the same DO block, immediately before it. Ordering there is
/// load-bearing and a reordering edit would silently re-expose the table.
#[tokio::test]
async fn revoked_forensic_ledger_is_unreadable_by_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping revoked_forensic_ledger_is_unreadable_by_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_revoke";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0008 + 0009");
    set_app_role_password(&owner).await;

    let app = app_role_pool(&db_url).await;

    let read = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM supersession_edge_cross_ocean_deletions",
    )
    .fetch_one(&app)
    .await;
    let err = read.expect_err(
        "the app role must NOT be able to read supersession_edge_cross_ocean_deletions — \
         0009 revokes it explicitly after the schema-wide GRANT",
    );
    let sqlstate = err
        .as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.to_string());
    assert_eq!(
        sqlstate.as_deref(),
        Some("42501"),
        "expected insufficient_privilege (42501) — a loud permission error is the whole \
         point of revoking rather than policy-filtering; got {err:?}"
    );

    // ...and the owner (the human-during-an-incident path) still reads it fine.
    let owner_read: i64 =
        sqlx::query_scalar("SELECT count(*) FROM supersession_edge_cross_ocean_deletions")
            .fetch_one(&owner)
            .await
            .expect("the owner connection must still read the forensic ledger");
    assert_eq!(owner_read, 0, "fresh scratch DB has no recorded deletions");

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// Migration 0018 (integration/graph-temporal merge) added `node_case_fold_log`:
/// the forensic record of every case/alias collision `nodes_case_insensitive_
/// identity` folded, naming both the survivor and loser node ids AND the
/// graph_id — on this plane, a member-private ocean identifier. Same shape as
/// `supersession_edge_cross_ocean_deletions` (the test immediately above),
/// same fix: `REVOKE ALL` instead of a policy, asserted the same way — the
/// revoke must survive 0009's schema-wide `GRANT ... ON ALL TABLES IN SCHEMA
/// public` that predates this table's own creation.
#[tokio::test]
async fn revoked_node_case_fold_log_is_unreadable_by_the_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping revoked_node_case_fold_log_is_unreadable_by_the_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_revoke_fold";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0009 + 0018");
    set_app_role_password(&owner).await;

    let app = app_role_pool(&db_url).await;

    let read = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM node_case_fold_log")
        .fetch_one(&app)
        .await;
    let err = read.expect_err(
        "the app role must NOT be able to read node_case_fold_log — 0018 revokes it \
         explicitly, the same way 0009 revokes supersession_edge_cross_ocean_deletions",
    );
    let sqlstate = err
        .as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.to_string());
    assert_eq!(
        sqlstate.as_deref(),
        Some("42501"),
        "expected insufficient_privilege (42501) — a loud permission error is the whole \
         point of revoking rather than policy-filtering; got {err:?}"
    );

    // ...and the owner (the human-during-an-incident path) still reads it fine.
    let owner_read: i64 = sqlx::query_scalar("SELECT count(*) FROM node_case_fold_log")
        .fetch_one(&owner)
        .await
        .expect("the owner connection must still read the fold ledger");
    assert_eq!(owner_read, 0, "fresh scratch DB has no recorded folds");

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// The Rust half of the session-identity mechanism: `app.user_id` must
/// round-trip inside the transaction that set it, and must NOT survive onto the
/// next borrower of the same pooled connection — the property that makes it
/// safe to scope a shared tenant pool per request.
#[tokio::test]
async fn tenant_user_guc_is_transaction_local_and_does_not_leak_between_members() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping tenant_user_guc_is_transaction_local_and_does_not_leak_between_members: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_scope";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    // max_connections(1) guarantees the second transaction reuses the exact
    // physical connection the first one used — the scenario that would expose a
    // session-scoped (rather than transaction-scoped) leak of one member's id
    // onto another member's request.
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect");

    let account = Uuid::new_v4();
    let member_a = Uuid::new_v4();
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&pool, account, member_a)
        .await
        .expect("begin_tenant_scoped");
    let seen: Uuid = sqlx::query_scalar("SELECT current_setting('app.user_id')::uuid")
        .fetch_one(&mut *tx)
        .await
        .expect("read back app.user_id inside the scoped transaction");
    assert_eq!(seen, member_a);
    let seen_account: Uuid = sqlx::query_scalar("SELECT current_setting('app.account_id')::uuid")
        .fetch_one(&mut *tx)
        .await
        .expect("begin_tenant_scoped must still set app.account_id");
    assert_eq!(seen_account, account);
    tx.commit().await.expect("commit");

    // A fresh transaction on the same physical connection must NOT inherit
    // member A's id. `app.*` is an unregistered placeholder GUC class, so it
    // reverts to `''` rather than to undefined — which is why 0009's helper
    // wraps it in NULLIF: `''` becomes NULL, i.e. "no member ocean visible",
    // never member A's real id.
    let mut tx2 = pool.begin().await.expect("begin unscoped transaction");
    let reverted: Option<String> =
        sqlx::query_scalar("SELECT current_setting('app.user_id', true)")
            .fetch_one(&mut *tx2)
            .await
            .expect("missing_ok current_setting must not error");
    assert_eq!(
        reverted.as_deref(),
        Some(""),
        "outside the scoped transaction `app.user_id` must revert to the placeholder base value, \
         never member A's real id"
    );
    tx2.rollback().await.ok();

    // The system principal is likewise transaction-local.
    let mut tx3 = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&pool, account)
        .await
        .expect("begin_tenant_system_scoped");
    let actor: String = sqlx::query_scalar("SELECT current_setting('app.tenant_actor')")
        .fetch_one(&mut *tx3)
        .await
        .unwrap();
    assert_eq!(actor, "system");
    tx3.commit().await.unwrap();

    let mut tx4 = pool.begin().await.unwrap();
    let actor: Option<String> =
        sqlx::query_scalar("SELECT current_setting('app.tenant_actor', true)")
            .fetch_one(&mut *tx4)
            .await
            .unwrap();
    assert_eq!(
        actor.as_deref(),
        Some(""),
        "`app.tenant_actor` must not survive its transaction — a leaked system principal would \
         silently disable the backstop for the next request on this connection"
    );
    tx4.rollback().await.ok();

    pool.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// CodeRabbit gap on PR #532 (`migrations_tenant/0032_membership_events.sql`):
/// this suite's tables above already prove `team_members`/`membership_events`
/// deny a non-member absent any grant (§1's generic per-table loop) and that a
/// real team MEMBER sees their own roster and audit trail (§3b). Neither
/// exercises the NEW OR-arm 0032 added — `tenant_rls_is_org_admin()` — which
/// is the entire point of that migration: post-OPS-4 cutover, running as the
/// non-BYPASSRLS `coralswarm_tenant_app` role, a non-member org admin
/// authorized at the HTTP route layer must actually be able to read a team's
/// roster and audit history, and must NOT thereby gain one atom of what that
/// team's ocean knows.
///
/// Both halves are asserted in the SAME session with the SAME grant, because
/// each is worthless without the other: a suite that only proved "the grant
/// widens roster visibility" would go green even if some future change
/// "fixed" a roster bug by widening `tenant_rls_visible_graph_ids()` itself
/// instead — the one thing 0032's own header calls out as the change this
/// design was built to avoid, since ~25 content tables (`atoms`, `nodes`,
/// `conversations`, `team_keys`, ...) all resolve through that single
/// function.
#[tokio::test]
async fn org_admin_grant_widens_team_governance_without_leaking_team_ocean_content() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_admin_grant_widens_team_governance_without_leaking_team_ocean_content: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_org_admin";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0032_membership_events");

    set_app_role_password(&owner).await;

    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // Member B: seeded as NOT a member of the fixture's one team (`seed`'s
    // `team_members` row names only `user_a`). Reused here as "the non-member
    // org admin" — the exact persona 0032 exists for.
    let org_admin = fx.user_b;

    // ── 0. Negative control: WITHOUT app.is_org_admin, a non-member sees
    // nothing. Without this, the assertions below could not distinguish "the
    // grant works" from "the policy already lets everyone through" — the same
    // failure mode `prove_denial_and_negative_control` guards against
    // elsewhere in this file, restated here because this test's own
    // fixture/grant combination is new.
    {
        let teams_n = count_as(&app, org_admin, "SELECT count(*) FROM teams").await;
        let members_n = count_as(&app, org_admin, "SELECT count(*) FROM team_members").await;
        let events_n = count_as(&app, org_admin, "SELECT count(*) FROM membership_events").await;
        assert_eq!(
            (teams_n, members_n, events_n),
            (0, 0, 0),
            "without app.is_org_admin set, a non-member must see zero rows across \
             teams/team_members/membership_events — otherwise this test cannot tell 'the grant \
             works' apart from 'the policy already lets everyone through'"
        );
    }

    // ── 1. The grant works ──────────────────────────────────────────────────
    // Same non-member, now WITH app.is_org_admin = 'true'. All three
    // governance tables become visible, one row each (the fixture seeds
    // exactly one team).
    {
        let teams_n = count_as_org_admin(&app, org_admin, "SELECT count(*) FROM teams").await;
        assert_eq!(
            teams_n, 1,
            "a non-member org admin must see the team's `teams` row"
        );

        let members_n =
            count_as_org_admin(&app, org_admin, "SELECT count(*) FROM team_members").await;
        assert_eq!(
            members_n, 1,
            "a non-member org admin must see the team's `team_members` roster"
        );

        let events_n =
            count_as_org_admin(&app, org_admin, "SELECT count(*) FROM membership_events").await;
        assert_eq!(
            events_n, 1,
            "a non-member org admin must see the team's `membership_events` audit trail"
        );

        // Load-bearing: every roster WRITE handler opens with exactly this
        // lock (`lock_team_row_scoped`'s `SELECT ... FOR UPDATE`, per 0032's
        // header). If this errors, the read grant above is real but every
        // roster write for a non-member admin still 404s.
        let locked = count_as_org_admin(
            &app,
            org_admin,
            &format!(
                "SELECT 1::bigint FROM teams WHERE id = '{}' FOR UPDATE",
                fx.team
            ),
        )
        .await;
        assert_eq!(
            locked, 1,
            "a non-member org admin must be able to `SELECT ... FOR UPDATE` the team row — every \
             roster write (add/re-role/remove a member) opens with this lock"
        );
    }

    // ── 2. The grant does NOT leak team-ocean content ───────────────────────
    // SAME non-member, SAME grant, still set. This is the assertion that would
    // catch someone "fixing" a future roster bug by widening
    // `tenant_rls_visible_graph_ids()` itself instead of adding a narrow
    // OR-arm to just these three governance tables — which would hand every
    // org admin the full contents (atoms, nodes, conversations, wrapped team
    // keys, ...) of every team ocean in the tenant, silently destroying the
    // entire security property 0032's design rests on.
    {
        for table in ["graphs", "atoms", "team_keys", "team_seat_grants"] {
            let sql = probe_sql(table, &fx);
            let n = count_as_org_admin(&app, org_admin, &sql).await;
            assert_eq!(
                n, 0,
                "the org-admin governance grant leaked `{table}` — a non-member org admin may \
                 read the team's roster and audit trail, and must gain NOTHING else. A nonzero \
                 count here means `tenant_rls_visible_graph_ids()` (or an equivalent OR-arm) was \
                 widened instead of the three governance-table policies staying narrow, which \
                 would silently expose every team ocean's contents to every org admin in the \
                 account"
            );
        }
    }

    // ── 3. `teams` keeps 0009's original WITH CHECK; team_members/
    // membership_events don't ───────────────────────────────────────────────
    // 0032's `teams` arm is deliberately USING-only, restating 0009's ORIGINAL
    // WITH CHECK verbatim — an org admin may READ a team row but may not
    // INSERT/UPDATE one, which is what stops them repointing `teams.graph_id`
    // at another ocean. `team_members`/`membership_events` stay BARE (derived
    // WITH CHECK), which is what makes the roster writes the whole migration
    // exists for actually work.
    {
        // teams: INSERT refused.
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(org_admin.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('app.is_org_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let ins = sqlx::query(
            "INSERT INTO teams(account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'org admin cannot create this', $3)",
        )
        .bind(fx.account)
        .bind(fx.g_team)
        .bind(org_admin)
        .execute(&mut *tx)
        .await;
        let err = ins.expect_err(
            "a non-member org admin's INSERT into `teams` must be refused — the grant is \
             USING-only, restating 0009's original WITH CHECK verbatim",
        );
        assert!(
            is_rls_denied(&err),
            "expected insufficient_privilege (42501) from `teams`' WITH CHECK on INSERT, got \
             {err:?}"
        );
        tx.rollback().await.ok();

        // teams: UPDATE refused — the exact "repoint graph_id at another
        // ocean" attack 0032's header names as the reason this arm stays
        // USING-only.
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(org_admin.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('app.is_org_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let upd =
            sqlx::query("UPDATE teams SET name = 'renamed by non-member org admin' WHERE id = $1")
                .bind(fx.team)
                .execute(&mut *tx)
                .await;
        let err = upd.expect_err(
            "a non-member org admin's UPDATE on `teams` must be refused — got a success instead",
        );
        assert!(
            is_rls_denied(&err),
            "expected insufficient_privilege (42501) from `teams`' WITH CHECK on UPDATE, got \
             {err:?}"
        );
        tx.rollback().await.ok();

        // team_members: INSERT succeeds (bare policy, derived WITH CHECK).
        let new_member = Uuid::new_v4();
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(org_admin.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('app.is_org_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let ins = sqlx::query(
            "INSERT INTO team_members(team_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(fx.team)
        .bind(new_member)
        .execute(&mut *tx)
        .await;
        assert!(
            ins.is_ok(),
            "a non-member org admin's INSERT into `team_members` must succeed — this IS the \
             roster-management grant 0032 exists to deliver: {ins:?}"
        );
        tx.rollback().await.ok();

        // membership_events: INSERT succeeds (bare policy, derived WITH
        // CHECK) — every roster write this grant enables emits its audit row
        // in the same transaction.
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(org_admin.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('app.is_org_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let ins = sqlx::query(
            "INSERT INTO membership_events(team_id, actor_user_id, target_user_id, action, new_role) \
             VALUES ($1, $2, $3, 'added', 'member')",
        )
        .bind(fx.team)
        .bind(org_admin)
        .bind(new_member)
        .execute(&mut *tx)
        .await;
        assert!(
            ins.is_ok(),
            "a non-member org admin's INSERT into `membership_events` must succeed: {ins:?}"
        );
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// The bootstrap chain: `create_team` must be able to write its own rows.
///
/// This is the test that gates `ORG_TEAMS_ENABLED`. Before
/// `0040_team_create_bootstrap_rls`, five of the six inserts below fail as the
/// real non-`BYPASSRLS` role, because `tenant_rls_visible_graph_ids()` gates a
/// team ocean on the `team_members` row that is itself the LAST insert — you
/// had to already be in a team to create it.
///
/// The sequence replayed here is `Store::create_team_tx_scoped`'s, in order and
/// with the same columns, including its client-minted ids: `INSERT ...
/// RETURNING` applies the SELECT policy, so reading an id back would fail on a
/// brand-new team ocean regardless of any `WITH CHECK`. If that method's insert
/// order changes, this test must change with it — that coupling is the point.
///
/// §0 is the negative control, and it is what stops this test passing
/// vacuously: WITHOUT `app.creating_graph_id`, the identical `teams` insert
/// must still be refused. Green therefore means "the bootstrap GUC is the only
/// reason these writes are permitted", not "the policies let anyone write".
#[tokio::test]
async fn create_team_bootstrap_writes_its_own_rows_under_rls() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping create_team_bootstrap_writes_its_own_rows_under_rls: set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_bootstrap";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree incl. 0040_team_create_bootstrap_rls");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The creator is a user with NO team membership anywhere in this tenant —
    // the exact persona the bootstrap exists for, and the one 0009's predicate
    // has nothing to say about. Deliberately not `fx.user_a` (already in the
    // fixture team, which would let the chain pass for the wrong reason).
    let creator = Uuid::new_v4();
    let g_new = Uuid::new_v4();
    let t_new = Uuid::new_v4();
    let k_new = Uuid::new_v4();

    // ── 0. Negative control: no bootstrap GUC, no write ─────────────────────
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(creator.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO graphs (id, account_id, name, kind) VALUES ($1, $2, 'x', 'team')")
            .bind(g_new)
            .bind(fx.account)
            .execute(&mut *tx)
            .await
            .expect("the `graphs` insert is permitted by 0009's WITH CHECK for kind='team'");
        let ins = sqlx::query(
            "INSERT INTO teams (id, account_id, graph_id, name, created_by) \
             VALUES ($1, $2, $3, 'x', $4)",
        )
        .bind(t_new)
        .bind(fx.account)
        .bind(g_new)
        .bind(creator)
        .execute(&mut *tx)
        .await;
        assert!(
            ins.as_ref().is_err_and(|e| is_rls_denied(e)),
            "WITHOUT app.creating_graph_id, the `teams` insert must still be refused (42501) — \
             otherwise this test cannot tell 'the bootstrap grant works' apart from 'these \
             policies let anyone write'. Got: {ins:?}"
        );
        tx.rollback().await.ok();
    }

    // ── 1. The full sequence, with the GUC ──────────────────────────────────
    let mut tx = app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(creator.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();

    sqlx::query("INSERT INTO graphs (id, account_id, name, kind) VALUES ($1, $2, 'New Team', 'team')")
        .bind(g_new)
        .bind(fx.account)
        .execute(&mut *tx)
        .await
        .expect("1/6 graphs");

    // `set_team_bootstrap_graph`'s SQL, verbatim. Set AFTER the graphs insert:
    // 0040's resolver checks `kind = 'team'`, so the row must exist.
    sqlx::query("SELECT set_config('app.creating_graph_id', $1, true)")
        .bind(g_new.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO teams (id, account_id, graph_id, name, created_by) \
         VALUES ($1, $2, $3, 'New Team', $4)",
    )
    .bind(t_new)
    .bind(fx.account)
    .bind(g_new)
    .bind(creator)
    .execute(&mut *tx)
    .await
    .expect("2/6 teams — the insert 0032's narrow WITH CHECK blocks without 0040's arm");

    sqlx::query("INSERT INTO team_keys (id, graph_id, version, created_by) VALUES ($1, $2, 1, $3)")
        .bind(k_new)
        .bind(g_new)
        .bind(creator)
        .execute(&mut *tx)
        .await
        .expect("3/6 team_keys");

    sqlx::query(
        "INSERT INTO team_member_key_envelopes (team_key_id, user_id, wrapped_key) \
         VALUES ($1, $2, '\\x00'::bytea)",
    )
    .bind(k_new)
    .bind(creator)
    .execute(&mut *tx)
    .await
    .expect("4/6 team_member_key_envelopes — the two-hop anchor");

    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(t_new)
        .bind(creator)
        .execute(&mut *tx)
        .await
        .expect("5/6 team_members — the row that cannot satisfy its own WITH CHECK");

    sqlx::query(
        "INSERT INTO membership_events (team_id, actor_user_id, target_user_id, action, new_role) \
         VALUES ($1, $2, $2, 'created', 'admin')",
    )
    .bind(t_new)
    .bind(creator)
    .execute(&mut *tx)
    .await
    .expect("6/6 membership_events");

    // ── 2. Self-extinguishing, in the SAME transaction ──────────────────────
    // The moment 5/6 landed, the graph has a member, so 0040's resolver stops
    // returning it. The grant is bounded by STATE, not merely by transaction
    // lifetime — a leaked GUC or a future call site that opens the scope
    // wrongly still buys nothing.
    let dead: bool = sqlx::query_scalar("SELECT tenant_rls_bootstrap_graph_id() IS NULL")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        dead,
        "0040's bootstrap arm must go dead the instant the team has a member — it is bounded by \
         state, not just by transaction lifetime"
    );

    // ── 3. And normal membership visibility has taken over ──────────────────
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM teams WHERE id = $1")
        .bind(t_new)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        visible, 1,
        "after the roster row lands, the creator must see the team through \
         `tenant_rls_visible_graph_ids()`'s ordinary membership arm"
    );

    tx.commit().await.expect("commit the bootstrapped team");

    // ── 4. The grant bought no content ──────────────────────────────────────
    // Same creator, fresh transaction, no GUC. They are now a legitimate member
    // of their OWN new team, so a bare `count(*)` is the WRONG probe here — it
    // would count their own team's key and read as a leak. Every probe is
    // therefore scoped to the FIXTURE's oceans (A's private reef and the team
    // the creator is not in), which is the boundary actually under test.
    for (table, sql) in [
        (
            "graphs",
            format!(
                "SELECT count(*) FROM graphs WHERE id IN ('{}', '{}')",
                fx.g_a, fx.g_team
            ),
        ),
        (
            "atoms",
            format!("SELECT count(*) FROM atoms WHERE graph_id = '{}'", fx.g_a),
        ),
        (
            "team_keys",
            format!(
                "SELECT count(*) FROM team_keys WHERE graph_id = '{}'",
                fx.g_team
            ),
        ),
        (
            "team_members",
            format!(
                "SELECT count(*) FROM team_members WHERE team_id = '{}'",
                fx.team
            ),
        ),
    ] {
        let n = count_as(&app, creator, &sql).await;
        assert_eq!(
            n, 0,
            "bootstrapping a team leaked `{table}` — creating one ocean must grant nothing about \
             any other"
        );
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// The bootstrap GUC must be unusable against a team ocean that already exists.
///
/// This is the assertion that makes `app.creating_graph_id` a bounded grant
/// rather than a bypass switch. 0040's resolver requires the named graph to
/// have ZERO `team_members` rows, so an established ocean can never be named —
/// which is why the arm does not need to trust its caller, and why a future
/// call site that sets the GUC carelessly cannot turn it into a hole.
#[tokio::test]
async fn team_create_bootstrap_arm_cannot_target_an_established_ocean() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping team_create_bootstrap_arm_cannot_target_an_established_ocean: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_bootstrap_neg";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // B is not a member of the fixture team; `fx.g_team` is established (the
    // fixture seeds a `team_members` row for A).
    let attacker = fx.user_b;

    let mut tx = app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(attacker.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.creating_graph_id', $1, true)")
        .bind(fx.g_team.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();

    let refuses: bool = sqlx::query_scalar("SELECT tenant_rls_bootstrap_graph_id() IS NULL")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        refuses,
        "0040's resolver must return NULL for a graph that already has team_members rows, \
         whatever `app.creating_graph_id` says"
    );

    // The two arms the GUC would otherwise unlock, against a team ocean the
    // actor cannot reach. `team_keys` is the one that matters most: it is the
    // key-rotation table for that ocean.
    let ins_team = sqlx::query(
        "INSERT INTO teams (account_id, graph_id, name, created_by) VALUES ($1, $2, 'pwn', $3)",
    )
    .bind(fx.account)
    .bind(fx.g_team)
    .bind(attacker)
    .execute(&mut *tx)
    .await;
    assert!(
        ins_team.as_ref().is_err_and(|e| is_rls_denied(e)),
        "naming an established team ocean in app.creating_graph_id must not permit a `teams` \
         insert into it. Got: {ins_team:?}"
    );
    tx.rollback().await.ok();

    let mut tx = app.begin().await.unwrap();
    sqlx::query("SELECT set_config('app.user_id', $1, true)")
        .bind(attacker.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.creating_graph_id', $1, true)")
        .bind(fx.g_team.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    // And it grants no READ either — asserted INSIDE this transaction, which
    // has `app.creating_graph_id` set. Probing from a transaction that does NOT
    // carry the GUC would only re-prove ordinary non-member denial (already
    // covered by the generic per-table loop above) and would say nothing at all
    // about whether the GUC widens reads — a check that passes because it
    // measured nothing.
    for (table, sql) in [
        ("teams", "SELECT count(*) FROM teams"),
        ("team_keys", "SELECT count(*) FROM team_keys"),
        ("team_members", "SELECT count(*) FROM team_members"),
        (
            "team_member_key_envelopes",
            "SELECT count(*) FROM team_member_key_envelopes",
        ),
    ] {
        let n: i64 = sqlx::query_scalar(sql).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(
            n, 0,
            "the bootstrap grant is INSERT-only — with app.creating_graph_id naming an \
             established team ocean, a non-member must still read zero rows from `{table}`"
        );
    }

    let ins_key =
        sqlx::query("INSERT INTO team_keys (graph_id, version, created_by) VALUES ($1, 2, $2)")
            .bind(fx.g_team)
            .bind(attacker)
            .execute(&mut *tx)
            .await;
    assert!(
        ins_key.as_ref().is_err_and(|e| is_rls_denied(e)),
        "naming an established team ocean must not permit writing a new `team_keys` row into it \
         — that is the key-rotation table. Got: {ins_key:?}"
    );
    tx.rollback().await.ok();

    // The ordinary non-member baseline, WITHOUT the GUC — kept as the explicit
    // contrast to the probes above, so "denied with the GUC" cannot be mistaken
    // for "denied only because something else was missing".
    let seen = count_as(&app, attacker, "SELECT count(*) FROM teams").await;
    assert_eq!(
        seen, 0,
        "sanity: a non-member reads nothing from `teams` without the bootstrap GUC either"
    );

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// `microsoft_calendar_sync_state`'s `member_ocean_isolation` `WITH CHECK`
/// (CodeRabbit round 1 on PR #692). The policy used to carry `USING` only —
/// a bare `USING`-only policy is `FOR ALL` and reuses `USING` as `WITH CHECK`
/// too, which gates on `graph_id IN (visible)` but says nothing about
/// whether `owner_user_id` is genuinely THAT graph's real owner. Proves,
/// against a REAL app-role connection (never `BYPASSRLS`):
///
///   * A may write a row pairing A's own `owner_user_id` with A's own
///     member ocean (`fx.g_a`) — the legitimate shape `workers::
///     microsoft_calendar_sync::sync_enabled` writes every tick.
///   * A may NOT pair B's `owner_user_id` with A's OWN `graph_id` — a graph
///     A can see (so a naive `graph_id IN (visible)` check alone would have
///     let this through), but is not B's ocean.
///   * B may NOT write a row claiming `owner_user_id = A` against A's graph
///     either — the same invariant from the other actor's session, where
///     `graphs`' OWN RLS additionally hides `g_a` from B entirely (a second,
///     independent backstop — see this migration's own header).
///   * A may NOT pair its own `owner_user_id` with a NON-'member'-kind graph
///     (`fx.g_team`) — the `graphs.kind = 'member'` arm of the `WITH CHECK`
///     subquery.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     microsoft_calendar_sync_state_write_check_enforces_owner_graph_invariant_pg
#[tokio::test]
async fn microsoft_calendar_sync_state_write_check_enforces_owner_graph_invariant_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping microsoft_calendar_sync_state_write_check_enforces_owner_graph_invariant_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_calsync_check";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The shared fixture's own `seed()` already writes one
    // `microsoft_calendar_sync_state` row (account, user_a, g_a) so the
    // roster's negative control has something to prove non-vacuously.
    // Clear it here: this test proves WITH CHECK denial, not uniqueness
    // conflicts, and every probe below is rolled back rather than committed
    // so this table starts (and stays) empty for the whole test.
    owner
        .execute("DELETE FROM microsoft_calendar_sync_state")
        .await
        .expect("clear fixture row");

    // Legit: A writes a row pairing its own owner_user_id with A's real
    // member ocean. Must succeed.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_tenant_app acting as A must be able to write a \
             microsoft_calendar_sync_state row pairing its own owner_user_id with A's real \
             member ocean — got {insert_own:?}"
        );
        tx.rollback().await.ok();
    }

    // Cross-owner: A attempts to pair B's owner_user_id with A's OWN graph.
    // Must be REJECTED.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_b)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.as_ref().is_err_and(is_rls_denied),
            "pairing a DIFFERENT owner_user_id (B) with A's own graph_id must be refused by \
             WITH CHECK — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();
    }

    // Same invariant, the other direction: B claims owner_user_id = A
    // against A's own graph. `graphs`' own RLS additionally hides `g_a`
    // from B entirely, so this is denied for two independent reasons.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_as_b_for_a = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_as_b_for_a.as_ref().is_err_and(is_rls_denied),
            "B writing a row claiming owner_user_id = A against A's own graph must be refused \
             — got {insert_as_b_for_a:?}"
        );
        tx.rollback().await.ok();
    }

    // A's own owner_user_id paired with a non-'member'-kind graph (the team
    // ocean, `fx.g_team`) must be REJECTED — the `graphs.kind = 'member'`
    // arm of the WITH CHECK subquery.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_team_kind = sqlx::query(
            "INSERT INTO microsoft_calendar_sync_state(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_team)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_team_kind.as_ref().is_err_and(is_rls_denied),
            "pairing owner_user_id with a non-'member'-kind graph_id (a team ocean) must be \
             refused — got {insert_team_kind:?}"
        );
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// `google_chat_watched_spaces`' `graph_id IS NULL` policy arm requires
/// `tenant_rls_is_system()` (security review round 2 on gsuite Train G4 PR
/// 3, finding H2). The ORIGINAL policy was `graph_id IS NULL OR graph_id IN
/// (tenant_rls_visible_graph_ids())` with NO account/actor anchor on the
/// NULL arm at all — unlike the control-plane twin, where that same arm is
/// conjoined with an `account_id = current_setting(...)` check.
/// `tenant_rls_visible_graph_ids()` resolves per-MEMBER ocean visibility,
/// not per-account, so a bare `graph_id IS NULL` was an UNCONDITIONAL
/// bypass: any real member session could read, update, and forge-insert an
/// UNROUTED row (one `ADDED_TO_SPACE` created for a space that has not yet
/// been routed to any ocean) belonging to ANOTHER member entirely.
///
/// Proves, against a REAL `coralswarm_tenant_app` connection scoped as
/// member A (never `BYPASSRLS`, never the system actor):
///   * An unrouted row (owner never picked a destination) is INVISIBLE to
///     a plain member-scoped SELECT.
///   * A member-scoped UPDATE against it affects ZERO rows (RLS filters it
///     out of the update's own visibility, the same "silently touches
///     nothing" shape every other `USING`-filtered UPDATE has).
///   * A fresh member-scoped INSERT of a NEW unrouted row is REJECTED by
///     `WITH CHECK` outright — a member session can never legitimately
///     create one; only the system-scoped `ADDED_TO_SPACE` webhook path
///     does.
///   * The negative control — under `tenant_rls_is_system()` (matching
///     `begin_tenant_system_scoped`, what every production caller of this
///     table actually uses today) — the SAME unrouted row IS visible,
///     proving the fix didn't also break the real, shipped code path.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     google_chat_watched_spaces_unrouted_row_requires_system_scope_pg
#[tokio::test]
async fn google_chat_watched_spaces_unrouted_row_requires_system_scope_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping google_chat_watched_spaces_unrouted_row_requires_system_scope_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_chat_unrouted";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The OWNER (bypasses RLS) seeds an unrouted row for the fixture's
    // account — the shape `connectors::google_chat::record_added_to_space`
    // writes: `graph_id IS NULL`, `mode IS NULL`, `owner_user_id IS NULL`
    // (the container archetype).
    owner
        .execute(
            "INSERT INTO google_chat_watched_spaces(account_id, space_id, space_type) \
             VALUES ('{account}', 'spaces/unrouted-secret', 'SPACE')"
                .replace("{account}", &fx.account.to_string())
                .as_str(),
        )
        .await
        .expect("seed unrouted row as owner");

    // A member-scoped SELECT (A) must see NOTHING.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/unrouted-secret'",
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            visible, 0,
            "a member-scoped session must NEVER see an unrouted google_chat_watched_spaces row \
             — the graph_id IS NULL arm must require system scope"
        );
        tx.rollback().await.ok();
    }

    // A member-scoped UPDATE (A) must affect ZERO rows.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let result = sqlx::query(
            "UPDATE google_chat_watched_spaces SET active = false, display_name = 'tampered' \
             WHERE space_id = 'spaces/unrouted-secret'",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            result.rows_affected(), 0,
            "a member-scoped UPDATE must touch ZERO rows of an unrouted space it cannot see — \
             this was a denial-of-ingest primitive before the H2 fix"
        );
        tx.rollback().await.ok();
    }

    // A member-scoped INSERT of a FRESH unrouted row (A's own account_id,
    // an account A genuinely belongs to) must be REJECTED by WITH CHECK —
    // no member session may ever create one; only the system-scoped
    // ADDED_TO_SPACE webhook path does.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let forged_insert = sqlx::query(
            "INSERT INTO google_chat_watched_spaces(account_id, space_id, space_type) \
             VALUES ($1, 'spaces/forged-by-a', 'SPACE')",
        )
        .bind(fx.account)
        .execute(&mut *tx)
        .await;
        assert!(
            forged_insert.as_ref().is_err_and(is_rls_denied),
            "a member-scoped INSERT of a fresh unrouted row must be refused by WITH CHECK — \
             got {forged_insert:?}"
        );
        tx.rollback().await.ok();
    }

    // Negative control: under the SYSTEM actor (what every production
    // caller of this table actually uses — begin_tenant_system_scoped),
    // the SAME unrouted row IS visible. Proves the fix didn't also break
    // the real, shipped code path.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let visible: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/unrouted-secret'",
            )
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            visible, 1,
            "under the system actor (begin_tenant_system_scoped's own GUC), the unrouted row \
             must still be visible — the fix must not have broken production's own read path"
        );
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// gsuite Train G4 PR 5 (G4.5) — the DM/group mailbox opt-in
/// (`docs/connectors/google-workspace.md` §9). Proves owner-only visibility
/// of a mailbox row comes for FREE from `google_chat_watched_spaces`'
/// EXISTING `member_ocean_isolation` policy (migration `0073`) once a real
/// `kind='member'` graph is the destination — no policy change was needed
/// for this train, and this test is what pins that claim against a REAL
/// `coralswarm_tenant_app` connection rather than asserting it from reading
/// the migration.
///
/// A mailbox row's `graph_id` is NEVER NULL (the archetype CHECK requires
/// `mode = 'single'` whenever `owner_user_id IS NOT NULL` — 0198's header),
/// so its visibility is governed purely by the policy's
/// `graph_id IN (tenant_rls_visible_graph_ids())` arm — and that function
/// already restricts a `kind='member'` graph to `owner_user_id =
/// tenant_rls_actor_user_id()` (0009). Two colleagues (A, B) in the SAME
/// account, each opting the SAME physical space into their OWN private
/// ocean (`uq_google_chat_watched_spaces_mailbox`, 0198's disjoint partial
/// unique index), must each see ONLY their own row — proving BOTH
/// directions (A cannot see B's, B cannot see A's) is what rules out a
/// same-account, one-way-only leak a weaker single-direction probe would
/// miss.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     google_chat_mailbox_optin_is_visible_only_to_its_own_owner_pg
#[tokio::test]
async fn google_chat_mailbox_optin_is_visible_only_to_its_own_owner_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping google_chat_mailbox_optin_is_visible_only_to_its_own_owner_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_tenant_rls_chat_mailbox";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The OWNER (bypasses RLS) seeds TWO independent mailbox opt-ins for the
    // SAME physical space — A into her own reef, B into his — exactly the
    // shape `connectors::google_chat::record_mailbox_optin` writes.
    sqlx::query(
        "INSERT INTO google_chat_watched_spaces \
            (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
         VALUES ($1, 'spaces/mailbox-shared', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
    )
    .bind(fx.account)
    .bind(fx.g_a)
    .bind(fx.user_a)
    .execute(&owner)
    .await
    .expect("seed A's mailbox opt-in");
    sqlx::query(
        "INSERT INTO google_chat_watched_spaces \
            (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
         VALUES ($1, 'spaces/mailbox-shared', 'GROUP_CHAT', 'single', $2, $3, $3)",
    )
    .bind(fx.account)
    .bind(fx.g_b)
    .bind(fx.user_b)
    .execute(&owner)
    .await
    .expect("seed B's mailbox opt-in");

    // A's own session sees ONLY her own opt-in.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let mine: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared' AND owner_user_id = $1",
        )
        .bind(fx.user_a)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(mine, 1, "A must see her OWN mailbox opt-in");
        let theirs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared' AND owner_user_id = $1",
        )
        .bind(fx.user_b)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            theirs, 0,
            "A must NEVER see B's mailbox opt-in — same physical space, same account, different \
             owner, and RLS must still separate them"
        );
        // The whole-row count (no owner filter) must ALSO be exactly 1 — a
        // filter-based probe alone could pass vacuously if RLS returned
        // every row and the `WHERE owner_user_id = ...` clause happened to
        // do the real filtering instead of the policy.
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(total, 1, "RLS itself — not the WHERE clause — must be doing the filtering");
        tx.rollback().await.ok();
    }

    // B's own session, symmetrically, sees ONLY his.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(total, 1, "B must see exactly his own row, never A's");
        let mine: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared' AND owner_user_id = $1",
        )
        .bind(fx.user_b)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(mine, 1);
        tx.rollback().await.ok();
    }

    // A member-scoped session can never forge a mailbox row into someone
    // ELSE's private ocean — `graph_id` must belong to a `kind='member'`
    // graph the ACTING user owns (0009's `WITH CHECK` on `graphs`), which
    // this table's own `graph_id IN (tenant_rls_visible_graph_ids())`
    // `WITH CHECK` inherits for free. A, acting as herself, tries to write a
    // row naming B's graph as the destination.
    //
    // **Two independent mechanisms now reject this, in a fixed order**
    // (CodeRabbit round 1 on PR #811 added the second one,
    // `google_chat_watched_spaces_validate_mailbox_owner_trg`, migration
    // `0076`/`0205`). Postgres runs BEFORE ROW triggers before evaluating a
    // policy's `WITH CHECK`, and the trigger's own `SELECT ... FROM graphs`
    // runs under A's OWN (member-scoped) session — under which B's graph is
    // itself invisible via `graphs`' `member_ocean_isolation` policy — so the
    // trigger's `SELECT` finds NO ROW and raises its "graph_id does not
    // exist" branch (SQLSTATE `P0001`) before `google_chat_watched_spaces`'
    // own `WITH CHECK` (SQLSTATE `42501`) is ever reached. Both outcomes are
    // the SAME correct answer — the write is refused — so this asserts on
    // the property that matters (refused, by ONE of the two DB-level
    // mechanisms this table now carries), not on which one happened to fire
    // first. See `google_chat_mailbox_owner_graph_mismatch_is_rejected_pg`
    // for the trigger's own dedicated, non-RLS-entangled probe (a mismatch
    // the trigger catches but a plain `graph_id` VISIBILITY check would not
    // — A's OWN graph, a DIFFERENT owner_user_id).
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let forged = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/forged-mailbox', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(fx.account)
        .bind(fx.g_b)
        .bind(fx.user_a)
        .execute(&mut *tx)
        .await;
        let rejected_by_rls_or_trigger = forged.as_ref().is_err_and(|e| {
            is_rls_denied(e) || matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("P0001"))
        });
        assert!(
            rejected_by_rls_or_trigger,
            "a member-scoped session must never be able to write a mailbox row pointing at a \
             DIFFERENT member's private ocean — got {forged:?}"
        );
        tx.rollback().await.ok();
    }

    // Negative control: the SYSTEM actor (`begin_tenant_system_scoped`,
    // what every production writer of this table actually runs under —
    // `connectors::google_chat::record_mailbox_optin`) sees BOTH rows.
    // Proves the per-owner isolation above is a real RLS boundary, not an
    // artefact of the fixture never having both rows visible to anybody.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/mailbox-shared'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(total, 2, "the system actor must see BOTH independently-opted-in rows");
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// CodeRabbit round 1 on PR #811 (gsuite Train G4 PR 5, G4.5): the archetype
/// CHECK constraint (0198) and the `member_ocean_isolation` policy (0073) both
/// guarantee a mailbox row's `graph_id` is NON-NULL and (under a plain
/// member-scoped session) VISIBLE to the acting user — but neither one ever
/// confirmed `graph_id` and `owner_user_id` actually name the SAME PERSON.
///
/// **This is the exploitable half the sibling test above does not cover.**
/// `google_chat_mailbox_optin_is_visible_only_to_its_own_owner_pg`'s own
/// "forged" probe uses B's graph (`fx.g_b`) with A's identity — RLS's
/// existing `graph_id IN (tenant_rls_visible_graph_ids())` `WITH CHECK`
/// already denies that shape on its own, because B's graph is not visible to
/// A's session at all. The shape THIS test probes is the mirror image and is
/// NOT caught by that mechanism: **A's own graph** (`fx.g_a` — visible to A,
/// so the existing `WITH CHECK` has nothing to object to) paired with **B's**
/// `owner_user_id`/`reader_user_id`. Before `migrations_tenant/
/// 0076_google_chat_mailbox_owner_trigger.sql`, nothing in the schema
/// rejected this: `workers::google_chat_sweep::sync_mailbox_space_locked`
/// would then load B's OWN `google_chat` OAuth credential and ingest B's
/// private DMs into A's ocean — A pulling a colleague's private messages into
/// her own private ocean using that colleague's own token, with neither
/// party's actual consent matching what happened.
///
/// Probed under BOTH postures a real write could reach this table with:
/// the SYSTEM actor (`begin_tenant_system_scoped`, what every production
/// writer — `connectors::google_chat::record_mailbox_optin` — actually runs
/// under, and under which RLS's OWN `WITH CHECK` offered zero protection
/// against this exact mismatch even before the trigger, since
/// `tenant_rls_visible_graph_ids()` treats every graph as visible under
/// system scope) and the schema-OWNER role (which bypasses RLS entirely, so
/// only a trigger — not a policy — can catch it there at all). A trigger,
/// not a widened CHECK, because Postgres CHECK constraints cannot reference
/// another table, and this invariant is inherently cross-table
/// (`google_chat_watched_spaces.owner_user_id` against `graphs.owner_user_id`).
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     google_chat_mailbox_owner_graph_mismatch_is_rejected_pg
#[tokio::test]
async fn google_chat_mailbox_owner_graph_mismatch_is_rejected_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping google_chat_mailbox_owner_graph_mismatch_is_rejected_pg: set ORGT_TEST_BASE_URL");
        return;
    };

    let db = "orgt_tenant_rls_chat_mailbox_owner_mismatch";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // Under the SYSTEM actor — the posture every production writer of this
    // table actually uses. Before the trigger this INSERT succeeded outright
    // (RLS's own WITH CHECK is vacuous under system scope for this table —
    // see `google_chat_watched_spaces_unrouted_row_requires_system_scope_pg`'s
    // own header for the general shape of why).
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.tenant_actor', 'system', true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let mismatched = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/owner-graph-mismatch-system', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(fx.account)
        .bind(fx.g_a) // A's own graph — visible under any session, RLS's WITH CHECK alone would allow this.
        .bind(fx.user_b) // ...but claimed as B's mailbox opt-in. Must be rejected.
        .execute(&mut *tx)
        .await;
        assert!(
            mismatched.is_err(),
            "a mailbox row naming A's graph_id but B's owner_user_id must be rejected even \
             under the SYSTEM actor — got {mismatched:?}"
        );
        tx.rollback().await.ok();
    }

    // Under the schema-OWNER role directly — RLS does not even apply here
    // (the owner role bypasses it), so only a TRIGGER (not a policy) can
    // catch this. Proves the fix is a real database constraint, not merely
    // an RLS-layer patch that a bulk-migration script or an admin tool
    // connecting as the owner role could still walk around.
    {
        let mismatched = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/owner-graph-mismatch-owner-role', 'GROUP_CHAT', 'single', $2, $3, $3)",
        )
        .bind(fx.account)
        .bind(fx.g_a)
        .bind(fx.user_b)
        .execute(&owner)
        .await;
        assert!(
            mismatched.is_err(),
            "the mismatch must be rejected even for a write that bypasses RLS entirely (the \
             schema-owner role) — got {mismatched:?}"
        );
    }

    // The legitimate, MATCHING pair (A's graph, A's own identity) must still
    // be perfectly writable — this migration must not have made every
    // mailbox opt-in unwritable.
    {
        let legit = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/owner-graph-match', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(fx.account)
        .bind(fx.g_a)
        .bind(fx.user_a)
        .execute(&owner)
        .await;
        assert!(legit.is_ok(), "a genuinely matching (graph_id, owner_user_id) pair must still be writable — got {legit:?}");
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// `outlook_everything_mode`'s `member_ocean_isolation` `WITH CHECK`
/// (tenant migration `0070`, m365 enterprise train PR 3.4) — the SAME
/// ownership-subquery shape as `microsoft_calendar_sync_state`'s own test
/// directly above (see that test's doc comment for the full "why a bare
/// `USING`-only policy would be wrong here" rationale; only the differences
/// are called out below). Proves, against a REAL app-role connection (never
/// `BYPASSRLS`):
///
///   * A may write a row pairing A's own `owner_user_id` with A's own
///     member ocean (`fx.g_a`) — the legitimate shape `routes::
///     outlook_everything_mode::set_everything_mode` writes on enable.
///   * A may NOT pair B's `owner_user_id` with A's OWN `graph_id`.
///   * B may NOT write a row claiming `owner_user_id = A` against A's graph.
///   * A may NOT pair its own `owner_user_id` with a NON-'member'-kind graph
///     (`fx.g_team`) — the same team-ocean carve-out this table's whole
///     feature exists to make structurally unreachable (contract §7
///     condition (b)).
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     outlook_everything_mode_write_check_enforces_owner_graph_invariant_pg
#[tokio::test]
async fn outlook_everything_mode_write_check_enforces_owner_graph_invariant_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping outlook_everything_mode_write_check_enforces_owner_graph_invariant_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_everything_mode_check";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The shared fixture's own `seed()` already writes one
    // `outlook_everything_mode` row (account, user_a, g_a) so the roster's
    // negative control has something to prove non-vacuously. Clear it here
    // (same precedent as `microsoft_calendar_sync_state`'s own WITH CHECK
    // test above): this test proves WITH CHECK denial, not uniqueness
    // conflicts, and every probe below is rolled back rather than committed
    // so this table starts (and stays) empty for the whole test.
    owner
        .execute("DELETE FROM outlook_everything_mode")
        .await
        .expect("clear fixture row");

    // Legit: A writes a row pairing its own owner_user_id with A's real
    // member ocean. Must succeed.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_tenant_app acting as A must be able to write an \
             outlook_everything_mode row pairing its own owner_user_id with A's real member \
             ocean — got {insert_own:?}"
        );
        tx.rollback().await.ok();
    }

    // Cross-owner: A attempts to pair B's owner_user_id with A's OWN graph.
    // Must be REJECTED.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_b)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.as_ref().is_err_and(is_rls_denied),
            "pairing a DIFFERENT owner_user_id (B) with A's own graph_id must be refused by \
             WITH CHECK — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();
    }

    // Same invariant, the other direction: B claims owner_user_id = A
    // against A's own graph. `graphs`' own RLS additionally hides `g_a`
    // from B entirely, so this is denied for two independent reasons.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_as_b_for_a = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_as_b_for_a.as_ref().is_err_and(is_rls_denied),
            "B writing a row claiming owner_user_id = A against A's own graph must be refused \
             — got {insert_as_b_for_a:?}"
        );
        tx.rollback().await.ok();
    }

    // A's own owner_user_id paired with a non-'member'-kind graph (the team
    // ocean, `fx.g_team`) must be REJECTED — the `graphs.kind = 'member'`
    // arm of the WITH CHECK subquery. This is the structural proof that
    // everything mode can never land in a team/shared ocean, contract §7
    // condition (b).
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_team_kind = sqlx::query(
            "INSERT INTO outlook_everything_mode(account_id, owner_user_id, graph_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_team)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_team_kind.as_ref().is_err_and(is_rls_denied),
            "pairing owner_user_id with a non-'member'-kind graph_id (a team ocean) must be \
             refused — got {insert_team_kind:?}"
        );
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

/// `gmail_everything_mode`'s `member_ocean_isolation` `WITH CHECK` (tenant
/// migration `0074`, gsuite enterprise train PR G2.4) — the SECOND
/// implementation of the SAME carve-out `outlook_everything_mode_write_
/// check_enforces_owner_graph_invariant_pg` (directly above) already proves
/// for Outlook; see that test's own doc comment for the full "why a bare
/// `USING`-only policy would be wrong here" rationale. Same four assertions,
/// against a REAL app-role connection (never `BYPASSRLS`):
///
///   * A may write a row pairing A's own `owner_user_id` with A's own
///     member ocean (`fx.g_a`) — the legitimate shape `routes::
///     gmail_everything_mode::set_everything_mode` writes on enable.
///   * A may NOT pair B's `owner_user_id` with A's OWN `graph_id`.
///   * B may NOT write a row claiming `owner_user_id = A` against A's graph.
///   * A may NOT pair its own `owner_user_id` with a NON-'member'-kind graph
///     (`fx.g_team`) — the same team-ocean carve-out this table's whole
///     feature exists to make structurally unreachable (contract §7
///     condition (b)).
///
/// Every probe below explicitly INSERTs `enabled = TRUE` — 0074's `WITH
/// CHECK` (fixed during adversarial review, see 0197's own header, "WITH
/// CHECK", for the full incident) gates its graph-ownership validation on
/// `NOT enabled OR EXISTS (...)`, specifically so a DISABLE never needs to
/// re-validate a graph it isn't touching. Omitting `enabled` here would
/// default to FALSE and every negative probe below would trivially
/// succeed (the check never even runs), proving nothing — unlike
/// `outlook_everything_mode`'s own twin above, whose `WITH CHECK` is still
/// unconditional (0192/0070 untouched by this PR).
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --test tenant_rls_pg \
///     gmail_everything_mode_write_check_enforces_owner_graph_invariant_pg
#[tokio::test]
async fn gmail_everything_mode_write_check_enforces_owner_graph_invariant_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping gmail_everything_mode_write_check_enforces_owner_graph_invariant_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_tenant_rls_gmail_everything_mode_check";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("apply tenant tree");

    set_app_role_password(&owner).await;
    let fx = seed(&owner).await;
    let app = app_role_pool(&db_url).await;

    // The shared fixture's own `seed()` already writes one
    // `gmail_everything_mode` row (account, user_a, g_a) so the roster's
    // negative control has something to prove non-vacuously. Clear it here
    // (same precedent as `outlook_everything_mode`'s own WITH CHECK test
    // above): this test proves WITH CHECK denial, not uniqueness conflicts,
    // and every probe below is rolled back rather than committed so this
    // table starts (and stays) empty for the whole test.
    owner
        .execute("DELETE FROM gmail_everything_mode")
        .await
        .expect("clear fixture row");

    // Legit: A writes a row pairing its own owner_user_id with A's real
    // member ocean. Must succeed.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_own = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_own.is_ok(),
            "coralswarm_tenant_app acting as A must be able to write a gmail_everything_mode \
             row pairing its own owner_user_id with A's real member ocean — got {insert_own:?}"
        );
        tx.rollback().await.ok();
    }

    // Cross-owner: A attempts to pair B's owner_user_id with A's OWN graph.
    // Must be REJECTED.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_cross_owner = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(fx.account)
        .bind(fx.user_b)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_cross_owner.as_ref().is_err_and(is_rls_denied),
            "pairing a DIFFERENT owner_user_id (B) with A's own graph_id must be refused by \
             WITH CHECK — got {insert_cross_owner:?}"
        );
        tx.rollback().await.ok();
    }

    // Same invariant, the other direction: B claims owner_user_id = A
    // against A's own graph. `graphs`' own RLS additionally hides `g_a`
    // from B entirely, so this is denied for two independent reasons.
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_b.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_as_b_for_a = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_a)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_as_b_for_a.as_ref().is_err_and(is_rls_denied),
            "B writing a row claiming owner_user_id = A against A's own graph must be refused \
             — got {insert_as_b_for_a:?}"
        );
        tx.rollback().await.ok();
    }

    // A's own owner_user_id paired with a non-'member'-kind graph (the team
    // ocean, `fx.g_team`) must be REJECTED — the `graphs.kind = 'member'`
    // arm of the WITH CHECK subquery. This is the structural proof that
    // everything mode can never land in a team/shared ocean, contract §7
    // condition (b).
    {
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.user_id', $1, true)")
            .bind(fx.user_a.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        let insert_team_kind = sqlx::query(
            "INSERT INTO gmail_everything_mode(account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(fx.account)
        .bind(fx.user_a)
        .bind(fx.g_team)
        .execute(&mut *tx)
        .await;
        assert!(
            insert_team_kind.as_ref().is_err_and(is_rls_denied),
            "pairing owner_user_id with a non-'member'-kind graph_id (a team ocean) must be \
             refused — got {insert_team_kind:?}"
        );
        tx.rollback().await.ok();
    }

    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}

#[tokio::test]
async fn managed_receipts_are_member_readable_but_system_mutated() {
    let Some(base) = base_url() else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "local PostgreSQL required");
        return;
    };
    let db = "orgt_managed_receipt_mutations";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);
    let owner = PgPoolOptions::new().max_connections(3).connect(&db_url).await.unwrap();
    let full = sqlx::migrate!("./migrations_tenant");
    sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(full.iter().filter(|migration| migration.version < 153).cloned().collect()),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    }.run(&owner).await.unwrap();
    set_app_role_password(&owner).await;
    let app = app_role_pool(&db_url).await;
    let identity: (String, String, bool, bool, i64) = sqlx::query_as(
        "SELECT current_user::text,session_user::text,rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user"
    ).fetch_one(&app).await.unwrap();
    assert_eq!(identity, ("coralswarm_tenant_app".into(), "coralswarm_tenant_app".into(), false, false, 0));
    let account = Uuid::new_v4();
    let member = Uuid::new_v4();
    let other = Uuid::new_v4();
    let graph = Uuid::new_v4();
    let foreign_graph = Uuid::new_v4();
    let unit = Uuid::new_v4();
    let run = Uuid::new_v4();
    for (id, user) in [(graph, member), (foreign_graph, other)] {
        sqlx::query("INSERT INTO graphs(id,account_id,name,kind,owner_user_id) VALUES($1,$2,'receipt policy fixture','member',$3)")
            .bind(id).bind(account).bind(user).execute(&owner).await.unwrap();
    }
    sqlx::query("INSERT INTO corpus_enrichment_runs(run_id,graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls) VALUES($1,$2,$3,$4,'execute','running','source-atoms-v1','fixture',1,100,4)")
        .bind(run).bind(graph).bind(account).bind(member).execute(&owner).await.unwrap();
    sqlx::query("INSERT INTO corpus_enrichment_units(unit_id,run_id,graph_id,kind,holding_id,state,charged_micro_usd) VALUES($1,$2,$3,'legacy_chunk',$4,'queued',1700)")
        .bind(unit).bind(run).bind(graph).bind(Uuid::new_v4()).execute(&owner).await.unwrap();
    let billed = Uuid::new_v4();
    let reserved = Uuid::new_v4();
    let stale = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let mut system = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&app, account).await.unwrap();
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd,reserved_micro_usd,charged_micro_usd,attempts) VALUES($1,'source_extraction',current_date,10000,1700,500,4)")
        .bind(account).execute(&mut *system).await.unwrap();
    for (id, home, unit_id, state, amount, settled) in [
        (billed, graph, None, "billed", 500_i64, Some(500_i64)),
        (reserved, graph, Some(unit), "reserved", 1000, None),
        (stale, graph, Some(unit), "reserved", 700, None),
        (foreign, foreign_graph, None, "not_generated", 500, Some(0)),
    ] {
        sqlx::query("INSERT INTO managed_attempt_receipts(receipt_id,account_id,purpose,period_start,graph_id,unit_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd,state,settled_micro_usd,settled_at,created_at) VALUES($1,$2,'source_extraction',current_date,$3,$4,$1,1,ARRAY['fixture'],1,1,$5,$6,$7,CASE WHEN $7 IS NULL THEN NULL ELSE now() END,now()-interval '1 hour')")
            .bind(id).bind(account).bind(home).bind(unit_id).bind(amount).bind(state).bind(settled)
            .execute(&mut *system).await.unwrap();
    }
    system.commit().await.unwrap();
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, member).await.unwrap();
    assert!(!sqlx::query_scalar::<_, bool>("SELECT tenant_rls_is_system()").fetch_one(&mut *tx).await.unwrap());
    let insert = sqlx::query("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'source_extraction',current_date,$2,$3,1,ARRAY['fixture'],1,1,1)")
        .bind(account).bind(graph).bind(Uuid::new_v4()).execute(&mut *tx).await;
    assert!(insert.as_ref().is_err_and(is_rls_denied));
    tx.rollback().await.unwrap();
    let mut system = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&app, account).await.unwrap();
    assert_eq!(sqlx::query("DELETE FROM managed_attempt_receipts").execute(&mut *system).await.unwrap().rows_affected(), 4);
    system.rollback().await.unwrap();
    full.run(&owner).await.unwrap();
    assert_eq!(count_as(&app, member, "SELECT count(*) FROM managed_attempt_receipts").await, 3);
    assert_eq!(count_as(&app, other, "SELECT count(*) FROM managed_attempt_receipts").await, 1);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM managed_attempt_receipts").fetch_one(&app).await.unwrap(), 0);
    let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, member).await.unwrap();
    let update = sqlx::query("UPDATE managed_attempt_receipts SET served_model='member-tamper' WHERE receipt_id=$1")
        .bind(billed).execute(&mut *tx).await;
    match update {
        Ok(result) => assert_eq!(result.rows_affected(), 0),
        Err(error) => assert!(is_rls_denied(&error)),
    }
    tx.rollback().await.unwrap();
    for id in [foreign, billed, reserved, stale] {
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(&app, account, member).await.unwrap();
        let deleted = sqlx::query("DELETE FROM managed_attempt_receipts WHERE receipt_id=$1")
            .bind(id).execute(&mut *tx).await.unwrap().rows_affected();
        tx.rollback().await.unwrap();
        assert_eq!(deleted, 0, "a member must not delete receipt {id}");
    }
    let mut system = coralswarm_backend::tenancy::scope::begin_tenant_system_scoped(&app, account).await.unwrap();
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM managed_attempt_receipts").fetch_one(&mut *system).await.unwrap(), 4);
    for expected in [true, false] {
        let settled: bool = sqlx::query_scalar("SELECT managed_settle($1,'billed',500,'fixture',NULL,NULL)")
            .bind(reserved).fetch_one(&mut *system).await.unwrap();
        assert_eq!(settled, expected);
    }
    let budget: (i64, i64, i64) = sqlx::query_as("SELECT reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods WHERE account_id=$1")
        .bind(account).fetch_one(&mut *system).await.unwrap();
    assert_eq!(budget, (700, 1000, 4));
    for expected in [1_i32, 0] {
        assert_eq!(sqlx::query_scalar::<_, i32>("SELECT managed_reap_stale(interval '10 minutes')").fetch_one(&mut *system).await.unwrap(), expected);
    }
    let budget: (i64, i64, i64) = sqlx::query_as("SELECT reserved_micro_usd,charged_micro_usd,attempts FROM managed_budget_periods WHERE account_id=$1")
        .bind(account).fetch_one(&mut *system).await.unwrap();
    assert_eq!(budget, (0, 1700, 4));
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT charged_micro_usd FROM corpus_enrichment_units WHERE unit_id=$1")
        .bind(unit).fetch_one(&mut *system).await.unwrap(), 1200);
    let outcomes: Vec<(String, i64)> = sqlx::query_as("SELECT state,settled_micro_usd FROM managed_attempt_receipts WHERE unit_id=$1 ORDER BY settled_micro_usd")
        .bind(unit).fetch_all(&mut *system).await.unwrap();
    assert_eq!(outcomes, vec![("billed".into(), 500), ("ambiguous".into(), 700)]);
    let before_delete: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT to_jsonb(receipt) FROM managed_attempt_receipts receipt ORDER BY receipt_id",
    ).fetch_all(&mut *system).await.unwrap();
    sqlx::query("SAVEPOINT immutable_receipt_history").execute(&mut *system).await.unwrap();
    let delete_error = sqlx::query("DELETE FROM managed_attempt_receipts")
        .execute(&mut *system).await.expect_err("upgraded source receipt history is immutable");
    let database_error = delete_error.as_database_error().expect("history guard database error");
    assert_eq!(database_error.code().as_deref(), Some("P0001"));
    assert_eq!(database_error.message(), "source receipt history cannot be deleted");
    sqlx::query("ROLLBACK TO SAVEPOINT immutable_receipt_history").execute(&mut *system).await.unwrap();
    let after_delete: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT to_jsonb(receipt) FROM managed_attempt_receipts receipt ORDER BY receipt_id",
    ).fetch_all(&mut *system).await.unwrap();
    assert_eq!(before_delete.len(), 4);
    assert_eq!(after_delete, before_delete, "failed deletion must preserve every receipt row and identity");
    system.rollback().await.unwrap();
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM managed_attempt_receipts").fetch_one(&owner).await.unwrap(), 4);
    app.close().await;
    owner.close().await;
    retained_database::finish(&mut admin_conn, db, false).await.unwrap();
}


/// The huddle policy is missing-ok on BOTH planes. A real pooled backend
/// retains an empty custom-GUC placeholder after local scope ends; that is
/// still no account, never a UUID to cast or authority to read/write.
#[tokio::test]
async fn slack_huddle_account_scope_denies_empty_and_reset_gucs_on_both_planes() {
    let Some(base) = base_url() else {
        eprintln!("skipping slack_huddle_account_scope_denies_empty_and_reset_gucs_on_both_planes: set ORGT_TEST_BASE_URL");
        return;
    };
    let mut admin_conn = admin(&base).await;
    let databases = ["orgt_huddle_scope_control", "orgt_huddle_scope_tenant"];
    let account_a = Uuid::new_v4();
    let account_b = Uuid::new_v4();
    let row_a = Uuid::new_v4();
    let row_b = Uuid::new_v4();
    let org_a = format!("org_huddle_scope_a_{account_a}");
    let org_b = format!("org_huddle_scope_b_{account_b}");
    for (plane, db, role) in [
        ("control", databases[0], "coralswarm_app"),
        ("tenant", databases[1], "coralswarm_tenant_app"),
    ] {
        recreate(&mut admin_conn, db).await;
        let db_url = with_db(&base, db);
        let owner = PgPoolOptions::new().max_connections(2).connect(&db_url).await.unwrap();
        if plane == "control" {
            sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
            let user_a = Uuid::new_v4();
            let user_b = Uuid::new_v4();
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2),($3,$4)")
                .bind(user_a).bind(format!("huddle-scope-a-{user_a}@test.local"))
                .bind(user_b).bind(format!("huddle-scope-b-{user_b}@test.local"))
                .execute(&owner).await.unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id,clerk_org_id) VALUES($1,$2,$3),($4,$5,$6)")
                .bind(account_a).bind(user_a).bind(&org_a)
                .bind(account_b).bind(user_b).bind(&org_b)
                .execute(&owner).await.unwrap();
            // Test-only password; no role attributes, membership or policies change.
            sqlx::query(&format!("ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"))
                .execute(&owner).await.unwrap();
        } else {
            sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
            set_app_role_password(&owner).await;
            sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,$1)")
                .bind(&org_a).execute(&owner).await.unwrap();
        }
        // B is a real other account in the paired control fixture; this tenant
        // contains only A's row, matching the one-org-per-tenant boundary.
        for (row, account) in [(row_a, account_a), (row_b, account_b)] {
            if plane == "tenant" && account == account_b { continue; }
            sqlx::query("INSERT INTO slack_huddle_consents(id,account_id,workspace_id,channel_id,huddle_thread_ts,fingerprint,non_attendee_count,capture_payload) VALUES($1,$2,'scope-workspace','scope-channel',$3,'scope-fixture',0,'{}')")
                .bind(row).bind(account).bind(row.to_string()).execute(&owner).await.unwrap();
        }
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&owner).await.unwrap(), 1);
        let opts = PgConnectOptions::from_str(&db_url).unwrap()
            .username(role).password(APP_ROLE_TEST_PASSWORD);
        // Keep this exact physical, ordinary-role connection through every state.
        let mut app = PgConnection::connect_with(&opts).await.unwrap();
        let (who, superuser, bypass): (String, bool, bool) = sqlx::query_as(
            "SELECT current_user::text,rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user",
        ).fetch_one(&mut app).await.unwrap();
        assert_eq!(who, role);
        assert!(!superuser && !bypass);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM pg_class WHERE relnamespace='public'::regnamespace AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)")
            .fetch_one(&mut app).await.unwrap(), 0);
        let initial: Option<String> = sqlx::query_scalar("SELECT current_setting('app.account_id',true)")
            .fetch_one(&mut app).await.unwrap();
        assert!(initial.is_none(), "fresh login must genuinely start with no account GUC");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap(), 0, "never-defined scope denies");

        for (account, expected) in [(account_a, 1_i64), (account_b, 0_i64)] {
            let mut tx = app.begin().await.unwrap();
            sqlx::query("SELECT set_config('app.account_id',$1,true)")
                .bind(account.to_string()).execute(&mut *tx).await.unwrap();
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
                .bind(row_a).fetch_one(&mut *tx).await.unwrap(), expected, "valid matching/foreign account scope");
            if plane == "control" && account == account_b {
                assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
                    .bind(row_b).fetch_one(&mut *tx).await.unwrap(), 1, "B is real and may read its own row");
            }
            let inserted = sqlx::query("INSERT INTO slack_huddle_consents(id,account_id,workspace_id,channel_id,huddle_thread_ts,fingerprint,non_attendee_count,capture_payload) VALUES($1,$2,'scope-workspace','scope-channel',$3,'scope-write',0,'{}')")
                .bind(Uuid::new_v4()).bind(account_a).bind(Uuid::new_v4().to_string())
                .execute(&mut *tx).await;
            if account == account_a {
                assert_eq!(inserted.unwrap().rows_affected(), 1, "own account may write");
            } else {
                assert!(inserted.as_ref().err().is_some_and(is_rls_denied), "foreign account write must fail WITH CHECK, not another error: {inserted:?}");
            }
            tx.rollback().await.unwrap();
            let reset: Option<String> = sqlx::query_scalar("SELECT current_setting('app.account_id',true)")
                .fetch_one(&mut app).await.unwrap();
            assert_eq!(reset.as_deref(), Some(""), "real rollback leaves the custom empty placeholder");
            assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
                .bind(row_a).fetch_one(&mut app).await.unwrap(), 0, "rollback restores no account authority");
        }
        let mut tx = app.begin().await.unwrap();
        sqlx::query("SELECT set_config('app.account_id',$1,true)").bind(account_a.to_string())
            .execute(&mut *tx).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut *tx).await.unwrap(), 1);
        tx.commit().await.unwrap();
        let reset: Option<String> = sqlx::query_scalar("SELECT current_setting('app.account_id',true)")
            .fetch_one(&mut app).await.unwrap();
        assert_eq!(reset.as_deref(), Some(""), "real commit also restores empty scope");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap(), 0);
        sqlx::query("SELECT set_config('app.account_id','',false)").execute(&mut app).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap(), 0, "explicit empty scope denies without parsing");
        sqlx::query("RESET app.account_id").execute(&mut app).await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,Option<String>>("SELECT current_setting('app.account_id',true)")
            .fetch_one(&mut app).await.unwrap().as_deref(), Some(""));
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap(), 0, "explicit empty/RESET denies without UUID error");
        let mut tx = app.begin().await.unwrap();
        let empty_write = sqlx::query("INSERT INTO slack_huddle_consents(id,account_id,workspace_id,channel_id,huddle_thread_ts,fingerprint,non_attendee_count,capture_payload) VALUES($1,$2,'scope-workspace','scope-channel',$3,'scope-empty',0,'{}')")
            .bind(Uuid::new_v4()).bind(account_a).bind(Uuid::new_v4().to_string())
            .execute(&mut *tx).await;
        assert!(empty_write.as_ref().err().is_some_and(is_rls_denied), "empty scope write must fail RLS WITH CHECK, not UUID parsing: {empty_write:?}");
        tx.rollback().await.unwrap();
        owner.execute("ALTER TABLE slack_huddle_consents DISABLE ROW LEVEL SECURITY").await.unwrap();
        let leaked = sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap();
        owner.execute("ALTER TABLE slack_huddle_consents ENABLE ROW LEVEL SECURITY").await.unwrap();
        assert_eq!(leaked, 1, "same empty-scope connection sees the real row only with RLS disabled");
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM slack_huddle_consents WHERE id=$1")
            .bind(row_a).fetch_one(&mut app).await.unwrap(), 0, "restored RLS denies again");
        app.close().await.unwrap();
        owner.close().await;
    }
    // Both planes passed before any cleanup; a failure retains the actual catalogs.
    for db in databases { retained_database::finish(&mut admin_conn, db, true).await.unwrap(); }
}

/// perf-auth-roundtrips: a member-scoped request transaction used to pay
/// `BEGIN` plus FOUR separate `set_config` statements (`search_path`,
/// `app.account_id`, `app.user_id`, `statement_timeout`) before its first
/// real query. `tenancy::scope` now sends all of them in ONE statement. This
/// pins both halves: exactly one statement after `BEGIN`, and every GUC it
/// carries in effect — transaction-locally — inside the transaction, and
/// reverted on the next borrower of the same physical connection.
#[tokio::test]
async fn member_scoped_transaction_sets_every_guc_in_one_statement() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping member_scoped_transaction_sets_every_guc_in_one_statement: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    use coralswarm_backend::sqlx_statement_log::record;
    use coralswarm_backend::tenancy::extractor::Tenant;
    use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantContext};

    let db = "orgt_tenant_rls_scope_one_stmt";
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let db_url = with_db(&base, db);
    // One physical connection, so the post-commit read below provably runs
    // on the connection the scoped transaction just released.
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("connect");

    let account = Uuid::new_v4();
    let member = Uuid::new_v4();
    let tenant = Tenant {
        store: None,
        pool: Some(pool.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: "org_scope_one_stmt".to_string(),
            account_id: account,
            user_id: member,
            pool: pool.clone(),
            page_pool: None,
        })),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };

    // sqlx does not log `BEGIN` (it goes out via `queue_simple_query`), so
    // the ONE recorded statement is the combined set_config.
    let (tx, log) = record(tenant.begin_member_scoped(account)).await;
    let mut tx = tx.expect("begin_member_scoped");
    eprintln!("org begin_member_scoped issued {} statement(s) after BEGIN:", log.count());
    for s in log.statements() {
        eprintln!("  {s}");
    }
    assert_eq!(
        log.count(),
        1,
        "an org member-scoped transaction must set every GUC in ONE statement after BEGIN"
    );
    assert_eq!(log.count_containing("set_config"), 1);

    let (search_path, account_seen, user_seen, timeout): (String, String, String, String) =
        sqlx::query_as(
            "SELECT current_setting('search_path'), current_setting('app.account_id'), \
             current_setting('app.user_id'), current_setting('statement_timeout')",
        )
        .fetch_one(&mut *tx)
        .await
        .expect("read back every GUC inside the scoped transaction");
    assert_eq!(search_path, "\"$user\", public", "search_path must be pinned (Neon pooler)");
    assert_eq!(account_seen, account.to_string());
    assert_eq!(user_seen, member.to_string());
    assert_eq!(timeout, "10s", "W2 request statement_timeout (10000 ms) must be in effect");
    tx.commit().await.expect("commit");

    // Same physical connection, next transaction: nothing survived.
    let mut tx2 = pool.begin().await.unwrap();
    let (account_after, user_after, timeout_after): (Option<String>, Option<String>, String) =
        sqlx::query_as(
            "SELECT current_setting('app.account_id', true), \
             current_setting('app.user_id', true), current_setting('statement_timeout')",
        )
        .fetch_one(&mut *tx2)
        .await
        .unwrap();
    assert_eq!(account_after.as_deref(), Some(""), "app.account_id must not leak");
    assert_eq!(user_after.as_deref(), Some(""), "app.user_id must not leak");
    assert_eq!(timeout_after, "0", "statement_timeout must revert at COMMIT (SET LOCAL semantics)");
    tx2.rollback().await.ok();

    // Personal / absent decision: EXACTLY `begin_account_scoped` — one
    // statement carrying search_path + app.account_id, no app.user_id and no
    // request statement_timeout.
    let personal = Tenant {
        store: None,
        pool: Some(pool.clone()),
        decision: Some(TenancyDecision::Personal),
        class: coralswarm_backend::route_class::RouteClass::Other,
    };
    let (tx, log) = record(personal.begin_member_scoped(account)).await;
    let mut tx = tx.expect("begin_member_scoped (personal)");
    assert_eq!(log.count(), 1, "personal plane: one statement after BEGIN");
    let (search_path, account_seen, user_seen, timeout): (String, String, Option<String>, String) =
        sqlx::query_as(
            "SELECT current_setting('search_path'), current_setting('app.account_id'), \
             current_setting('app.user_id', true), current_setting('statement_timeout')",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(search_path, "\"$user\", public");
    assert_eq!(account_seen, account.to_string());
    assert_eq!(user_seen.as_deref(), Some(""), "no per-user boundary on the consumer plane");
    assert_eq!(timeout, "0", "the request statement_timeout is an org-plane-only bound");
    tx.rollback().await.ok();

    pool.close().await;
    retained_database::finish(&mut admin_conn, db, true)
        .await
        .ok();
}
