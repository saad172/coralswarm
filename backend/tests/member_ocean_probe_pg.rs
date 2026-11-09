//! Member-private-oceans PR 11 — the assembled adversarial probe suite
//! (`docs/plans/member-private-oceans.md` §6). Modelled directly on
//! `cross_tenant_probe_pg.rs`: real scratch Postgres databases, the real
//! gate → extractor → route → resolver chain (never a direct resolver/Store
//! call), `ORGT_TEST_BASE_URL`-gated.
//!
//! Individual PRs (0/1/2/3/4/5/6/10) each added narrow tests for their own
//! surface as they landed — `group_b_probe_pg.rs` (PR 0's Group-B leaks),
//! `member_ocean_promotion_pg.rs` (PR 6's intra-DB promotion fork),
//! `member_ocean_provisioning_pg.rs` / `member_ocean_reconciler_pg.rs` (PR 3/4's
//! triggers), `reachability_pg.rs` / `reachable_graphs_parity_pg.rs` (the
//! predicate itself). This file does NOT re-prove any of those in isolation;
//! it assembles the SYSTEMATIC matrix §6 asks for — every ocean-resolving
//! surface, exercised against ONE shared adversarial fixture, for a plain
//! member AND an org admin, live AND archived — with an assertion on the
//! RESPONSE BODY (never status-code-only) that member A's content never
//! appears.
//!
//! # Fixture (shared by every test; see `setup`)
//!
//! One org tenant. Members **A** and **B** (`org:member`) and **C**
//! (`org:admin`). A and B each own a primary `kind='member'` ocean seeded with
//! a reef (node), a coral (atom), a meeting transcript, and a conversation. An
//! org commons ocean (`kind='org'`) and a team ocean (`kind='team'`) containing
//! B but not A. Plus A's CONSUMER-plane personal account and its Primary
//! ocean, for the plane-split probes. Every string unique to A's private
//! content embeds the marker [`A_SECRET`] — every "must not leak" assertion in
//! this file greps the raw response body for it, not just the status code.
//!
//! # Coverage matrix (plan §6; ✓ = this file, live in the fixture above)
//!
//! | # | Surface | Test |
//! |---|---|---|
//! | 1 | `GET /v1/graphs` | `read_deny_matrix_denies_member_b` / `..._org_admin_c` |
//! | 2 | `graphs/{A}`, `/atoms`, `/sources`, `/stats`, `/atlas` | same |
//! | 2 | `/v1/nodes?graph_id={A}`, `/v1/nodes/{A's reef}/atoms` | same |
//! | 2 | `/v1/atoms/{A's coral}` (+ R2 never fetched) | same |
//! | 8 | atlas, meetings/summary, sessions, relationships | same |
//! | 5 | `queries`/`queries/stream` scoped to A | same |
//! | 5 | `queries` history, `activity` | `query_history_and_activity_are_scoped_per_user` |
//! | 3 | `sync/pull` (no ops for A) | `sync_pull_must_not_leak_another_members_ocean` (this probe found and confirmed a real leak here; fixed and merged as #345) |
//! | 3 | `sync/push` naming A (refused, no auto-provision) | `sync_push_refuses_to_write_into_another_members_ocean` |
//! | 6 | MCP read tools (all 12) + default-ocean resolution | `mcp_read_tools_deny_member_b_and_resolve_default_to_own` / `..._org_admin_c` |
//! | 7 | `ask_ocean` mixed-scope: exact denial before task creation | folded into the MCP tests above |
//! | 7 | Mixed private scope is denied; permitted typed evidence must independently resolve to reachable owning graphs | `ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id` / `..._for_org_admin_c` |
//! | 7 | supersession READ side: an out-of-scope SUPERSEDER reef id must never surface via `superseded_by` (this probe found and fixed a real leak — see defect 3 below) | `supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean` |
//! | 7 | supersession READ side, `GET /v1/nodes/{id}/supersessions`: another member holding A's reef id must get nothing — asserted on TITLES (content) as well as ids, and on the far endpoint of an edge the caller's OWN reef is an endpoint of | `supersessions_listing_denies_another_members_reef` |
//! | 7 | supersession WRITE side: `POST`/`DELETE /v1/nodes/{id}/supersede` must refuse an endpoint the caller cannot REACH, and refuse to connect two different oceans at all — and so must the OTHER write door, `/v1/sync/push`, whose op-level authorization only ever sees the op's declared `graph_id` | `supersede_refuses_to_create_a_cross_ocean_edge` |
//! | 7 | the paired migration (control 0094 / tenant 0008): pre-existing cross-ocean edges are deleted, recorded first, and legitimate same-ocean edges survive | `migration_deletes_and_records_cross_ocean_supersession_edges_control_tree` / `..._tenant_tree` |
//! | 6 | `add_context` with A's ocean id | `mcp_add_context_denied_for_as_ocean` |
//! | 4/17 | promotion: admin not exempt | `promotion_admin_is_not_exempt` (member/member + target-refusal already exhaustively covered by `member_ocean_promotion_pg.rs`) |
//! | — | shares (create/list) deny B/C for A's meeting | folded into the read-deny matrix |
//! | — | teams: `teams.graph_id` can never functionally reach a `kind='member'` graph | `teams_graph_id_pointing_at_member_kind_still_denied` |
//! | 5/12 | Archived ⇒ unreachable for EVERYONE including A and admin C | `member_ocean_archived_is_unreachable_including_owner_and_admin` |
//! | 5 | Cloud whole-original link requires selected consent; live/archive scoped Store lookup remains enforced | `archived_link_share_remains_viewable` |
//! | 5 | Session-share tenant-tree twin resolves + archived_at gate (Store layer — no authed org-tenant view route exists yet) | `session_share_tenant_twin_and_archived_gate_on_org_tenant_plane` |
//! | 10 | Reachability parity + defense-in-depth if a `kind='member'` row ever existed on the consumer plane | `member_kind_row_creation_blocked_on_consumer_plane` |
//! | P11 | plane split: org host excludes consumer ocean, apex excludes member ocean | `plane_split_org_claim_and_personal_token_never_cross` |
//! | P11 | `kind='member'` refused via the public API on any plane | `member_kind_row_creation_blocked_on_consumer_plane` |
//!
//! Not attempted here (out of scope for this PR — see the PR body):
//! restore-at-day-30/61 and purge-at-day-60 (plan matrix rows 13-14) depend on
//! the archive/restore worker and purge worker that PRs 7/8 own and have not
//! landed on this branch; revoked-org-then-reconcile (row 15) depends on the
//! same reconciler. Row 11 (rolling-deploy skew) is a deploy-topology property,
//! not something a single-binary test can exercise — `reachability.rs`'s own
//! module doc argues it structurally (the predicate lives in a DB function, not
//! compiled-in Rust). Stage 2 rows 19-23 (`kind='channel'`) don't exist in the
//! schema yet.
//!
//! # Content leaks vs. IDENTIFIER leaks (what the `A_SECRET` grep cannot see)
//!
//! Every "must not leak" assertion described above greps for [`A_SECRET`], a
//! marker planted in A's CONTENT. That is a complete test for content
//! disclosure and a blind spot for IDENTIFIER disclosure — a response naming
//! A's coral or reef id while quoting none of A's text passes every one of
//! them. The gap is not theoretical: it was demonstrated by breaking
//! `run_ask`'s scope filter, at which point A's coral id travelled back in
//! `ask_ocean`'s `citations` array while all 12 pre-existing tests here stayed
//! GREEN and `post_mcp_rpc`'s raw-bytes marker grep never fired (the response
//! in `sources` mode is `answer: null`, `citation_details: []`, and a bare
//! `Vec<Uuid>` of citations — there is no text in it to grep).
//!
//! Ids are not a lesser disclosure here. A coral id is a durable handle into
//! `get_atom`, `search_atoms` and the sync protocol; it confirms the ocean
//! exists and holds a coral matching an attacker-chosen query, which is a
//! content oracle; and `multi_graph_retrieve`'s per-ocean floor guarantee
//! promotes each in-scope ocean's best hit past the global cap, so any such
//! regression is a repeatable per-ocean oracle rather than a one-off.
//!
//! The two probes added for this class therefore assert on IDS, and assert the
//! property POSITIVELY — "every referenced coral's owning ocean, resolved from
//! the database, is in the caller's real `GET /v1/graphs` reachable set" —
//! rather than blacklisting A's specific ids, so they catch a reference into
//! ANY unreachable ocean. Both carry the positive controls the non-vacuity
//! rules below demand, and both were verified to FAIL when the guarded
//! behavior is broken (see the PR body for the two induced-leak runs).
//!
//! # Three real defects this suite found
//!
//! 1. **FIXED (#345).** `routes::sync::pull`'s hand-rolled SQL join gated
//!    `kind='team'` but never `kind='member'` (and never excluded archived
//!    oceans) — `GET /v1/sync/pull` returned every op (full entity payloads)
//!    for ANY member's private ocean to every org member, once
//!    `MEMBER_OCEANS_ENABLED` was live. `push` was fine (it delegates to
//!    `ReachabilityResolver`); only `pull`'s direct-SQL predicate had never
//!    been updated for member ocean. Caught by this exact probe on its first
//!    real CI run — nothing else in the tree caught it. Fixed and merged as
//!    #345 (`security(sync): gate /v1/sync/pull on member ownership and
//!    archived state`); `sync_pull_must_not_leak_another_members_ocean` is no
//!    longer `#[ignore]`d.
//! 2. **FIXED (#347).** Authed `GET /v1/meetings/shares/view/{token}` was
//!    completely broken for ANY org-tenant meeting share — live or archived,
//!    "link" or "team" visibility. `routes::shares::resolve_share_by_token`
//!    runs `SELECT resolve_meeting_share_account_id($1)` against
//!    `tenant.pool` under an Org decision, but that `SECURITY DEFINER`
//!    bootstrap function was defined ONLY in `migrations_control/
//!    0086_webhook_share_bootstrap_functions.sql` — it had never been given a
//!    tenant-tree twin (unlike the paired reachability functions PR 2 added),
//!    so the call 500'd with "function does not exist" before the
//!    visibility/reachability/`archived_at` check was ever reached. This test
//!    originally targeted a narrower hypothesis (only the `archived_at` join
//!    was missing, so a live "link" share would incorrectly stay viewable
//!    after archive) — the broader, worse truth, confirmed by asserting a
//!    LIVE share also 500'd, was that the archived-specific gap couldn't even
//!    be *observed*, because the whole path already errored first for
//!    org-tenant content. Consumer-plane (personal-account) shares were
//!    unaffected — this was specific to the org-tenant content plane. Fixed
//!    by `migrations_tenant/0007_share_account_id_functions.sql` (the
//!    tenant-tree twin, for both the meeting- and session-share resolvers)
//!    plus an `archived_at` join added to `Store::get_active_meeting_share_
//!    by_token[_scoped]`/`get_active_session_share_by_token[_scoped]`. See
//!    `archived_link_share_remains_viewable` (now un-`#[ignore]`d, asserting
//!    BOTH that a live share resolves with real content and an archived one
//!    404s) and `session_share_tenant_twin_and_archived_gate_on_org_tenant_
//!    plane` (the session-share half, at the Store layer — that surface has
//!    no authed org-tenant HTTP route yet).
//! 3. **FIXED (#348, the PR that added the identifier-disclosure section
//!    above).** Cross-ocean
//!    SUPERSEDER identifier disclosure. `Store::list_supersession_targets`
//!    matches `supersession_edge` on `superseded_id` only — nothing constrains
//!    which ocean the SUPERSEDER lives in, and the table has no same-graph
//!    CHECK. `POST /v1/nodes/{id}/supersede` authorizes both endpoints with
//!    `node_owned_by_account`, and in an org tenant every member-private ocean
//!    shares the one org account, so member A can record "the org commons reef
//!    is superseded by MY private reef" using only ids she already holds. Any
//!    other member then retrieving that org commons reef got A's private reef
//!    id back in `search_atoms`' `superseded_by` (and, with an LLM configured,
//!    `ask_ocean`'s `citation_details[].superseded_by` and the `[SUPERSEDED —
//!    see reef <id>]` prose annotation) — from a query that never named A's
//!    ocean and carried none of A's content. Latent behind
//!    `SUPERSESSION_RANKING_ENABLED` (defaulted off at the time; flipped to
//!    default ON by §3.9), which is exactly why it was worth closing before
//!    the flip rather than after: that flag existed to be flipped. Fixed by
//!    `multi_graph_retrieve`'s superseder scope guard; probed by
//!    `supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean`.
//!    **The paired WRITE-side gap is now fixed too** (the follow-up PR that
//!    added `supersede_refuses_to_create_a_cross_ocean_edge`). `POST /v1/nodes/
//!    {id}/supersede` and its `DELETE` sibling authorize both endpoints through
//!    `ReachabilityResolver` instead of `node_owned_by_account`, and `POST`
//!    additionally refuses two endpoints in DIFFERENT oceans — reachability
//!    alone does not cover that case, because a caller who legitimately reaches
//!    both the org commons and her own member ocean could otherwise still write
//!    the row every other member reads back. `supersession_edge` also gained the
//!    `graph_id` column it was the only edge table to lack (migrations 0094/
//!    0008), which is what made the same-ocean rule expressible as an invariant
//!    of the row rather than a check some caller might forget; the same
//!    migration deletes every pre-existing cross-ocean edge after copying it
//!    into `supersession_edge_cross_ocean_deletions`.
//!
//!    The read-side guard #348 added is deliberately KEPT and still probed. It
//!    is now defense in depth rather than the only barrier, and
//!    `supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean`
//!    manufactures its forbidden row in raw SQL precisely because the API can
//!    no longer produce one — a last line of defense is only worth having if it
//!    is still tested with the layers in front of it assumed to have failed.
//!
//!    **The read sibling is now fixed too** (the follow-up PR that added
//!    `supersessions_listing_denies_another_members_reef`). `GET /v1/nodes/{id}/
//!    supersessions` was account-scoped, so any member holding another member's
//!    reef id could enumerate that reef's edges — and this surface returns
//!    `superseder_title`/`superseded_title`, i.e. CONTENT, which makes it a
//!    strictly worse disclosure than the identifier leak above. It also needed
//!    no cross-ocean edge: an ordinary valid edge inside A's own ocean sufficed.
//!    It now uses the same `authorize_reachable_node!` the writes do, and
//!    `Store::list_supersession_edges` additionally restricts to the authorized
//!    ocean so the OTHER endpoint's title cannot cross the boundary either.
//!
//! Also recorded (not a live security bypass, but a residual gap the plan's
//! own §2.3 flagged and left unresolved): nothing at the DATABASE level stops
//! a `kind='member'` row from being inserted on the consumer/control plane —
//! `graphs_member_requires_owner` only requires a non-null `owner_user_id`, it
//! does not know which plane it's running on. The public API refuses it
//! (`POST /v1/graphs` only accepts `kind IN ('personal','org')`), and the
//! consumer arm's `SECURITY DEFINER` functions defensively re-apply the
//! owner-only predicate even to such a row — so no read path is actually
//! bypassable — but neither of §2.3's two proposed hard backstops (a
//! parity-script-allowlisted consumer-only CHECK, or an explicit reject in
//! `create_graph`/`create_graph_scoped`) was implemented. Likewise
//! `teams.graph_id` has no CHECK tying it to `graphs.kind = 'team'` — only
//! `create_team`'s own code (which never accepts a caller-supplied graph id)
//! keeps this from happening in practice. See
//! `member_kind_row_creation_blocked_on_consumer_plane` and
//! `teams_graph_id_pointing_at_member_kind_still_denied` for the
//! defense-in-depth proof that both gaps are inert today.
//!
//! # Non-vacuity discipline (why every denial here has a twin)
//!
//! The failure mode this file is most exposed to is not a missing assertion —
//! it is an assertion that PASSES FOR THE WRONG REASON. A security probe that
//! reads as proof while asserting nothing is worse than no probe, because it
//! retires the reviewer's suspicion. Three rules, all of them earned by a real
//! instance found in review on this very file:
//!
//! 1. **Never let an empty collection stand in for a denial.** Every "X must
//!    not appear in the list" assertion is preceded by a POSITIVE CONTROL
//!    proving the same list is non-empty for the same caller (the org commons
//!    ocean in `/v1/graphs` and `list_oceans`, B's own op in `sync/pull`, B's
//!    own member + team oceans in the rogue-`teams`-row probe, A's own row in
//!    `/v1/activity` and `/v1/queries`, A's own reachable ocean before it is
//!    archived, and — on the consumer plane — both the unrelated account's own
//!    ocean and A's own Primary ocean).
//!    Found the hard way: the unrelated-account arm of
//!    `member_kind_row_creation_blocked_on_consumer_plane` seeded a `users`
//!    and an `accounts` row but no `account_members` row, so that user
//!    resolved ZERO reachable graphs and the assertion passed for "sees
//!    nothing at all" — it would have stayed green with the plane guard
//!    deleted outright.
//! 2. **Never let an error stand in for an OWNERSHIP denial.** Pin the reason,
//!    not the shape — `assert_denied_not_found` requires the tool's
//!    `not_found` code, never a bare `tool_is_error`. Found the hard way: the
//!    `add_context` probes were passing on `insufficient_scope` (the fixture
//!    had no MCP write grant), so they never reached the ocean-ownership check
//!    they claimed to prove; and `ask_ocean`'s "zero citations" assertion was
//!    reading `citations` off an `{"error": ...}` envelope, where
//!    `.as_array()` is `None` and the `unwrap_or(0)` supplied the very zero
//!    being asserted.
//! 3. **Never let a parse failure become a passing leak check.** `mcp_call`
//!    and `post_mcp_rpc` parse STRICTLY (panic, never `unwrap_or(Value::Null)`
//!    — a `Null` stringifies to "null", which contains no marker and passes
//!    every `!contains(A_SECRET)` grep no matter what the raw bytes carried),
//!    and `post_mcp_rpc` additionally greps the RAW response bytes for
//!    [`A_SECRET`] before parsing, so no shape of malformed payload can route
//!    around the leak check. Demonstrated, not assumed: with a tool patched to
//!    answer an unreachable ocean in non-JSON text echoing the caller's
//!    question, the pre-fix suite passed GREEN while A's marker travelled
//!    verbatim in the `/mcp` payload; the current suite fails on the raw grep.
//!
//! # Regression-proofing (what it does and does not catch)
//!
//! §6 asks for the matrix to be derived from a checked-in route list, "in the
//! spirit of `check_rls_coverage.sh`", so a new ocean-resolving route can't
//! quietly ship without a probe. Implemented as **two separate mechanisms**,
//! not one, because the HTTP route surface and the MCP tool surface are
//! registered completely differently in this codebase (`axum::Router::route`
//! calls vs. an `rmcp` `#[tool_router]` macro over annotated methods) and no
//! single grep pattern covers both honestly:
//!
//! 1. **`scripts/check_member_ocean_probe_coverage.sh`** (new script, added by
//!    this PR) — mechanically parses every `.route("...", ...)` call inside
//!    `backend/src/lib.rs`'s `v1_routes()` (the ONE `/v1` route table) and
//!    cross-checks it against a checked-in
//!    manifest, `backend/tests/member_ocean_probe_routes.manifest`, exactly the
//!    way `check_rls_coverage.sh` cross-checks its table roster against its
//!    manifest: every registered route must appear with a `probed`, `pending`,
//!    or `exempt` status (an `exempt` entry must carry a non-blank
//!    `# exempt: <why>` rationale comment immediately above it), and the
//!    script fails if a NEW route is added to `v1_routes()` without a
//!    corresponding manifest line. **What this catches:** any future route
//!    added to the shared route table with no manifest entry at all — the
//!    exact "quietly ships, nobody notices" failure mode `reachability_pg.rs`
//!    suffered (unwired from CI for months). **What it does NOT catch:** it
//!    cannot tell whether a route that IS listed `probed` is actually
//!    reachability-gated (that would require a function-body analysis this
//!    script deliberately does not attempt, for the same false-confidence
//!    reason `check_rls_coverage.sh`'s own header warns about — "a grep, not a
//!    data-flow proof"); it also cannot verify a `probed` entry's claimed test
//!    function actually exists or actually exercises that exact path — that
//!    link is reviewer-owned, exactly like `check_rls_coverage.sh`'s
//!    `# converted:`/`# exempt:` comments are.
//! 2. **The MCP tool roster is NOT covered by the same script** — `rmcp`
//!    registers tools via macro-annotated methods, not a textual `.route(...)`
//!    table, so there is no equivalently cheap mechanical parse. Instead,
//!    `assert_mcp_deny_matrix` hardcodes the full 12-tool read roster
//!    (`READ_TOOL_NAMES`) AND the total read+write count
//!    (`EXPECTED_TOTAL_TOOL_COUNT`, 16), and asserts BOTH against a real
//!    `tools/list` call driven over `/mcp`: every name in `READ_TOOL_NAMES`
//!    must be present (catches a rename/removal), AND the advertised list's
//!    TOTAL length must equal `EXPECTED_TOTAL_TOOL_COUNT` exactly (catches an
//!    ADDED tool — an earlier version of this file only asserted presence of
//!    the 12 known names, so a 13th tool would have made `advertised` grow
//!    while every assertion still passed, shipping with zero isolation probe;
//!    flagged in review, see the PR's CodeRabbit-response notes). **Honesty
//!    note:** this is still the weaker of the two mechanisms — it catches "the
//!    tool roster's size or named members changed", which forces a human to
//!    look at this file, but (unlike mechanism 1) it does not independently
//!    derive "which tools are ocean-resolving" from the source; that
//!    classification is still a human judgment recorded in the table, and
//!    `EXPECTED_TOTAL_TOOL_COUNT` is a hand-maintained constant (not derived
//!    from `mcp::tools::WRITE_TOOLS`, a `pub(crate)` item this external test
//!    crate cannot see).
//!
//! **The approximation's overall honest limit:** neither mechanism proves a
//! *listed* route/tool is correctly gated — only that every route/tool that
//! exists is *accounted for* in a reviewed, checked-in list, so a new one
//! cannot silently skip review. Whether the gating itself is correct is what
//! the rest of this file's assertions are for.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights), same scratch-DB pattern as every other `*_pg.rs` file.
//! **This suite is NOT YET wired into CI** — see the PR body; a concurrent PR
//! owns `.github/workflows/ci.yml`.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_probe_pg -- --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use coralswarm_core::r2::R2Client;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const TEST_MASTER_KEK_HEX: &str =
    "3333333333333333333333333333333333333333333333333333333333333333";

/// Embedded in every string unique to member A's private content (reef title,
/// coral text, transcript, conversation title). Every "must not leak"
/// assertion in this file greps the raw response body text for this marker —
/// a status-code-only assertion cannot tell a 200-with-empty-list apart from
/// a 200-that-echoes-the-name, and both would pass a naive check.
const A_SECRET: &str = "A_PRIVATE_MARKER_9f3d2c7a";
const B_MARK: &str = "B_OWN_MARKER_1a2b3c4d";
/// Marker unique to the ORG COMMONS ocean's seeded coral. Every org member —
/// plain member B and org admin C alike — can reach that ocean, so this is the
/// one piece of content that can serve as a POSITIVE CONTROL for BOTH callers
/// in the mixed-scope citation probes below (B's own member ocean is reachable
/// by B but NOT by C, so it cannot play that role for the shared helper).
const ORG_MARK: &str = "ORG_COMMONS_MARKER_7e1f0b55";

/// The phrase every seeded coral in this fixture embeds, and the question the
/// mixed-scope citation probes ask.
///
/// The suite's other MCP probes drive `ask_ocean`/`search_atoms` with a
/// question built from [`A_SECRET`], which by construction matches NOTHING the
/// caller can legitimately reach — fine for a pure denial assertion, useless
/// for a probe that must prove *some* citations come back. Retrieval here is
/// keyword-only (the fixture seeds `atoms.content_tsv` but no `embedding`
/// vectors, and `StubEmbedder` produces no real ones), so the question has to
/// be a phrase that genuinely full-text-matches the seeded corals:
/// `websearch_to_tsquery('simple', ...)` ANDs its terms, and each of A's, B's
/// and the org commons' coral bodies contains all three of these.
const SHARED_RETRIEVAL_PHRASE: &str = "private coral content";

/// Serializes this file's process-global env mutations against each other —
/// same pattern as `cross_tenant_probe_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

// ── Scratch-DB plumbing (duplicated per this directory's documented
//    "no shared test-support module across files" convention) ───────────────

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
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}
fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

async fn body_bytes(resp: axum::response::Response) -> Vec<u8> {
    to_bytes(resp.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap()
        .to_vec()
}
async fn body_text(resp: axum::response::Response) -> String {
    String::from_utf8_lossy(&body_bytes(resp).await).into_owned()
}
fn org_token(sub: &str, org_id: &str, role: &str) -> String {
    // Real Clerk session JWTs carry `sid`; without it the request is a
    // non-interactive credential and an admin's moderation authority is
    // withheld (settings 2.8, `middleware::auth::authority_allowed`).
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": org_id, "org_role": role, "sid": format!("sess_{sub}"),
    }))
}
fn personal_token(sub: &str) -> String {
    mint(json!({ "iss": ISSUER, "sub": sub, "exp": future_exp() }))
}
/// Resource-bound OAuth access token for MCP; REST session fixtures stay separate.
fn mcp_org_token(sub: &str, org_id: &str, role: &str) -> String {
    mint_oauth(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "client_id": "member_ocean_probe_client", "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": role,
    }))
}

fn req_get(token: &str, host: &str, uri: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("GET")
        .uri(uri)
        .header("host", host)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
fn req_json(method: &str, token: &str, host: &str, uri: &str, body: Value) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method(method)
        .uri(uri)
        .header("host", host)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

// ── Fixture ───────────────────────────────────────────────────────────────────

/// Not every field is read by every test (each exercises a different slice) —
/// `#[allow(dead_code)]` rather than pruning fields that document the fixture's
/// full shape, matching `member_ocean_promotion_pg.rs`'s own convention.
#[allow(dead_code)]
struct Fx {
    control: PgPool,
    tenant: PgPool,
    org_id: String,
    org_account: Uuid,
    org_graph: Uuid,
    /// Reef + coral seeded in the org commons ocean, so a mixed-scope
    /// `ask_ocean`/`search_atoms` call has something EVERY org member (B and
    /// admin C both) can legitimately be cited — the positive control the
    /// citation-identifier probes need.
    org_reef: Uuid,
    org_atom: Uuid,
    team_graph: Uuid,
    team_id: Uuid,
    member_a: Uuid,
    member_a_sub: String,
    member_b: Uuid,
    member_b_sub: String,
    member_c: Uuid,
    member_c_sub: String,
    a_graph: Uuid,
    a_reef: Uuid,
    a_atom: Uuid,
    a_meeting_source_id: String,
    a_conversation: Uuid,
    a_share_token: String,
    b_graph: Uuid,
    b_reef: Uuid,
    b_atom: Uuid,
    personal_account: Uuid,
    personal_graph: Uuid,
}

/// Seed one reef + one coral (linked via `node_atoms`) into `graph`, returning
/// `(reef_id, coral_id)`.
///
/// `content_tsv` is populated here on purpose. Production stamps it during
/// ingest; nothing in this test fixture goes through ingest, so without this
/// every `keyword_search` in the tree returns ZERO rows for this fixture
/// (`WHERE content_tsv IS NOT NULL AND content_tsv @@ ...`) and any probe that
/// needs real retrieval output — the mixed-scope citation probes below — would
/// be asserting over a permanently empty candidate pool. The `'simple'`
/// configuration matches `PostgresStore::keyword_search[_scoped]`'s own
/// `QUERY_LANGUAGE` constant exactly; using anything else here would silently
/// stop matching.
async fn seed_reef_and_coral(
    tenant: &PgPool,
    graph: Uuid,
    reef_title: &str,
    coral_content: &str,
    source_id: &str,
) -> (Uuid, Uuid) {
    let reef: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(graph)
    .bind(reef_title)
    .fetch_one(tenant)
    .await
    .unwrap();
    let atom: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             \"timestamp\", content_inline, content_tsv) \
         VALUES (gen_random_uuid(), $1, 'note', $2, 'unused', now(), $3, \
                 to_tsvector('simple', $3)) RETURNING atom_id",
    )
    .bind(graph)
    .bind(source_id)
    .bind(coral_content)
    .fetch_one(tenant)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(reef)
        .bind(atom)
        .execute(tenant)
        .await
        .unwrap();
    (reef, atom)
}

async fn setup(base: &str, ctl: &str, ten: &str, org_id: &str) -> Fx {
    let mut conn = admin(base).await;
    for db in [ctl, ten] {
        recreate(&mut conn, db).await;
    }

    let control: PgPool = PgPoolOptions::new().connect(&with_db(base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let org_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("owner-{}@test.local", Uuid::new_v4()))
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(org_owner)
    .bind(org_id)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, 'moprobe', 'SECRET_MOPROBE', 'active')",
        )
        .bind(org_id)
        .bind(org_account),
    )
    .await
    .unwrap();

    async fn seed_user(control: &PgPool, label: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let sub = format!("member_ocean_probe_{label}_{id}");
        sqlx::query("INSERT INTO users(id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(format!("{label}-{id}@test.local"))
            .bind(&sub)
            .execute(control)
            .await
            .unwrap();
        (id, sub)
    }
    let (member_a, member_a_sub) = seed_user(&control, "a").await;
    let (member_b, member_b_sub) = seed_user(&control, "b").await;
    let (member_c, member_c_sub) = seed_user(&control, "c").await;

    // A and B are ordinary members; C is org:admin (claim-only — the
    // reachability predicate consults NO role anywhere, which is exactly
    // P0 #2's assertion).
    for (uid, role) in [(member_a, "member"), (member_b, "member"), (member_c, "admin")] {
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
            .bind(org_account)
            .bind(uid)
            .bind(role)
            .execute(&control)
            .await
            .unwrap();
    }

    // ── A's CONSUMER-plane personal account + Primary ocean (plane-split
    //    fixture) ───────────────────────────────────────────────────────────
    let personal_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(member_a)
            .fetch_one(&control)
            .await
            .unwrap();
    let personal_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) \
         VALUES ($1, 'A Consumer Primary Ocean', 'personal', true) RETURNING id",
    )
    .bind(personal_account)
    .fetch_one(&control)
    .await
    .unwrap();
    // `resolve_reachable_graphs`'s consumer arm is `account_id IN (SELECT
    // account_id FROM account_members WHERE user_id = $1)` — this fixture
    // seeds the personal account/ocean directly (bypassing
    // `ensure_personal_account`, which would normally insert this row on
    // first sight), so it must be seeded explicitly here too, or A's own
    // personal-plane request would see zero reachable graphs.
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(personal_account)
        .bind(member_a)
        .execute(&control)
        .await
        .unwrap();

    // Seed one query_history + one activity_events row for A directly in the
    // CONTROL... no: both tables are tenant-tree. Seeded below, after the
    // tenant pool exists.

    // ── Tenant DB ──────────────────────────────────────────────────────────
    let tenant: PgPool = PgPoolOptions::new().connect(&with_db(base, ten)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&tenant)
        .await
        .unwrap();

    // Org DEK, so R2/DEK-adjacent code paths (get_atom) resolve fully rather
    // than short-circuiting on "account has no DEK".
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");
    let org_dek = coralswarm_core::kek::generate_dek();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account)
        .bind(coralswarm_core::kek::wrap_dek(&kek, &org_dek).unwrap())
        .execute(&tenant)
        .await
        .unwrap();

    let org_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind) \
         VALUES (gen_random_uuid(), $1, 'Org Commons Ocean', 'org') RETURNING id",
    )
    .bind(org_account)
    .fetch_one(&tenant)
    .await
    .unwrap();

    // Reef + coral in the org commons ocean. Its body deliberately embeds
    // [`SHARED_RETRIEVAL_PHRASE`] (exactly like A's and B's private corals) so
    // ONE keyword question matches all three — which is what makes the
    // mixed-scope probes' positive control ("the reachable ocean's coral IS
    // cited") and their exclusion ("A's never is") two halves of the SAME
    // retrieval, rather than two unrelated calls.
    let (org_reef, org_atom) = seed_reef_and_coral(
        &tenant,
        org_graph,
        &format!("Org commons reef {ORG_MARK}"),
        &format!("Org commons private coral content {ORG_MARK}"),
        "org-commons-coral-source",
    )
    .await;

    let team_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (id, account_id, name, kind) \
         VALUES (gen_random_uuid(), $1, 'Team Ocean', 'team') RETURNING id",
    )
    .bind(org_account)
    .fetch_one(&tenant)
    .await
    .unwrap();
    let team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'Team', $3) RETURNING id",
    )
    .bind(org_account)
    .bind(team_graph)
    .bind(member_a) // creator identity is irrelevant to this fixture
    .fetch_one(&tenant)
    .await
    .unwrap();
    // B is in the team; A deliberately is NOT.
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) \
         VALUES ($1, $2, 'member', NULL)",
    )
    .bind(team_id)
    .bind(member_b)
    .execute(&tenant)
    .await
    .unwrap();

    // ── A's owner-scoped kind='member' ocean: reef + coral + transcript +
    //    conversation, every string embedding A_SECRET ─────────────────────
    async fn member_ocean(
        tenant: &PgPool,
        org_account: Uuid,
        owner: Uuid,
        label: &str,
        secret: &str,
    ) -> (Uuid, Uuid, Uuid, String, Uuid) {
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (id, account_id, name, kind, owner_user_id, is_primary) \
             VALUES (gen_random_uuid(), $1, $2, 'member', $3, true) RETURNING id",
        )
        .bind(org_account)
        .bind(format!("{label}'s private ocean {secret}"))
        .bind(owner)
        .fetch_one(tenant)
        .await
        .unwrap();
        // Body wording is load-bearing: `{label}'s private coral content ...`
        // embeds [`SHARED_RETRIEVAL_PHRASE`], so A's, B's and the org commons'
        // corals are all matched by ONE keyword question.
        let (reef, atom) = seed_reef_and_coral(
            tenant,
            graph,
            &format!("{label}'s private reef {secret}"),
            &format!("{label}'s private coral content {secret}"),
            &format!("{label}-coral-source"),
        )
        .await;

        let meeting_source_id = format!("{label}-meeting-{secret}");
        sqlx::query(
            "INSERT INTO meeting_transcripts (graph_id, source_id, raw_inline, duration_ms) \
             VALUES ($1, $2, $3, 60000)",
        )
        .bind(graph)
        .bind(&meeting_source_id)
        .bind(format!("{label}'s raw transcript {secret}"))
        .execute(tenant)
        .await
        .unwrap();

        let conversation: Uuid = sqlx::query_scalar(
            "INSERT INTO conversations (account_id, graph_id, user_id, title) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(org_account)
        .bind(graph)
        .bind(owner)
        .bind(format!("{label}'s private conversation {secret}"))
        .fetch_one(tenant)
        .await
        .unwrap();

        (graph, reef, atom, meeting_source_id, conversation)
    }

    let (a_graph, a_reef, a_atom, a_meeting_source_id, a_conversation) =
        member_ocean(&tenant, org_account, member_a, "A", A_SECRET).await;
    let (b_graph, b_reef, b_atom, _b_meeting, _b_conv) =
        member_ocean(&tenant, org_account, member_b, "B", B_MARK).await;

    // A pre-existing "link"-visibility share for A's meeting (used by the
    // archived-share known-defect test; a live share is a deliberate,
    // A-initiated disclosure act, so its mere existence is not itself a leak
    // — see that test's own doc comment).
    let a_share_token = format!("moprobe-share-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO meeting_shares (graph_id, source_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'link', $4)",
    )
    .bind(a_graph)
    .bind(&a_meeting_source_id)
    .bind(&a_share_token)
    .bind(member_a)
    .execute(&tenant)
    .await
    .unwrap();

    // One query_history + one activity_events row for A, so
    // `query_history_and_activity_are_scoped_per_user` has something real to
    // prove is invisible to B/C.
    sqlx::query(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text, answer_summary) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(org_account)
    .bind(a_graph)
    .bind(member_a)
    .bind(format!("what is {A_SECRET}?"))
    .bind(format!("the answer involves {A_SECRET}"))
    .execute(&tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO activity_events (account_id, user_id, graph_id, event_type, payload) \
         VALUES ($1, $2, $3, 'atom_created', $4)",
    )
    .bind(org_account)
    .bind(member_a)
    .bind(a_graph)
    .bind(json!({ "marker": A_SECRET }))
    .execute(&tenant)
    .await
    .unwrap();

    Fx {
        control,
        tenant,
        org_id: org_id.to_string(),
        org_account,
        org_graph,
        org_reef,
        org_atom,
        team_graph,
        team_id,
        member_a,
        member_a_sub,
        member_b,
        member_b_sub,
        member_c,
        member_c_sub,
        a_graph,
        a_reef,
        a_atom,
        a_meeting_source_id,
        a_conversation,
        a_share_token,
        b_graph,
        b_reef,
        b_atom,
        personal_account,
        personal_graph,
    }
}

async fn teardown(base: &str, fx: Fx, dbs: [&str; 2]) {
    fx.control.close().await;
    fx.tenant.close().await;
    let mut conn = admin(base).await;
    for db in dbs {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// The org API address is RETIRED for REST (claim-based tenancy Phase 4
/// cutover step 3): every REST request in this file now knocks on `APEX` and
/// is routed by its token's `org_id` claim.
///
/// **`/mcp` now uses `APEX` too — no exception left.** Through claim-based-
/// tenancy §Phase 3 PR 3.2, `/mcp` still had its OWN host classification
/// (`mcp::McpConfig::resolve_resource`), and a former `MCP_ORG_HOST` constant
/// here (`moprobe.coralswarm.example`) carried every MCP call in this file so the
/// org claim would still bind a tenant. PR 3.3 retired that classification
/// (`resolve_resource`'s `Subdomain` arm now collapses to `personal_identity`
/// like the apex), and the SAME PR deleted `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`
/// in `lib.rs` — the constant that, until it was deleted, was the only thing
/// keeping a subdomain host from being 410'd by `retired_org_host_guard`
/// before `/mcp` ever ran at all. A single-label host under `APEX` is now
/// UNREACHABLE for `/mcp` the same way it already was for REST: 410, before
/// routing. Every MCP call in this file now goes to `APEX` directly and
/// relies on `select_tenant` (see `select_org` below) instead of a claim+Host
/// combination to reach org content — the replacement door PR 3.2 built for
/// exactly this reason.

/// Build the real HTTP app: control pool as the consumer/ambient plane, a real
/// `TenantRuntime` resolving this fixture's `db_secret_ref`, MASTER_KEK_HEX
/// already set by the caller (env, so it must be set before this runs), and a
/// dummy-but-real `R2Client` — dummy credentials never actually dial the
/// network unless a code path incorrectly reaches the blob-fetch branch (see
/// `cross_tenant_probe_pg.rs`'s identical trick), so "the response comes back
/// fast with `content: null`/404" is itself part of the "R2 never fetched"
/// proof for every denial in this file.
async fn build_app(fx: &Fx, base: &str, ten: &str) -> axum::Router {
    build_app_inner(fx, base, ten, true).await
}

/// Permitted inline-evidence controls do not configure R2; the typed cloud
/// selector reads genuinely unmapped fixture chunks without a provider call.
async fn build_app_inline_content(fx: &Fx, base: &str, ten: &str) -> axum::Router {
    build_app_inner(fx, base, ten, false).await
}

async fn build_app_inner(fx: &Fx, base: &str, ten: &str, with_r2: bool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(fx.control.clone());
    state.store = Some(Arc::new(PostgresStore::new(fx.control.clone())));
    state.r2 = if with_r2 {
        Some(Arc::new(
            R2Client::new("dummy-account", "dummy-key", "dummy-secret", "dummy-bucket").await,
        ))
    } else {
        None
    };
    let resolver = StaticMapResolver::from_pairs([("SECRET_MOPROBE", with_db(base, ten))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

/// Set every env var this file's MCP-driving tests need, guarded by
/// `ENV_LOCK` (held by the caller for the whole test body — same convention
/// as `cross_tenant_probe_pg.rs`).
///
/// # Safety
/// Every var here is mutated only while the caller holds `_env_guard` for its
/// whole test body, matching this file's sibling `*_pg.rs` tests.
unsafe fn set_shared_env(jwks_url: &str) {
    std::env::set_var("CLERK_ISSUER", ISSUER);
    std::env::set_var("CLERK_JWKS_URL", jwks_url);
    std::env::set_var("MCP_ENABLED", "1");
    std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
    std::env::set_var("RATE_LIMIT_BACKEND", "pg");
    std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
    std::env::remove_var("MCP_DEFAULT_SCOPES");
    // Bound the initial wait; permitted calls then use durable task polling.
    // Scope denial is immediate and is asserted without task creation.
    std::env::set_var("MCP_ASK_SYNC_WAIT_MS", "0");
}
/// # Safety
/// Same contract as [`set_shared_env`]: mutates process-global env, so
/// callers must hold `ENV_LOCK` for the whole test body.
unsafe fn clear_shared_env() {
    std::env::remove_var("CLERK_ISSUER");
    std::env::remove_var("CLERK_JWKS_URL");
    std::env::remove_var("MCP_ENABLED");
    std::env::remove_var("MCP_RESOURCE_URL");
    std::env::remove_var("RATE_LIMIT_BACKEND");
    std::env::remove_var("MASTER_KEK_HEX");
    std::env::remove_var("MCP_ASK_SYNC_WAIT_MS");
}

// ── MCP call helper (JSON-RPC over the real `/mcp` router) ───────────────────

/// Shared transport: POST a JSON-RPC body to the REAL `/mcp` router (never
/// the in-crate dispatcher — this is an external test crate), asserting the
/// transport itself succeeds, and return the parsed JSON-RPC envelope
/// (`{"result": ..., ...}`), handling both the plain-JSON and SSE
/// (`text/event-stream`) response shapes `/mcp` may return. `mcp_call` and
/// `list_tools_names` used to each duplicate this whole block (CodeRabbit
/// #343) — extracted so a transport change only needs one edit.
async fn post_mcp_rpc(app: &axum::Router, token: &str, body: Value) -> Value {
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/mcp")
        // `APEX`, not an org subdomain: claim-based-tenancy §Phase 3 PR 3.3
        // retired `/mcp`'s own host classification, and an org-subdomain Host
        // is now 410'd outright by `retired_org_host_guard` before `/mcp`
        // ever runs (the exemption that used to let it through,
        // `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`, is deleted). Every org-claim
        // token in this file (`mcp_org_token`) gets its claim stripped here
        // exactly like a personal token would; reaching org content now
        // requires `select_tenant` first (`select_org`, below) — never a
        // Host.
        .header("host", APEX)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "/mcp transport must succeed");
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = body_bytes(resp).await;
    // BELT (CodeRabbit #343): grep the RAW transport bytes for [`A_SECRET`]
    // BEFORE any parsing. Every `/mcp` call in this file is adversarial — B or
    // admin C reaching for A's content, or any caller reaching into an
    // ARCHIVED ocean — so A's marker must never appear anywhere in the
    // envelope, including in an `error.message` or a field no per-tool
    // assertion below happens to read. Doing it here, on the bytes, means a
    // payload that fails to parse (or parses into an unexpected shape) can
    // never route around the check.
    let raw = String::from_utf8_lossy(&bytes);
    assert!(
        !raw.contains(A_SECRET),
        "/mcp raw response leaked A's content; request was {body}, response: {raw}"
    );
    if content_type.contains("text/event-stream") {
        let joined: String = raw
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|r| r.strip_prefix(' ').unwrap_or(r))
            .collect::<Vec<_>>()
            .join("\n");
        serde_json::from_str(&joined)
            .unwrap_or_else(|e| panic!("SSE json-rpc payload is not JSON ({e}): {joined}"))
    } else {
        // BRACES: parse strictly. `unwrap_or(Value::Null)` here used to turn an
        // unparsable body into a `Null` that stringifies to the literal "null"
        // and sails through every downstream `!contains(A_SECRET)` assertion —
        // a probe passing for the wrong reason, which in a security suite is
        // worse than no probe at all (CodeRabbit #343).
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("/mcp response is not JSON ({e}): {raw}"))
    }
}

/// Call an MCP tool through the REAL `/mcp` router and return the tool
/// result's inner JSON body (whatever `tool_success`/`tool_error` put in
/// `content[0].text`).
async fn mcp_call(app: &axum::Router, token: &str, tool: &str, args: Value) -> Value {
    let rpc = post_mcp_rpc(
        app,
        token,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": args},
        }),
    )
    .await;
    let result = &rpc["result"];
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("{tool}: tool result has no text content: {rpc:?}"));
    // BRACES (the belt is `post_mcp_rpc`'s raw-bytes grep): parse STRICTLY.
    // `unwrap_or(Value::Null)` used to degrade an unparsable tool payload into
    // `Null`, whose `.to_string()` is the literal "null" — the leak-only
    // probes (`search_atoms`, `get_meeting`, `get_atom`, `get_transcript`)
    // would then grep "null" for A_SECRET and PASS even if the raw text
    // carried A's transcript verbatim (CodeRabbit #343).
    serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("{tool}: tool result text is not JSON ({e}): {text}"))
}
fn tool_is_error(body: &Value) -> bool {
    body.get("error").is_some()
}

/// Persist a tenant selection for `token`, over `/mcp` — claim-based-tenancy
/// §Phase 3 PR 3.3 retires the org MCP resource (`ORG_HOST` above is no
/// longer a distinct arm of `resolve_mcp_tenancy` at all; every `/mcp` host
/// now resolves personal, and decision 5's catalogue gate refuses any
/// non-tenancy tool for a caller with ≥1 org membership and no active
/// selection), so every `mcp_org_token` caller in this file now has to
/// `select_tenant` BEFORE calling any content tool, where before this PR the
/// org-claim + org-Host combination alone was sufficient. Every call site
/// below inserts exactly this call, right after minting an `mcp_org_token`.
async fn select_org(app: &axum::Router, token: &str, org_id: &str) {
    let body = mcp_call(app, token, "select_tenant", json!({"org_id": org_id})).await;
    assert!(
        !tool_is_error(&body),
        "select_tenant must succeed for a real, live org membership: {body:?}"
    );
}

/// Complete a permitted retained Ask, requiring the durable task envelope rather
/// than interpreting a failed task or missing result as an empty success.
async fn ask_ocean_and_wait(app: &axum::Router, token: &str, args: Value) -> Value {
    let first = mcp_call(app, token, "ask_ocean", args).await;
    assert!(!tool_is_error(&first), "permitted Ask must be admitted: {first}");
    // A fast completion may arrive within the zero wait budget's first poll.
    if first.get("mode").is_some() {
        assert_eq!(first["support_status"], "complete", "{first}");
        return first;
    }
    assert_eq!(first["status"], "working", "Ask must create a working task: {first}");
    let task_id = first["task_id"].as_str().expect("Ask task UUID");
    Uuid::parse_str(task_id).expect("Ask task_id must be a UUID");
    const MAX_ATTEMPTS: u32 = 40;
    for attempt in 0..MAX_ATTEMPTS {
        let r = mcp_call(app, token, "get_task_result", json!({"task_id": task_id})).await;
        assert!(!tool_is_error(&r), "permitted task delivery must succeed: {r}");
        match r["status"].as_str() {
            Some("completed") => {
                assert!(r["result"].is_object(), "completed task must carry its result: {r}");
                assert_eq!(r["result"]["support_status"], "complete", "{r}");
                return r["result"].clone();
            }
            Some("working") => {}
            _ => panic!("permitted task must not fail or return an unknown status: {r}"),
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }
    panic!("get_task_result never reached completion for task {task_id}");
}

/// Assert a tool result is the OWNERSHIP denial, pinning the REASON rather
/// than settling for "some error".
///
/// Every read tool in `mcp::tools::read` answers an unreachable ocean with
/// `not_found` ("ocean/reef/coral/meeting not found or access denied") and
/// reserves `service_unavailable` for "no database configured"/"internal
/// error" — so a bare `tool_is_error` check would stay green if the whole
/// tool started erroring for an unrelated reason (a broken pool, a dropped
/// scope, a panic) and the probe would silently stop testing isolation. That
/// is precisely how the `add_context` probes were once passing on
/// `insufficient_scope` without ever reaching the ownership check (a
/// scope refusal instead of the ownership denial). Pin the code everywhere.
fn assert_denied_not_found(r: &Value, who: &str, what: &str) {
    assert!(tool_is_error(r), "{who}: {what} must error: {r}");
    assert_eq!(
        r["error"]["code"],
        json!("not_found"),
        "{who}: {what} must be denied as `not_found` (the ownership denial); another error \
         class means the probe never reached the reachability check and is passing for the \
         wrong reason: {r}"
    );
}

// ── Identifier-disclosure helpers (mixed-scope citation probes) ──────────────

/// Parse a JSON array of UUID strings STRICTLY: panic on a missing key, a
/// non-array, or an element that isn't a parseable UUID.
///
/// Deliberately NOT `as_array().into_iter().flatten().filter_map(...)`, which
/// silently degrades a missing/renamed/reshaped key into an EMPTY VEC. Every assertion the citation probes make over the parsed
/// list is a "no element may be X" universal, which an empty vec satisfies
/// vacuously — the exact failure mode that made `ask_ocean`'s original
/// zero-citation assertion meaningless (module doc, non-vacuity rule 1). So
/// here the parse itself must be the thing that fails.
fn parse_uuid_array(v: &Value, what: &str) -> Vec<Uuid> {
    let arr = v
        .as_array()
        .unwrap_or_else(|| panic!("{what}: expected a JSON array, got {v}"));
    arr.iter()
        .map(|e| {
            let s = e
                .as_str()
                .unwrap_or_else(|| panic!("{what}: element is not a string: {e} (in {v})"));
            Uuid::parse_str(s)
                .unwrap_or_else(|err| panic!("{what}: element {s:?} is not a UUID ({err})"))
        })
        .collect()
}

/// The caller's ACTUAL reachable ocean set, derived from the real
/// `GET /v1/graphs` for that same caller rather than hardcoded per test.
///
/// Deriving it (instead of writing `[org_graph, b_graph]` by hand) is what
/// makes the citation assertions below hold for BOTH callers from one shared
/// helper — B reaches her own member ocean, admin C does not — and makes them
/// catch a citation into ANY ocean the caller can't reach, not just A's.
async fn reachable_graph_ids(app: &axum::Router, token: &str, who: &str) -> Vec<Uuid> {
    let resp = app.clone().oneshot(req_get(token, APEX, "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{who}: list_graphs must 200");
    let text = body_text(resp).await;
    let v: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("{who}: /v1/graphs is not JSON ({e}): {text}"));
    let arr = v
        .as_array()
        .unwrap_or_else(|| panic!("{who}: /v1/graphs is not a JSON array: {text}"));
    arr.iter()
        .map(|g| {
            let s = g["id"]
                .as_str()
                .unwrap_or_else(|| panic!("{who}: /v1/graphs entry has no string id: {g}"));
            Uuid::parse_str(s).unwrap_or_else(|e| panic!("{who}: bad graph id {s:?} ({e})"))
        })
        .collect()
}

/// Every identifier that BELONGS TO member A's private ocean, read back out of
/// the tenant database rather than off the `Fx` struct.
///
/// Reading them from the DB matters: `Fx` names only the handful of A's rows
/// the fixture happens to bind to fields, so a future fixture that seeds one
/// more reef/coral into A's ocean would silently drop out of the sweep. This
/// enumerates whatever is actually there.
async fn a_ocean_identifiers(fx: &Fx) -> Vec<(&'static str, Uuid)> {
    let mut ids: Vec<(&'static str, Uuid)> = vec![("A's ocean", fx.a_graph)];
    for (label, sql) in [
        ("A's reef", "SELECT node_id FROM nodes WHERE graph_id = $1"),
        ("A's coral", "SELECT atom_id FROM atoms WHERE graph_id = $1"),
        (
            "A's conversation",
            "SELECT id FROM conversations WHERE graph_id = $1",
        ),
    ] {
        let rows: Vec<Uuid> = sqlx::query_scalar(sql)
            .bind(fx.a_graph)
            .fetch_all(&fx.tenant)
            .await
            .unwrap();
        ids.extend(rows.into_iter().map(|id| (label, id)));
    }
    // Sanity: the sweep must have something to sweep for. A silently-empty
    // identifier list would make `assert_no_a_ocean_identifiers` a no-op.
    assert!(
        ids.len() >= 4,
        "fixture regression: expected A's ocean to carry at least an ocean + reef + coral + \
         conversation id to sweep for, found {ids:?}"
    );
    ids
}

/// Grep a response payload for EVERY identifier belonging to A's private ocean.
///
/// This is the identifier-space twin of this file's `A_SECRET` content grep,
/// and exists because those two catch different leaks: a response can name A's
/// coral id while quoting none of A's text (see
/// `ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id`'s doc
/// comment for the concrete shape), and the content grep is blind to that.
///
/// Scan the complete payload: retained scope denials never echo private UUIDs.
fn assert_no_a_ocean_identifiers(payload: &str, ids: &[(&str, Uuid)], who: &str, what: &str) {
    for (label, id) in ids {
        assert!(
            !payload.contains(&id.to_string()),
            "{who}: {what} disclosed an identifier from member A's unreachable private ocean \
             ({label} {id}) — no content of A's need appear for this to be a real disclosure: \
             {payload}"
        );
    }
}

/// A scope denial is immediate, with no durable task or private identifier echo.
fn assert_scope_denied(r: &Value, ids: &[(&str, Uuid)], who: &str, what: &str) {
    assert_denied_not_found(r, who, what);
    assert!(r.get("task_id").is_none(), "denied scope must not create a task: {r}");
    assert_no_a_ocean_identifiers(&r.to_string(), ids, who, what);
}

/// These fixtures contain genuinely unmapped inline chunks, not invented
/// retained source/passages. Independently resolve every emitted atom's graph.
async fn assert_permitted_legacy_evidence(
    fx: &Fx, r: &Value, scope: &[Uuid], required: &[Uuid], who: &str, what: &str,
) {
    assert!(!tool_is_error(r), "{who}: {what} must succeed: {r}");
    assert_eq!(r["mode"], "retrieval_only", "{who}: {what}: {r}");
    assert_eq!(r["support_status"], "complete", "{who}: {what}: {r}");
    assert_eq!(r["answer"], Value::Null, "sources-only must not invent an answer: {r}");
    assert_eq!(r["citations"], json!([]), "legacy IDs are not retained citations: {r}");
    assert_eq!(r["retained_citations"], json!([]), "unmapped fixture must not invent source passages: {r}");
    assert_eq!(r["outcomes"], json!([]), "unmapped fixture must not invent semantic outcomes: {r}");
    let mut accepted = parse_uuid_array(&r["library_scope"]["graph_ids"], what);
    let mut expected = scope.to_vec();
    accepted.sort(); expected.sort(); expected.dedup();
    assert_eq!(accepted, expected, "{who}: accepted scope changed: {r}");
    let evidence = r["legacy_evidence"].as_array().expect("typed legacy_evidence array");
    assert!(!evidence.is_empty(), "{who}: {what} must have actual permitted evidence: {r}");
    let mut atoms = Vec::new();
    for entry in evidence {
        assert_eq!(entry["kind"], "legacy_chunk", "{entry}");
        assert_eq!(entry["representation"], "unmapped_legacy_chunk", "{entry}");
        assert!(!entry["content"].as_str().expect("legacy content").is_empty(), "{entry}");
        let atom = Uuid::parse_str(entry["atom_id"].as_str().expect("legacy atom UUID")).unwrap();
        let graph = Uuid::parse_str(entry["graph_id"].as_str().expect("legacy graph UUID")).unwrap();
        let owning: Uuid = sqlx::query_scalar("SELECT graph_id FROM atoms WHERE atom_id=$1")
            .bind(atom).fetch_one(&fx.tenant).await.unwrap();
        assert_eq!(owning, graph, "{who}: {what} mislabeled atom {atom}");
        assert!(scope.contains(&owning), "{who}: {what} emitted unreachable atom {atom}: {r}");
        atoms.push(atom);
    }
    for atom in required {
        assert!(atoms.contains(atom), "{who}: {what} must return permitted control atom {atom}: {r}");
    }
    assert_references_are_reachable(fx, &atoms, scope, who, what).await;
    assert_no_a_ocean_identifiers(&r.to_string(), &a_ocean_identifiers(fx).await, who, what);
}

async fn list_tools_names(app: &axum::Router, token: &str) -> Vec<String> {
    let rpc = post_mcp_rpc(
        app,
        token,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
    )
    .await;
    rpc["result"]["tools"]
        .as_array()
        .map(|a| a.iter().filter_map(|t| t["name"].as_str().map(String::from)).collect())
        .unwrap_or_default()
}

// ═══════════════════════════════════════════════════════════════════════════
// 1/2/8. The HTTP read-deny matrix — every ocean-resolving GET/POST surface
// this plan names, run once each for member B and once for admin C (plan §6
// item 9: "identical denials"). Never status-only: every branch also asserts
// the raw response body never contains A_SECRET.
// ═══════════════════════════════════════════════════════════════════════════

async fn assert_read_deny_matrix(app: &axum::Router, token: &str, who: &str, fx: &Fx) {
    // 1. GET /v1/graphs: B's/C's own + org/team, never A's — and never A's name.
    let resp = app.clone().oneshot(req_get(token, APEX, "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{who}: list_graphs must 200");
    let text = body_text(resp).await;
    // Positive control FIRST: both B and admin C are members of this org
    // account, so the org commons ocean must be in the list. Without this, a
    // `/v1/graphs` that returned `[]` for everyone would satisfy both
    // exclusion assertions below and the probe would silently stop testing
    // narrowing at all.
    assert!(
        text.contains(&fx.org_graph.to_string()),
        "{who}: control — list_graphs must contain the org commons ocean, proving the \
         exclusions below test scoping rather than an empty list: {text}"
    );
    assert!(!text.contains(&fx.a_graph.to_string()), "{who}: graphs list must not contain A's graph id: {text}");
    assert!(!text.contains(A_SECRET), "{who}: graphs list must not leak A's ocean name: {text}");

    // 2. graphs/{A}, /atoms, /sources, /stats, /atlas, /node-atoms.
    //
    // `/node-atoms` is W3's BULK reef↔coral edge contract
    // (docs/plans/request-reliability-and-scale.md §6.3) and is the highest-
    // stakes entry in this list. It exists to replace one authorization check
    // per reef with ONE check for the whole ocean — so where the old per-reef
    // route would leak a single reef on a wrong check, this one would leak
    // A's entire edge graph. Both of its projections are probed: the edge
    // page, and `?facets=true`, which is a different query shape on the same
    // route and would otherwise be an unprobed path to the same tables.
    for suffix in [
        "",
        "/atoms",
        "/sources",
        "/stats",
        "/atlas",
        "/node-atoms",
        "/node-atoms?facets=true",
    ] {
        let uri = format!("/v1/graphs/{}{}", fx.a_graph, suffix);
        let resp = app.clone().oneshot(req_get(token, APEX, &uri)).await.unwrap();
        assert!(
            resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::NOT_FOUND,
            "{who}: GET {uri} must deny, got {}",
            resp.status()
        );
        let text = body_text(resp).await;
        assert!(!text.contains(A_SECRET), "{who}: {uri} leaked A's content: {text}");
    }

    // 2. /v1/nodes?graph_id={A}
    let resp = app
        .clone()
        .oneshot(req_get(token, APEX, &format!("/v1/nodes?graph_id={}", fx.a_graph)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{who}: list_nodes(A) must 403");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: list_nodes(A) leaked: {text}");

    // 2. /v1/nodes/{A's reef}/atoms
    let resp = app
        .clone()
        .oneshot(req_get(token, APEX, &format!("/v1/nodes/{}/atoms", fx.a_reef)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: get_node_atoms(A's reef) must 404");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: get_node_atoms(A's reef) leaked: {text}");

    // 2. /v1/atoms/{A's coral} — and the R2 blob is never fetched: with a
    //    dummy (never-dialed unless reached) R2Client configured, a prompt 404
    //    with no `content` field proves the code never got past reachability
    //    to the R2 branch at all (cross_tenant_probe_pg.rs's own reasoning for
    //    the identical dummy-client trick).
    let resp = app
        .clone()
        .oneshot(req_get(token, APEX, &format!("/v1/atoms/{}", fx.a_atom)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: get_atom(A's coral) must 404");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: get_atom(A's coral) leaked: {text}");
    assert!(!text.contains("\"content\""), "{who}: get_atom(A's coral) must not even carry a content field: {text}");

    // 8. meetings/summary
    let resp = app
        .clone()
        .oneshot(req_get(
            token,
            APEX,
            &format!("/v1/meetings/summary?graph_id={}&source_id={}", fx.a_graph, fx.a_meeting_source_id),
        ))
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::NOT_FOUND,
        "{who}: meeting_summary(A) must deny, got {}",
        resp.status()
    );
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: meeting_summary(A) leaked: {text}");

    // 8. relationships
    let resp = app
        .clone()
        .oneshot(req_get(token, APEX, &format!("/v1/graphs/{}/relationships", fx.a_graph)))
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::NOT_FOUND,
        "{who}: relationships(A) must deny, got {}",
        resp.status()
    );
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: relationships(A) leaked: {text}");

    // 8. sessions
    let resp = app
        .clone()
        .oneshot(req_get(token, APEX, &format!("/v1/sessions?graph_id={}", fx.a_graph)))
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::NOT_FOUND,
        "{who}: sessions(A) must deny, got {}",
        resp.status()
    );
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: sessions(A) leaked: {text}");

    // 5. POST /v1/queries scoped to A
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            token,
            APEX,
            "/v1/queries",
            json!({"text": "what is the secret?", "graph_id": fx.a_graph}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: scoped create_query(A) must 404");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: create_query(A) leaked: {text}");
    let denied: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(denied["error"], "source not found or access denied", "{denied}");
    assert_no_a_ocean_identifiers(&text, &a_ocean_identifiers(fx).await, who, "create_query(A)");

    // 5. POST /v1/queries/stream scoped to A — must refuse BEFORE the SSE
    //    response is committed (a plain JSON 404, not an `error` SSE event).
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            token,
            APEX,
            "/v1/queries/stream",
            json!({"text": "what is the secret?", "graph_id": fx.a_graph}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: scoped create_query_stream(A) must 404");
    assert_eq!(resp.headers().get("content-type").unwrap(), "application/json", "denial must precede SSE");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: create_query_stream(A) leaked: {text}");
    let denied: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(denied["error"], "source not found or access denied", "{denied}");
    assert_no_a_ocean_identifiers(&text, &a_ocean_identifiers(fx).await, who, "create_query_stream(A)");

    // shares: create + list, naming A's meeting.
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            token,
            APEX,
            "/v1/meetings/shares",
            json!({"graph_id": fx.a_graph, "source_id": fx.a_meeting_source_id}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT, "{who}: whole-original cloud share requires selected consent");
    let text = body_text(resp).await;
    let denied: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(denied["code"], "selected_evidence_consent_required", "{denied}");
    assert!(!text.contains(A_SECRET), "{who}: create_share(A) leaked: {text}");
    let resp = app
        .clone()
        .oneshot(req_get(
            token,
            APEX,
            &format!("/v1/meetings/shares?graph_id={}&source_id={}", fx.a_graph, fx.a_meeting_source_id),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: list_shares(A) must 404");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "{who}: list_shares(A) leaked: {text}");
}

#[tokio::test]
async fn read_deny_matrix_denies_member_b() {
    let Some(base) = base_url() else {
        eprintln!("skipping read_deny_matrix_denies_member_b: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_rdm_b_ctl", "moprobe_rdm_b_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_rdm_b").await;
    let app = build_app(&fx, &base, ten).await;
    let token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");

    assert_read_deny_matrix(&app, &token, "member B", &fx).await;

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn read_deny_matrix_denies_org_admin_c() {
    let Some(base) = base_url() else {
        eprintln!("skipping read_deny_matrix_denies_org_admin_c: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_rdm_c_ctl", "moprobe_rdm_c_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_rdm_c").await;
    let app = build_app(&fx, &base, ten).await;
    // P0 #2: admins are not exempt — same claim shape as a member, only
    // `org_role` differs, and no path in this codebase ever consults it.
    let token = org_token(&fx.member_c_sub, &fx.org_id, "org:admin");

    assert_read_deny_matrix(&app, &token, "org admin C", &fx).await;

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Query history + activity are per-user (already narrowly proven by
// `group_b_probe_pg.rs` for the general Group-B case; this is the
// member-ocean-specific instance, using a REAL seeded row for A).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn query_history_and_activity_are_scoped_per_user() {
    let Some(base) = base_url() else {
        eprintln!("skipping query_history_and_activity_are_scoped_per_user: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_qha_ctl", "moprobe_qha_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_qha").await;
    let app = build_app(&fx, &base, ten).await;
    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");

    let resp = app.clone().oneshot(req_get(&b_token, APEX, "/v1/queries")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "B's query history must not contain A's row: {text}");

    let resp = app.clone().oneshot(req_get(&b_token, APEX, "/v1/activity")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET), "B's activity feed must not contain A's row: {text}");

    // Sanity: A herself DOES see her own rows (proves the assertions above
    // are testing narrowing, not an entirely-broken/empty endpoint — without
    // this, if `/v1/activity` started returning an empty array for EVERYONE,
    // the B assertion above would stay green while silently testing nothing,
    // per CodeRabbit #343).
    let a_token = org_token(&fx.member_a_sub, &fx.org_id, "org:member");
    let resp = app.clone().oneshot(req_get(&a_token, APEX, "/v1/queries")).await.unwrap();
    let text = body_text(resp).await;
    assert!(text.contains(A_SECRET), "A must see her OWN query history row: {text}");

    let resp = app.clone().oneshot(req_get(&a_token, APEX, "/v1/activity")).await.unwrap();
    let text = body_text(resp).await;
    assert!(
        text.contains(A_SECRET),
        "A must see her OWN activity row (proves the B assertion above tests narrowing, not an \
         empty/broken feed): {text}"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. sync/push refuses to write into another member's ocean (live, passes
// today — push delegates to ReachabilityResolver, unlike pull below).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn sync_push_refuses_to_write_into_another_members_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping sync_push_refuses_to_write_into_another_members_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_push_ctl", "moprobe_push_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_push").await;
    let app = build_app(&fx, &base, ten).await;
    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");

    let forged_atom_id = Uuid::new_v4();
    let push_body = json!({
        "ops": [{
            "seq": 1,
            "op_id": Uuid::new_v4(),
            "entity": "atom",
            "entity_id": forged_atom_id,
            "op_type": "upsert",
            "payload": {"content_inline": "B trying to write into A's ocean"},
            "version": 1,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "b-device",
            "graph_id": fx.a_graph,
        }]
    });
    let resp = app
        .clone()
        .oneshot(req_json("POST", &b_token, APEX, "/v1/sync/push", push_body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "sync push naming A's graph must be refused");

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
        .bind(forged_atom_id)
        .fetch_one(&fx.tenant)
        .await
        .unwrap();
    assert_eq!(count, 0, "nothing must land from the refused push");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. sync/pull must not leak another member's ocean.
//
// FORMERLY a KNOWN DEFECT, found by this exact probe on its first CI run:
// `routes::sync::pull`'s hand-rolled SQL join gated `kind='team'` but never
// `kind='member'` (and never excluded archived oceans), so `GET /v1/sync/pull`
// returned every op (full entity payloads) for ANY member's private ocean to
// every org member the moment `MEMBER_OCEANS_ENABLED` flipped. Fixed and
// merged as #345 (`security(sync): gate /v1/sync/pull on member ownership and
// archived state`) — this test is un-ignored now that `main` carries the fix;
// nothing else in the tree caught the gap.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn sync_pull_must_not_leak_another_members_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping sync_pull_must_not_leak_another_members_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_pull_ctl", "moprobe_pull_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_pull").await;
    let app = build_app(&fx, &base, ten).await;

    // Two real op_log rows: one in A's ocean (which B must NOT get) and one in
    // B's OWN ocean (which B MUST get). The second is the positive control
    // that makes the first assertion mean something — without it, a `pull`
    // that returned an empty op list to everyone (a broken join, a dropped
    // `account_id` bind, the endpoint 200-ing with `{"ops":[]}`) would satisfy
    // the exclusion below while proving nothing. This is the single
    // highest-value test in the file — it is the one that caught the real
    // #345 leak — so it is the last place a vacuous pass can be tolerated.
    for (graph, atom, marker, device) in [
        (fx.a_graph, fx.a_atom, A_SECRET, "a-device"),
        (fx.b_graph, fx.b_atom, B_MARK, "b-other-device"),
    ] {
        sqlx::query(
            "INSERT INTO op_log (seq, op_id, entity, entity_id, op_type, payload, \
                                  version, updated_at, device_id, graph_id) \
             VALUES (nextval('op_log_seq_seq'), gen_random_uuid(), 'atom', $1, 'upsert', $2, \
                     1, now(), $4, $3)",
        )
        .bind(atom.to_string())
        .bind(json!({ "content_inline": marker }))
        .bind(graph)
        .bind(device)
        .execute(&fx.tenant)
        .await
        .unwrap();
    }

    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");
    let resp = app
        .clone()
        .oneshot(req_get(&b_token, APEX, "/v1/sync/pull?after=0&limit=100"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(
        text.contains(B_MARK) && text.contains(&fx.b_graph.to_string()),
        "control: sync pull must return B's OWN op, proving the exclusion below tests member \
         scoping rather than an empty/broken feed: {text}"
    );
    assert!(
        !text.contains(A_SECRET) && !text.contains(&fx.a_graph.to_string()),
        "sync pull must return zero ops for A's private ocean to B: {text}"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 6/7. MCP surface: all 13 read tools + default-ocean resolution + the
// mixed-scope ask_ocean citation check, once for B and once for admin C.
// ═══════════════════════════════════════════════════════════════════════════

/// The full read-only tool roster this file's own coverage claims (see the
/// module doc's "regression-proofing" section for what asserting against this
/// list does and does not catch).
///
/// `get_task_result` (MCP spec 2026-07-28 PR 2, task-based `ask_ocean`) is
/// listed here for the roster-size check, but is NOT new isolation surface
/// this file needs its own member/org-admin probe row for — same
/// classification precedent as `promote` below. Its scoping shape is
/// per-TASK-OWNER (a caller-supplied `task_id`, checked against the
/// authenticated caller who created it), not per-ocean-reachability like
/// every other tool here, so this file's ocean-reachability-shaped probes
/// (`assert_mcp_deny_matrix`, the mixed-scope citation checks) have no
/// analogous shape to extend it with. Its cross-caller isolation is instead
/// covered end-to-end by the dedicated new suite,
/// `mcp_task_based_ask_ocean_pg.rs` (`get_task_result_refuses_a_different_
/// callers_task` / `tasks_get_refuses_a_different_callers_task`).
const READ_TOOL_NAMES: &[&str] = &[
    "list_oceans",
    "ask_ocean",
    "list_reefs",
    "get_reef",
    "list_meetings",
    "get_meeting",
    "get_atom",
    "search_atoms",
    "get_transcript",
    "recent_activity",
    "get_ocean_stats",
    "list_sessions",
    "get_task_result",
];

/// The COMPLETE tool roster (read + write) `/mcp` advertises to a caller
/// holding both scopes — 13 read (`READ_TOOL_NAMES`) + 5 write (`add_context`,
/// `confirm_reef`, `reject_reef`, `update_reef`, `promote`,
/// `mcp::tools::WRITE_TOOLS`).
/// Asserting the roster's TOTAL length (not just that `READ_TOOL_NAMES` is a
/// subset) is what actually closes the gap CodeRabbit found (#343): without
/// it, a newly-added 13th tool makes `advertised` grow but every assertion
/// here still passes, so the new tool ships with zero isolation probe —
/// exactly the failure mode this mechanism exists to prevent. This constant
/// must be bumped (and a probe added for the new tool) the moment a real tool
/// is added; it is intentionally NOT derived from `mcp::tools::WRITE_TOOLS`
/// (a `pub(crate)` item this external test crate cannot see) — see the module
/// doc's regression-proofing section for the honest limit this implies.
///
/// Bumped 16→17 (integration/graph-temporal's merge of main): main's #416
/// ("make writing to your own private ocean discoverable") added `promote` as
/// a 5th write tool. Classified per this constant's own instructions —
/// `promote` is NOT new isolation surface this file needs its own probe for:
/// it dispatches to the same `routes::nodes::promote_node` this whole repo's
/// promotion isolation coverage already exercises end-to-end
/// (`member_ocean_promotion_pg.rs`'s same-pool tests, `cross_tenant_probe_pg
/// .rs`'s `promotion_never_crosses_a_database_org_plane_refuses_a_consumer_
/// plane_source`, `promote_node_corals_pg.rs`) — the MCP tool is a thin
/// argument-shape wrapper over that route, not a new query path.
///
/// Bumped 17→18 (MCP spec 2026-07-28 PR 2): `get_task_result`, a 13th read
/// tool. See `READ_TOOL_NAMES`'s own doc comment for why this is not new
/// isolation surface this file's probes need to extend.
///
/// Bumped 18→20 (claim-based tenancy §Phase 3 PR 3.2): `list_tenants` /
/// `select_tenant` (`mcp::tools::tenancy`), 2 new tools that are neither in
/// `READ_TOOL_NAMES` (`select_tenant` is not `readOnlyHint: true` — see that
/// file's module doc) nor in `mcp::tools::WRITE_TOOLS` (selecting a tenant
/// needs only read scope). Deliberately given NO per-name presence check
/// here, on purpose: both operate purely on the CALLER's own
/// `(user_id, client_id)` — `WHERE user_id = $1`, bound to the authenticated
/// caller, never a client-supplied target user id — so there is no
/// cross-member/ocean-reachability surface for THIS file's probes
/// (`assert_mcp_deny_matrix`'s A-vs-B/admin-vs-member ocean isolation shape)
/// to extend, the same classification precedent `get_task_result`/`promote`
/// document above. Their own isolation property (a stale/revoked membership
/// is refused on the very next call, never served from a cached selection)
/// is covered end-to-end by the dedicated `tests/mcp_tenant_selection_pg.rs`
/// instead.
const EXPECTED_TOTAL_TOOL_COUNT: usize = 20;

async fn assert_mcp_deny_matrix(app: &axum::Router, mcp_token: &str, who: &str, fx: &Fx) {
    // Roster check: every name this file hardcodes must actually be an
    // advertised tool (catches a rename; see module doc's honesty note on
    // what this substitute mechanism does and doesn't catch) — AND the total
    // advertised count must match exactly, so an ADDED tool (not just a
    // rename/removal) also fails loudly and forces a human to classify and
    // probe it (CodeRabbit #343: the previous version only asserted presence
    // of the 12 known names, so a 13th tool would have shipped silently).
    let advertised = list_tools_names(app, mcp_token).await;
    assert_eq!(
        advertised.len(),
        EXPECTED_TOTAL_TOOL_COUNT,
        "{who}: MCP tool roster changed size (expected {EXPECTED_TOTAL_TOOL_COUNT} read+write \
         tools) — classify the new/removed tool and update READ_TOOL_NAMES/EXPECTED_TOTAL_TOOL_COUNT \
         plus a probe row: {advertised:?}"
    );
    for name in READ_TOOL_NAMES {
        assert!(advertised.contains(&name.to_string()), "{who}: expected read tool {name} missing from tools/list: {advertised:?}");
    }

    // list_oceans: A's graph must never appear — but the org commons ocean
    // MUST (positive control: an empty roster would satisfy the exclusions
    // below while proving nothing).
    let r = mcp_call(app, mcp_token, "list_oceans", json!({})).await;
    let text = r.to_string();
    assert!(
        text.contains(&fx.org_graph.to_string()),
        "{who}: control — list_oceans must contain the org commons ocean, proving the \
         exclusions below test scoping rather than an empty roster: {text}"
    );
    assert!(!text.contains(&fx.a_graph.to_string()), "{who}: list_oceans leaked A's graph id: {text}");
    assert!(!text.contains(A_SECRET), "{who}: list_oceans leaked A's ocean name: {text}");

    // Private scope admission must fail before creating a durable Ask task.
    let r = mcp_call(
        app,
        mcp_token,
        "ask_ocean",
        json!({"question": format!("what is {A_SECRET}?"), "ocean_ids": [fx.a_graph]}),
    )
    .await;
    assert!(!r.to_string().contains(A_SECRET), "{who}: ask_ocean(A) leaked content: {r}");
    assert_scope_denied(&r, &a_ocean_identifiers(fx).await, who, "ask_ocean(A)");

    // Exact cloud Library scopes reject the whole request when any graph is
    // inaccessible; denied input never creates a task or a partial answer.
    let r = mcp_call(app, mcp_token, "ask_ocean", json!({
        "question": "private coral content", "ocean_ids": [fx.org_graph, fx.a_graph],
    })).await;
    assert_scope_denied(&r, &a_ocean_identifiers(fx).await, who, "mixed-scope Ask");

    // Every remaining read tool, driven at A's ids. Each denial pins the
    // REASON (`not_found`), not merely "an error came back" — see
    // `assert_denied_not_found`'s doc for why a bare `tool_is_error` check is
    // a vacuous pass waiting to happen.

    // list_reefs, explicit A scope.
    let r = mcp_call(app, mcp_token, "list_reefs", json!({"ocean_id": fx.a_graph})).await;
    assert_denied_not_found(&r, who, "list_reefs(A)");

    // get_reef, A's real reef id.
    let r = mcp_call(app, mcp_token, "get_reef", json!({"reef_id": fx.a_reef})).await;
    assert_denied_not_found(&r, who, "get_reef(A)");

    // list_meetings / get_meeting, explicit A scope.
    let r = mcp_call(app, mcp_token, "list_meetings", json!({"ocean_id": fx.a_graph})).await;
    assert_denied_not_found(&r, who, "list_meetings(A)");
    let r = mcp_call(
        app,
        mcp_token,
        "get_meeting",
        json!({"ocean_id": fx.a_graph, "meeting_id": fx.a_meeting_source_id}),
    )
    .await;
    assert_denied_not_found(&r, who, "get_meeting(A)");
    assert!(!r.to_string().contains(A_SECRET), "{who}: get_meeting(A) leaked: {r}");

    // get_atom, A's real coral id: denied, never fetched from R2.
    let r = mcp_call(app, mcp_token, "get_atom", json!({"atom_id": fx.a_atom})).await;
    assert_denied_not_found(&r, who, "get_atom(A)");
    assert!(!r.to_string().contains(A_SECRET), "{who}: get_atom(A) leaked: {r}");

    // search_atoms, explicit A scope: refused outright (every named ocean is
    // unreachable), and no results referencing A's content.
    let r = mcp_call(
        app,
        mcp_token,
        "search_atoms",
        json!({"query": A_SECRET, "ocean_ids": [fx.a_graph]}),
    )
    .await;
    assert!(!r.to_string().contains(A_SECRET), "{who}: search_atoms(A) leaked: {r}");
    assert_denied_not_found(&r, who, "search_atoms(A)");

    // get_transcript, explicit A scope.
    let r = mcp_call(
        app,
        mcp_token,
        "get_transcript",
        json!({"ocean_id": fx.a_graph, "meeting_id": fx.a_meeting_source_id}),
    )
    .await;
    assert_denied_not_found(&r, who, "get_transcript(A)");
    assert!(!r.to_string().contains(A_SECRET), "{who}: get_transcript(A) leaked: {r}");

    // get_ocean_stats, explicit A scope.
    let r = mcp_call(app, mcp_token, "get_ocean_stats", json!({"ocean_id": fx.a_graph})).await;
    assert_denied_not_found(&r, who, "get_ocean_stats(A)");

    // list_sessions (required ocean_id, no default): explicit A scope.
    let r = mcp_call(app, mcp_token, "list_sessions", json!({"ocean_id": fx.a_graph})).await;
    assert_denied_not_found(&r, who, "list_sessions(A)");

    // recent_activity, explicit A scope (reachability-gated, per its own doc
    // comment on why an explicit ocean_id is re-checked).
    let r = mcp_call(app, mcp_token, "recent_activity", json!({"ocean_id": fx.a_graph})).await;
    assert_denied_not_found(&r, who, "recent_activity(A)");
}

// Exact-scope denial plus permitted evidence controls prevent both content
// and identifier disclosure without relying on empty legacy citation arrays.
async fn assert_mixed_scope_references_stay_inside_the_reachable_set(
    app: &axum::Router, http_token: &str, mcp_token: &str, who: &str, fx: &Fx,
) {
    let reachable = reachable_graph_ids(app, http_token, who).await;
    assert!(reachable.contains(&fx.org_graph), "org commons positive control must be reachable");
    assert!(!reachable.contains(&fx.a_graph), "private member A graph must remain unreachable");
    let ids = a_ocean_identifiers(fx).await;
    let mixed = [fx.org_graph, fx.a_graph];
    let ask_denied = mcp_call(app, mcp_token, "ask_ocean", json!({
        "question": SHARED_RETRIEVAL_PHRASE, "ocean_ids": mixed, "style": "sources-only",
    })).await;
    assert_scope_denied(&ask_denied, &ids, who, "mixed-scope Ask");
    let search_denied = mcp_call(app, mcp_token, "search_atoms", json!({
        "query": SHARED_RETRIEVAL_PHRASE, "ocean_ids": mixed,
    })).await;
    assert_scope_denied(&search_denied, &ids, who, "mixed-scope search");

    // Both callers have real permitted evidence; admin C's scope excludes B's
    // private graph because it is derived from C's own HTTP reachable roster.
    let ask = ask_ocean_and_wait(app, mcp_token, json!({
        "question": SHARED_RETRIEVAL_PHRASE, "ocean_ids": reachable, "style": "sources-only",
    })).await;
    assert_permitted_legacy_evidence(fx, &ask, &reachable, &[fx.org_atom], who, "permitted Ask").await;
    let search = mcp_call(app, mcp_token, "search_atoms", json!({
        "query": SHARED_RETRIEVAL_PHRASE, "ocean_ids": reachable,
    })).await;
    assert_eq!(search["results"], json!([]), "ordinary retained search has no legacy result projection");
    assert_eq!(search["unreachable_oceans"], json!([]), "scope was fully admitted");
    assert_permitted_legacy_evidence(fx, &search, &reachable, &[fx.org_atom], who, "permitted search").await;
}

/// Resolve each referenced coral id to its OWNING ocean (straight out of the
/// tenant database, not from anything the response claimed) and require that
/// ocean to be in the caller's reachable set.
///
/// Resolving server-side is what makes this an authorization assertion rather
/// than a self-report: a leaking response that mislabels A's coral as belonging
/// to the org commons ocean would still fail here. An id that resolves to NO
/// row at all is also a failure, not a skip — either the pipeline invented an
/// id (a synthesis-manifest bug worth failing on) or it named a row this probe
/// cannot see, and silently ignoring the case would re-open the "unparseable
/// input becomes a passing leak check" hole (module doc, non-vacuity rule 3).
async fn assert_references_are_reachable(
    fx: &Fx,
    atom_ids: &[Uuid],
    reachable: &[Uuid],
    who: &str,
    what: &str,
) {
    for atom_id in atom_ids {
        let owning: Option<Uuid> =
            sqlx::query_scalar("SELECT graph_id FROM atoms WHERE atom_id = $1")
                .bind(atom_id)
                .fetch_optional(&fx.tenant)
                .await
                .unwrap();
        let owning = owning.unwrap_or_else(|| {
            panic!("{who}: {what} referenced coral {atom_id}, which matches no row in the tenant \
                    database at all — an invented or foreign identifier, not a citation")
        });
        assert!(
            reachable.contains(&owning),
            "{who}: {what} referenced coral {atom_id}, which lives in ocean {owning} — NOT in \
             this caller's reachable set {reachable:?}. This is an identifier disclosure even \
             though no content of that ocean's appears in the response."
        );
    }
}

/// Sets `SUPERSESSION_RANKING_ENABLED=1` for as long as it is alive and clears
/// it on drop — **including while unwinding from a failed assertion**.
///
/// The caller must hold `ENV_LOCK` for at least as long as this value, and
/// must declare this AFTER the lock guard so it drops first (Rust drops
/// locals in reverse declaration order).
struct SupersessionRankingFlag;

impl SupersessionRankingFlag {
    /// # Safety
    /// Mutates process-global env; caller must hold `ENV_LOCK` for this
    /// value's whole lifetime, matching this file's other env helpers.
    fn on() -> Self {
        // SAFETY: per the contract above — caller holds `ENV_LOCK`.
        unsafe { std::env::set_var("SUPERSESSION_RANKING_ENABLED", "1") };
        Self
    }
}

impl Drop for SupersessionRankingFlag {
    fn drop(&mut self) {
        // SAFETY: per `on`'s contract — `ENV_LOCK` is still held here, because
        // this guard is declared after (and so drops before) the lock guard.
        unsafe { std::env::remove_var("SUPERSESSION_RANKING_ENABLED") };
    }
}

/// The actual legacy retrieval annotation retains its allowed/private controls
/// through a feature-gated projection. Cloud MCP separately proves typed,
/// permitted evidence and never fabricates this old supersession annotation.
///
/// A REAL cross-ocean identifier disclosure this sweep found, and the fix that
/// closes it (`routes::queries::multi_graph_retrieve`'s superseder scope
/// guard).
///
/// # The leak
///
/// `Store::list_supersession_targets` matches `supersession_edge` rows on
/// `superseded_id` alone — nothing constrains which ocean the SUPERSEDER node
/// lives in, and the table has no same-graph CHECK. `POST /v1/nodes/{id}/
/// supersede` authorizes both endpoints with `node_owned_by_account`, and in an
/// org tenant EVERY member-private ocean shares the one org account, so that
/// check passes for another member's private reef. Member A can therefore
/// legitimately record "the org commons reef is superseded by my private reef"
/// using only ids she already holds.
///
/// From then on, any OTHER member retrieving the org commons reef got A's
/// private reef id back in `search_atoms`' `superseded_by` (and, with an LLM
/// configured, `ask_ocean`'s `citation_details[].superseded_by` and the
/// `[SUPERSEDED — see reef <id>]` prose annotation) — while the query never
/// touched A's ocean and not one byte of A's CONTENT appeared in the response.
/// Every marker-grep assertion in this file was structurally blind to it; this
/// probe asserts on the IDENTIFIER, which is the only thing that moves.
///
/// # Why it is worth fixing rather than recording
///
/// It was latent behind `SUPERSESSION_RANKING_ENABLED` (`multi_graph_retrieve`
/// zeroes `superseded_atoms` on the way out when the flag is off) — not live
/// in production while the flag defaulted off. §3.9 has since flipped that
/// default to ON, so the guard this probe exercises is no longer merely
/// defense-in-depth for a future flip; it is what keeps this disclosure closed
/// TODAY. A latent disclosure that arms itself on a planned config change was
/// worth closing while it was cheap — and turned out to be worth closing
/// before the change landed, not after.
///
/// This test therefore turns the flag ON deliberately: with it off the probe
/// would pass for the wrong reason (the empty-map short-circuit), proving
/// nothing about the guard it exists to protect.
#[tokio::test]
async fn supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping supersession_never_discloses_a_superseder_reef_from_an_unreachable_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };
    // SAFETY: same guard. See this test's doc comment for why the flag must be
    // ON here — with it off, `multi_graph_retrieve` returns an empty
    // `superseded_atoms` map unconditionally and the assertions below would
    // hold no matter how broken the scope guard was.
    //
    // Set through a DROP GUARD, not a plain `set_var` + trailing `remove_var`
    // (CodeRabbit, PR #348). `ENV_LOCK` is a `tokio::sync::Mutex`, which does
    // NOT poison, so a panic in any assertion below would release the lock
    // with this flag still `"1"` — and every later `#[tokio::test]` in this
    // same binary would then run with supersession ranking silently ON,
    // turning one real failure into a cascade of unrelated ones. In a suite
    // whose entire purpose is that a result means what it says, a failure mode
    // that corrupts OTHER tests' results is worse than the bug it would be
    // reporting. `_flag` is declared AFTER `_env_guard`, so it drops FIRST —
    // the env is restored while `ENV_LOCK` is still held.
    let _flag = SupersessionRankingFlag::on();

    let (ctl, ten) = ("moprobe_ssedge_ctl", "moprobe_ssedge_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_ssedge").await;

    // ── Two supersession edges, identical in every respect but one ───────────
    //
    // 1. FORBIDDEN: the org commons reef (B CAN reach) superseded by A's
    //    private reef (B CANNOT) — exactly what `POST /v1/nodes/{id}/supersede`
    //    writes for member A today.
    // Seeded in raw SQL, and that is now the ONLY way this row can exist:
    // `POST /v1/nodes/{id}/supersede` refuses to create it since the write-side
    // authz fix (both endpoints must be REACHABLE by the caller AND live in the
    // same ocean — see `supersede_refuses_to_create_a_cross_ocean_edge` below),
    // and migrations 0094/0008 deleted every such row that already existed.
    // Manufacturing it here is deliberate: `multi_graph_retrieve`'s superseder
    // scope guard is the LAST line of defense for exactly this shape, and a
    // defense-in-depth guard is only worth anything if it is still proven to
    // work when the layers in front of it are assumed to have failed.
    // `graph_id` (NOT NULL as of 0094/0008) is bound to the SUPERSEDED
    // endpoint's ocean, which is what the derived column would hold.
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'manual', $3)",
    )
    .bind(fx.org_reef)
    .bind(fx.a_reef)
    .bind(fx.org_graph)
    .execute(&fx.tenant)
    .await
    .unwrap();

    // 2. ALLOWED (the positive control): a second org commons reef superseded
    //    by a THIRD org commons reef — both inside B's reachable scope.
    //
    // This is what makes the assertion below mean "the guard FILTERS by ocean"
    // rather than "supersession was switched off wholesale". Without it, the
    // probe would stay green if the fix had simply deleted
    // `list_supersession_targets`' result, or if the feature flag silently
    // stopped taking effect — both of which suppress the disclosure and
    // suppress the legitimate behavior with it, and neither of which is the
    // property this test claims to prove.
    let (superseded_reef, superseded_atom) = seed_reef_and_coral(
        &fx.tenant,
        fx.org_graph,
        &format!("Org commons outdated reef {ORG_MARK}"),
        &format!("Org commons outdated private coral content {ORG_MARK}"),
        "org-commons-outdated-source",
    )
    .await;
    let superseder_reef: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
    )
    .bind(fx.org_graph)
    .bind(format!("Org commons replacement reef {ORG_MARK}"))
    .fetch_one(&fx.tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'manual', $3)",
    )
    .bind(superseded_reef)
    .bind(superseder_reef)
    .bind(fx.org_graph)
    .execute(&fx.tenant)
    .await
    .unwrap();

    let app = build_app_inline_content(&fx, &base, ten).await;
    let mcp_token = mcp_org_token(&fx.member_b_sub, &fx.org_id, "org:member");
    select_org(&app, &mcp_token, &fx.org_id).await;
    let a_ids = a_ocean_identifiers(&fx).await;

    // Scope: ONLY the org commons ocean. B never names A's ocean at all here —
    // which is what makes this worse than the mixed-scope case. There is no
    // caller-supplied id to hide behind: every A identifier in this response
    // would be one B could not have known.
    let r = mcp_call(
        &app,
        &mcp_token,
        "search_atoms",
        json!({"query": SHARED_RETRIEVAL_PHRASE, "ocean_ids": [fx.org_graph]}),
    )
    .await;
    assert!(!tool_is_error(&r), "member B: search_atoms must succeed: {r}");

    assert_permitted_legacy_evidence(&fx, &r, &[fx.org_graph],
        &[fx.org_atom, superseded_atom], "member B", "supersession search").await;
    assert_eq!(r["results"], json!([]), "typed cloud search does not invent legacy superseded_by fields");
    assert_no_a_ocean_identifiers(&r.to_string(), &a_ids, "member B", "search_atoms (supersession)");

    // Preserve the original legacy retrieval guard with a non-vacuous allowed
    // superseder control, although cloud MCP now uses the typed retained path.
    let store = PostgresStore::new(fx.tenant.clone());
    let (retrieved, superseders) = coralswarm_backend::test_support_legacy_supersession_projection(
        &store, &fx.tenant, fx.member_b, fx.org_graph, SHARED_RETRIEVAL_PHRASE,
    ).await;
    assert!(retrieved.contains(&fx.org_atom) && retrieved.contains(&superseded_atom),
        "actual legacy retrieval must contain both control atoms: {retrieved:?}");
    assert_eq!(superseders.get(&superseded_atom), Some(&vec![superseder_reef]),
        "actual legacy retrieval must retain the permitted superseder");
    assert!(!superseders.contains_key(&fx.org_atom),
        "actual legacy retrieval must remove the private superseder: {superseders:?}");
    assert!(!superseders.values().flatten().any(|id| *id == fx.a_reef),
        "private superseder must never survive the actual retrieval guard");

    let r = ask_ocean_and_wait(&app, &mcp_token, json!({
        "question": SHARED_RETRIEVAL_PHRASE, "ocean_ids": [fx.org_graph], "style": "sources-only",
    })).await;
    assert_permitted_legacy_evidence(&fx, &r, &[fx.org_graph],
        &[fx.org_atom, superseded_atom], "member B", "supersession Ask").await;
    assert_no_a_ocean_identifiers(&r.to_string(), &a_ids, "member B", "ask_ocean (supersession)");

    teardown(&base, fx, [ctl, ten]).await;
    // `SUPERSESSION_RANKING_ENABLED` is cleared by `_flag`'s Drop, so it is
    // restored on the panic path too — see where it is set, above.
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// Migrations 0094 (control) / 0008 (tenant): the cross-ocean cleanup itself.
// ═══════════════════════════════════════════════════════════════════════════

/// Which of the two migration trees a cleanup case is exercising. The two
/// migrations' SQL bodies are byte-identical (`scripts/check_schema_parity.sh`
/// proves the resulting SCHEMAS agree; nothing proves the DML does), so both
/// are run rather than one being taken as evidence for the other.
#[derive(Clone, Copy)]
enum MigTree {
    Control,
    Tenant,
}

/// One row of `supersession_edge_cross_ocean_deletions` in the SELECT order
/// used below. Named rather than written inline because four of its eight
/// columns are `Uuid` and two of those (`superseded_graph_id`/
/// `superseder_graph_id`) are ADJACENT — precisely the shape
/// `tenancy::reachability::GraphRow`'s own doc warns about, where a transposed
/// pair still compiles and still binds. The assertions below pin each slot to a
/// distinct expected value, so a swap fails loudly rather than silently.
type AuditedDeletion = (
    Uuid,                                  // superseded_id
    Uuid,                                  // superseder_id
    Uuid,                                  // superseded_graph_id
    Uuid,                                  // superseder_graph_id
    String,                                // source
    Option<Uuid>,                          // confirmed_by
    Option<chrono::DateTime<chrono::Utc>>, // confirmed_at
    String,                                // deleted_by
);

/// Seeds the pre-migration world — three reefs across two oceans, two
/// cross-ocean edges (one in each direction) and one legitimate same-ocean
/// edge — then applies ONLY the migration under test and asserts what it did.
///
/// Applying the tree up to, but not including, the migration under test is the
/// whole point: running the full tree first would leave `supersession_edge`
/// already carrying a NOT NULL `graph_id`, and the cross-ocean rows this
/// migration exists to remove could never be seeded at all. The delete path
/// would then be "tested" against an empty table — green, and worthless.
async fn assert_cross_ocean_cleanup(base: &str, db: &str, tree: MigTree) {
    let label = match tree {
        MigTree::Control => "control 0094",
        MigTree::Tenant => "tenant 0008",
    };
    let mut conn = admin(base).await;
    recreate(&mut conn, db).await;
    let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();

    let (target, migrator) = match tree {
        MigTree::Control => (94_i64, sqlx::migrate!("./migrations_control")),
        MigTree::Tenant => (8_i64, sqlx::migrate!("./migrations_tenant")),
    };
    // Run through the real `Migrator`, not by executing each file's text: the
    // trees are not a pile of independent scripts (0083, for one, reads
    // `_sqlx_migrations` to assert what it is layered on top of), so raw
    // execution is not the same thing as migrating and would only prove that
    // the SQL parses.
    // `Migrator` is not `Clone`; its fields are `#[doc(hidden)] pub` precisely
    // so `migrate!()` can construct one, so a filtered copy is built the same
    // way, carrying the original's flags rather than guessing at defaults.
    let subset = |keep: &dyn Fn(i64) -> bool| sqlx::migrate::Migrator {
        migrations: migrator
            .iter()
            .filter(|x| keep(x.version))
            .cloned()
            .collect::<Vec<_>>()
            .into(),
        // The second call below resolves ONLY the migration under test, so the
        // ledger already holds every earlier version — which the default
        // validation rejects as "previously applied but missing". Relaxed
        // deliberately, and it is inert on the first call (empty ledger).
        ignore_missing: true,
        locking: migrator.locking,
        no_tx: migrator.no_tx,
    };
    subset(&|v| v < target)
        .run(&pool)
        .await
        .unwrap_or_else(|e| panic!("{label}: applying the tree below {target}: {e}"));

    // Sanity: the column must NOT exist yet, or this test is silently running
    // against a tree where the migration under test already ran and every
    // assertion below would be about the wrong thing.
    let pre_col: Option<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'supersession_edge' AND column_name = 'graph_id'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(
        pre_col.is_none(),
        "{label}: supersession_edge.graph_id already exists BEFORE the migration under test — \
         the version filter above is not doing what it claims"
    );

    // ── Two oceans in ONE account: exactly the shape that made the defect
    //    possible (an account-scoped check cannot tell them apart) ───────────
    let account_id = Uuid::new_v4();
    let owner_user_id = Uuid::new_v4();
    if matches!(tree, MigTree::Control) {
        // Control-tree-only FKs: graphs → accounts → users, and
        // graphs.owner_user_id → users. The tenant tree has neither table.
        sqlx::query("INSERT INTO users(id, email) VALUES ($1, $2)")
            .bind(owner_user_id)
            .bind(format!("mig-{owner_user_id}@test.local"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(owner_user_id)
            .execute(&pool)
            .await
            .unwrap();
    }
    let commons: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Commons', 'org') RETURNING id",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let member_ocean: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) \
         VALUES ($1, 'A private', 'member', $2) RETURNING id",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    async fn reef(pool: &PgPool, graph: Uuid, title: &str) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO nodes (graph_id, type, title, body) \
             VALUES ($1, 'entity', $2, '{}'::jsonb) RETURNING node_id",
        )
        .bind(graph)
        .bind(title)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    let commons_old = reef(&pool, commons, "Commons outdated").await;
    let commons_new = reef(&pool, commons, "Commons replacement").await;
    let a_reef = reef(&pool, member_ocean, "A's private reef").await;

    let confirmed_at = Utc::now();
    // (1) The real attack: commons reef superseded by a MEMBER-ocean reef.
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source, confirmed_at) \
         VALUES ($1, $2, 'user', $3)",
    )
    .bind(commons_old)
    .bind(a_reef)
    .bind(confirmed_at)
    .execute(&pool)
    .await
    .unwrap();
    // (2) The SAME violation with the endpoints swapped — the predicate is
    //     graph inequality, which is symmetric, and a fix that only scanned one
    //     direction would leave this row behind.
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source) \
         VALUES ($1, $2, 'auto')",
    )
    .bind(a_reef)
    .bind(commons_new)
    .execute(&pool)
    .await
    .unwrap();
    // (3) POSITIVE CONTROL — a perfectly legitimate same-ocean edge. Without
    //     this the whole test would pass against a migration that simply
    //     TRUNCATEd the table, which is the over-broad deletion the predicate
    //     was chosen to avoid.
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source) \
         VALUES ($1, $2, 'user')",
    )
    .bind(commons_old)
    .bind(commons_new)
    .execute(&pool)
    .await
    .unwrap();

    // ── Apply ONLY the migration under test ─────────────────────────────────
    assert_eq!(
        subset(&|v| v == target).iter().count(),
        1,
        "{label}: migration {target} must exist in the tree"
    );
    subset(&|v| v == target)
        .run(&pool)
        .await
        .unwrap_or_else(|e| panic!("{label}: applying migration {target}: {e}"));

    // ── 1. Exactly the legitimate edge survives ─────────────────────────────
    let surviving: Vec<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT superseded_id, superseder_id, graph_id FROM supersession_edge",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        surviving,
        vec![(commons_old, commons_new, commons)],
        "{label}: exactly the ONE same-ocean edge must survive, with graph_id backfilled to its \
         endpoints' shared ocean — a different set here means the delete predicate is either \
         over-broad (legitimate data destroyed) or under-broad (a disclosure left in place)"
    );

    // ── 2. Both cross-ocean rows were RECORDED before being removed ─────────
    let audited: Vec<AuditedDeletion> = sqlx::query_as(
        "SELECT superseded_id, superseder_id, superseded_graph_id, superseder_graph_id, \
                source, confirmed_by, confirmed_at, deleted_by \
           FROM supersession_edge_cross_ocean_deletions \
          ORDER BY source",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        audited.len(),
        2,
        "{label}: both cross-ocean edges must be in the forensic ledger — a deletion that leaves \
         no record destroys the evidence of the incident along with the incident: {audited:?}"
    );

    // ORDER BY source: 'auto' (the swapped-direction row) then 'user'.
    let (swapped, original) = (&audited[0], &audited[1]);
    assert_eq!(
        (swapped.0, swapped.1, swapped.2, swapped.3, swapped.4.as_str()),
        (a_reef, commons_new, member_ocean, commons, "auto"),
        "{label}: the swapped-direction row must be recorded with BOTH resolved graph ids, in the \
         right slots"
    );
    assert_eq!(
        (original.0, original.1, original.2, original.3, original.4.as_str()),
        (commons_old, a_reef, commons, member_ocean, "user"),
        "{label}: the original attack row must be recorded with BOTH resolved graph ids"
    );
    assert_eq!(
        original.6.map(|t| t.timestamp_millis()),
        Some(confirmed_at.timestamp_millis()),
        "{label}: the row's own confirmed_at must be preserved — the ledger records the EDGE, not \
         just that some edge existed"
    );
    assert!(
        original.7.contains("supersession_edge_graph_id"),
        "{label}: the ledger must name the migration that removed the row: {}",
        original.7
    );
    let deleted_at_all_set: bool = sqlx::query_scalar(
        "SELECT bool_and(deleted_at IS NOT NULL) FROM supersession_edge_cross_ocean_deletions",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(deleted_at_all_set, "{label}: every ledger row must carry a deletion timestamp");

    // ── 3. The SURVIVING edge must NOT be in the ledger ─────────────────────
    let false_positives: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM supersession_edge_cross_ocean_deletions \
          WHERE superseded_id = $1 AND superseder_id = $2",
    )
    .bind(commons_old)
    .bind(commons_new)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        false_positives, 0,
        "{label}: the legitimate same-ocean edge must not appear in the deletion ledger"
    );

    // ── 4. The schema half actually landed ──────────────────────────────────
    let nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns \
          WHERE table_name = 'supersession_edge' AND column_name = 'graph_id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        nullable, "NO",
        "{label}: graph_id must be NOT NULL — the backfill always resolves (superseded_id is a \
         NOT NULL FK to nodes, and nodes.graph_id is itself NOT NULL), so there is no reason to \
         leave the column nullable and every reason not to"
    );
    let has_index: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_indexes \
                         WHERE tablename = 'supersession_edge' \
                           AND indexname = 'idx_supersession_edge_graph_id')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_index, "{label}: the ocean-scoped index must exist");

    pool.close().await;
    let mut conn = admin(base).await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
}

#[tokio::test]
async fn migration_deletes_and_records_cross_ocean_supersession_edges_control_tree() {
    let Some(base) = base_url() else {
        eprintln!("skipping migration_deletes_and_records_cross_ocean_supersession_edges_control_tree: set ORGT_TEST_BASE_URL");
        return;
    };
    assert_cross_ocean_cleanup(&base, "moprobe_mig0094_ctl", MigTree::Control).await;
}

#[tokio::test]
async fn migration_deletes_and_records_cross_ocean_supersession_edges_tenant_tree() {
    let Some(base) = base_url() else {
        eprintln!("skipping migration_deletes_and_records_cross_ocean_supersession_edges_tenant_tree: set ORGT_TEST_BASE_URL");
        return;
    };
    assert_cross_ocean_cleanup(&base, "moprobe_mig0008_tnt", MigTree::Tenant).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// The READ side of the same predicate bug: `GET /v1/nodes/{id}/supersessions`.
// ═══════════════════════════════════════════════════════════════════════════

/// `GET /v1/nodes/{id}/supersessions` authorized its node id with
/// `node_owned_by_account` alone — the same predicate #348/#349 established is
/// a no-op across the member-ocean boundary, because every member-private
/// ocean in an org shares the one org `account_id`.
///
/// # Why this one is worse than the disclosure #348 closed
///
/// It returns `superseder_title` and `superseded_title`. Those are CONTENT, not
/// identifiers — the thing this suite's `A_SECRET` marker grep was built to
/// catch, on a surface nothing was grepping. And unlike the write-side defect,
/// it needed **no cross-ocean edge to exist**: an ordinary, entirely valid
/// supersession edge inside A's own private ocean was enough. Any member (or
/// admin) holding one of A's reef ids got A's reef titles back.
///
/// # Two doors again, and the second is the QUERY, not a transport
///
/// Authorizing the QUERIED reef is not sufficient, because every row also
/// carries the OTHER endpoint's title. An edge whose far endpoint lives in a
/// different ocean would disclose a title the caller was never authorized for —
/// through a reef the caller legitimately owns. ATTACK 3 below seeds exactly
/// that row (in raw SQL: since #349 no write path can produce one) and asserts
/// A does not get B's reef title back from a query about A's OWN reef.
///
/// # Non-vacuity
///
/// Three positive controls, because "404" and "empty list" are precisely what a
/// broken endpoint also returns: A must get her own edge back WITH its titles;
/// B must get a 200 and real titles for the org commons reef she genuinely
/// reaches; and B's own reachable-set is proven non-trivial by the same call.
/// Every denial asserts on BOTH content (`A_SECRET` in the raw body) and
/// IDENTIFIERS (`assert_no_a_ocean_identifiers`) — #348 established that a
/// marker grep is blind to id-only disclosure, and this surface can leak either.
#[tokio::test]
async fn supersessions_listing_denies_another_members_reef() {
    let Some(base) = base_url() else {
        eprintln!("skipping supersessions_listing_denies_another_members_reef: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_sslist_ctl", "moprobe_sslist_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_sslist").await;

    // A second reef in A's ocean and one in the org commons, so both the
    // private edge under test and the org-commons positive control exist.
    let (a_reef2, _) = seed_reef_and_coral(
        &fx.tenant,
        fx.a_graph,
        &format!("A superseding reef {A_SECRET}"),
        &format!("A superseding coral content {A_SECRET}"),
        "a-superseding-source",
    )
    .await;
    let (org_reef2, _) = seed_reef_and_coral(
        &fx.tenant,
        fx.org_graph,
        &format!("Org commons superseding reef {ORG_MARK}"),
        &format!("Org commons superseding coral content {ORG_MARK}"),
        "org-superseding-source",
    )
    .await;

    async fn seed_edge(fx: &Fx, superseded: Uuid, superseder: Uuid, graph: Uuid) {
        sqlx::query(
            "INSERT INTO supersession_edge (superseded_id, superseder_id, source, graph_id) \
             VALUES ($1, $2, 'user', $3)",
        )
        .bind(superseded)
        .bind(superseder)
        .bind(graph)
        .execute(&fx.tenant)
        .await
        .unwrap();
    }

    // 1. A perfectly legitimate edge, wholly inside A's private ocean. This is
    //    the one that made the defect reachable without any cross-ocean row.
    seed_edge(&fx, fx.a_reef, a_reef2, fx.a_graph).await;
    // 2. The org-commons equivalent, for B's positive control.
    seed_edge(&fx, fx.org_reef, org_reef2, fx.org_graph).await;

    let app = build_app(&fx, &base, ten).await;
    let a_ids = a_ocean_identifiers(&fx).await;
    let a_token = org_token(&fx.member_a_sub, &fx.org_id, "org:member");
    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");
    let c_token = org_token(&fx.member_c_sub, &fx.org_id, "org:admin");

    // ── POSITIVE CONTROL: the owner gets her own edge, titles and all ────────
    let resp = app
        .clone()
        .oneshot(req_get(
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersessions", fx.a_reef),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let owner_body = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control — A must still be able to list supersessions for her OWN reef: {owner_body}"
    );
    assert!(
        owner_body.contains(A_SECRET),
        "control — the owner's own response MUST carry the reef titles. If it does not, every \
         denial below is indistinguishable from this endpoint returning nothing to anyone, and \
         the `A_SECRET` greps prove nothing at all: {owner_body}"
    );

    // ── ATTACK 3 (the second door, checked on that same response) ────────────
    //
    // Seeded AFTER the control above so the control is a clean read: an edge
    // whose SUPERSEDER is A's reef but whose superseded endpoint — and
    // therefore whose `graph_id` — is in member B's private ocean. A is
    // legitimately authorized for `a_reef`; without the query's ocean
    // restriction she would receive `superseded_title`, i.e. B's private reef
    // TITLE, from a request about her own reef.
    seed_edge(&fx, fx.b_reef, fx.a_reef, fx.b_graph).await;
    let resp = app
        .clone()
        .oneshot(req_get(
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersessions", fx.a_reef),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let owner_body = body_text(resp).await;
    assert_eq!(status, StatusCode::OK, "A's own listing must still succeed: {owner_body}");
    assert!(
        owner_body.contains(A_SECRET),
        "control — A's own edge must still be listed alongside the excluded one, so the \
         exclusion below cannot be satisfied by the whole listing going empty: {owner_body}"
    );
    assert!(
        !owner_body.contains(B_MARK),
        "member B's private reef TITLE crossed the ocean boundary into A's own listing — the \
         queried reef being authorized says nothing about where the far endpoint lives: \
         {owner_body}"
    );
    assert!(
        !owner_body.contains(&fx.b_reef.to_string()),
        "member B's private reef ID crossed into A's own listing: {owner_body}"
    );

    // ── ATTACK 4: the same leak through a STALE edge graph_id ────────────────
    //
    // CodeRabbit, MAJOR, on this PR: ATTACK 3's row is excluded by
    // `se.graph_id`, which is a DENORMALIZED copy of the superseded endpoint's
    // ocean. Filtering on it alone would leave the guard resting on the very
    // "some other layer keeps these in agreement" assumption it exists to stop
    // relying on. This row is the counter-case: its `graph_id` says A's ocean —
    // so it sails through that filter — while its SUPERSEDER endpoint really
    // lives in B's. Only re-checking the joined `nodes` rows excludes it.
    //
    // Not producible by any write path (that is the point: it is what a stale
    // or hand-corrupted row looks like), so it is seeded in raw SQL.
    seed_edge(&fx, fx.a_reef, fx.b_reef, fx.a_graph).await;
    let resp = app
        .clone()
        .oneshot(req_get(
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersessions", fx.a_reef),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let owner_body = body_text(resp).await;
    assert_eq!(status, StatusCode::OK, "A's own listing must still succeed: {owner_body}");
    assert!(
        owner_body.contains(A_SECRET),
        "control — A's own edge must still be listed, so the exclusions below cannot be \
         satisfied by the listing going empty: {owner_body}"
    );
    assert!(
        !owner_body.contains(B_MARK),
        "member B's private reef TITLE crossed the boundary through an edge whose graph_id \
         claimed A's ocean while its endpoint lives in B's — the denormalized column cannot be \
         the only thing checked: {owner_body}"
    );
    assert!(
        !owner_body.contains(&fx.b_reef.to_string()),
        "member B's private reef ID crossed via the stale-graph_id row: {owner_body}"
    );

    // ── DENIAL: another member, and an org admin, holding A's reef id ────────
    for (token, who) in [(&b_token, "member B"), (&c_token, "org admin C")] {
        let resp = app
            .clone()
            .oneshot(req_get(
                token,
                APEX,
                &format!("/v1/nodes/{}/supersessions", fx.a_reef),
            ))
            .await
            .unwrap();
        let status = resp.status();
        let text = body_text(resp).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{who} must not be able to list supersessions for member A's private reef — and the \
             refusal must not distinguish 'exists but unreachable' from 'does not exist': {text}"
        );
        // CONTENT: the titles this endpoint returns are A's reef names.
        assert!(
            !text.contains(A_SECRET),
            "{who}: A's reef TITLE leaked through the supersessions listing: {text}"
        );
        // IDENTIFIERS: #348's lesson — a marker grep is structurally blind to a
        // response that names A's reef while quoting none of A's text.
        assert_no_a_ocean_identifiers(&text, &a_ids, who, "GET /v1/nodes/{id}/supersessions");
    }

    // ── POSITIVE CONTROL: B is not simply locked out of the endpoint ─────────
    //
    // Without this, B's 404 above would be equally well explained by
    // `list_supersessions` being broken, unroutable, or refusing every caller.
    let resp = app
        .clone()
        .oneshot(req_get(
            &b_token,
            APEX,
            &format!("/v1/nodes/{}/supersessions", fx.org_reef),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control — B must be able to list supersessions for the ORG COMMONS reef she really \
         reaches: {text}"
    );
    assert!(
        text.contains(ORG_MARK),
        "control — B's org-commons listing must carry real titles, or her 404 on A's reef is \
         about the endpoint rather than about the ocean: {text}"
    );
    assert!(
        !text.contains(A_SECRET),
        "B's org-commons listing must not carry any of A's content: {text}"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// The WRITE side of defect 3: `POST /v1/nodes/{id}/supersede` must not be able
// to CREATE the cross-ocean edge in the first place.
// ═══════════════════════════════════════════════════════════════════════════

/// #348 closed the READ side of the cross-ocean supersession defect
/// (`multi_graph_retrieve` no longer surfaces a superseder from an ocean the
/// caller cannot reach) and explicitly left the WRITE side open: the row could
/// still be created, because `POST /v1/nodes/{id}/supersede` authorized both
/// endpoints with `node_owned_by_account` — a predicate that, in an org tenant
/// where every member-private ocean shares the one org `account_id`, is
/// satisfied by EVERY member's private reef simultaneously. This probe is the
/// write-side counterpart.
///
/// # Why "refused" needs more than one attack to mean anything
///
/// The two illegal shapes fail for DIFFERENT reasons, and a fix that closed
/// only one of them would look complete from the other's perspective:
///
/// * **Cross-ocean between two oceans the caller DOES reach** (the original
///   attack: the org commons reef superseded by A's own private reef).
///   Reachability alone does NOT refuse this — A legitimately reaches both
///   oceans. It is refused by the same-ocean rule, which exists precisely
///   because reachability is per-CALLER while the row is not: every OTHER org
///   member reaches the commons and not A's ocean, and reads the row back.
/// * **An endpoint the caller does NOT reach** (B's private reef). Refused by
///   `ReachabilityResolver`; the same-ocean rule would be irrelevant, and in
///   the both-endpoints-in-B's-ocean variant it is *satisfied*, so only
///   reachability stands between A and writing into B's knowledge graph.
/// * **The same cross-ocean write through the OTHER door, `/v1/sync/push`**
///   (ATTACK 5), and its DELETE mirror image (ATTACK 6). That transport
///   authorizes the op's DECLARED `graph_id`, so the push is legitimately
///   ACCEPTED and neither of the route-level guards is on the path at all — the
///   refusal has to live in the apply. Fixing the HTTP handler alone would have
///   left both holes open behind a 200.
///
/// Asserting both is what makes this a probe of the ocean BOUNDARY rather than
/// of one arbitrary rejection.
///
/// # Non-vacuity (this file's three rules)
///
/// Rule 1 — a refusal that cannot be told apart from a broken endpoint proves
/// nothing, so the FIRST thing this test does is a POSITIVE CONTROL: A
/// supersedes one of her own reefs with another of her own reefs, and that must
/// 200 AND land a row. Every refusal below is then measured against a surface
/// demonstrated, in the same test, on the same fixture, to work. There are
/// positive controls for the DELETE half too (A can delete her own edge) and
/// for the tamper case (B's edge must SURVIVE A's attempt — "no row was
/// deleted" asserted as a count, not as a status code).
///
/// Rule 2 — pin the reason, not the shape. Each refusal asserts an exact status
/// AND the exact error string the handler chose, so a 404 produced by a
/// misrouted URL, a 503 from a missing pool, or a 500 from a resolver error can
/// never be mistaken for an ownership denial.
///
/// Rule 3 — every assertion that a row was not written reads the DATABASE, not
/// the response. A handler that returns 400 and writes the row anyway passes
/// any status-only check.
#[tokio::test]
async fn supersede_refuses_to_create_a_cross_ocean_edge() {
    let Some(base) = base_url() else {
        eprintln!("skipping supersede_refuses_to_create_a_cross_ocean_edge: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_ssedge_write_ctl", "moprobe_ssedge_write_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_ssedge_write").await;
    let app = build_app(&fx, &base, ten).await;
    let a_token = org_token(&fx.member_a_sub, &fx.org_id, "org:member");

    // A second reef in each private ocean, so both the legitimate case and the
    // "both endpoints inside B's ocean" tamper case have two endpoints to work
    // with.
    let (a_reef2, _) = seed_reef_and_coral(
        &fx.tenant,
        fx.a_graph,
        &format!("A second private reef {A_SECRET}"),
        &format!("A second private coral content {A_SECRET}"),
        "a-second-source",
    )
    .await;
    let (b_reef2, _) = seed_reef_and_coral(
        &fx.tenant,
        fx.b_graph,
        &format!("B second reef {B_MARK}"),
        &format!("B second private coral content {B_MARK}"),
        "b-second-source",
    )
    .await;

    async fn edge_count(fx: &Fx, superseded: Uuid, superseder: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM supersession_edge \
             WHERE superseded_id = $1 AND superseder_id = $2",
        )
        .bind(superseded)
        .bind(superseder)
        .fetch_one(&fx.tenant)
        .await
        .unwrap()
    }

    // ── POSITIVE CONTROL: an entirely in-scope supersede still works ─────────
    //
    // A marks her own reef `a_reef2` as superseded by her own reef `a_reef`.
    // Both endpoints are in A's own ocean, which A reaches. If this does not
    // 200, every refusal below is meaningless.
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &a_token,
            APEX,
            &format!("/v1/nodes/{a_reef2}/supersede"),
            json!({"superseded_by": fx.a_reef}),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control — a fully in-scope supersede inside A's OWN ocean must still succeed, or the \
         refusals below prove nothing but that the endpoint is broken: {text}"
    );
    assert_eq!(
        edge_count(&fx, a_reef2, fx.a_reef).await,
        1,
        "control — the in-scope supersede must actually have written a row"
    );
    // The schema half of this change, proven end-to-end through the real route
    // rather than from the migration: the new NOT NULL `graph_id` is populated,
    // and populated with the endpoints' shared ocean.
    let written_graph: Uuid = sqlx::query_scalar(
        "SELECT graph_id FROM supersession_edge WHERE superseded_id = $1 AND superseder_id = $2",
    )
    .bind(a_reef2)
    .bind(fx.a_reef)
    .fetch_one(&fx.tenant)
    .await
    .unwrap();
    assert_eq!(
        written_graph, fx.a_graph,
        "the edge's graph_id must be its endpoints' own ocean, derived server-side"
    );

    // ── ATTACK 1: the original defect — org commons reef superseded by A's
    //    OWN private reef. A reaches BOTH oceans, so reachability does not
    //    refuse this. Since settings 2.10 the reef write scope refuses it
    //    first: a plain member may read the commons but not write it. ────────
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersede", fx.org_reef),
            json!({"superseded_by": fx.a_reef}),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "member A must not be able to declare the ORG COMMONS reef superseded by her own \
         PRIVATE reef — every other org member reads that row back: {text}"
    );
    assert!(
        text.contains("node not found"),
        "the refusal must be the write-scope denial, indistinguishable from a missing reef: {text}"
    );
    assert_eq!(
        edge_count(&fx, fx.org_reef, fx.a_reef).await,
        0,
        "no cross-ocean edge may be persisted by a refused request (asserted against the \
         database, not the response)"
    );

    // ── ATTACK 1b: the same-ocean rule itself. Admin C may moderate BOTH the
    //    commons and (once on the team) the team ocean, so the write scope
    //    admits both endpoints and only the same-ocean rule stands between C
    //    and a commons edge pointing into the team ocean. ──────────────────────
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) VALUES ($1, $2, 'member', NULL)",
    )
    .bind(fx.team_id)
    .bind(fx.member_c)
    .execute(&fx.tenant)
    .await
    .unwrap();
    let (team_reef, _) = seed_reef_and_coral(
        &fx.tenant,
        fx.team_graph,
        "Team reef for the same-ocean probe",
        "Team coral for the same-ocean probe",
        "team-same-ocean-source",
    )
    .await;
    let c_token = org_token(&fx.member_c_sub, &fx.org_id, "org:admin");
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &c_token,
            APEX,
            &format!("/v1/nodes/{}/supersede", fx.org_reef),
            json!({"superseded_by": team_reef}),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "admin C must not be able to supersede a commons reef by a TEAM reef: {text}"
    );
    assert!(
        text.contains("same ocean"),
        "the refusal must be the same-ocean rule, not some incidental 400: {text}"
    );
    assert_eq!(edge_count(&fx, fx.org_reef, team_reef).await, 0);

    // ── ATTACK 2: an endpoint A cannot reach at all — B's private reef as the
    //    SUPERSEDER. This is the pure reachability case. ────────────────────
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersede", fx.a_reef),
            json!({"superseded_by": fx.b_reef}),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "member A must not be able to name member B's private reef as a superseder: {text}"
    );
    assert!(
        text.contains("superseder node not found"),
        "the refusal must be the ownership/reachability denial, and must NOT distinguish \
         'exists but unreachable' from 'does not exist' — otherwise the endpoint is an \
         existence oracle for every reef id in the org: {text}"
    );
    assert!(
        !text.contains(B_MARK),
        "the refusal must not echo any of B's content: {text}"
    );
    assert_eq!(edge_count(&fx, fx.a_reef, fx.b_reef).await, 0);

    // ── ATTACK 3: BOTH endpoints inside B's ocean. The same-ocean rule is
    //    SATISFIED here, so only reachability stands between A and writing
    //    into another member's knowledge graph. ──────────────────────────────
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &a_token,
            APEX,
            &format!("/v1/nodes/{}/supersede", fx.b_reef),
            json!({"superseded_by": b_reef2}),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "member A must not be able to write a supersession edge entirely inside member B's \
         private ocean: {text}"
    );
    assert!(
        text.contains("node not found"),
        "the refusal must be the ownership/reachability denial: {text}"
    );
    assert!(!text.contains(B_MARK), "the refusal must not echo B's content: {text}");
    assert_eq!(edge_count(&fx, fx.b_reef, b_reef2).await, 0);

    // ── ATTACK 4 (DELETE): tampering with an edge A cannot reach ─────────────
    //
    // Seed a legitimate edge inside B's ocean, then have A try to remove it.
    // The interesting assertion is not the status code — it is that the row
    // SURVIVES.
    sqlx::query(
        "INSERT INTO supersession_edge (superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'user', $3)",
    )
    .bind(fx.b_reef)
    .bind(b_reef2)
    .bind(fx.b_graph)
    .execute(&fx.tenant)
    .await
    .unwrap();
    assert_eq!(edge_count(&fx, fx.b_reef, b_reef2).await, 1, "seed B's own edge");

    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("DELETE")
                .uri(format!("/v1/nodes/{}/supersede/{}", fx.b_reef, b_reef2))
                .header("host", APEX)
                .header("authorization", format!("Bearer {a_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "member A must not be able to delete an edge inside member B's private ocean: {text}"
    );
    assert_eq!(
        edge_count(&fx, fx.b_reef, b_reef2).await,
        1,
        "B's edge must SURVIVE A's delete attempt — a 404 with the row gone would be the same \
         tampering the status code claims to have refused"
    );

    // ── ATTACK 5: the SECOND write door — /v1/sync/push ─────────────────────
    //
    // CodeRabbit, CRITICAL, on this PR: fixing the HTTP handler is not the same
    // as fixing the write. `routes::sync::push` authorizes each op's DECLARED
    // `graph_id`, not its endpoints, so A can stamp a supersession op with her
    // OWN reachable ocean — the push is legitimately accepted — while naming the
    // org commons reef and her private reef as the two endpoints. The apply then
    // used to derive `graph_id` from the superseded endpoint and write exactly
    // the row `POST /supersede` had just been taught to refuse.
    //
    // Asserted at the ROW, not the response: the push itself is expected to
    // return 200 (it names a graph A really can reach — that is precisely why
    // this door is not covered by the existing sync-push probe, which asserts a
    // 403). The security property is that the edge does not exist afterwards.
    let push_body = json!({
        "ops": [{
            "seq": 1,
            "op_id": Uuid::new_v4(),
            "entity": "supersession_edge",
            "entity_id": format!("{}:{}", fx.a_reef, fx.org_reef),
            "op_type": "upsert",
            "payload": {
                "superseder_id": fx.a_reef.to_string(),
                "superseded_id": fx.org_reef.to_string(),
                "graph_id": fx.a_graph.to_string(),
                "source": "user",
            },
            "version": 0,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "a-device",
            "graph_id": fx.a_graph,
        }]
    });
    let resp = app
        .clone()
        .oneshot(req_json("POST", &a_token, APEX, "/v1/sync/push", push_body))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control — the push must be ACCEPTED (A really can reach the ocean she stamped on the \
         op); if it 403s, the assertion below would pass without the apply-side guard existing \
         at all: {text}"
    );
    assert_eq!(
        edge_count(&fx, fx.org_reef, fx.a_reef).await,
        0,
        "member A must not be able to create the cross-ocean edge through /v1/sync/push either \
         — the HTTP handler is not the only write door into supersession_edge"
    );

    // ── ATTACK 6: the sync door again, in the DELETE direction ──────────────
    //
    // CodeRabbit, MAJOR, on this PR: the mirror image of ATTACK 5. The apply's
    // supersession DELETE matched on the `(superseder_id, superseded_id)` pair
    // alone, so an op stamped with an ocean A really can reach could remove an
    // edge from a DIFFERENT ocean whose two reef ids she happens to know —
    // exactly the tampering ATTACK 4 closes over HTTP, arriving through the
    // transport that never runs that handler.
    //
    // B's edge (seeded in ATTACK 4 and still present, as that attack asserted)
    // is the target. Again the push is expected to SUCCEED and the property is
    // asserted at the row.
    assert_eq!(
        edge_count(&fx, fx.b_reef, b_reef2).await,
        1,
        "control — B's edge must still be present for ATTACK 6 to have anything to tamper with"
    );
    let push_body = json!({
        "ops": [{
            "seq": 1,
            "op_id": Uuid::new_v4(),
            "entity": "supersession_edge",
            "entity_id": format!("{}:{}", b_reef2, fx.b_reef),
            "op_type": "delete",
            "payload": {
                "superseder_id": b_reef2.to_string(),
                "superseded_id": fx.b_reef.to_string(),
                "graph_id": fx.a_graph.to_string(),
                "source": "user",
            },
            "version": 0,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "a-device",
            "graph_id": fx.a_graph,
        }]
    });
    let resp = app
        .clone()
        .oneshot(req_json("POST", &a_token, APEX, "/v1/sync/push", push_body))
        .await
        .unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "control — the delete push must be ACCEPTED (A really can reach the ocean she stamped); \
         a 403 here would make the survival assertion below meaningless: {text}"
    );
    assert_eq!(
        edge_count(&fx, fx.b_reef, b_reef2).await,
        1,
        "B's edge must SURVIVE a sync delete op stamped with A's own ocean — the delete must \
         reach exactly as far as the authorization did, and no further"
    );

    // ── POSITIVE CONTROL for DELETE: A can still delete her OWN edge ─────────
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("DELETE")
                .uri(format!("/v1/nodes/{a_reef2}/supersede/{}", fx.a_reef))
                .header("host", APEX)
                .header("authorization", format!("Bearer {a_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NO_CONTENT,
        "control — A must still be able to delete an edge in her own ocean, or the tamper \
         refusal above is indistinguishable from DELETE being broken outright"
    );
    assert_eq!(
        edge_count(&fx, a_reef2, fx.a_reef).await,
        0,
        "control — A's own edge must actually be gone"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id() {
    let Some(base) = base_url() else {
        eprintln!("skipping ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_cite_b_ctl", "moprobe_cite_b_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_cite_b").await;
    // See `build_app_inline_content`'s doc: the citation probes are the one
    // place in this file that needs retrieval to return something.
    let app = build_app_inline_content(&fx, &base, ten).await;

    let mcp_token = mcp_org_token(&fx.member_b_sub, &fx.org_id, "org:member");
    select_org(&app, &mcp_token, &fx.org_id).await;
    assert_mixed_scope_references_stay_inside_the_reachable_set(
        &app,
        &org_token(&fx.member_b_sub, &fx.org_id, "org:member"),
        &mcp_token,
        "member B",
        &fx,
    )
    .await;

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

/// Admin twin (plan §6 item 9, "identical denials"). Note admin C reaches
/// FEWER oceans than B here — she has no member ocean of her own and is not in
/// the team — so permitted evidence is checked against HER independently
/// resolved roster, rather than assuming B's private ocean is shared.
#[tokio::test]
async fn ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id_for_org_admin_c() {
    let Some(base) = base_url() else {
        eprintln!("skipping ask_ocean_mixed_scope_never_cites_an_unreachable_members_coral_id_for_org_admin_c: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_cite_c_ctl", "moprobe_cite_c_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_cite_c").await;
    let app = build_app_inline_content(&fx, &base, ten).await;

    let mcp_token = mcp_org_token(&fx.member_c_sub, &fx.org_id, "org:admin");
    select_org(&app, &mcp_token, &fx.org_id).await;
    assert_mixed_scope_references_stay_inside_the_reachable_set(
        &app,
        &org_token(&fx.member_c_sub, &fx.org_id, "org:admin"),
        &mcp_token,
        "org admin C",
        &fx,
    )
    .await;

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn mcp_read_tools_deny_member_b_and_resolve_default_to_own() {
    let Some(base) = base_url() else {
        eprintln!("skipping mcp_read_tools_deny_member_b_and_resolve_default_to_own: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_mcpb_ctl", "moprobe_mcpb_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_mcpb").await;

    // The original retrieval coral remains unchanged. An independent note
    // supplies literal authority for B's complete title before canonical binding.
    let title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE graph_id=$1 AND node_id=$2")
        .bind(fx.b_graph).bind(fx.b_reef).fetch_one(&fx.tenant).await.unwrap();
    let raw = format!("My own reef is {title}.");
    let support_atom = Uuid::new_v4();
    let support_source = "mcp-b-own-title-support";
    let mut tx = fx.tenant.begin().await.unwrap();
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,timestamp) VALUES($1,$2,'note',$3,$4,$5,$6,now())")
        .bind(support_atom).bind(fx.b_graph).bind(support_source)
        .bind(format!("{}/{support_atom}",fx.b_graph)).bind(&raw).bind(fx.member_b)
        .execute(&mut *tx).await.unwrap();
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:mcp-own-title',$2,'permitted') RETURNING source_item_id")
        .bind(fx.b_graph).bind(support_source).fetch_one(&mut *tx).await.unwrap();
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,representation,ingested_by) VALUES($1,$2,$3,'note','fixture:mcp-own-title',$4,encode(sha256(convert_to($4,'UTF8')),'hex'),'normalized_text',$5) RETURNING version_id")
        .bind(fx.b_graph).bind(item).bind(support_source).bind(&raw).bind(fx.member_b)
        .fetch_one(&mut *tx).await.unwrap();
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE graph_id=$1 AND source_item_id=$3")
        .bind(fx.b_graph).bind(version).bind(item).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
        .bind(fx.b_graph).bind(version).bind(support_atom).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(fx.b_reef).bind(support_atom).execute(&mut *tx).await.unwrap();
    let consistent: bool = sqlx::query_scalar("SELECT a.ingested_by=$4 AND v.ingested_by=$4 AND a.source_id=v.source_id AND v.source_id=s.source_id AND a.content_inline=$5 AND v.raw_inline=$5 AND v.content_sha256=encode(sha256(convert_to(v.raw_inline,'UTF8')),'hex') AND a.source_evidence_bound AND s.access_state='permitted' AND s.current_version_id=v.version_id FROM source_version_chunks c JOIN atoms a ON a.atom_id=c.atom_id AND a.graph_id=c.graph_id JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE c.graph_id=$1 AND c.atom_id=$2 AND c.source_version_id=$3")
        .bind(fx.b_graph).bind(support_atom).bind(version).bind(fx.member_b).bind(&raw)
        .fetch_one(&mut *tx).await.unwrap();
    assert!(consistent, "B's independent permitted note must retain identical owned source/version/chunk bytes and hash");
    tx.commit().await.unwrap();

    let mut scoped = fx.tenant.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE coralswarm_tenant_app").execute(&mut *scoped).await.unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)")
        .bind(fx.org_account.to_string()).bind(fx.member_b.to_string())
        .execute(&mut *scoped).await.unwrap();
    let supported: (Option<String>, i64, i64) = sqlx::query_as("SELECT library_node_title($1,$2),(SELECT count(*) FROM atoms WHERE graph_id=$1),(SELECT count(*) FROM node_atoms l WHERE l.node_id=$2 AND library_atom_permitted($1,l.atom_id))")
        .bind(fx.b_graph).bind(fx.b_reef).fetch_one(&mut *scoped).await.unwrap();
    assert_eq!(supported, (Some(title),2,2), "B's scoped reef must have its exact literal title and both genuine permitted corals");
    scoped.commit().await.unwrap();

    let app = build_app(&fx, &base, ten).await;
    let mcp_token = mcp_org_token(&fx.member_b_sub, &fx.org_id, "org:member");
    select_org(&app, &mcp_token, &fx.org_id).await;

    assert_mcp_deny_matrix(&app, &mcp_token, "member B", &fx).await;

    // MCP default-ocean resolution: `list_reefs` with NO `ocean_id` must
    // resolve to B's OWN primary ocean (never A's, never org's).
    let r = mcp_call(&app, &mcp_token, "list_reefs", json!({})).await;
    let text = r.to_string();
    assert!(text.contains(B_MARK), "B's default-resolved list_reefs must contain her own reef: {r}");
    assert!(!text.contains(A_SECRET), "B's default-resolved list_reefs must never contain A's content: {r}");

    // add_context naming A's ocean id: denied, nothing written.
    let r = mcp_call(
        &app,
        &mcp_token,
        "add_context",
        json!({"ocean_id": fx.a_graph, "content": "B trying to write into A's ocean"}),
    )
    .await;
    // Pin the exact denial reason, not just "some error": B has an explicit
    // MCP write grant on this account (seeded in `setup`), so a regression
    // back to a scope-only denial (`insufficient_scope`, which would never
    // even reach the ocean-ownership check this test exists to prove) must
    // fail loudly here rather than silently keep passing for the wrong
    // reason.
    assert_denied_not_found(&r, "member B", "add_context(A)");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(fx.a_graph)
        .fetch_one(&fx.tenant)
        .await
        .unwrap();
    assert_eq!(count, 1, "A's ocean must still contain exactly its own one seeded coral, nothing from B");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn mcp_read_tools_deny_org_admin_c() {
    let Some(base) = base_url() else {
        eprintln!("skipping mcp_read_tools_deny_org_admin_c: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_mcpc_ctl", "moprobe_mcpc_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_mcpc").await;
    let app = build_app(&fx, &base, ten).await;
    let mcp_token = mcp_org_token(&fx.member_c_sub, &fx.org_id, "org:admin");
    select_org(&app, &mcp_token, &fx.org_id).await;

    assert_mcp_deny_matrix(&app, &mcp_token, "org admin C", &fx).await;

    let r = mcp_call(
        &app,
        &mcp_token,
        "add_context",
        json!({"ocean_id": fx.a_graph, "content": "C (admin) trying to write into A's ocean"}),
    )
    .await;
    // Pin the exact denial reason (C also has an explicit MCP write grant —
    // see `setup`), mirroring B's assertion: a regression to a scope-only
    // denial would never reach the ocean-ownership check this proves.
    assert_denied_not_found(&r, "org admin C", "add_context(A)");
    // Mirror B's "nothing was written" check: a future write path that
    // errors AFTER inserting would be caught for B and missed here without
    // this (CodeRabbit #343).
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(fx.a_graph)
        .fetch_one(&fx.tenant)
        .await
        .unwrap();
    assert_eq!(count, 1, "A's ocean must still contain exactly its own one seeded coral, nothing from admin C");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 4/17. Promotion: admin is not exempt (member/member and target-refusal
// directions are already exhaustively covered by member_ocean_promotion_pg.rs
// — this adds the one case that file, written before C existed in any
// fixture, could not: an ORG-ADMIN attempting to promote another member's
// private reef).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn promotion_admin_is_not_exempt() {
    let Some(base) = base_url() else {
        eprintln!("skipping promotion_admin_is_not_exempt: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_promo_ctl", "moprobe_promo_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_promo").await;
    let app = build_app(&fx, &base, ten).await;
    let c_token = org_token(&fx.member_c_sub, &fx.org_id, "org:admin");

    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &c_token,
            APEX,
            &format!("/v1/nodes/{}/promote", fx.a_reef),
            json!({"target_graph_id": fx.org_graph}),
        ))
        .await
        .unwrap();
    // Was 404 before fix/promotion-same-db-only: gate 1 refused admin C at the
    // routing step, the request fell through to the deleted cross-DB service,
    // and that 404'd only because A's tenant-only reef id did not resolve on
    // the consumer plane. Now the routing fork refuses it directly. The
    // property under test — org:admin is NOT exempt from gate 1, and no reef
    // content leaks — is unchanged; only the status the refusal wears.
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "admin C promoting A's reef must be refused"
    );
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET));

    let promotions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM node_promotions WHERE source_node_id = $1",
    )
    .bind(fx.a_reef)
    .fetch_one(&fx.tenant)
    .await
    .unwrap();
    assert_eq!(promotions, 0, "no promotion provenance must be recorded from a denied attempt");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// Teams: `teams.graph_id` can never FUNCTIONALLY reach a `kind='member'`
// graph, even if a row somehow pointed there — defense in depth, since (see
// the module doc's "residual gap" note) nothing at the schema level actually
// stops such a row from being inserted; only `create_team`'s own code (never
// accepting a caller-supplied graph id) keeps it from happening in practice.
// This proves the reachability predicate's "pure equality, no OR-arm"
// design (plan §2.2) makes such a row inert even if one existed.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn teams_graph_id_pointing_at_member_kind_still_denied() {
    let Some(base) = base_url() else {
        eprintln!("skipping teams_graph_id_pointing_at_member_kind_still_denied: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_teams_ctl", "moprobe_teams_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_teams").await;

    // Simulate the hypothetical misconfiguration/bug: a `teams` row whose
    // `graph_id` is A's kind='member' graph, with B added as a member of
    // that "team". Nothing in this codebase's application logic can produce
    // this today (create_team always mints its own fresh kind='team' graph)
    // — this is a direct-SQL probe of the schema/predicate layer only.
    let rogue_team_id: Uuid = sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'rogue', $3) RETURNING id",
    )
    .bind(fx.org_account)
    .bind(fx.a_graph)
    .bind(fx.member_a)
    .fetch_one(&fx.tenant)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) VALUES ($1, $2, 'member', NULL)",
    )
    .bind(rogue_team_id)
    .bind(fx.member_b)
    .execute(&fx.tenant)
    .await
    .unwrap();

    let app = build_app(&fx, &base, ten).await;
    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");

    // Even though B is now (per the rogue row) a "member" of a team whose
    // graph_id is A's member ocean, the graph's OWN kind is still 'member'
    // and its owner is still A — the reachability predicate evaluates
    // `g.kind` directly, so the team-membership OR-arm (which only ever
    // fires for `kind='team'`) never applies to this graph at all.
    let resp = app.clone().oneshot(req_get(&b_token, APEX, &format!("/v1/graphs/{}/stats", fx.a_graph))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "a rogue teams row must not grant B access to A's member ocean");
    let text = body_text(resp).await;
    assert!(!text.contains(A_SECRET));

    let resp = app.clone().oneshot(req_get(&b_token, APEX, "/v1/graphs")).await.unwrap();
    let text = body_text(resp).await;
    assert!(
        text.contains(&fx.b_graph.to_string()) && text.contains(&fx.team_graph.to_string()),
        "control: B must still see her own member ocean AND the real kind='team' ocean she \
         belongs to — proving the rogue row neither broke her list nor is being excluded by \
         a blanket failure: {text}"
    );
    assert!(!text.contains(&fx.a_graph.to_string()), "A's graph must still not appear in B's list: {text}");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. Archived ⇒ unreachable for EVERYONE, including A herself and admin C.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn member_ocean_archived_is_unreachable_including_owner_and_admin() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_ocean_archived_is_unreachable_including_owner_and_admin: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_arch_ctl", "moprobe_arch_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_arch").await;
    let app = build_app(&fx, &base, ten).await;

    // Positive control BEFORE archiving: A can reach her own LIVE ocean.
    // Without it, every assertion below would stay green if A's access were
    // broken for some unrelated reason — the test would be proving "nobody can
    // reach this ocean, ever", not "archiving is what made it unreachable".
    let a_token_live = org_token(&fx.member_a_sub, &fx.org_id, "org:member");
    let resp = app
        .clone()
        .oneshot(req_get(&a_token_live, APEX, &format!("/v1/graphs/{}/stats", fx.a_graph)))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "control: A must reach her own ocean while it is LIVE, so the denials below are \
         attributable to `archived_at` and nothing else"
    );

    // Archive A's ocean directly — the predicate this exercises
    // (`archived_at IS NULL`) shipped in PR 2; the archive/restore WORKER
    // (PR 7) is out of scope for this branch, so this stamps the column the
    // same way that worker eventually will, without depending on it.
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(fx.a_graph)
        .execute(&fx.tenant)
        .await
        .unwrap();

    for (sub, role, who) in [
        (&fx.member_a_sub, "org:member", "A (the OWNER herself)"),
        (&fx.member_b_sub, "org:member", "B"),
        (&fx.member_c_sub, "org:admin", "admin C"),
    ] {
        let token = org_token(sub, &fx.org_id, role);

        let resp = app.clone().oneshot(req_get(&token, APEX, &format!("/v1/graphs/{}/stats", fx.a_graph))).await.unwrap();
        assert!(
            resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::NOT_FOUND,
            "{who}: an archived member ocean must be unreachable even to its own owner, got {}",
            resp.status()
        );

        let resp = app.clone().oneshot(req_get(&token, APEX, &format!("/v1/atoms/{}", fx.a_atom))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{who}: archived ocean's coral must 404");

        let resp = app.clone().oneshot(req_get(&token, APEX, "/v1/graphs")).await.unwrap();
        let text = body_text(resp).await;
        assert!(!text.contains(&fx.a_graph.to_string()), "{who}: an archived ocean must not appear in list_graphs: {text}");

        let mcp_token = mcp_org_token(sub, &fx.org_id, role);
        select_org(&app, &mcp_token, &fx.org_id).await;
        let r = mcp_call(&app, &mcp_token, "get_ocean_stats", json!({"ocean_id": fx.a_graph})).await;
        assert_denied_not_found(&r, who, "MCP get_ocean_stats on an archived ocean");
    }

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn archived_link_share_remains_viewable() {
    let Some(base) = base_url() else {
        eprintln!("skipping archived_link_share_remains_viewable: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_archshare_ctl", "moprobe_archshare_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_archshare").await;
    let app = build_app(&fx, &base, ten).await;
    let b_token = org_token(&fx.member_b_sub, &fx.org_id, "org:member");

    let store = PostgresStore::new(fx.tenant.clone());
    let owner: Option<Uuid> = sqlx::query_scalar("SELECT resolve_meeting_share_account_id($1)")
        .bind(&fx.a_share_token).fetch_one(&fx.tenant).await.unwrap();
    assert_eq!(owner, Some(fx.org_account), "tenant share resolver must resolve its real owner");
    let mut tx = fx.tenant.begin().await.unwrap();
    let live = store.get_active_meeting_share_by_token_scoped(&mut tx, &fx.a_share_token)
        .await.unwrap().expect("live share must resolve at the Store boundary");
    assert_eq!(live.graph_id, fx.a_graph);
    assert_eq!(live.source_id, fx.a_meeting_source_id);
    tx.rollback().await.unwrap();

    // Whole-original cloud publication is deliberately refused before copy
    // work. Its exact replacement code is required, for live and archived rows.
    for archived in [false, true] {
        if archived {
            sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
                .bind(fx.a_graph).execute(&fx.tenant).await.unwrap();
        }
        let resp = app.clone().oneshot(req_get(&b_token, APEX,
            &format!("/v1/meetings/shares/view/{}", fx.a_share_token))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT, "cloud whole-original link must require selected consent");
        let text = body_text(resp).await;
        let denied: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(denied["code"], "selected_evidence_consent_required", "{text}");
        assert!(!text.contains(A_SECRET), "refused share must not disclose private content: {text}");
        assert_no_a_ocean_identifiers(&text, &a_ocean_identifiers(&fx).await,
            "member B", "whole-original link refusal");
    }
    let mut tx = fx.tenant.begin().await.unwrap();
    let archived = store.get_active_meeting_share_by_token_scoped(&mut tx, &fx.a_share_token)
        .await.unwrap();
    tx.rollback().await.unwrap();
    assert!(archived.is_none(), "archive must retire the actual scoped share lookup");

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ── Session-share surface: the tenant-tree twin + the archived_at gate ──────
//
// `routes::session_shares` has no authed org-tenant "view" route at all
// today — only `create_session_share`/`revoke_session_share` (both gated
// through the axum `Tenant` extractor), plus a PUBLIC viewer
// (`public_session_share`) that is hardcoded to `Tenant::legacy` (consumer
// plane only — see that function's own doc comment: "Token resolution is
// deliberately CONSUMER-plane only"). That means `resolve_session_share_
// account_id` is never actually invoked against an ORG TENANT pool by any
// live route today, so — unlike the meeting-share surface proven above —
// there is no live HTTP 500 to reproduce here; this file's own audit found
// no other coverage of the tenant-tree half of this surface either (`grep -rn
// resolve_session_share_account_id backend/tests` before this test existed
// turned up only `provisioning_and_grants_rls_pg.rs`'s CONTROL-plane-only
// privilege check).
//
// The tenant-tree twin (`migrations_tenant/0007_share_account_id_functions.
// sql`) is still added, for the same forward-looking parity reasons that
// migration's header documents: a future authed org-tenant session-share view
// route (the natural next step, mirroring `routes::shares::view_share`) would
// otherwise hit the exact function-does-not-exist 500 the meeting surface
// did. This test proves the DB-layer primitives directly against the real
// org tenant pool — the SQL bootstrap function AND the Store-layer
// archived_at gate — since no HTTP route exists yet to exercise them end to
// end.
#[tokio::test]
async fn session_share_tenant_twin_and_archived_gate_on_org_tenant_plane() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping session_share_tenant_twin_and_archived_gate_on_org_tenant_plane: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_sessshare_ctl", "moprobe_sessshare_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_sessshare").await;

    let session_id = format!("moprobe-session-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO coding_sessions (graph_id, session_id) VALUES ($1, $2)")
        .bind(fx.a_graph)
        .bind(&session_id)
        .execute(&fx.tenant)
        .await
        .unwrap();

    let live_token = format!("moprobe-sess-share-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO coding_session_shares (graph_id, session_id, token, visibility, created_by) \
         VALUES ($1, $2, $3, 'link', $4)",
    )
    .bind(fx.a_graph)
    .bind(&session_id)
    .bind(&live_token)
    .bind(fx.member_a)
    .execute(&fx.tenant)
    .await
    .unwrap();

    let revoked_token = format!("moprobe-sess-share-revoked-{}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO coding_session_shares \
            (graph_id, session_id, token, visibility, created_by, revoked_at) \
         VALUES ($1, $2, $3, 'link', $4, now())",
    )
    .bind(fx.a_graph)
    .bind(&session_id)
    .bind(&revoked_token)
    .bind(fx.member_a)
    .execute(&fx.tenant)
    .await
    .unwrap();

    // ── 1. The tenant-tree twin resolves correctly against the ORG TENANT
    //    pool: this is the exact call `routes::session_shares`'s private
    //    `resolve_share_by_token` makes whenever `tenant.pool` is Some (any
    //    future org-tenant caller) — before this migration it would 42883
    //    ("function does not exist") here, exactly like the meeting-share
    //    twin did. Active-only + bare-id-only, mirroring
    //    `webhook_share_rls_b3_pg.rs`'s control-plane proof of the meeting
    //    pair. ──────────────────────────────────────────────────────────────
    for (token, want) in [
        (live_token.as_str(), Some(fx.org_account)),
        (revoked_token.as_str(), None),
        ("moprobe-sess-share-never-existed", None),
    ] {
        let got: Option<Uuid> = sqlx::query_scalar("SELECT resolve_session_share_account_id($1)")
            .bind(token)
            .fetch_one(&fx.tenant)
            .await
            .expect(
                "resolve_session_share_account_id must exist and answer on the tenant tree \
                 (migrations_tenant/0007_share_account_id_functions.sql)",
            );
        assert_eq!(got, want, "resolve_session_share_account_id({token:?})");
    }

    // ── 2. `get_active_session_share_by_token_scoped`'s archived_at gate,
    //    exercised directly against a real tenant-pool transaction — the same
    //    Store method + predicate the meeting-share HTTP test above proves
    //    through the full route stack, applied here at the Store layer since
    //    no authed route exists yet to drive it through HTTP. ──────────────
    let store = PostgresStore::new(fx.tenant.clone());
    let mut tx = fx.tenant.begin().await.unwrap();
    let live = store
        .get_active_session_share_by_token_scoped(&mut tx, &live_token)
        .await
        .expect("scoped lookup")
        .expect("a live share for a live ocean must resolve before archive");
    assert_eq!(live.graph_id, fx.a_graph);
    assert_eq!(live.session_id, session_id);
    tx.rollback().await.ok();

    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(fx.a_graph)
        .execute(&fx.tenant)
        .await
        .unwrap();

    let mut tx = fx.tenant.begin().await.unwrap();
    let archived = store
        .get_active_session_share_by_token_scoped(&mut tx, &live_token)
        .await
        .expect("scoped lookup after archive");
    tx.rollback().await.ok();
    assert!(
        archived.is_none(),
        "a session share for a session in an ARCHIVED member ocean must be dead, even though \
         it is only reachable through the Store layer today (no authed view route exists yet)"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

// ═══════════════════════════════════════════════════════════════════════════
// P11. Plane split + the residual "kind='member' has no schema-level plane
// guard" gap, with a defense-in-depth proof that it's inert today.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn plane_split_org_claim_and_personal_token_never_cross() {
    let Some(base) = base_url() else {
        eprintln!("skipping plane_split_org_claim_and_personal_token_never_cross: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_plane_ctl", "moprobe_plane_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_plane").await;
    let app = build_app(&fx, &base, ten).await;

    // A, with her ORG token. Phase 4 cutover step 3 retired the org host, so
    // the token's `org_id` claim is now the ONLY thing selecting the plane:
    // both arms below hit the SAME apex and differ only in the token, which
    // makes this a sharper test than the host-vs-host version it replaces —
    // there is no second variable left that could be doing the work.
    // Her list must contain her member ocean, and must
    // NEVER contain her consumer-plane Primary ocean — proven here for A
    // specifically because she is the one user in this fixture who
    // genuinely has graphs on BOTH planes (a vacuous check for B/C, who have
    // no consumer-plane graphs at all).
    let a_org_token = org_token(&fx.member_a_sub, &fx.org_id, "org:member");
    let resp = app.clone().oneshot(req_get(&a_org_token, APEX, "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(text.contains(&fx.a_graph.to_string()), "A's org claim must reach her own member ocean on the apex: {text}");
    assert!(
        !text.contains(&fx.personal_graph.to_string()),
        "A's org claim must NEVER reach her consumer-plane Primary ocean: {text}"
    );

    // A, on the APEX (personal token, no org claims): the reverse — her
    // consumer Primary ocean, never her member ocean.
    let a_personal_token = personal_token(&fx.member_a_sub);
    let resp = app.clone().oneshot(req_get(&a_personal_token, APEX, "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(text.contains(&fx.personal_graph.to_string()), "A on the apex must see her consumer Primary ocean: {text}");
    assert!(
        !text.contains(&fx.a_graph.to_string()),
        "A on the apex must NEVER see her org member ocean (it lives in a different physical database entirely): {text}"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}

#[tokio::test]
async fn member_kind_row_creation_blocked_on_consumer_plane() {
    let Some(base) = base_url() else {
        eprintln!("skipping member_kind_row_creation_blocked_on_consumer_plane: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { set_shared_env(&jwks_url) };

    let (ctl, ten) = ("moprobe_memberkind_ctl", "moprobe_memberkind_ten");
    let fx = setup(&base, ctl, ten, "org_moprobe_memberkind").await;
    let app = build_app(&fx, &base, ten).await;
    let a_personal_token = personal_token(&fx.member_a_sub);

    // API level: the public route only ever accepts kind IN ('personal','org')
    // — 'member' is refused outright, on ANY plane.
    let resp = app
        .clone()
        .oneshot(req_json(
            "POST",
            &a_personal_token,
            APEX,
            "/v1/graphs",
            json!({"name": "sneaky", "kind": "member"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "kind='member' must be refused via the public API on the consumer plane");

    // DB level (documented residual gap — see module doc): nothing in the
    // shared `graphs_member_requires_owner` CHECK constraint knows which
    // PLANE it is running on; it only requires a non-null owner_user_id.
    // Simulating a hypothetical future bypass (a backfill script, a bug in
    // some other write path) by inserting directly: this INSERT is expected
    // to SUCCEED — recording the gap, not a false failure — and the
    // meaningful assertion is the one below: the row is still gated
    // owner-only by the consumer arm's SECURITY DEFINER functions'
    // defensive `kind <> 'member' OR owner_user_id = $u` clause, so no read
    // path is actually bypassable even though the row itself can exist.
    let rogue_owner = Uuid::new_v4();
    let rogue: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id) \
         VALUES ($1, 'rogue consumer-plane member row', 'member', $2) RETURNING id",
    )
    .bind(fx.personal_account)
    .bind(rogue_owner)
    .fetch_one(&fx.control)
    .await
    .expect("documented gap: no schema-level plane guard rejects this insert today");

    // A totally unrelated personal-account user must never see it (basic
    // account_id scoping — unaffected by this gap) ...
    let other_user_sub = format!("moprobe_other_{}", Uuid::new_v4());
    let other_graph: Uuid = {
        let uid: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
        )
        .bind(format!("{other_user_sub}@test.local"))
        .bind(&other_user_sub)
        .fetch_one(&fx.control)
        .await
        .unwrap();
        let acct: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(uid)
                .fetch_one(&fx.control)
                .await
                .unwrap();
        // Same reason as A's personal account in `setup`: the consumer arm of
        // `resolve_reachable_graphs` keys off `account_id IN (SELECT
        // account_id FROM account_members WHERE user_id = $1)`, and seeding
        // the account directly bypasses `ensure_personal_account`, which is
        // what would normally write this row. WITHOUT IT this user resolves
        // ZERO reachable graphs, and the "must never see the rogue row"
        // assertion below passes for "sees nothing at all" rather than for
        // "scoping works" — it would stay green with the plane guard removed
        // entirely (CodeRabbit #343).
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(acct)
        .bind(uid)
        .execute(&fx.control)
        .await
        .unwrap();
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'Other Consumer Primary Ocean', 'personal', true) RETURNING id",
        )
        .bind(acct)
        .fetch_one(&fx.control)
        .await
        .unwrap()
    };
    let other_token = personal_token(&other_user_sub);
    let resp = app.clone().oneshot(req_get(&other_token, APEX, "/v1/graphs")).await.unwrap();
    let text = body_text(resp).await;
    assert!(
        text.contains(&other_graph.to_string()),
        "control: the unrelated account must see its OWN ocean, proving the assertion below \
         tests scoping rather than an empty list: {text}"
    );
    assert!(!text.contains(&rogue.to_string()), "an unrelated account must never see the rogue row: {text}");

    // ... and even the ROGUE ROW'S OWN ACCOUNT (A's personal account) is
    // gated owner-only: A herself (not `rogue_owner`) must not see it either,
    // proving the defensive kind<>'member' predicate — not mere account
    // scoping — is what's carrying the weight here. A's own Primary ocean
    // lives in that same account, so asserting she still sees IT is what
    // makes this the `kind='member'` predicate's doing and not an empty list.
    let resp = app.clone().oneshot(req_get(&a_personal_token, APEX, "/v1/graphs")).await.unwrap();
    let text = body_text(resp).await;
    assert!(
        text.contains(&fx.personal_graph.to_string()),
        "control: A must still see her own consumer Primary ocean in the SAME account that \
         holds the rogue row — otherwise the exclusion below proves only that the list is \
         empty: {text}"
    );
    assert!(
        !text.contains(&rogue.to_string()),
        "even the rogue row's OWNING account's real owner (A, not the row's fake owner_user_id) must not see it: {text}"
    );

    teardown(&base, fx, [ctl, ten]).await;
    // SAFETY: `_env_guard` (ENV_LOCK) is held for this whole test body.
    unsafe { clear_shared_env() };
}
