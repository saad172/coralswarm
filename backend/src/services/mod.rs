//! Crate-private business-logic services shared by more than one transport
//! (HTTP routes, remote MCP tools). Extracted from `routes/*.rs` handlers so
//! the underlying pipeline is never duplicated between the two surfaces —
//! see `docs/plans/remote-mcp-v1.md` §3.3 and Phase 2 step 4-5.

/// Account deletion (settings launch 2.4): the resumable job behind
/// `POST /v1/me/delete-account` and the Clerk `user.deleted` webhook.
pub(crate) mod account_deletion;
/// Production wiring for `account_deletion`'s connector and tenant-plane
/// dependencies.
pub(crate) mod account_deletion_live;
/// Today activity cards (`GET /v1/journal/activities`): the journal's atoms
/// grouped into meeting / agent / doc / Slack / note cards, with the detail
/// panel's payload. In-process so an MCP tool can call it later; flag
/// `TODAY_ACTIVITIES_ENABLED`. Contract: `docs/implementation/
/// journal-activities-api.md`. `pub` so the live-Postgres suite
/// (`backend/tests/journal_activities_pg.rs`) can drive it directly.
pub mod journal_activities;
/// Answer feedback: one thumbs vote per (answer message, user) with optional
/// reasons and a note (`PUT|DELETE /v1/conversations/{id}/messages/{id}/
/// feedback`, flag `MESSAGE_FEEDBACK_ENABLED`), returned on the conversation
/// read. In-process so an MCP tool can call it later. Contract:
/// `docs/implementation/message-feedback-api.md`.
pub mod message_feedback;
/// In-app org deletion (control 0387): the resumable job behind
/// `POST /v1/orgs/{org_id}/deletion`, the 30-days-unpaid entry and the
/// Clerk-dashboard adoption. `pub` so the live-Postgres integration suite
/// (`backend/tests/org_destroy_pg.rs`) can drive the destroy step with the
/// local provisioner and a real `coralswarm_provisioner` login.
pub mod org_deletion;
/// Production wiring for `org_deletion` (tenant export, R2 export store).
pub mod org_deletion_live;
/// D16's assertion ladder (CS-443, org-onboarding plan §3 item 6): the
/// ordered "works here" evidence auto-add (item 9) will read, and the
/// verdict this PR's own enqueue path already records into
/// `org_join_requests.evidence`. `pub(crate)` for the same reason.
pub(crate) mod assertion_ladder;
pub(crate) mod atom_title;
/// [`PromotionBlobStore`](blob_store::PromotionBlobStore) — the shared R2
/// get/put/delete abstraction `deletion` and `retraction` both need. `pub`
/// for the same reason as its two consumers.
pub mod blob_store;
/// Connector compliance audit log (m365 enterprise train PR 4.2) — see the
/// module's own doc comment for the durability contract that distinguishes
/// it from `query_audit`'s deliberately weaker one. `pub`, not `pub(crate)`
/// (like `deletion`/`retraction` above): `services::connector_audit::Actor`
/// is a parameter type of the now-`pub` `deletion::delete_source`, and
/// `backend/tests/deletion_pg.rs` — a live-Postgres integration test in a
/// SEPARATE compilation unit — has to be able to construct one.
pub mod connector_audit;
/// D16's connector-health record (CS-446, org-onboarding plan §3 item 16):
/// the durable, ADMIN-facing fact that an org which could reach the
/// ladder's rung 1 yesterday cannot today, because the member whose Google
/// grant carried `directory.readonly` has left. Distinct from the
/// `connector_health = true` TRACING FIELD of the same name (see the
/// module's own doc comment) — that one is for an operator tailing logs,
/// which is exactly the "silently" D16's build note objects to. `pub(crate)`
/// for the same reason as its neighbours: its consumers
/// (`routes::connector_health`, `clerk_provision::remove_org_membership`)
/// live in this crate.
pub(crate) mod connector_health;
/// Home dashboard metrics (`GET /v1/home/metrics`): KPI tiles, ingested-per-day
/// matrix, oceans by volume and the source-type split, one windowed GROUP BY
/// per metric. `pub(crate)`: its consumer is `routes::home_metrics` (and a
/// future MCP tool), both in this crate.
pub(crate) mod home_metrics;
/// The `reefs_formed` admission as a list of reef ids, for the pg test that
/// compares it with `library_node_permitted` reef by reef
/// (`tests/home_metrics_pg.rs`). Test-only surface; the route and the worker
/// go through `home_metrics::compute`.
#[doc(hidden)]
pub use home_metrics::reefs_admitted_sql;
/// `GET /v1/connectors/status`: the per-connection health view behind the
/// Home dashboard's Connectors card and panel (state rule, thresholds and
/// sources in the module doc). Not `connector_health`: that one is D16's
/// admin-only, org-level degradation record. `pub(crate)`: its one consumer
/// is `routes::connector_status`.
pub(crate) mod connector_status;
/// Home dashboard "From your workspaces" (`GET /v1/home/workspace-activity`):
/// one headline row per shared ocean with new corals in the window, derived
/// from atoms and their reef links under the Library's permitted-atom, node
/// and title rules. `pub(crate)`: its consumer is `routes::workspace_activity`
/// (and a future MCP tool), both in this crate.
pub(crate) mod workspace_activity;
/// Home launchpad reads (`GET /v1/home/continue`, `/v1/home/whats-new`,
/// `/v1/home/attention`): plain index-bounded reads of the viewer's own
/// recent items, recently formed reefs under the shared admitted-title rule,
/// and the first broken connector under the connector-status rule.
/// `pub(crate)`: its consumer is `routes::home_launchpad`.
pub(crate) mod home_launchpad;
/// Correction propagation (contract §8.4 / C15) — rewrite an atom's stored
/// content, re-embed it, and invalidate any reef it fully explained, once a
/// transcript correction is applied/reverted/restored. `pub` for the same
/// reason as `deletion`/`retraction`: live Postgres integration tests drive
/// it directly.
pub mod corrections;
pub(crate) mod content_replacement;
/// Chat find: `source_results` tiles on the scoped answer and the Cmd-K
/// title-only search (`GET /v1/find/titles`). `pub` so the live-Postgres suite
/// (`backend/tests/find_results_pg.rs`) drives it directly.
pub mod find_results;
/// Deletion (§7.3) — the coral/source/subject entry points over
/// `delete_atom_cascade`. `pub` for the same reason as `retraction`: live
/// Postgres integration tests drive it directly.
pub mod deletion;
/// Retract-and-re-ingest for an upstream item EDITED after we ingested it
/// (Google Calendar event edits, 2026-10-06): compares the stored
/// `atoms.content_hash`/`timestamp`/`participants` against the re-fetched
/// item and retracts the stale coral — confined to ONE graph — so the
/// insert that follows lands the new content. `pub(crate)`: its consumers
/// (`routes::google_calendar_push`, `routes::connectors`) are in this crate.
pub(crate) mod source_reingest;
/// Auto-add (CS-447, org-onboarding plan §3 item 9, D1/D11/D12/D16): the
/// three gates — a DNS-verified domain match via the ladder, a free slot in
/// the chosen tier, and no removal tombstone — plus the off-by-default
/// toggle they hang from and the `org_auto_adds` ledger every add is
/// reported through. `pub(crate)`: its consumers (`routes::org_join_requests`,
/// `routes::org_auto_add`, `workers::org_join_request_digest`) are all in
/// this crate.
pub(crate) mod org_auto_add;
/// Org domain verification (CS-443, org-onboarding plan §3 item 37, D11):
/// normalisation, the claim/remove writes, the two proofs (DNS TXT over
/// DNS-over-HTTPS, and a signed confirmation link mailed to an address AT
/// the claimed domain), and the `verified_at IS NOT NULL` read every gate in
/// the system depends on. `pub(crate)`: its consumers —
/// `routes::org_domains`, `connectors::slack::verified_org_domain`, and
/// `services::assertion_ladder` — all live in this crate.
pub(crate) mod org_domains;
/// Phase G item 48: the 30-day clock that keeps org-install capture
/// reachable until connect-your-own emails (items 49 and 50) have been
/// sending for 30 days, then makes those paths unreachable.
pub(crate) mod org_install_grace;
/// The `org_join_requests` queue (CS-437, org-onboarding plan §3 items 4 +
/// 14): enqueue with D10's durable rejection suppression and D19's
/// personal-org refusal, the two admin reads, and the guarded decision
/// write. `pub(crate)`: its only caller today is
/// `routes::org_join_requests`, in this same crate — no separate-
/// compilation-unit test drives it directly (its own live-Postgres suite,
/// `backend/tests/org_join_requests_pg.rs`, drives it through the HTTP
/// routes instead, same as `member_ocean_lifecycle_pg.rs` drives its own
/// writers through the real webhook endpoint).
pub(crate) mod org_join_requests;
/// Agent-platform identity: the canonical platform registry, alias
/// canonicalization for caller-declared `platform` values, and User-Agent
/// detection. `pub` so the PG e2e tests can assert against `KNOWN_PLATFORMS`.
pub mod platform;
/// Query-audit disclosure log (m365 enterprise train PR 4.3). `pub` for the
/// same reason as `deletion`/`retraction`: a live Postgres integration test
/// (`backend/tests/query_audit_pg.rs`) drives [`query_audit::record`] and
/// [`query_audit::list_page`] directly.
pub mod query_audit;
/// Retraction (§4.9) — revoke + unpublish. `pub` (one of the module tree's
/// few exceptions to crate-privacy) so live Postgres integration tests can
/// drive it directly. Same-pool only: there is no cross-DB promotion or
/// retraction service (org-tenancy PR #414 deleted the one that used to
/// exist — see `scripts/check_no_cross_plane_promotion.sh`).
pub mod retraction;
/// FM-5 (B2 unit 02) — the append-only `retraction_audit_log` write path.
/// `pub` for the same reason as `deletion`/`retraction`: live Postgres
/// integration tests drive it directly.
pub mod retraction_audit;
pub mod source_publication;
pub(crate) mod supersession;
/// Meeting-transcript resolution + the org-plane RLS scope dispatch behind
/// it. `pub` for the same reason as `deletion`/`retraction`/`corrections`
/// above, and for one specific test: `transcript_scope` is the function whose
/// Org arm returned "no scope" and shipped an unscoped read (PR #850), and a
/// live-Postgres integration test in a SEPARATE compilation unit
/// (`backend/tests/transcript_scope_org_rls_pg.rs`) has to be able to call
/// that dispatch itself. Proving the PRIMITIVE is correct does not prove the
/// dispatch reaches it, and the dispatch is where the defect was.
///
/// Only the scope dispatch is widened. Everything else in the module —
/// `get_transcript`, `chunk_text`, `TranscriptError`, the chunker — stays
/// `pub(crate)` and is unreachable from outside the crate.
///
/// Widening the dispatch must not widen the ability to FORGE a scope, and one
/// shape of that is easy to ship by accident: `TranscriptScope` began as
/// `pub type TranscriptScope<'a> = Option<TranscriptScopeTarget<'a>>`, and a
/// public `Option` alias is a public constructor for its own `None` — which
/// is the input that selects the UNSCOPED store read. It is now an opaque
/// wrapper with a private field, `TranscriptScopeTarget` is `pub(crate)` and
/// reachable only through a `pub(crate)`, read-only borrow, and
/// `TranscriptScope::is_scoped` is the whole of what an external caller can
/// learn. So "`transcript_scope` is the only way to obtain a scope" now holds
/// at the crate boundary too: an external caller cannot assemble one with a
/// different acting user, and cannot assemble one with no scope at all.
pub mod transcripts;
/// The email side of "email the unmatched" (chat-per-user plan PR 6): the
/// per-recipient copy matrix, the Resend send call, and the persisted,
/// atomically-claimed cooldown that stops a real inbox being emailed twice.
/// Provider-agnostic by design (a container label + an email address is all
/// it needs) even though its only caller today is
/// `workers::slack_unmatched_email` — kept out of `connectors::slack` so a
/// future Teams/Chat discovery module can call it too without importing
/// Slack. `pub(crate)`, not `pub`: every consumer lives inside this crate
/// (the worker), unlike the `pub` services above whose tests drive them from
/// a separate `backend/tests/*.rs` compilation unit.
pub(crate) mod unmatched_recipient_email;

pub mod teams_meeting_capture;
pub(crate) mod google_meet_capture;
/// The Today gist (`GET /v1/journal/gist`, flag `TODAY_GIST_ENABLED`): its
/// compile step runs inside `workers::journal_sweep`. `pub` so the offline
/// eval (`backend/tests/journal_gist_eval.rs`) can build and parse prompts.
pub mod journal_gist;
pub mod outlook_schedule;

pub mod reef_identity;

pub mod atlas_page;

pub mod conversation_page;

pub mod members_page;

pub mod usage_facts;

pub mod page_titles;
