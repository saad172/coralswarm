#!/usr/bin/env bash
#
# check_schema_parity.sh — the drift guard between the two migration trees.
#
# The tenant baseline (backend/migrations_tenant/0001_tenant_baseline.sql) is a
# squashed snapshot of the tenant-table subset of the control tree. Once split,
# a schema change to a content table MUST land as a paired migration in BOTH
# trees, or new tenant DBs silently diverge from the control/consumer plane.
# This job proves they still agree:
#
#   1. apply the CONTROL tree to a scratch DB, drop the control-only tables
#      (leaving the shared tenant tables, with cross-plane FKs stripped by CASCADE);
#   2. apply the TENANT tree to a second scratch DB, drop the tenant-only
#      additions (tenant_meta, account_keys);
#   3. normalized `pg_dump --schema-only` of both, diff. Any difference fails.
#
# Env:
#   DATABASE_URL   base connection with CREATE DATABASE rights (points at any DB
#                  on the target server, e.g. .../postgres). REQUIRED.
#   PG_DUMP        pg_dump binary (default: pg_dump on PATH). Must match server
#                  major version. CI installs the matching postgresql-client.
#   SQLX           sqlx binary (default: sqlx on PATH, else ~/.cargo/bin/sqlx).
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

: "${DATABASE_URL:?set DATABASE_URL to a base connection with createdb rights}"
PG_DUMP="${PG_DUMP:-pg_dump}"
SQLX="${SQLX:-$(command -v sqlx || echo "$HOME/.cargo/bin/sqlx")}"

# Read-model storage, subscriptions and durable fences have identical policies/functions on both planes;
# keep them in the shared comparison (accounts FK disappears with accounts).
# Control-only tables — dropped from the control-tree DB so only the shared
# tenant tables remain to compare. Adding a NEW control-only table? Add it here
# in the same PR (the paired-migration discipline). Tenant-only additions
# (tenant_meta, account_keys) and sqlx bookkeeping are excluded from the dump.
# Library rollout proof is control-only operator metadata, scoped to its
# sealed run owner; target tenant ids are metadata, not content-plane copies.
CONTROL_TABLES="read_model_control_revisions read_model_control_bumps read_model_oauth_changes library_rollout_runs library_rollout_samples library_rollout_windows users accounts account_members account_prefs sessions auth_codes \
composio_connections \
user_prefs user_private_keys escrow_recovery_log escrow_recovery_pubkey \
escrow_shares escrow_wrapped_keys rate_limit_window calendar_prefs \
tenant_databases tenant_db_credentials tenant_provisioning_jobs org_selfserve_creations org_trials org_trial_claims user_account_prefs user_active_graph \
mcp_tenant_selection personal_orgs \
org_membership_tombstones org_revocations \
member_ocean_desired_state member_ocean_lifecycle_log \
user_access_log platform_metrics_daily platform_active_users waitlist_snapshot \
source_publications connector_installs \
model_rates credit_conversion \
credit_ledger credit_balances query_billing_state managed_credit_reservations plans grants enforcement_events \
member_allowance_periods \
stripe_subscriptions stripe_webhook_events \
usage_reconciliation_daily \
seat_tiers credit_packs grant_seat_tiers \
extra_usage_rates extra_usage_settings extra_usage_balances extra_usage_ledger \
extra_usage_periods extra_usage_reloads billing_notices \
oauth_state_jti \
org_connector_flags \
microsoft_admin_consents \
retraction_audit_log \
addin_tokens addin_pairing_codes \
addin_microsoft_links addin_microsoft_sessions \
slack_unmatched_email_notices \
chat_cold_start_notices \
slot_reservations \
github_installations \
org_join_requests \
org_join_request_digests \
org_domains \
connector_health \
teams_container_subscriptions teams_container_watchers \
graph_mail_routes \
slack_member_channels \
cold_start_notices \
org_auto_add_settings org_auto_adds \
google_calendar_sync_attempts \
microsoft_calendar_sync_attempts \
org_install_capture_grace \
teams_meeting_contexts \
capture_admission_health \
pipeline_stage_health \
account_deletions \
org_deletions \
org_deletion_policy \
org_unpaid_watch \
_sqlx_migrations"

# oauth_state_jti (migration 0173, foundation train PR 1.4): consumed-jti
# replay store for signed OAuth `state` tokens. `state` tokens are minted and
# verified entirely against the control-plane `accounts` table (see that
# migration's own header), so there is no tenant-tree twin, ever.
#
# org_connector_flags (migration 0176, Foundation train PR 1.3): a routing/
# config table keyed on a bare Clerk `clerk_org_id` (no local `accounts` FK),
# same control-only posture as `connector_installs` — see that migration's own
# header for the "no coherent per-tenant version of a pre-tenant-resolution
# lookup" argument, which applies identically here (the org gate this table
# feeds runs before any tenant pool is chosen).
#
# seat_tiers/credit_packs (migrations 0165/0166, billing self-serve launch PR
# B) are global catalog/reference data, same posture as `plans` — no
# account_id, no tenant twin needed. grant_seat_tiers (migration 0168, same
# PR) is account-anchored and RLS-covered but control-plane ONLY (its parent
# `grants` already is), same shape as `member_allowance_periods`/
# `stripe_subscriptions` above.
#
# microsoft_admin_consents (migration 0188, m365 enterprise train PR 3.2):
# same control-only posture as oauth_state_jti/org_connector_flags directly
# above — written and read exclusively by `routes::oauth::callback`'s
# admin-consent branch, which resolves `account_id` off the control-only
# `accounts` table before any tenant pool is chosen. No coherent per-tenant
# copy exists.
#
# slack_unmatched_email_notices (migration 0227/0228, chat-per-user plan PR 6,
# corrected 2026-09-20): the per-recipient email-nudge cooldown + unsubscribe
# suppression list. MOVED here from CONTROL_ONLY_RLS_TABLES (a tenant-tree
# twin, migrations_tenant/0093/0095, shipped and was deleted before ever being
# applied anywhere) once it became clear the public, unauthenticated
# `routes::email_unsubscribe` route has no per-request tenancy decision to
# pick a plane with, so this table can only ever be correct if it lives on
# the ONE plane that route can resolve without deciding anything. Same
# class as `oauth_tokens`/`slack_integrations` in spirit (send/connector
# bookkeeping, no ocean anchor) but, unlike those two, genuinely has no
# tenant-tree twin at all — see migration 0227's own "Control-only, no
# tenant-tree twin" header section for the full reasoning.
#
# chat_cold_start_notices (migration 0258): the Chat in-space connect
# notice cap. Control-only, no tenant twin — a container id and a counter,
# no ocean anchor. cold_start_notices is 0259, the Slack/Teams twin of the
# same idea; it does not share this table.
#
# retraction_audit_log (migration 0196, B2 unit 02 / FM-5, founder decision
# 2026-08-25): deliberately CONTROL-ONLY, unlike its sibling audit tables
# query_audit_log/connector_audit_log (0191/0069, 0193/0071) which ARE
# paired control+tenant. The deciding reason (see the migration's own
# header): D8's cross-ocean author view is one query instead of a tenant-DB
# fan-out, and FM-4's `not_a_member` row must be writable for an ex-member —
# on the tenant plane that write targets a database the actor just lost.
# Same posture as `member_ocean_lifecycle_log` (0092) for the same "a row
# about tenant data must survive independently of the tenant DB and the
# actor's own membership in it" reason.
#
# addin_tokens / addin_pairing_codes (migrations 0210/0211, Gmail Add-on
# pairing): control-only for the same reason oauth_state_jti/
# org_connector_flags directly above are — both are resolved entirely
# BEFORE any tenant pool exists. `addin_tokens` backs the bearer credential
# `middleware::auth::require_auth` verifies ahead of the binding gate (no
# `TenancyDecision` yet when it's read); `addin_pairing_codes` backs the
# `pair/start`+`pair/poll` handshake, both of which run on routes with no
# `Tenant` extractor at all (unauthenticated by design — see 0211's own
# header). No coherent per-tenant copy exists for either.
#
# addin_microsoft_links / addin_microsoft_sessions (migration 0264, Microsoft
# SSO for the Teams/Outlook panes): control-only for the identical reason —
# the `csms_` session credential is verified by `middleware::auth::require_auth`
# BEFORE any tenant pool exists, and `POST /v1/addin/microsoft/session` runs
# with no `Tenant` at all. The same migration adds `kind`/`ms_tenant_id`/
# `ms_object_id` to `addin_pairing_codes` (already listed) and the control-only
# SECURITY DEFINER resolver `resolve_microsoft_connected_users` (listed in
# CONTROL_FUNCTIONS below).
#
# slot_reservations (migration 0233, CS-435, org-onboarding plan §3 items
# 7/15, D9/D15): a claim in flight against an account's purchased-slot pool.
# Control-only for the same reason `grants`/`grant_seat_tiers` are (both
# already listed above) — the pool it reserves against is billing state, one
# copy per account, with no coherent per-tenant meaning. See the migration's
# own header for the full RLS/index rationale.
#
# user_active_graph (migration 0201, claim-based tenancy decision 8): the
# per-(user, account) active-ocean pointer. Control-only for the same reason
# `user_account_prefs` (0080) is, and NOT because of what it points at: it is
# keyed on (user_id, account_id), and both `users` and `accounts` are
# control-plane tables with no tenant-side twin to key a copy on. Note the
# apparent oddity this resolves — its `graph_id` names a CONTENT-plane row
# that, for an org account, lives in that org's own tenant database. That is
# exactly why the column carries NO foreign key (a cross-database FK is not
# expressible) and why the pointer is advisory rather than authoritative; see
# the migration's own header. Storing it per tenant instead would mean N
# places to look for one user's selection and no home at all for the personal
# plane.

# github_installations (migration 0236, GitHub connector PR A): the
# provider-specific installation-metadata side table (account_login,
# account_type, suspended_at) `connector_installs` deliberately does not
# carry. Control-only for the same "no RLS, resolved before any tenant scope
# exists" posture as `connector_installs` itself (0121) — see that
# migration's own header for the full "I8 routing must resolve before
# touching tenant data" argument, which applies identically here.

# mcp_tenant_selection (migration 0203, claim-based tenancy §Phase 3 PR 3.2):
# the per-(user, client_id) MCP tenant-selection row. Control-only for the
# same shape of reason as user_active_graph above — keyed on `users.id` (a
# control-plane table) plus an OAuth `client_id` string, neither of which has
# a tenant-side twin — and its `clerk_org_id` column carries no foreign key
# for the same "advisory, never authoritative" reason `org_revocations`
# (0082) already established for the identical TEXT-org-id shape: it must
# survive an org's `accounts` row being deleted and recreated. See the
# migration's own header for the full key-shape rationale (why (user_id,
# client_id) and not (user_id, account_id) like every sibling table).

# NOTE: this list is consumed by an unquoted `for fn in $CONTROL_FUNCTIONS`,
# i.e. it is WHITESPACE-SPLIT. Every signature must therefore be written with
# NO internal spaces — use the alias spellings Postgres accepts in a
# `DROP FUNCTION` signature (`timestamptz`, not `timestamp with time zone`;
# `int4`/`float8` over multi-word forms), and no space after the argument
# commas. A signature containing a space silently shatters into several
# garbage entries, every `DROP FUNCTION` no-ops, and the parity diff then
# fails on functions that were supposed to have been dropped.
#
# Control-only FUNCTIONS — a trigger attached to a control-only (or dropped)
# table goes away with the table's DROP ... CASCADE, but its standalone
# function does not, and pg_dump would then diff it against the tenant
# baseline. Same discipline as CONTROL_TABLES: register control-only functions
# here in the PR that adds them.
#
# `resolve_oauth_account_id`/`resolve_reachable_graphs`/
# `resolve_graph_reachable_by_user` (migration 0084_bootstrap_scope_functions,
# org-tenancy Phase S): control-plane-only in purpose (their Rust callers only
# ever run against the shared consumer pool), and for the latter two,
# control-plane-only by necessity (`account_members` is not a tenant-tree
# table). The migration's other two new functions
# (`resolve_graph_account_id`, `resolve_atoms_account_id`) ARE paired into
# `migrations_tenant/0003_bootstrap_scope_functions.sql` with a byte-identical
# body, so they are deliberately NOT listed here — they must keep matching.
#
# `resolve_meeting_share_account_id(text)` / `resolve_session_share_account_id
# (text)` (0086/0087) were control-tree-only from the day they landed — the
# tenant-tree-twin gap this left is what made `GET /v1/meetings/shares/
# view/{token}` 500 for every org-tenant share (function-does-not-exist on
# `tenant.pool`; see `migrations_tenant/0007_share_account_id_functions.sql`'s
# header for the full incident). Both now HAVE byte-identical tenant-tree
# twins in that migration, so — same discipline as `resolve_graph_account_id`/
# `resolve_atoms_account_id` above — they are deliberately REMOVED from this
# list: they must keep matching, not be allowed to silently diverge again.
# `resolve_meeting_share_graph_id(uuid)` / `resolve_session_share_graph_id
# (uuid)` (the revoke-path siblings) still have no tenant-tree twin and stay
# listed below; that is a separate, not-yet-fixed gap (see the share-viewer
# PR body).
# `resolve_connector_install_account_ids(text,text)` (migration
# 0121_connector_installs): reads `connector_installs`, itself already listed
# in CONTROL_TABLES above and dropped via CASCADE before this loop runs — so
# this entry is belt-and-suspenders (Postgres already tracks the LANGUAGE-sql
# function's dependency on the table it queries), not strictly load-bearing,
# but kept explicit for the same "control-only functions are declared, not
# discovered" discipline as every other entry here. See that migration's
# header for the full control-only justification (stated with evidence, not
# merely asserted — see that header's note on the 0106/team_seat_grants
# false-premise incident it was written to avoid repeating).
#
# `resolve_oauth_token_account_ids(text,text)` / `resolve_slack_integration_
# account_ids(text)` (same migration, self-healing fallback resolvers): UNLIKE
# the entry above, these two ARE load-bearing here — they read `oauth_tokens`/
# `slack_integrations`, SHARED tables that are NOT in CONTROL_TABLES and so
# stay in the parity diff. Same control-plane-only-by-necessity reasoning
# 0084's header already gives for `resolve_oauth_account_id`: their one Rust
# caller (`routes/slack.rs::resolve_slack_account_ids`' fallback path) only
# ever reads `state.db`, the shared consumer pool, never a tenant pool.
#
# `resolve_stripe_customer_account_id` (migration 0162, Phase 4a) is control-only
# by NECESSITY rather than merely by purpose: it reads `stripe_subscriptions`,
# which is in CONTROL_TABLES above and so has no tenant twin at all, meaning a
# tenant-side copy would reference a table that does not exist there. That is a
# stronger reason than several 0084/0086 resolvers above it, which read tables
# present on BOTH planes and are listed only because their callers never run
# against a tenant pool.
#
# `resolve_outlook_everything_mode_due()` (migration 0192, m365 enterprise
# train PR 3.4) is the SAME `SECURITY DEFINER` cross-account-sweep-discovery
# pattern as `resolve_google_watch_channels_due(timestamptz)` directly above —
# `workers::outlook_everything_mode`'s CONSUMER-plane tick needs it for the
# identical reason (no single account to scope a plain `SELECT` against); an
# org tenant tick already knows its one account_id and never calls it. See
# that migration's own header for the full rationale (which itself points
# back at 0181's header for the original incident).
#
# `resolve_gmail_everything_mode_due()` (migration 0197, gsuite enterprise
# train PR G2.4) is the SAME pattern again, this time for Gmail's own
# everything-mode carve-out — `workers::gmail_everything_mode`'s CONSUMER-
# plane tick needs it for the identical reason. This is the SECOND connector
# to implement this carve-out (founder decision: Outlook 3.4 shipped first,
# G2.4 consumes its precedent rather than re-deriving it — see 0197's own
# migration header).
#
# `resolve_chat_mailbox_space_owners(text)` (migration 0208, gsuite Train G4
# PR 5) is the mailbox-archetype twin of `resolve_chat_space_account(text)`
# directly below — same cross-account push-dispatch bootstrap problem, same
# Personal-plane-only-by-necessity reasoning (see that migration's header).
#
# `resolve_google_meet_sync_due()` (migration 0208, gsuite Train G3 PR 2) is
# the SAME `SECURITY DEFINER` cross-account-sweep-discovery pattern again, for
# Meet's own pull-only sweep cursor — `workers::google_meet_sweep`'s
# CONSUMER-plane tick needs it for the identical reason every sibling above
# does (no single account to scope a plain SELECT against). Unlike
# `resolve_gmail_everything_mode_due()` there is no `enabled` gate: every
# `google_meet_sync_state` row exists because a member completed a
# `google_meet` OAuth connect, with no separate opt-in toggle to filter on.
#
# `account_members_default_seat_tier()` (migration 0169, billing self-serve
# launch PR B) is control-only by the same NECESSITY as
# `resolve_stripe_customer_account_id` above: it reads `accounts`/
# `seat_tiers`, and its trigger is attached to `account_members` itself —
# none of the three exist on the tenant tree at all.
#
# `resolve_graph_account_ids(uuid[])` (migration
# 0225_resolve_graph_account_ids, `GET /v1/library` perf fix PR D): the
# batched sibling of `resolve_graph_account_id` above. Control-plane-only by
# purpose, same as `resolve_oauth_account_id`/`resolve_reachable_graphs`: its
# one Rust caller (`Store::graph_account_ids`) backs
# `routes/library.rs::query_library_consumer`'s account-grouping fan-out,
# which only ever runs on the consumer plane (the org-tenant path never
# crosses accounts). NOT paired into `migrations_tenant` for that reason —
# see the migration's own header.
#
# `accounts_sync_is_org_account()` (migration 0169, billing self-serve launch
# PR B, review-round fix) is control-only for the identical reason: its
# trigger is attached to `accounts` itself, which is in CONTROL_TABLES above
# (dropped from the control-tree scratch DB before comparison) — but a
# standalone FUNCTION is not dropped by `DROP TABLE ... CASCADE` on the table
# its trigger is attached to (only the TRIGGER object is; the function is an
# independent object), so it would otherwise survive into the parity diff
# with nothing on the tenant tree to match it against. Declared here rather
# than discovered, same discipline as every other entry in this list.
#
# `resolve_unmatched_email_notice_account_id(text)` (migration
# 0228_slack_unmatched_email_notices_unsubscribe, chat-per-user plan PR 6
# follow-up, "unsubscribe"): the SAME `resolve_meeting_share_account_id`-
# shaped bootstrap resolver 0086 established, for the public one-click
# unsubscribe route's token lookup. Control-plane-only by NECESSITY, not
# merely by purpose: its one caller (`services::unmatched_recipient_email::
# suppress_by_token`, called from the unauthenticated `routes::
# email_unsubscribe` handlers) resolves a token with no tenant context at
# all, and can only ever see the physical database it is installed in.
#
# CORRECTED (CodeRabbit round 1, #1005): an earlier version of this comment
# said an org account's `slack_unmatched_email_notices` row lives in that
# org's own tenant database and that this function therefore could not reach
# it — that was true of an EARLIER design, fixed before this table shipped:
# `slack_unmatched_email_notices` has NO tenant-tree twin at all (see
# migration 0227's own "Control-only, no tenant-tree twin" header section),
# every recipient's row — personal account or org account alike — is written
# to THIS control database unconditionally by
# `workers::slack_unmatched_email::process_job`, and this resolver therefore
# resolves an org account's token exactly as completely as a personal
# account's. There is no remaining cross-plane gap here — unlike
# `routes::shares::public_share`'s genuinely distinct limitation for
# meeting-share tokens minted in an org tenant (a real tenant-tree table),
# which this comment used to (wrongly) equate this one with.
#
# `resolve_unmatched_email_notice_account_id_by_prefill_token(text)`
# (migration 0229_slack_unmatched_email_notices_prefill_token, CodeRabbit
# round 1, #1005): the SAME bootstrap-resolver shape as the entry directly
# above, keyed on `signup_prefill_token` instead of `unsubscribe_token`, for
# the unauthenticated `routes::email_prefill` route's token lookup. Same
# control-plane-only necessity, same org-tenant open item, same reasoning —
# see that migration's own header.
#
# `resolve_open_join_request_account_ids(uuid)` (migration
# 0239_resolve_open_join_request_account_ids, CS-437 CodeRabbit follow-up):
# the SAME bare-account-id SECURITY DEFINER resolver 0084/0228 established,
# for `GET /v1/me/join-requests`. Control-plane-only by NECESSITY:
# `org_join_requests` is in CONTROL_TABLES above and has no tenant-tree
# twin. Returns account ids only; the content read still runs inside
# `begin_account_scoped` in `services::org_join_requests::list_open_for_user`.
#
# `resolve_slack_user_token_owners(text,text)` (migration
# 0281_resolve_slack_user_token_owners): the Slack member-channel index
# resolver for `member_joined_channel` / `member_left_channel`, which carry no
# account id. Control-only by NECESSITY: its only consumer
# (`connectors::slack_member_capture::apply_membership_event`) writes
# `slack_member_channels`, which is in CONTROL_TABLES above with no tenant
# twin. Returns ids and a liveness flag only. 0282 replaces the body so the
# flag also requires the owner to be a current member of the account.
# Composio toolkit verification and pending/departed connection routing
# (0311/0312/0314) read control-only composio_connections, accounts,
# tenant_databases and account_members; none has a tenant-tree twin.
# Migration 0368 is control-only offboarding bookkeeping over accounts and
# oauth_tokens; its predicate has no tenant counterpart.
# 0370 deletion helpers are control-account operations; SQL/plpgsql bodies
# retain name-based references after the control-only table is stripped.
# `tenant_provisioning_*` (control 0381): the self-serve provisioning job's
# SECURITY DEFINER writes to control-only tables (routing, credentials and the
# job table itself); no tenant twin.
# `org_deletion_*` / `org_deletions_guard_approval` (control 0387): in-app org
# deletion's job, gate, sweeps and approval over the control-only
# `org_deletions` / `org_unpaid_watch`; an org's deletion is decided on the
# control plane and destroys the tenant, so there is no tenant twin.
# L4 metering is intentionally plane-specific: tenant aggregate DTO vs a
# control-plane 42501 refusal. Its authorization/equality is covered by PG tests.
CONTROL_FUNCTIONS="read_model_oauth_change_attribution() read_model_org_usage_facts(uuid,timestamptz) read_model_control_fact_changed() tenant_provisioning_store_credential(text,bytea) tenant_provisioning_delete_credential(text) tenant_provisioning_insert_routing(text,uuid,text,text,text) tenant_provisioning_record_schema_version(text,text,interval,bigint) tenant_provisioning_activate(text,bigint) tenant_provisioning_delete_routing(text) tenant_provisioning_enqueue(text,uuid,text) tenant_provisioning_claim(text,interval,text) tenant_provisioning_set_step(text,text,interval,text,text) tenant_provisioning_defer(text,text,integer,text,interval,text) tenant_provisioning_record_create_intent(text,text,interval) tenant_provisioning_clear_create_intent(text,text,interval) tenant_provisioning_record_database(text,text,interval,text,text) tenant_provisioning_record_credential_stored(text,text,interval) tenant_provisioning_fail(text,text,text,boolean) tenant_provisioning_cancel(text) tenant_provisioning_mark_clerk_org_cleanup_done(text,timestamptz) tenant_provisioning_retry(text) tenant_provisioning_claim_clerk_org_cleanup(text,interval) tenant_provisioning_release_clerk_org_cleanup(text,text,timestamptz,integer) account_deletion_cancel(uuid) account_deletion_deletable_accounts(uuid) account_deletion_note_clerk_deleted(text) account_deletion_purge_restricted(uuid) account_deletion_purge_user_rows(uuid) account_deletion_state_for_account(uuid) account_deletion_state_for_clerk_user(text) account_deletion_stuck(integer) account_deletion_user_keyed_tables() oauth_grant_held_elsewhere(text[],text,text,boolean,uuid,uuid) library_rollout_manifest_immutable() billing_enter_account(uuid) billing_require_current_entry(uuid) billing_guard_account_row() billing_guard_seat_row() billing_guard_account_identity() billing_guard_tenant_binding() billing_receipt_debit_history() managed_credit_binding_immutable() composio_verified_toolkits() \
org_trial_already_claimed(uuid,text) org_trial_claim(uuid,uuid,text) org_trial_accounts_due() org_trial_claims_expire_cards(integer) \
org_deletion_claim(integer) org_deletion_state_for_org(text) org_deletion_completed_count() org_deletion_approval_first_n() org_deletion_may_relax_approval() org_deletion_unpaid_forget(uuid) org_deletion_unpaid_sweep(integer,integer) org_deletion_adoptable_clerk_deletions(integer,integer) org_deletion_expired_exports(integer) org_deletion_stuck(integer) org_deletion_approval_overdue(integer,integer) org_deletion_delete_routing(uuid) org_deletion_approve(uuid,text) org_deletions_guard_approval() \
resolve_composio_pending_connections(text,int4) \
resolve_composio_departed_unrevoked(text,int4) \
account_members_block_revoked() \
account_members_default_seat_tier() \
accounts_sync_is_org_account() \
resolve_oauth_account_id(text,text) \
resolve_reachable_graphs(uuid) \
resolve_graph_reachable_by_user(uuid,uuid) \
resolve_slack_integration_account_id(text) \
resolve_graph_subscription_account_id(text) \
resolve_meeting_share_graph_id(uuid) \
resolve_session_share_graph_id(uuid) \
resolve_node_account_id(uuid) \
resolve_expiring_subscriptions(timestamptz) \
resolve_watched_conversation_accounts() \
resolve_teams_watched_accounts() \
resolve_connector_install_account_ids(text,text) \
resolve_oauth_token_account_ids(text,text) \
resolve_slack_integration_account_ids(text) \
resolve_stripe_customer_account_id(text) \
extra_usage_reload_candidates(int4) \
extra_usage_stale_reloads(interval,interval,int4) \
resolve_google_watch_channels_due(timestamptz) \
resolve_google_calendar_personal_accounts(int) \
resolve_google_watch_channel_account_id(text) \
resolve_gmail_watch_owner(text) \
resolve_gmail_watch_owners(text) \
resolve_outlook_everything_mode_due() \
resolve_microsoft_calendar_personal_accounts(int) \
resolve_google_identity_by_sub(text) \
resolve_chat_space_account(text) \
resolve_gmail_everything_mode_due() \
resolve_chat_mailbox_space_owners(text) \
resolve_google_meet_sync_due() \
resolve_graph_account_ids(uuid[]) \
resolve_unmatched_email_notice_account_id(text) \
resolve_unmatched_email_notice_account_id_by_prefill_token(text) \
resolve_open_join_request_account_ids(uuid) \
resolve_microsoft_connected_users(text,text) \
resolve_slack_user_token_owners(text,text) \
retraction_audit_log_survivors_are_uuids(jsonb) \
chat_cold_start_due_accounts() \
retire_chat_cold_start_container(text) \
resolve_teams_personal_capture_due(int) \
resolve_google_meet_subscription(text)"

# Control-only RLS — the consumer-plane RLS backstop (org-tenancy PR 3.4,
# migration 0083_consumer_rls) enables Row-Level Security + a policy on these
# SHARED tenant tables, but ONLY on the control/consumer plane: tenant DBs get
# whole-database isolation instead and must never carry these policies (the
# plan is explicit that RLS is a consumer-plane-only backstop). Unlike
# CONTROL_TABLES (whole tables dropped from the comparison) these tables stay
# in the diff — only the RLS/policy DDL layered onto them is stripped before
# dumping, so the rest of each table's shape is still compared normally.
#
# 0083 itself covers 36 tables (39 content tables as of that migration minus
# the 3 deliberately uncovered: jobs, connector_identities, sync_cursors — see
# 0083's header; 0395 later dropped sync_cursors). `slack_watched_channels` (migration
# 0101_slack_watched_channels, added after 0083) makes 37. `membership_events`
# (migration 0125_membership_events, added after 0083) makes 38 — its own
# `account_isolation` policy lives in that later migration, not 0083, since
# 0083 is already merged and append-only. `reef_identifiers` (migration
# 0138_reef_identifiers, slice 0 T2) makes 39, anchored on its one `node_id`
# column the same way `conflict_dismissal` anchors on `node_a_id`.
# `reef_judgement` (migration 0139_reef_judgement, judgement-graph resolver
# slice PR1) makes 40 — anchored DIRECTLY on `graph_id` (not via a
# `node_a_id -> nodes -> graphs` join like `conflict_dismissal`/
# `reef_identifiers`), because this table deliberately drops the FK on its
# `node_a_id`/`node_b_id` endpoints (see that migration's header) and a
# node-anchored policy would hide a row under RLS the moment either endpoint
# is deleted. `mcp_tasks` (migration 0140_mcp_tasks, MCP spec 2026-07-28 PR
# 2's task-based `ask_ocean`) makes 41 — anchored DIRECTLY on `account_id`
# (the majority shape here, same as `query_history`/`queries`), since every
# row always carries one and the boundary this table protects is the
# account, not any one ocean. `llm_usage_pending` (migration
# 0149_llm_usage_pending, usage-metering PR4's reserve/finalize choke point)
# makes 42 — anchored DIRECTLY on `account_id`, same shape and same
# reasoning as `llm_usage`'s own entry here (a pending row is scoped
# identically to the real `llm_usage` row it either finalizes into or is
# swept away without becoming). `google_watch_channels` (migration 0180,
# gsuite Train G1 PR 2) — anchored DIRECTLY on `account_id`, same shape as
# `graph_subscriptions`' own entry here (bookkeeping for a push-notification
# watch, no ocean/graph anchor to speak of). This was a schema-parity debt
# left by 0180 itself (the table shipped with its `account_isolation` policy
# but was never added to this roster) — closed by gsuite Train G2 PR 1 (item
# 7 of that PR's design), not by 0180. `outlook_everything_mode` (migration
# 0192, m365 enterprise train PR 3.4) — anchored on `graph_id`, same shape as
# `microsoft_calendar_sync_state`'s own entry here; see that migration's
# header for the opt-in-vs-default-on distinction from its structural sibling.
# `google_chat_watched_spaces` (migration 0198, gsuite Train G4 PR 3) —
# anchored on `graph_id`, same explicit-WITH-CHECK shape as
# `slack_watched_channels`' own entry here (not `gmail_watched_threads`'
# USING-only gap — see that migration's header, #692's finding carried
# forward).
# `gmail_everything_mode` (migration 0197, gsuite enterprise train PR G2.4) —
# the SECOND implementation of the same opt-in everything-mode carve-out,
# anchored on `graph_id` exactly like `outlook_everything_mode` above (whose
# precedent this migration consumes rather than re-derives — see 0197's own
# header).
# `google_meet_sync_state` (migration 0204, gsuite Train G3 PR 2) and
# `google_meet_ingested_conferences` (migration 0205, same PR) — Meet's
# pull-only sweep cursor and per-conference ledger, respectively. BOTH
# anchored DIRECTLY on `account_id`, same shape as `google_watch_channels`
# above (no `graph_id` — the fan-out destination is resolved fresh per
# attendee at ingest time, never stored; see `connectors::google_meet`'s own
# module doc).
#
# (CodeRabbit round-1 fix on the G2.1 PR: the per-migration running count
# this comment used to narrate up to ("... makes 43") had already drifted
# from the actual list below by the time this paragraph was written — the
# list itself is 48 entries as of this PR, not 43. Rather than keep hand-
# maintaining a tally that has already proven itself unreliable, this
# comment stops claiming one: verify the real count directly, e.g.
# `echo $CONTROL_ONLY_RLS_TABLES | wc -w` after sourcing this file, or count
# the roster below.)
#
# Keep this list in lockstep with EVERY migration that ENABLEs a
# consumer-plane `account_isolation`-shaped policy (not just 0083) — for a
# table that exists on BOTH the control and tenant trees. It is deliberately
# a SUBSET of backend/tests/consumer_rls_pg.rs's `RLS_TABLES`, not an
# identical set: that test's roster additionally covers nine tables
# (`credit_balances`, `credit_ledger`, `enforcement_events`,
# `grant_seat_tiers`, `grants`, `member_allowance_periods`,
# `source_publications`, `stripe_subscriptions`, `stripe_webhook_events`)
# that are control-plane-ONLY — no tenant-tree twin at all — and are
# therefore already listed in `CONTROL_TABLES` above (whole-table dropped
# before the diff runs), so adding them here too would be redundant, not
# merely harmless: this roster's job is stripping RLS/policy DDL from a
# table that STAYS in the diff, which only makes sense for a table that
# exists on both trees. `consumer_rls_pg.rs`'s test, by contrast, verifies
# real RLS enforcement against the live control-plane DB regardless of
# whether a tenant-tree twin exists at all — so its roster rightly includes
# those nine. When you add a table to ONE list, ask which of these two
# questions it answers: "does this table's shape need comparing across both
# trees, with its RLS DDL stripped first?" (→ here) vs "does this table
# enforce `account_isolation` on the control plane at all?" (→
# `RLS_TABLES`, always; → here too, ONLY if it also exists on the tenant
# tree).
#
# `github_watched_repos` (control migration 0235, paired tenant 0095, GitHub
# connector PR A) joins this roster same as `slack_watched_channels` did:
# anchored on `graph_id`, NULL-friendly `WITH CHECK` for its own pending-row
# (not-yet-routed) state — see that migration's own header.
#
# `github_code_groups` (control migration 0237, paired tenant 0096, GitHub
# connector PR B part 2, the summary tier) joins this roster too: anchored
# on `graph_id`, but WITHOUT the NULL-friendly branch `github_watched_repos`
# needs — this table's `graph_id` is NOT NULL (a group is only ever detected
# for an already-routed watch row) — see that migration's own header.
CONTROL_ONLY_RLS_TABLES="library_listing_state library_listing_changes library_listing_generations library_listing_source_heads library_listing_staged_sources library_listing_outcomes library_listing_staged_outcomes library_node_evidence library_node_item_evidence library_evidence_changes library_evidence_state graph_workspace_counts managed_budget_periods managed_attempt_receipts managed_receipt_recovery_queue managed_embedding_inputs managed_enrichment_inputs standing_floor_overrides source_items source_version_chunks source_versions semantic_atoms semantic_atom_passages node_semantic_atoms semantic_atom_windows source_retraction_outbox assertion_disagreements corpus_enrichment_runs corpus_enrichment_units corpus_capture_admissions answer_support_receipts source_version_observations temporal_evidence_records fact_supersession_edges semantic_atom_embeddings answer_temporal_receipts composio_capture_scopes \
activity_events atoms coding_session_shares coding_sessions \
conflict_dismissal conversation_messages conversation_oceans conversation_surfaced_edges conversations \
evaluation_ledger extraction_cache \
gmail_everything_mode \
gmail_hidden_messages \
gmail_watched_threads \
google_chat_watched_spaces \
google_meet_ingested_conferences \
google_meet_sync_state \
google_watch_channels \
graph_subscriptions graphs journal_gists journal_rollups llm_usage llm_usage_pending mcp_tasks mcp_tool_calls meeting_shares meeting_transcripts \
membership_events \
message_feedback \
microsoft_calendar_sync_state \
non_supersession_pair node_atoms node_merges node_promotions nodes oauth_tokens op_log \
outlook_everything_mode \
outlook_watched_conversations outlook_workflows projects queries query_history \
reef_identifiers reef_judgement relationship_edge slack_huddle_captures slack_integrations slack_watched_channels \
seat_grant_sources supersession_edge team_keys \
team_member_key_envelopes team_members team_seat_grants teams \
teams_watched_resources transcript_corrections \
connector_audit_log query_audit_log \
github_watched_repos \
github_code_groups"

# Tenant-only RLS — the MIRROR-IMAGE asymmetry of CONTROL_ONLY_RLS_TABLES.
# `migrations_tenant/0009_tenant_rls` enables RLS + a `member_ocean_isolation`
# policy on these 31 shared tables, but ONLY on the tenant plane, because the
# boundary it enforces (member ocean: `graphs.kind='member'` is reachable only
# by `owner_user_id`) does not exist on the consumer plane — there, one physical
# DB holds every account and the boundary is `account_id`, which 0083 covers.
# The two rosters are deliberately DIFFERENT, not copies: e.g. `conversations`
# and `teams` are anchored on `account_id` by 0083 and on `graph_id` by 0009,
# and `queries`/`graph_subscriptions`/`oauth_tokens`/`slack_*` are covered by
# 0083 but have no ocean anchor at all so 0009 leaves them out. `extraction_cache`
# (migration 0121/tenant 0030, content-hash extraction dedup) is the same class,
# for the opposite reason: it is DELIBERATELY DB-wide on the tenant plane (the
# whole point of the cross-ocean fan-out dedup it exists for), so it carries no
# tenant-plane RLS at all — see that migration's header.
#
# Same treatment as CONTROL_ONLY_RLS_TABLES: the tables stay in the diff, only
# the RLS/policy DDL layered onto them is stripped, so the rest of each table's
# shape is still compared normally.
#
# 0009 itself covers 31 tables. `slack_watched_channels` (tenant migration
# 0013_slack_watched_channels, paired with control's 0101) makes 32 — its own
# `member_ocean_isolation` policy lives in that later migration, not 0009
# (already merged, append-only; see the one documented `ALLOWED_EDITS`
# exception in `scripts/check_migrations.sh`, which is unrelated to this
# table). Unlike `slack_integrations`/`slack_huddle_captures` (deliberately
# left OFF this roster — see `tenant_rls_pg.rs`'s `DELIBERATELY_UNCOVERED`),
# `slack_watched_channels` DOES belong here: it is meant to work on the org
# plane, and `graph_id` is exactly the per-member/per-team ocean boundary this
# roster protects. `membership_events` (tenant migration
# 0032_membership_events, paired with control's 0125, added after 0009 for
# the same reason `team_seat_grants`/0017 was) belongs here too, anchored
# identically to `team_members`/`team_seat_grants`: `team_id -> teams.graph_id
# -> tenant_rls_visible_graph_ids()`. `reef_identifiers` (tenant migration
# 0045_reef_identifiers, paired with control's 0138, slice 0 T2) belongs here
# too, anchored on its one `node_id` column via `tenant_rls_visible_graph_ids()`.
# `reef_judgement` (tenant migration 0046_reef_judgement, paired with
# control's 0139, judgement-graph resolver slice PR1) belongs here too,
# anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()` — see
# that migration's (and its control twin's) header for why it does not
# follow the node-endpoint-anchor shape most other tables here use.
# `mcp_tasks` (tenant migration 0047_mcp_tasks, paired with control's 0140,
# MCP spec 2026-07-28 PR 2's task-based `ask_ocean`) belongs here too,
# anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()` — same
# shape as `reef_judgement`/`query_history`, see that migration's header.
# `llm_usage_pending` (tenant migration 0055_llm_usage_pending, paired with
# control's 0149, usage-metering PR4's reserve/finalize choke point) belongs
# here too, anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()`
# — same predicate as `llm_usage`'s own tenant-plane policy, since a pending
# row is scoped identically to the real row it either finalizes into or is
# swept away without becoming. `outlook_everything_mode` (tenant migration
# 0070, paired with control's 0192, m365 enterprise train PR 3.4) belongs
# here too, anchored on `graph_id` via `tenant_rls_visible_graph_ids()` —
# same shape as `microsoft_calendar_sync_state`'s own entry (0067) directly
# above. `google_chat_watched_spaces` (tenant migration 0073, paired with
# control's 0198, gsuite Train G4 PR 3) belongs here too, anchored on
# `graph_id` via `tenant_rls_visible_graph_ids()` — with an explicit
# `WITH CHECK` from the start (not `gmail_watched_threads`'s USING-only
# gap), see that migration's header.
# `gmail_everything_mode` (tenant migration 0074, paired with
# control's 0197, gsuite enterprise train PR G2.4) belongs here too, anchored
# on `graph_id` via `tenant_rls_visible_graph_ids()` — the SECOND
# implementation of `outlook_everything_mode`'s own carve-out, same shape.
#
# Keep in lockstep with EVERY migration that ENABLEs a tenant-plane
# `member_ocean_isolation`-shaped policy (not just 0009's own §6) AND with
# backend/tests/tenant_rls_pg.rs's RLS_TABLES.
TENANT_ONLY_RLS_TABLES="library_listing_state library_listing_changes library_listing_generations library_listing_source_heads library_listing_staged_sources library_listing_outcomes library_listing_staged_outcomes library_node_evidence library_node_item_evidence library_evidence_changes library_evidence_state graph_workspace_counts managed_budget_periods managed_attempt_receipts managed_receipt_recovery_queue managed_embedding_inputs managed_enrichment_inputs standing_floor_overrides source_items source_version_chunks source_versions semantic_atoms semantic_atom_passages node_semantic_atoms semantic_atom_windows source_retraction_outbox assertion_disagreements corpus_enrichment_runs corpus_enrichment_units composio_capture_scopes corpus_capture_admissions answer_support_receipts source_version_observations temporal_evidence_records fact_supersession_edges semantic_atom_embeddings answer_temporal_receipts \
activity_events atoms coding_session_shares coding_sessions \
conflict_dismissal conversation_messages conversation_oceans conversation_surfaced_edges conversations \
evaluation_ledger gmail_watched_threads google_chat_watched_spaces graphs journal_gists journal_rollups llm_usage llm_usage_pending mcp_tasks mcp_tool_calls meeting_shares meeting_transcripts \
membership_events \
message_feedback \
microsoft_calendar_sync_state \
node_atoms \
node_merges node_promotions nodes non_supersession_pair op_log \
gmail_everything_mode gmail_hidden_messages \
outlook_everything_mode \
outlook_watched_conversations outlook_workflows projects query_history \
reef_identifiers reef_judgement relationship_edge seat_grant_sources slack_watched_channels supersession_edge team_keys \
team_member_key_envelopes team_members team_seat_grants \
teams teams_watched_resources transcript_corrections \
connector_audit_log query_audit_log \
github_watched_repos \
github_code_groups"

# Tenant-only FUNCTIONS — the counterpart of CONTROL_FUNCTIONS, for the
# opposite direction. These four exist ONLY in the tenant tree: the first three
# come from 0009 and read the tenant-plane session GUCs (`app.user_id`,
# `app.tenant_actor`) that no control-plane policy consults, with
# `tenant_rls_visible_graph_ids()` the SECURITY DEFINER member-ocean predicate
# every 0009 policy resolves to. `tenant_rls_is_org_admin()`
# (0032_membership_events) is the fourth, and tenant-only for the same reason:
# it reads `app.is_org_admin`, the governance grant that lets a verified org
# admin see `teams`/`team_members`/`membership_events` for a team they are not
# a member of. That GUC has NO control-plane reader and deliberately never
# will — see 0125_membership_events's "No org-admin arm here, and why".
#
# Dropped AFTER the policies above (the policies depend on the function), so no
# CASCADE is needed and a dependency error would be a real signal rather than
# something silently swallowed.
#
# Same whitespace-splitting hazard as CONTROL_FUNCTIONS: NO internal spaces in
# a signature.
# `tenant_rls_visible_graph_windows()` (tenant migration
# `0064_team_seat_grants_history_from`, paired with control's
# `0184_team_seat_grants_history_from`, m365 enterprise train PR 4.1) makes
# eight. Tenant-only for the same reason as the rest of this list: it reads
# `tenant_rls_is_system()`/`tenant_rls_actor_user_id()` (tenant-only GUC
# readers) and `team_seat_grants.history_from`, which the control tree has no
# retrieval path to window against (control's own `team_seat_grants` carries
# the column for schema-parity only — see that migration's header). Dropped
# here, same as the others, so the tenant-only `atoms` SELECT policy that
# resolves through it does not leave a dangling dependency for the DROP
# FUNCTION loop below to trip on.
TENANT_FUNCTIONS="read_model_org_usage_facts(uuid,timestamptz) tenant_rls_visible_graph_ids() \
tenant_rls_visible_graph_windows() \
tenant_rls_actor_user_id() \
tenant_rls_is_system() \
tenant_rls_is_org_admin() \
tenant_rls_bootstrap_graph_id() \
tenant_rls_bootstrap_team_ids() \
tenant_rls_bootstrap_team_key_ids()"

# Functions that exist on BOTH trees under the same signature but whose BODY
# is plane-specific by design, so they are dropped from both scratch DBs
# before the diff (everything else about the paired migration still
# compares). Connected-memory packages, 2026-10-12/13:
#   * `library_answer_permitted_before_temporal(uuid)` — the 0305/0140 answer
#     gate, renamed by 0309/0144; control resolves reachability through
#     `resolve_graph_reachable_by_user`, tenant through
#     `resolve_tenant_graph_reachable_by_user` with the `app.account_id` GUC.
#     `library_answer_permitted(uuid)` wraps it identically on both planes and
#     stays in the diff.
#   * the three `library_*_unattached_upload` cleanup functions (0308/0143,
#     P8 durable failed-upload cleanup) — the tenant bodies additionally fence
#     on `tenant_rls_is_system()` ("capture cleanup requires tenant service
#     scope"), the control bodies on the account GUC.
#   * `graph_workspace_counts_shareable(uuid)` (control 0353 / tenant 0184):
#     whether a caller may be shown a graph's stored workspace counts. The
#     tenant body excludes team members with a history window
#     (`tenant_rls_visible_graph_windows()`, tenant-only); control RLS is per
#     account, so the control body only checks the graph is visible.
# SQL-language bodies are not dependency-tracked, so a plain DROP suffices;
# a dependency error here would be a real signal.
PLANE_DIVERGENT_FUNCTIONS="library_evidence_shareable(uuid) library_evidence_context(uuid) library_answer_permitted_before_temporal(uuid) \
library_record_unattached_upload(uuid,uuid,uuid,text,text,text,text) \
library_stage_unattached_upload(uuid) \
library_finish_unattached_upload(uuid,boolean) \
graph_workspace_counts_shareable(uuid)"

bash scripts/prepare_ci_pg_roles.sh --localhost

# derive a scratch-DB URL by swapping the database name on the base URL
scratch_url() { echo "$DATABASE_URL" | sed -E "s#(://[^/]+/)[^?]*#\1$1#"; }
CTL_DB="parity_ctl_$$"
TNT_DB="parity_tnt_$$"
CTL_URL="$(scratch_url "$CTL_DB")"
TNT_URL="$(scratch_url "$TNT_DB")"

cleanup() {
  psql "$DATABASE_URL" -X -qc "DROP DATABASE IF EXISTS $CTL_DB;" >/dev/null 2>&1 || true
  psql "$DATABASE_URL" -X -qc "DROP DATABASE IF EXISTS $TNT_DB;" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Every psql below carries `-X -v ON_ERROR_STOP=1` (CodeRabbit MAJOR: a SQL
# error could leave a scratch DB partially stripped while `set -e` still saw
# success, so the diff ran against mixed state). Both flags, for two different
# reasons, and the split is worth recording because the obvious one is not the
# one that actually bites:
#
#   `-X` is the load-bearing flag. psql reads ~/.psqlrc even for `-c`, so a
#   developer's personal settings execute inside this script. Reproduced on
#   PG 17 with a psqlrc containing `\set AUTOCOMMIT off`:
#
#       psql -qc "DROP TABLE keep;"   -> exit 0, and `keep` STILL EXISTS
#
#   That is precisely the reported failure mode — a silent partial strip that
#   `set -e` cannot see, because psql genuinely succeeded. The same psqlrc with
#   `-X` drops the table. Output-formatting settings (\pset, \timing) would
#   corrupt this script's parsing the same way.
#
#   `ON_ERROR_STOP=1` is defence in depth, not the fix. Measured: a SINGLE
#   `-c "stmt"` that errors already exits non-zero, so `set -e` does catch
#   today's invocations. It becomes load-bearing the moment anyone puts two
#   statements in one `-c`, switches to a heredoc, or adds `-f` — all of which
#   run without it by default. Cheap insurance against the next edit.
echo "parity: creating scratch DBs"
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -qc "DROP DATABASE IF EXISTS $CTL_DB;" >/dev/null
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -qc "DROP DATABASE IF EXISTS $TNT_DB;" >/dev/null
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -qc "CREATE DATABASE $CTL_DB;" >/dev/null
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -qc "CREATE DATABASE $TNT_DB;" >/dev/null

echo "parity: applying control tree -> $CTL_DB"
DATABASE_URL="$CTL_URL" "$SQLX" migrate run --source backend/migrations_control >/dev/null
# reduce control DB to the shared tenant-table set
DROP_LIST=$(echo "$CONTROL_TABLES" | tr ' ' '\n' | grep -v '^$' | paste -sd, -)
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP TABLE IF EXISTS $DROP_LIST CASCADE;" >/dev/null
for fn in $CONTROL_FUNCTIONS; do
  psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP FUNCTION IF EXISTS $fn CASCADE;" >/dev/null
done
# strip the consumer-plane-only RLS/policy DDL from the shared tables it was
# layered onto, so the rest of each table's shape still compares normally.
# Every table in CONTROL_ONLY_RLS_TABLES except `node_promotions` carries one
# bare `account_isolation` policy (dropped generically below, and it does not
# matter to this diff whether that policy also carries an explicit WITH CHECK
# — e.g. `slack_watched_channels`, see its own migration — the policy NAME is
# still `account_isolation` either way); `node_promotions` is the one
# exception (org-tenancy PR 3.4 CodeRabbit MAJOR fix — a bare USING-only
# policy is FOR ALL and reuses USING as WITH CHECK, which let one account
# write a row referencing another account's graph; node_promotions needs a
# permissive READ policy split from a strict WRITE one) and carries 4 named
# policies instead.
#
# `query_audit_log` (migration 0191, m365 enterprise train PR 4.3) and
# `connector_audit_log` (migration 0193, m365 enterprise train PR 4.2) are
# two more exceptions added to this same branch rather than each given its
# own: both are append-only by DESIGN (no UPDATE/DELETE policy exists for
# either at all — see each migration's header), so each carries only
# `account_isolation_select`/`account_isolation_insert`. Attempting to drop
# the `_update`/`_delete` names too is a harmless `IF EXISTS` no-op, so
# reusing the exact 4-name drop loop (rather than a third/fourth, narrower
# branch) costs nothing and keeps this script's branching from growing one
# arm per split table.
for t in $CONTROL_ONLY_RLS_TABLES; do
  if [ "$t" = "node_promotions" ] || [ "$t" = "query_audit_log" ] || [ "$t" = "connector_audit_log" ]; then
    for p in account_isolation_select account_isolation_insert account_isolation_update account_isolation_delete; do
      psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS $p ON $t;" >/dev/null
    done
  else
    psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS account_isolation ON $t;" >/dev/null
  fi
  psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS graph_visibility ON $t;" >/dev/null
  psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "ALTER TABLE $t DISABLE ROW LEVEL SECURITY;" >/dev/null
done

# P8 selected-evidence lifecycle (control 0308 / tenant 0143, #1236): four
# shared tables carry a policy of the SAME NAME on both planes with a
# plane-specific predicate — the `app.account_id` GUC on control, and
# `tenant_rls_is_system()` / `tenant_rls_visible_graph_ids()` on tenant (0143
# re-creates them at its tail). Neither name shape is in the generic loops
# above or below, so the tenant copies kept `tenant_rls_visible_graph_ids()`
# alive and its `DROP FUNCTION` below failed. Strip the policy DDL on BOTH
# sides, so the rest of each table's shape still compares normally.
# `purge_dependency_scope` is byte-identical on both planes and stays in the
# diff.
P8_PLANE_SCOPED_POLICIES="source_purge_events:purge_event_scope \
source_purge_reviews:purge_review_scope \
selected_evidence_publications:selected_publication_scope \
unattached_capture_uploads:unattached_capture_scope"
strip_p8_plane_scoped_policies() {
  local url="$1" entry t p
  for entry in $P8_PLANE_SCOPED_POLICIES; do
    t="${entry%%:*}"; p="${entry##*:}"
    psql "$url" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS $p ON $t;" >/dev/null
    psql "$url" -X -v ON_ERROR_STOP=1 -qc "ALTER TABLE $t DISABLE ROW LEVEL SECURITY;" >/dev/null
  done
}
strip_p8_plane_scoped_policies "$CTL_URL"

# Reef identity (control 0325 / tenant 0157) shares table/function structure,
# but these three policies intentionally use each plane's reachability helper.
# Strip only their policy DDL in both disposable projections; keep the table,
# constraints, indexes, triggers and functions in the schema comparison.
strip_reef_identity_plane_policies() {
  local url="$1" policy
  for policy in reef_identity_read reef_identity_insert reef_identity_retire; do
    psql "$url" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS $policy ON reef_identity_corrections;" >/dev/null
  done
  psql "$url" -X -v ON_ERROR_STOP=1 -qc "ALTER TABLE reef_identity_corrections DISABLE ROW LEVEL SECURITY;" >/dev/null
}
strip_reef_identity_plane_policies "$CTL_URL"

# Control-only FORCE. These five tables are SHARED, and their policies are
# byte-identical on both planes, so they stay fully in the diff. The control
# plane FORCEs RLS on them (0260/0263/0266/0269). The tenant plane must not:
# 0009_tenant_rls §7 forbids FORCE on every tenant table, and tenant
# migration 0119_tenant_no_force_rls removed the FORCE that tenant
# 0105/0107/0109/0111 had copied over by mistake. Only the FORCE flag is
# stripped here, not the policies or ENABLE.
CONTROL_ONLY_FORCE_RLS_TABLES="google_meet_captures outlook_capture_receipts \
outlook_schedule_intents teams_meeting_captures teams_meeting_installations"
for t in $CONTROL_ONLY_FORCE_RLS_TABLES; do
  psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "ALTER TABLE $t NO FORCE ROW LEVEL SECURITY;" >/dev/null
done

# Control-only Meet push routing (control 0286). Inbound Workspace Events deliveries carry only
# the subscription name, which is resolved ACROSS accounts on the control plane through
# `resolve_google_meet_subscription` (listed in CONTROL_FUNCTIONS) and its routing index
# (unique in 0286, non-unique since 0290 so one subscription can back several accounts). A
# tenant database never routes a push, so tenant 0123 deliberately carries the shared-table
# column/CHECK changes but not this routing index; drop it from the control copy before the diff.
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP INDEX IF EXISTS uq_google_meet_subscription;" >/dev/null
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP INDEX IF EXISTS idx_google_meet_subscription;" >/dev/null

# Control-only personal Teams capture (0288). `teams_meeting_captures` stays in the
# diff, but two control-only policy DDLs are normalized away first: the personal-graph
# arm of the capture-owner WITH CHECK (a personal account has no tenant database, so
# tenant 0105 deliberately stays member-only) and the owner-role scheduler SELECT policy
# the SECURITY DEFINER due-capture resolver needs under FORCE RLS. The tenant-shared
# member arm is restored byte-for-byte, so any other drift in the table still fails.
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS teams_personal_capture_scheduler ON teams_meeting_captures;" >/dev/null
# (DROP TABLE accounts ... CASCADE above may already have removed the widened policy, since
# its expression references accounts; recreate it unconditionally in the shared 0260 form.)
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS meeting_capture_owner ON teams_meeting_captures;" >/dev/null
psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "CREATE POLICY meeting_capture_owner ON teams_meeting_captures USING (account_id = nullif(current_setting('app.account_id',true),'')::uuid AND user_id = nullif(current_setting('app.user_id',true),'')::uuid) WITH CHECK (account_id = nullif(current_setting('app.account_id',true),'')::uuid AND user_id = nullif(current_setting('app.user_id',true),'')::uuid AND EXISTS (SELECT 1 FROM graphs g WHERE g.id=graph_id AND g.account_id=teams_meeting_captures.account_id AND g.owner_user_id=teams_meeting_captures.user_id AND g.kind='member' AND g.archived_at IS NULL));" >/dev/null

echo "parity: applying tenant tree -> $TNT_DB"
DATABASE_URL="$TNT_URL" "$SQLX" migrate run --source backend/migrations_tenant >/dev/null
# remove tenant-only additions so both DBs hold exactly the shared tenant tables
psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP TABLE IF EXISTS tenant_meta, account_keys CASCADE;" >/dev/null
# strip the tenant-plane-only RLS/policy DDL (migrations 0009_tenant_rls and
# 0040_team_create_bootstrap_rls) from the shared tables it was layered onto —
# the mirror image of what the control side does above. Most tables in
# TENANT_ONLY_RLS_TABLES carry one bare `member_ocean_isolation` policy; a few
# (`node_promotions` per 0009 §6(e), and the four team-lifecycle tables per
# 0040) carry the `_select`/`_insert`/`_update`/`_delete` command split.
#
# ALL FIVE names are attempted for EVERY table rather than keeping a roster of
# which tables are split: `DROP POLICY IF EXISTS` on an absent name is a no-op,
# so this costs four extra no-op statements per table and in exchange there is
# no second list to keep in sync. A table that migrates from bare to split (or
# back) needs no edit here at all.
#
# `client_min_messages=warning` for this loop only: dropping four absent names
# per bare-policy table emits ~136 `NOTICE: policy ... does not exist, skipping`
# lines, which is exactly the kind of noise that trains people to stop reading a
# script's output. WARNING and ERROR still print, and `ON_ERROR_STOP=1` still
# aborts — this silences the expected no-ops, not real failures.
for t in $TENANT_ONLY_RLS_TABLES; do
  for p in graph_visibility member_ocean_isolation \
           member_ocean_isolation_select member_ocean_isolation_insert \
           member_ocean_isolation_update member_ocean_isolation_delete; do
    PGOPTIONS='-c client_min_messages=warning' \
      psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS $p ON $t;" >/dev/null
  done
  psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "ALTER TABLE $t DISABLE ROW LEVEL SECURITY;" >/dev/null
done
# Tenant 0150 splits receipt system writes from member-visible reads; control
# 0318 uses account_isolation.
psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS member_ocean_read ON managed_attempt_receipts;" >/dev/null
psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS member_ocean_read ON managed_embedding_inputs;" >/dev/null
psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS member_ocean_read ON managed_enrichment_inputs;" >/dev/null
# standing_floor_overrides: the tenant plane reads as system or member (no
# write policy); the control plane's SELECT-only account_isolation is stripped
# by the loop above.
psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP POLICY IF EXISTS member_ocean_read ON standing_floor_overrides;" >/dev/null
strip_p8_plane_scoped_policies "$TNT_URL"
strip_reef_identity_plane_policies "$TNT_URL"
# Pure backstop: fail loudly rather than diffing to a confusing schema mismatch
# if any tenant-plane policy survived the strip. With the unconditional drop
# above, that can now only mean a table carrying such a policy is missing from
# TENANT_ONLY_RLS_TABLES, or a policy was created under a new name shape.
LEFTOVER=$(psql "$TNT_URL" -X -tAc "SELECT count(*) FROM pg_policies WHERE schemaname='public' AND policyname LIKE 'member_ocean_isolation%';")
if [ "$LEFTOVER" != "0" ]; then
  echo "parity: $LEFTOVER tenant policy/policies survived the strip — add the table to TENANT_ONLY_RLS_TABLES (or the policy's name shape to the drop loop)" >&2
  psql "$TNT_URL" -X -tAc "SELECT tablename||' -> '||policyname FROM pg_policies WHERE schemaname='public' AND policyname LIKE 'member_ocean_isolation%';" >&2
  exit 1
fi
for fn in $TENANT_FUNCTIONS; do
  psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP FUNCTION IF EXISTS $fn;" >/dev/null
done
for fn in $PLANE_DIVERGENT_FUNCTIONS; do
  psql "$CTL_URL" -X -v ON_ERROR_STOP=1 -qc "DROP FUNCTION IF EXISTS $fn;" >/dev/null
  psql "$TNT_URL" -X -v ON_ERROR_STOP=1 -qc "DROP FUNCTION IF EXISTS $fn;" >/dev/null
done

# 0391/0206 deliberately use different access-class visibility contracts:
# control has no history floor and needs explicit user reachability; tenant
# uses its RLS visibility windows. Canonicalize only the exact reviewed lines
# in this exact signature. Keep all other body/security/signature drift visible.
norm() {
  "$PG_DUMP" --schema-only --no-owner --no-privileges --no-comments -n public \
    -T '*._sqlx_migrations' -d "$1" \
    | awk '
        /^CREATE FUNCTION / {
          evidence_class = ($0 == "CREATE FUNCTION public.library_evidence_class(g uuid) RETURNS jsonb")
          legacy_headline = ($0 == "CREATE FUNCTION public.library_legacy_headline_sha256(g uuid, a_id uuid) RETURNS text")
        }
        legacy_headline && $0 == "  AND COALESCE(resolve_tenant_graph_reachable_by_user(a.graph_id,current_setting(\047app.account_id\047)::uuid,NULLIF(current_setting(\047app.user_id\047,true),\047\047)::uuid),false)" {
          $0 = "  AND COALESCE(resolve_graph_reachable_by_user(a.graph_id,NULLIF(current_setting(\047app.user_id\047,true),\047\047)::uuid),false)"
        }
        evidence_class && $0 == "  \047history_floor\047,(SELECT w.history_floor AT TIME ZONE \047UTC\047 FROM public.tenant_rls_visible_graph_windows() w WHERE w.graph_id=$1)," {
          $0 = "  \047history_floor\047,NULL::timestamp,"
        }
        evidence_class && $0 == "   THEN EXISTS(SELECT 1 FROM public.graphs x WHERE x.id=$1 AND x.account_id=NULLIF(current_setting(\047app.account_id\047,true),\047\047)::uuid)" {
          print "   THEN EXISTS(SELECT 1 FROM public.graphs x WHERE x.id=$1 AND x.account_id=NULLIF(current_setting(\047app.account_id\047,true),\047\047)::uuid"
          $0 = "    AND COALESCE(public.resolve_graph_reachable_by_user($1,NULLIF(current_setting(\047app.user_id\047,true),\047\047)::uuid),false))"
        }
        { print }
        /^\$\$;$/ { legacy_headline = 0; evidence_class = 0 }
      ' \
    | grep -vE '^\\|^SET |^SELECT pg_catalog\.set_config|^--|^$' | LC_ALL=C sort
}

CTL_OUT="$(mktemp)"; TNT_OUT="$(mktemp)"
norm "$CTL_URL" > "$CTL_OUT"
norm "$TNT_URL" > "$TNT_OUT"

if diff -u "$CTL_OUT" "$TNT_OUT"; then
  echo "schema parity: OK — control tree and tenant baseline agree on all shared tables"
  rm -f "$CTL_OUT" "$TNT_OUT"
else
  echo "schema parity: FAILED — the tenant baseline diverges from the control tree." >&2
  echo "A content-table change must land as a PAIRED migration in both trees, or" >&2
  echo "regenerate 0001_tenant_baseline.sql from the control tree. (< control, > tenant)" >&2
  rm -f "$CTL_OUT" "$TNT_OUT"
  exit 1
fi
