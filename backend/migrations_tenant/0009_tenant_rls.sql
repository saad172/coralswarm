-- Migration: 0009_tenant_rls (tenant tree)
-- Description: Tenant-plane Row-Level Security BACKSTOP — the tenant-tree
--              counterpart of `migrations_control/0083_consumer_rls.sql`, and
--              deliberately NOT a byte-identical copy of it, because the two
--              planes protect DIFFERENT boundaries. Read 0083's header first;
--              this header only records what is different here and why.
--
-- ══ 1. Why the boundary is different ═════════════════════════════════════════
--
-- On the consumer plane one physical database holds EVERY account's content,
-- so the boundary RLS must enforce there is cross-ACCOUNT
-- (`account_id = current_setting('app.account_id')::uuid`).
--
-- A tenant database holds exactly ONE org (ADR-0007 D2-impl; the registry
-- refuses to publish a pool whose `tenant_meta.org_id` disagrees with the
-- routing row it was resolved from). Cross-org isolation is therefore already
-- PHYSICAL — there is no second org's row in this database for a policy to
-- hide. Copying 0083's predicate here would produce 31 policies that are all
-- tautologically true, i.e. a backstop in name only.
--
-- The boundary that actually exists INSIDE a tenant database, and that this
-- migration enforces, is the MEMBER-OCEAN boundary
-- (docs/plans/member-private-oceans.md):
--
--   * `graphs.kind = 'member'`  — an owner-private ocean. Reachable ONLY by
--                                 `graphs.owner_user_id`.
--   * `graphs.kind = 'team'`    — reachable only by a non-departing member of
--                                 the team whose `teams.graph_id` is that ocean.
--   * everything else           — org commons / personal-shaped oceans:
--                                 reachable by any member of the org, which is
--                                 to say by anyone who can open a connection to
--                                 this database at all.
--
-- **`account_id` is NOT that boundary, and any check written at account scope
-- is a no-op across it.** Every member ocean in an org shares the org's single
-- `account_id`. Two production bugs (2026-07-25) crossed this exact line by
-- checking account scope and believing it sufficient — an account-scoped
-- `supersede` authz check, and a `sync/pull` that returned another member's
-- ocean. This migration exists so that the fourth such bug, the one nobody
-- catches in review, is refused by Postgres instead of served.
--
-- The predicate is the SAME one the application already enforces in
-- `resolve_tenant_reachable_graphs` / `resolve_tenant_graph_reachable_by_user`
-- (`migrations_tenant/0006_member_ocean_reachability_functions.sql`, called
-- from `backend/src/tenancy/reachability.rs`), with ONE deliberate omission —
-- see §5 on `archived_at`.
--
-- ══ 2. The session-identity mechanism — NEW on this plane ════════════════════
--
-- 0083's policies read `app.account_id`, a transaction-local GUC that the
-- consumer plane's app-side plumbing already sets from ~166 `begin_account_
-- scoped` call sites across ~50 files (58 of them marked `converted` in
-- `scripts/rls_coverage.manifest`). That is why 0083 needed only two phases:
-- its app-side phase had effectively already shipped.
--
-- **None of that transfers.** `app.account_id` cannot discriminate member
-- oceans (§1) — it is uniform across every one of them — so the tenant plane
-- needs the ACTING USER, and no such GUC exists anywhere in the codebase today.
-- This migration defines the contract; a SEPARATE PR implements it:
--
--   `app.user_id`       REQUIRED. The acting end user's uuid, as text.
--                       Transaction-local (`set_config(..., true)`). Must be
--                       the gate-verified `TenantContext::user_id` — the one
--                       user the request's context is valid for (see
--                       `reachability::require_matching_user`) — never a
--                       handler parameter a caller could get wrong.
--                       Rust side: `tenancy::scope::set_tenant_user`,
--                       `begin_tenant_scoped`, `Tenant::begin_member_scoped`.
--                       UNSET/'' => no member-private or team ocean is
--                       visible. See §3 — that failure mode is the single most
--                       consequential decision in this design.
--
--   `app.tenant_actor`  OPTIONAL. The single recognised value is the exact
--                       string `'system'`; anything else, including unset and
--                       '', is not the service principal. The explicit,
--                       greppable escape for background work that is
--                       legitimately not acting as any one member: the
--                       embed/extract/summary workers, the member-ocean
--                       reconciler, purge/export tooling. Rust side:
--                       `tenancy::scope::begin_tenant_system_scoped`. See §4
--                       for why this exists and what it costs.
--
-- Both are read through the helper functions below rather than inline, so the
-- parsing rule lives in exactly one place, and both REQUIRE an open transaction
-- (`SET LOCAL` semantics on a pooled connection). A tenant-plane query executed
-- directly against a `&PgPool` outside any transaction cannot carry either GUC
-- — those paths are enumerated in the Phase-2 runbook, not hand-waved.
--
-- ══ 3. Fail-CLOSED, not fail-LOUD — a deliberate divergence from 0083 ════════
--
-- 0083 uses the STRICT single-argument `current_setting('app.account_id')`, so
-- an unscoped session hard-ERRORS rather than quietly returning nothing. That
-- is the right call there: on the consumer plane the GUC is the ONLY scope, so
-- an unscoped query is broken no matter what, and an error is a better way to
-- find out than an empty result.
--
-- It is the wrong call here, and this migration uses the `missing_ok` form
-- (`current_setting('app.user_id', true)`, NULL when unset) instead:
--
--   * An unscoped tenant session is NOT unbounded. The database it is
--     connected to still holds exactly one org, so the cross-org boundary — the
--     one with real blast radius — is intact regardless of the GUC. What an
--     unset `app.user_id` costs is member-ocean and team-ocean visibility, and
--     nothing else.
--   * A strict predicate would take the ENTIRE tenant plane down at the moment
--     the DSN is repointed (Phase 3 / OPS-4-T): the workers run ambient against
--     tenant pools by design (`workers::embed::JobScope::for_job` returns the
--     ambient arm for `TenantId::Org(_)`, on the — until now correct —
--     rationale that "tenant DBs get whole-DB isolation, never RLS"), so every
--     background tick would begin hard-erroring on the first query. Shipping a
--     backstop whose activation is an outage is how backstops get reverted.
--   * The blast radii are not symmetric. A missed call site under DENY loses
--     member-ocean visibility on ONE code path; under ERROR it 500s that path
--     outright. And because the deny is scoped to member/team rows — the org
--     commons stays readable — this is NOT a "silent deny-all across a whole
--     tenant", which would indeed be its own outage. It is a partial,
--     bounded, testable degradation.
--   * NULL therefore denies exactly the rows the boundary protects and leaves
--     everything else readable. `owner_user_id = NULL` is NULL, i.e. not-true,
--     i.e. the row is hidden. That is fail-closed on the protected boundary.
--
-- The cost of fail-closed-instead-of-loud is that a code path which forgets to
-- scope is silently degraded rather than loudly broken. That is paid for by
-- (a) `backend/tests/tenant_rls_pg.rs`, which asserts the unscoped-session
-- behavior explicitly rather than leaving it to be discovered, and (b) the
-- Phase-3 runbook's post-cutover probe, which proves a forbidden row is
-- actually invisible from a live connection instead of assuming it.
--
-- ══ 4. `app.tenant_actor = 'system'` — the one deliberate bypass ═════════════
--
-- Background workers are not acting on behalf of a member; they embed, extract,
-- summarise, reconcile and purge across every ocean in the tenant. There is no
-- honest `app.user_id` to give them. Three options existed:
--
--   (a) a second, more-privileged connection role for workers — rejected: a
--       tenant's DSN is ONE Fly secret (`tenant_databases.db_secret_ref`), and
--       a second role means a second secret, a second rotation, and a second
--       thing to get wrong per tenant;
--   (b) convert every worker to a scoped transaction that knows which member's
--       ocean each job belongs to — the right end state, but that is the
--       tenant-plane equivalent of the consumer plane's entire B.0 `_scoped`
--       Store rollout, and it does not belong in the migration-landing PR;
--   (c) an explicit service-principal GUC — chosen.
--
-- What (c) costs, stated plainly: any code path that sets `app.tenant_actor`
-- gets the pre-RLS view of this database. It is a bypass. It is mitigated by
-- being (i) opt-IN — the default, and the value on every unscoped connection,
-- is deny; (ii) settable through exactly one Rust function
-- (`tenancy::scope::begin_tenant_system_scoped`), so `grep -rn
-- begin_tenant_system_scoped backend/src` is a complete and reviewable list of
-- every place the backstop is stood down; and (iii) irrelevant to the bug class
-- this migration targets — all three of the 2026-07-25 incidents were REQUEST
-- paths carrying a real user, not worker paths.
--
-- It is NOT a defense against SQL injection under the app role: an attacker who
-- can execute arbitrary SQL can `set_config('app.tenant_actor','system',true)`
-- for themselves. 0083's header makes the same disclaimer about forging
-- `app.account_id` and it applies here unchanged.
--
-- ══ 5. What the predicate deliberately does NOT include ══════════════════════
--
-- `resolve_tenant_reachable_graphs` also requires `g.archived_at IS NULL`.
-- This migration does not. Archival is a LIFECYCLE state, not an authorization
-- boundary: an archived member ocean still belongs to its owner, and the
-- restore, export (`backend/src/bin/export_member_ocean.rs`), purge
-- (`workers::member_ocean_purge`) and reconciler paths all exist precisely to
-- operate on archived oceans. Folding `archived_at` into the row-level
-- predicate would break every one of them while adding zero isolation — an
-- archived ocean is not reachable by a NON-owner either way, because the
-- ownership arm already denies that.
--
-- ══ 6. Table coverage — 31 covered, 11 deliberately not ══════════════════════
--
-- The tenant tree has 42 tables (39 shared content tables in
-- `0001_tenant_baseline`, `tenant_meta` and `account_keys` from 0002, and
-- `supersession_edge_cross_ocean_deletions` from 0008).
-- "Ocean-scoped" means the row can be attributed to a specific `graphs` row,
-- because that attribution is the only thing a member-ocean policy can be
-- written against.
--
-- (a) THE ANCHOR (1) — `graphs`. Split USING/WITH CHECK; see its policy.
--
-- (b) DIRECT `graph_id`, NOT NULL, one hop to `graphs` (19):
--       atoms, coding_session_shares, coding_sessions, conversation_oceans,
--       conversations, meeting_shares, meeting_transcripts, node_merges,
--       nodes, outlook_watched_conversations, outlook_workflows, projects,
--       query_history, relationship_edge, supersession_edge, team_keys, teams,
--       teams_watched_resources, transcript_corrections.
--
--     `supersession_edge` joined this group in rebase, not in the original
--     drafting: migration 0008 (PR #349, merged while this was in review) gave
--     it the `graph_id` column it was the only edge table to lack. It was
--     previously anchored through `superseded_id -> nodes` in (d). See §3d for
--     why the direct anchor is strictly stronger rather than merely tidier.
--
--     `meeting_transcripts.graph_id`, `transcript_corrections.graph_id` and
--     `coding_session_shares.graph_id` carry the column but no FK to `graphs`
--     ("soft", noted in 0056's own header). The subquery works identically
--     whether or not the FK exists.
--
--     NOTE on `conversations` and `teams`: 0083 covers both under (a)-style
--     `account_id` predicates, because on the consumer plane `account_id` IS
--     the boundary. Here they must be anchored on their `graph_id` instead —
--     both columns are NOT NULL and both FK to `graphs` — because an
--     `account_id` predicate is the exact no-op §1 warns about. This is the
--     single most important place where copying 0083 verbatim would have
--     produced a policy that looked right and enforced nothing.
--
-- (c) DIRECT `graph_id`, NULLABLE (4): activity_events, llm_usage,
--     mcp_tool_calls, op_log.
--
--     A NULL `graph_id` makes `graph_id IN (...)` evaluate to NULL, i.e.
--     not-true, i.e. the row is HIDDEN. Same accepted fail-closed consequence
--     0083 documents for its own nullable columns, and taken here for the same
--     reason: a row that cannot be attributed to an ocean cannot be proven
--     reachable, and guessing in the permissive direction on `op_log` in
--     particular would reopen precisely the surface the 2026-07-25 `sync/pull`
--     leak came through.
--
--     **`op_log` has NO foreign key to `graphs`** — verified against
--     `0001_tenant_baseline.sql`'s constraint section, not taken from prose
--     elsewhere in the repo that claims `ON DELETE CASCADE` coverage for it.
--     It has `op_log_pkey`, `op_log_op_id_key`, and no FK at all. Two
--     consequences worth stating: deleting a graph does NOT delete its op_log
--     rows (they linger, and after this migration they linger INVISIBLY to the
--     app role, since the anchor they reference no longer exists), and the
--     policy's subquery is the only thing tying an op to an ocean.
--
--     This is a real residual: an app path that appends an op WITHOUT a
--     `graph_id` (the column is `Option<Uuid>` in `store::NewOp`, bound
--     verbatim by all five `INSERT INTO op_log` sites) writes a row that this
--     backstop can neither attribute nor protect — it simply disappears from
--     the app role's view. The cutover runbook makes counting those rows a
--     mandatory pre-flight step per tenant, so the founder sees exactly what
--     would go dark BEFORE cutting over rather than after. Proper fix (a
--     follow-up, not this PR): give `op_log.graph_id` a real FK and a
--     backfill, then `NOT NULL`.
--
-- (d) ONE FURTHER HOP, then to `graphs` (6):
--       node_atoms (node_id -> nodes), non_supersession_pair (node_a_id ->
--       nodes), conversation_messages (conversation_id -> conversations),
--       conversation_surfaced_edges (conversation_id -> conversations),
--       team_members (team_id -> teams), team_member_key_envelopes
--       (team_key_id -> team_keys).
--
--     `non_supersession_pair` anchors on ONE endpoint because its two are
--     same-graph by construction. That is the same phrasing 0083 used for
--     `supersession_edge` — and 0008's header is blunt that it "was an
--     assumption, never a constraint" there, one the supersede route actually
--     violated. It holds for `non_supersession_pair` (its only producer is a
--     single-graph path) but it is an invariant maintained by code, not by the
--     schema, and giving that table a real `graph_id` too is the obvious
--     follow-up. The hop is safe in the meantime because `nodes` is itself
--     RLS'd under this same migration, so the subquery cannot return an
--     endpoint in an unreachable ocean.
--
-- (e) TWO ANCHORS, split by command (1): node_promotions. Verbatim the same
--     problem and the same four-policy fix as 0083's (d) — a bare policy is
--     `FOR ALL` and reuses `USING` as `WITH CHECK`, so an OR-across-two-oceans
--     read predicate reused for writes would let the owner of ocean A insert a
--     promotion whose `target_graph_id` is member B's private ocean. READ is
--     permissive (either anchor), WRITE requires BOTH.
--
-- (f) NOT COVERED (11). Named, with the reason, per 0083's "flag it, don't
--     leave it silently uncovered" discipline. Ten of the eleven have no ocean
--     anchor to write a policy against; the eleventh
--     (`supersession_edge_cross_ocean_deletions`) does, and gets something
--     stronger than a policy instead:
--
--       - `queries`            — `account_id` + `user_id`, no `graph_id`. The
--                                account column is the §1 no-op. A `user_id`
--                                predicate would be a DIFFERENT boundary
--                                (per-user query privacy) that no other layer
--                                enforces today; introducing it here, invisibly,
--                                as a side effect of an ocean-isolation
--                                migration, is the wrong way to make that
--                                product decision. Flagged for a follow-up that
--                                decides it on purpose.
--       - `graph_subscriptions`— despite the name, no `graph_id`: it is
--                                (account_id, subscription_id, resource) for
--                                Microsoft Graph webhook subscriptions. Nothing
--                                to anchor.
--       - `oauth_tokens`       — `account_id` only. Org-wide connector
--                                credentials, genuinely not per-ocean. Sensitive,
--                                and left on the broad grant deliberately:
--                                narrowing it is a real change to connector
--                                behavior, not a backstop.
--       - `slack_integrations` — `account_id` only; one row per org workspace.
--       - `slack_huddle_captures` — `account_id` + Slack ids; no ocean column.
--       - `connector_identities` — PK `(provider, workspace_id,
--                                external_user_id)`, no account or graph column
--                                and no FK to either. Shared reference data
--                                (external id -> display name). Same call as
--                                0083.
--       - `sync_cursors`       — PK is `peer TEXT` alone; one row per sync peer.
--                                No account dimension at all. Same call as 0083.
--       - `jobs`               — the target entity lives in an untyped `payload`
--                                JSONB whose shape varies per `kind`; 0004 added
--                                a nullable `account_id`, which is the §1 no-op
--                                here. A JSONB-path policy keyed on an assumed
--                                `payload->>'graph_id'` would silently stop
--                                protecting any job kind that doesn't carry that
--                                key. Same call as 0083: a fragile policy is
--                                worse than an honestly absent one.
--       - `tenant_meta`        — one row: this database's org fingerprint. Not
--                                content. Write access IS narrowed below (the
--                                app only ever reads it; `onboard_tenant.sh`
--                                writes it).
--       - `account_keys`       — the org's wrapped DEK, keyed by `account_id`.
--                                One row per tenant, not ocean-scoped, and a
--                                policy could not be written against an ocean
--                                even in principle. Left readable/writable: the
--                                app both reads it (`crypto::resolve_dek*`) and
--                                provisions it (`clerk_provision`). The
--                                protection for this row is that it is in the
--                                tenant DB at all (a control-plane compromise
--                                yields no content keys) — RLS adds nothing.
--       - `supersession_edge_cross_ocean_deletions` — NEW in migration 0008 (PR
--                                #349). The forensic ledger of the cross-ocean
--                                supersession edges that PR recorded and then
--                                deleted. It is NOT covered by a policy, but it
--                                is the only table here that gets an explicit
--                                `REVOKE ALL` from `coralswarm_tenant_app` on
--                                top (see the grants block below) — which is
--                                STRONGER than a policy, not weaker.
--
--                                Why revoke rather than police: 0008's own
--                                header puts this table "in the same category
--                                as `_sqlx_migrations`" — written once by that
--                                migration, read by a human during an incident,
--                                with no application reader on any plane — and
--                                adds "if a future change gives it an
--                                application reader, it must get a policy in the
--                                same PR". A revoke makes that instruction
--                                self-enforcing: the future reader gets a loud
--                                42501 and has to come here and decide, instead
--                                of silently receiving policy-filtered rows and
--                                never noticing the question was asked.
--
--                                Why it needs ANY control: it stores
--                                `superseded_id`/`superseder_id` plus BOTH
--                                endpoint graph ids, deliberately — 0008 keeps
--                                both sides precisely because they disagree.
--                                Those are member-private ocean identifiers, and
--                                identifier disclosure across this exact
--                                boundary is what #348 had to close. Without the
--                                revoke, this migration's own schema-wide
--                                `GRANT SELECT ... ON ALL TABLES` would hand
--                                every member a readable index of which private
--                                reefs were involved. It anchors fine on
--                                `superseded_graph_id` if a policy is ever
--                                wanted; the revoke is simply a tighter answer
--                                today.
--
--     1 + 19 + 4 + 6 + 1 = 31 covered; + 11 uncovered = 42 = the whole tree.
--     (41 before migration 0008 / PR #349 added the deletions ledger.)
--
-- ══ 7. `FORCE ROW LEVEL SECURITY` — considered and deliberately NOT used ═════
--
-- Plain `ENABLE` does not apply to the table OWNER. The owner is the role that
-- ran `0001_tenant_baseline` and every migration since, and — TODAY, before
-- Phase 3 — it is also the role the application connects as. So the question
-- "which tables need FORCE" has to be answered against three different
-- consumers of the owner role, and the answer is "none of them", for three
-- independent reasons any one of which is sufficient:
--
--   1. FORCE would make this migration NOT inert. The whole two-phase design
--      (0083's header; §8 below) rests on Phase 1 changing zero query results
--      because the app is the owner and the owner is exempt. FORCE removes that
--      exemption the instant this migration commits: every member ocean would
--      vanish from the live app, which sets no `app.user_id` yet. That is not a
--      backstop landing inert, it is an outage landing quietly.
--
--   2. FORCE would break the policy predicate itself, by construction.
--      `tenant_rls_visible_graph_ids()` is `SECURITY DEFINER` specifically so
--      that its reads of `graphs`/`teams`/`team_members` are NOT re-filtered by
--      those tables' own policies — which is what breaks the cycle
--      (`graphs`' policy -> the function -> `graphs`) that Postgres would
--      otherwise reject at runtime with "infinite recursion detected in policy
--      for relation graphs". SECURITY DEFINER runs as the function's owner;
--      under FORCE the owner is no longer exempt and the recursion is back.
--      The non-recursive predicate and FORCE are mutually exclusive.
--
--   3. FORCE would break future migrations and ops tooling — backfills,
--      `export_member_ocean`, purge — all of which run as the owner and all of
--      which legitimately need the unfiltered view.
--
-- FORCE's only real purpose is to close the hole where the APPLICATION connects
-- as the table owner. Phase 3 closes that hole directly, by connecting as a
-- non-owner. Because "the app role is not an owner" is now the load-bearing
-- assumption that FORCE would otherwise have covered, this migration ASSERTS it
-- rather than assuming it: the DO block below raises if `coralswarm_tenant_app`
-- owns any table in `public`, on every run, and the cutover runbook re-checks it
-- from the live connection after cutover. An unasserted invariant is how a
-- future `ALTER TABLE ... OWNER TO` silently turns all 34 policies off.
--
-- ══ 8. This migration is INERT ═══════════════════════════════════════════════
--
-- Same land-inert-switch-separately discipline as 0083, but THREE phases rather
-- than two, because 0083 inherited an app side that had already shipped and
-- this one does not (§2):
--
--   Phase 1 (THIS migration) — create `coralswarm_tenant_app` with NO PASSWORD
--     (a migration file is committed to git; a real credential in it would
--     defeat the point of having one), enable RLS, create the policies, grant
--     the role what it will need, and land the Rust primitives additively with
--     zero call sites. The app still connects as the owner, the owner is exempt
--     from `ENABLE`-only RLS, and so ZERO query results change.
--
--   Phase 2 (SEPARATE PR) — set `app.user_id` on every tenant-plane path that
--     touches ocean-scoped data, per §2's contract. Sized and enumerated in
--     `docs/tenant-rls-cutover-runbook.md`. Still inert (the app is
--     still BYPASSRLS), so it can land and soak independently.
--
--   Phase 3 (ops task, "OPS-4-T") — set a password out-of-band, point each
--     tenant's `db_secret_ref` Fly secret at a DSN for the new role, restart,
--     verify. Same runbook. **Only valid once Phase 2 is proven** — cutting
--     over before it does not corrupt anything (the deny is fail-closed, §3)
--     but it does make every member-private ocean invisible to the app.
--     Until Phase 3 runs, the safety catch is on, on purpose.
--
-- Depends on: 0001_tenant_baseline (all 39 content tables),
--             0002_tenant_meta_and_account_keys (tenant_meta, account_keys),
--             0004_jobs_account_id (jobs.account_id — flagged, not covered),
--             0005_member_ocean_columns (graphs.owner_user_id,
--             graphs.archived_at, graphs_kind_check widened to admit 'member'),
--             0006_member_ocean_reachability_functions (the application-side
--             predicate this migration mirrors),
--             0008_supersession_edge_graph_id (supersession_edge.graph_id —
--             this migration anchors that table DIRECTLY on it rather than
--             hopping through `nodes`, so 0008 is a hard ordering dependency,
--             not merely a lower number; and
--             supersession_edge_cross_ocean_deletions, which the grants block
--             below revokes).
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON activity_events;
--   DROP POLICY IF EXISTS member_ocean_isolation ON atoms;
--   DROP POLICY IF EXISTS member_ocean_isolation ON coding_session_shares;
--   DROP POLICY IF EXISTS member_ocean_isolation ON coding_sessions;
--   DROP POLICY IF EXISTS member_ocean_isolation ON conversation_messages;
--   DROP POLICY IF EXISTS member_ocean_isolation ON conversation_oceans;
--   DROP POLICY IF EXISTS member_ocean_isolation ON conversation_surfaced_edges;
--   DROP POLICY IF EXISTS member_ocean_isolation ON conversations;
--   DROP POLICY IF EXISTS member_ocean_isolation ON graphs;
--   DROP POLICY IF EXISTS member_ocean_isolation ON llm_usage;
--   DROP POLICY IF EXISTS member_ocean_isolation ON mcp_tool_calls;
--   DROP POLICY IF EXISTS member_ocean_isolation ON meeting_shares;
--   DROP POLICY IF EXISTS member_ocean_isolation ON meeting_transcripts;
--   DROP POLICY IF EXISTS member_ocean_isolation ON node_atoms;
--   DROP POLICY IF EXISTS member_ocean_isolation ON node_merges;
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON node_promotions;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON node_promotions;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON node_promotions;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON node_promotions;
--   DROP POLICY IF EXISTS member_ocean_isolation ON nodes;
--   DROP POLICY IF EXISTS member_ocean_isolation ON non_supersession_pair;
--   DROP POLICY IF EXISTS member_ocean_isolation ON op_log;
--   DROP POLICY IF EXISTS member_ocean_isolation ON outlook_watched_conversations;
--   DROP POLICY IF EXISTS member_ocean_isolation ON outlook_workflows;
--   DROP POLICY IF EXISTS member_ocean_isolation ON projects;
--   DROP POLICY IF EXISTS member_ocean_isolation ON query_history;
--   DROP POLICY IF EXISTS member_ocean_isolation ON relationship_edge;
--   DROP POLICY IF EXISTS member_ocean_isolation ON supersession_edge;
--   DROP POLICY IF EXISTS member_ocean_isolation ON team_keys;
--   DROP POLICY IF EXISTS member_ocean_isolation ON team_member_key_envelopes;
--   DROP POLICY IF EXISTS member_ocean_isolation ON team_members;
--   DROP POLICY IF EXISTS member_ocean_isolation ON teams;
--   DROP POLICY IF EXISTS member_ocean_isolation ON teams_watched_resources;
--   DROP POLICY IF EXISTS member_ocean_isolation ON transcript_corrections;
--   -- (ALTER TABLE ... DISABLE ROW LEVEL SECURITY on each of the above, if
--   -- reverting fully.)
--   REVOKE EXECUTE ON FUNCTION tenant_rls_actor_user_id() FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION tenant_rls_is_system() FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION tenant_rls_visible_graph_ids() FROM coralswarm_tenant_app;
--   DROP FUNCTION IF EXISTS tenant_rls_visible_graph_ids();
--   DROP FUNCTION IF EXISTS tenant_rls_is_system();
--   DROP FUNCTION IF EXISTS tenant_rls_actor_user_id();
--   GRANT INSERT, UPDATE, DELETE ON tenant_meta TO coralswarm_tenant_app;
--   GRANT ALL ON _sqlx_migrations TO coralswarm_tenant_app;
--   GRANT ALL ON supersession_edge_cross_ocean_deletions TO coralswarm_tenant_app;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA public
--     REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM coralswarm_tenant_app;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA public
--     REVOKE USAGE, SELECT ON SEQUENCES FROM coralswarm_tenant_app;
--   -- (schema-less, matching the forward statement — the `IN SCHEMA public`
--   --  form is a silent no-op for FUNCTIONS/PUBLIC; see the forward comment)
--   ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
--   REVOKE EXECUTE ON FUNCTION resolve_graph_account_id(uuid) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid,uuid) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid,uuid,uuid) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_session_share_account_id(text) FROM coralswarm_tenant_app;
--   REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM coralswarm_tenant_app;
--   REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM coralswarm_tenant_app;
--   REVOKE USAGE ON SCHEMA public FROM coralswarm_tenant_app;
--   -- REVOKE the default-privileges grants BEFORE DROP ROLE — an ALTER DEFAULT
--   -- PRIVILEGES entry is itself a dependent object.
--   DROP ROLE IF EXISTS coralswarm_tenant_app;

-- Each `ENABLE ROW LEVEL SECURITY` takes a brief ACCESS EXCLUSIVE lock on its
-- table. Same discipline as 0083/0005: a migration must never be able to block
-- indefinitely behind a long-running reader on a live tenant.
SET LOCAL lock_timeout = '3s';

-- ── 1. The tenant application role ──────────────────────────────────────────
-- A role is a row in the CLUSTER-WIDE `pg_authid` catalog, shared across every
-- database on the instance. Tenant databases may or may not share a cluster
-- with each other, and in CI dozens of scratch tenant DBs are created and
-- migrated CONCURRENTLY on one cluster, so this section carries 0083's
-- advisory-lock + retry loop. Every statement inside is idempotent, so retrying
-- the whole block from scratch on a transient shared-catalog conflict is always
-- safe. In a real one-connection deploy the loop iterates exactly once and the
-- retry path never fires.
--
-- **The advisory lock does NOT make the common case collision-free here, and
-- 0083's header is wrong to say that it does.** Postgres advisory locks are
-- scoped to the CURRENT DATABASE (the lock tag includes the database OID), but
-- `pg_authid` is shared across the whole cluster. Two sessions migrating two
-- DIFFERENT scratch databases therefore take two entirely different locks and
-- do not serialise against each other at all — which is exactly the scenario
-- that matters, because that is how CI and the `migrate_tenants` orchestrator
-- both run. The lock still earns its place (it serialises repeated runs against
-- the SAME database, e.g. a retried deploy), but **the retry is the only real
-- guarantee**, and the set of SQLSTATEs it catches has to be complete.
--
-- Caught, and why each is a shared-catalog race rather than a bug worth
-- surfacing:
--   * `23505` unique_violation — two sessions both pass the `IF NOT EXISTS`
--     guard, both run `CREATE ROLE`, and the loser hits
--     `pg_authid_rolname_index`. **Observed for real** in CI on this migration's
--     first run (`tenant_registry_pg` / `tenant_migrate_pg`, which each build
--     several scratch tenant DBs at once); 0083 catches only `XX000` and has
--     the same latent gap, unfixable in place since merged migrations are
--     append-only.
--   * `42710` duplicate_object — the same race, reported under a different
--     code depending on server version and on which index catches it first.
--   * `XX000` internal_error — `tuple concurrently updated`, the code 0083
--     observed for concurrent `ALTER ROLE`/`GRANT` against the shared catalog.
--
-- Deliberately NOT a blanket `WHEN OTHERS`: that would also swallow the
-- ownership `RAISE EXCEPTION` below (a real, must-not-be-retried misconfiguration)
-- and any typo'd object name, hiding either behind ten silent retries. Every
-- statement in the block is `CREATE ROLE`/`ALTER ROLE`/`GRANT`/`REVOKE`/a
-- catalog `SELECT`, so a duplicate-key error from any of them is a catalog race
-- by construction; after 10 attempts it re-raises regardless.
--
-- Deliberately a DIFFERENT role name from the consumer plane's
-- `coralswarm_app`. The two planes have different grants, different policies,
-- different passwords and different rotation schedules; sharing a name would
-- invite sharing a credential, and would make "which plane is this connection
-- on?" unanswerable from `current_user` in an incident.
DO $$
DECLARE
    attempt int := 0;
    owned_by_app text;
    app_super bool;
    app_bypassrls bool;
    app_replication bool;
    app_createdb bool;
    app_createrole bool;
    wrong_attrs text[];
    rls_critical text;
    still_wrong_attrs text[];
BEGIN
    LOOP
        BEGIN
            -- Transaction-scoped: released at this migration's COMMIT (or at
            -- each failed attempt's rollback-to-savepoint), never outlives it.
            PERFORM pg_advisory_xact_lock(hashtext('coralswarm_tenant_app_role_ddl'));

            -- LOGIN with NO PASSWORD: the role IS eventually the app's login
            -- role (Phase 3), but it is unusable for authentication until a
            -- password is set out-of-band. NOBYPASSRLS is the load-bearing
            -- attribute — it is what makes every policy below actually apply.
            -- It is already the `CREATE ROLE` default; spelled out so the
            -- intent cannot silently flip under a copy-edit.
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
                CREATE ROLE coralswarm_tenant_app
                    WITH LOGIN
                         NOSUPERUSER
                         NOCREATEDB
                         NOCREATEROLE
                         NOREPLICATION
                         NOBYPASSRLS
                         CONNECTION LIMIT -1;
            END IF;

            -- The CREATE above is guarded by IF NOT EXISTS, so a
            -- `coralswarm_tenant_app` that already existed for ANY reason (a
            -- partial prior run, a hand-created role, a role someone once made
            -- BYPASSRLS "just to unblock something") would keep its existing
            -- attributes and silently bypass every policy below — forever,
            -- including after Phase 3, with no error and no signal. 0083 was
            -- given an unconditional re-assertion during review of PR #277 for
            -- exactly that reason. The SAME property is enforced here, but it
            -- CANNOT be enforced the same way.
            --
            -- ── Why this is not one unconditional `ALTER ROLE` ──────────────
            -- It was, and that is precisely what aborted the v184 deploy:
            --
            --   [testorg] — FAIL migrate: while executing migration 9:
            --   permission denied to alter role
            --
            -- 0083 runs against the CONSUMER plane: one Fly Postgres whose
            -- migration DSN is a superuser, where the unconditional form is
            -- free. Tenant databases are NEON projects, and Neon offers no
            -- superuser to anyone — the tenant owner role is a NOSUPERUSER
            -- role holding CREATEDB + CREATEROLE. Against that owner there are
            -- TWO independent gates on `ALTER ROLE`, and the merged form of
            -- this statement tripped the first one on every tenant:
            --
            --   1. ATTRIBUTE GATE. Postgres 16 ("Allow CREATEROLE to manage
            --      roles", unchanged in 17) requires the caller to hold an
            --      attribute in order to name it in an ALTER — and it checks
            --      whether the CLAUSE WAS WRITTEN, not whether the value would
            --      change:
            --
            --        SUPERUSER   / NOSUPERUSER    requires SUPERUSER
            --        REPLICATION / NOREPLICATION  requires REPLICATION
            --        BYPASSRLS   / NOBYPASSRLS    requires BYPASSRLS
            --
            --      So `ALTER ROLE ... WITH NOSUPERUSER` is refused even when
            --      the role is ALREADY NOSUPERUSER and the caller created it
            --      microseconds earlier. Reproduced on 16.14 and 17.10; the
            --      SQLSTATE is 42501 and the DETAIL reads "Only roles with the
            --      SUPERUSER attribute may change the SUPERUSER attribute."
            --      sqlx renders only the message field, which is why the
            --      deploy log showed the bare, undiagnosable `permission
            --      denied to alter role`.
            --
            --   2. ADMIN-OPTION GATE. Independently, ANY `ALTER ROLE` — even
            --      `NOCREATEDB`, which needs no special attribute — requires
            --      CREATEROLE **plus ADMIN OPTION on the target role**. PG 16+
            --      grants the creator ADMIN OPTION automatically, so the
            --      normal path has it; a `coralswarm_tenant_app` created by
            --      somebody ELSE does not, and then every ALTER fails with the
            --      same bare message. Both gates were verified independently,
            --      including the case where ADMIN OPTION is held and gate 1
            --      still refuses.
            --
            -- There is no DSN that escapes gate 1 on Neon: the attribute
            -- required is SUPERUSER, and the platform has none.
            --
            -- ── Why the security property survives the change ───────────────
            -- Note the asymmetry PG 16 introduced. `CREATE ROLE` refuses to
            -- turn these attributes ON without the privilege, but accepts the
            -- NO- forms from any CREATEROLE holder (verified on 16/17). The
            -- role the block above creates is therefore NOBYPASSRLS,
            -- NOSUPERUSER and NOREPLICATION **by construction**, on a
            -- connection that is INCAPABLE of making it otherwise. The
            -- unconditional ALTER was only ever load-bearing for the role this
            -- migration did NOT create — the hand-made or tampered one.
            --
            -- ── The shape: verify first, ALTER only what is actually wrong ──
            -- Read the catalog back and issue DDL only on real drift. Three
            -- properties follow, and all three are the point:
            --
            --   * In the steady state — every run after the first, on every
            --     tenant — this block issues NO `ALTER ROLE` at all. The
            --     migration therefore needs no role privilege whatsoever,
            --     which is what makes it re-runnable on a tenant DB whose role
            --     was provisioned out-of-band, by any owner, with or without
            --     ADMIN OPTION. That case is the overwhelming majority.
            --   * On a superuser cluster (dev, CI, the consumer plane's own
            --     shape) a genuinely drifted role is still corrected in place,
            --     so 0083's self-healing behaviour is preserved exactly.
            --   * On Neon a genuinely drifted role cannot be corrected — and
            --     is REFUSED, not skipped. `RAISE EXCEPTION` fails the
            --     migration closed, so a BYPASSRLS `coralswarm_tenant_app` can
            --     never reach Phase 3. That is the property the unconditional
            --     ALTER existed to guarantee, and it is unchanged. What
            --     changes is only that the operator is told which attribute is
            --     wrong and what to run, instead of `permission denied to
            --     alter role`.
            --
            -- CONNECTION LIMIT and LOGIN are deliberately NOT reconciled here.
            -- Both are legitimate operator levers for taking this role out of
            -- service during an incident, and a migration that silently undoes
            -- an emergency lockout on the next deploy is a worse failure than
            -- the drift it corrects. Neither affects RLS.
            SELECT r.rolsuper, r.rolbypassrls, r.rolreplication,
                   r.rolcreatedb, r.rolcreaterole
              INTO app_super, app_bypassrls, app_replication,
                   app_createdb, app_createrole
              FROM pg_catalog.pg_roles r
             WHERE r.rolname = 'coralswarm_tenant_app';

            IF NOT FOUND THEN
                -- Raced: another session DROPped the role between our CREATE
                -- and this read. `pg_authid` is cluster-wide and the advisory
                -- lock above is per-database (see this section's header), so
                -- this is the same class of race the loop already retries —
                -- just one that surfaces as a missing row rather than as a
                -- SQLSTATE, and so cannot be caught by the handler below.
                -- Retried explicitly rather than left to fail two statements
                -- later on a confusing "role does not exist" from a GRANT.
                attempt := attempt + 1;
                IF attempt >= 10 THEN
                    RAISE EXCEPTION
                        'coralswarm_tenant_app disappeared between CREATE and attribute '
                        'verification on % consecutive attempts — concurrent role DDL is '
                        'running against this cluster. Re-run the deploy once it has settled.',
                        attempt;
                END IF;
                PERFORM pg_sleep(0.05 * attempt + random() * 0.05);
                CONTINUE;
            END IF;

            -- BYPASSRLS leads the list: it is the one that makes every policy
            -- in this file a no-op, so it heads the message when several are
            -- wrong. The `::text` casts are load-bearing, not decoration —
            -- `text[] || 'literal'` binds to `anyarray || anyarray`, so
            -- Postgres tries to parse the literal AS an array and the
            -- statement dies with `malformed array literal: "NOBYPASSRLS"`.
            -- Uncast, this block would be silently correct on the healthy path
            -- (the array stays empty, no append ever runs) and broken on
            -- exactly the tamper path it exists to handle.
            wrong_attrs := ARRAY[]::text[];
            IF app_bypassrls   THEN wrong_attrs := wrong_attrs || 'NOBYPASSRLS'::text;   END IF;
            IF app_super       THEN wrong_attrs := wrong_attrs || 'NOSUPERUSER'::text;   END IF;
            IF app_replication THEN wrong_attrs := wrong_attrs || 'NOREPLICATION'::text; END IF;
            IF app_createdb    THEN wrong_attrs := wrong_attrs || 'NOCREATEDB'::text;    END IF;
            IF app_createrole  THEN wrong_attrs := wrong_attrs || 'NOCREATEROLE'::text;  END IF;

            IF cardinality(wrong_attrs) > 0 THEN
                -- Which of the drifted attributes are gate-1 attributes, i.e.
                -- unfixable by anything short of a superuser. Decides the
                -- remedy the operator is given below, so it is computed from
                -- the same array the ALTER is built from rather than restated.
                --
                -- This IS a `string_agg` that returns NULL over an empty set,
                -- unlike the post-condition below — deliberately, and safely:
                -- it only chooses which of two remedies to print, both branches
                -- of the CASE still `RAISE`, and it is reached only when
                -- `wrong_attrs` is already known non-empty. NULL here means
                -- "the drift is not RLS-critical", which is a real answer, not
                -- an absent subject.
                SELECT string_agg(a, ' ')
                  INTO rls_critical
                  FROM unnest(wrong_attrs) AS a
                 WHERE a IN ('NOBYPASSRLS', 'NOSUPERUSER', 'NOREPLICATION');

                BEGIN
                    -- Dynamic only in WHICH of five fixed literals it
                    -- concatenates; every element of `wrong_attrs` is written
                    -- verbatim above, in this file. No external input reaches
                    -- this string.
                    EXECUTE 'ALTER ROLE coralswarm_tenant_app WITH '
                            || array_to_string(wrong_attrs, ' ');
                EXCEPTION WHEN insufficient_privilege THEN
                    -- The remedy goes in MESSAGE, not HINT. sqlx's error
                    -- Display renders only the message field, so a `USING
                    -- HINT` is invisible in the one place this text exists to
                    -- be read: the `release_command` output of a failed
                    -- deploy. HINT is still set, for psql and interactive
                    -- callers, but it only elaborates.
                    RAISE EXCEPTION
                        -- ONE `%` for the whole diagnosis+remedy, not two
                        -- adjacent ones: `%%` in a RAISE format string is an
                        -- ESCAPED LITERAL PERCENT, not two substitutions, so
                        -- the two CASEs are concatenated into a single
                        -- argument instead of being passed separately.
                        'coralswarm_tenant_app has role attribute(s) that defeat this migration '
                        '(needs: %), and the migration connection (%) cannot clear them. %'
                        'Exact commands: docs/tenant-rls-cutover-runbook.md, section '
                        '"3.7 Repairing tenant app-role attributes".',
                        array_to_string(wrong_attrs, ' '),
                        current_user,
                        -- The consequence, stated only when it is true. A
                        -- CREATEDB-only drift is a tamper signal worth failing
                        -- on, but it is NOT an RLS bypass, and saying so would
                        -- send the responder hunting the wrong incident.
                        (CASE
                          WHEN rls_critical IS NOT NULL THEN
                            'BYPASSRLS or SUPERUSER on this role makes every policy in '
                            '0009_tenant_rls a no-op; REPLICATION lets it stream the WAL and '
                            'read every member-private ocean beneath RLS entirely. '
                          ELSE
                            'These attributes do not themselves bypass RLS, but this role is '
                            'not supposed to have them and something set them — treat the '
                            'drift as unexplained until you know what did. '
                        END)
                        ||
                        (CASE
                          WHEN rls_critical IS NOT NULL THEN
                            'FIX: a non-superuser owner (every Neon tenant) CANNOT clear '
                            || rls_critical
                            || ' — DROP the role and let this migration recreate it correctly. '
                          ELSE
                            'FIX: this is an ADMIN OPTION failure, not an attribute one — the '
                            'role was created by a different principal. Re-run as that '
                            'principal, or DROP the role and let this migration recreate it. '
                        END)
                    USING HINT =
                        'Re-run the deploy once the role is corrected; the tenant tree is '
                        'idempotent and resumes at migration 9.';
                END;
            END IF;

            -- Post-condition, asserted rather than assumed. Cheap, and it is
            -- the statement that actually guarantees the invariant every
            -- policy below rests on — independent of which branch above ran,
            -- of whether a future edit drops one of the five checks, and of an
            -- `ALTER ROLE` that somehow reported success without taking
            -- effect.
            --
            -- ── Why this re-reads with INTO/FOUND and not with an aggregate ─
            -- The first revision of this check aggregated straight over
            -- `pg_roles`:
            --
            --   SELECT string_agg(t.a, ' ') INTO still_wrong
            --     FROM pg_catalog.pg_roles r CROSS JOIN LATERAL (VALUES …)
            --    WHERE r.rolname = 'coralswarm_tenant_app' AND t.bad;
            --
            -- `string_agg` over ZERO ROWS returns NULL, and the zero-row case
            -- is not only "the role is clean" — it is also "the role is GONE".
            -- A concurrent `DROP ROLE` between the reconciliation above and
            -- this check therefore produced `still_wrong IS NULL` and fell
            -- straight through to install 34 policies, having verified
            -- nothing. "Verified correct" and "subject no longer exists" must
            -- not be the same outcome anywhere, and least of all here: this is
            -- the statement the entire backstop is documented to rest on, and
            -- role existence is precisely what the `IF NOT FOUND` earlier in
            -- this block concedes is racy. An assertion that cannot tell those
            -- two apart is not an assertion.
            --
            -- `INTO` + `FOUND` distinguishes them by construction: a missing
            -- role sets `FOUND` false and cannot reach the attribute test at
            -- all.
            SELECT r.rolsuper, r.rolbypassrls, r.rolreplication
              INTO app_super, app_bypassrls, app_replication
              FROM pg_catalog.pg_roles r
             WHERE r.rolname = 'coralswarm_tenant_app';

            IF NOT FOUND THEN
                -- The same cluster-wide race as above, caught at the other end
                -- of the block. Retried rather than raised on the spot: redoing
                -- the work IS the correct response to a concurrent DROP, the
                -- loop is idempotent, and the attempt ceiling still turns a
                -- persistent disappearance into a hard failure rather than a
                -- silent pass. Safe to `CONTINUE` from here — every grant and
                -- revoke in this block comes AFTER this point, so nothing has
                -- been applied yet that a re-run would double up on.
                attempt := attempt + 1;
                IF attempt >= 10 THEN
                    RAISE EXCEPTION
                        'coralswarm_tenant_app disappeared during role reconciliation on % '
                        'consecutive attempts — concurrent role DDL is running against this '
                        'cluster. Re-run the deploy once it has settled.', attempt;
                END IF;
                PERFORM pg_sleep(0.05 * attempt + random() * 0.05);
                CONTINUE;
            END IF;

            still_wrong_attrs := ARRAY[]::text[];
            IF app_super       THEN still_wrong_attrs := still_wrong_attrs || 'SUPERUSER'::text;   END IF;
            IF app_bypassrls   THEN still_wrong_attrs := still_wrong_attrs || 'BYPASSRLS'::text;   END IF;
            IF app_replication THEN still_wrong_attrs := still_wrong_attrs || 'REPLICATION'::text; END IF;
            IF cardinality(still_wrong_attrs) > 0 THEN
                RAISE EXCEPTION
                    'coralswarm_tenant_app still holds % after reconciliation — refusing to '
                    'install policies that this role would bypass. See '
                    'docs/tenant-rls-cutover-runbook.md, section '
                    '"3.7 Repairing tenant app-role attributes".',
                    array_to_string(still_wrong_attrs, ' ');
            END IF;

            -- §7: `ENABLE`-only RLS exempts the table OWNER, so the entire
            -- backstop rests on the app role never becoming an owner. Assert it
            -- instead of documenting it. This fires on every migration run, so
            -- an `ALTER TABLE ... OWNER TO coralswarm_tenant_app` anywhere in
            -- this database's future is caught the next time the tree is
            -- applied rather than discovered during an incident.
            SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
              INTO owned_by_app
              FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
             WHERE c.relnamespace = 'public'::regnamespace
               AND c.relkind IN ('r', 'p')
               AND r.rolname = 'coralswarm_tenant_app';
            IF owned_by_app IS NOT NULL THEN
                RAISE EXCEPTION
                    'coralswarm_tenant_app owns table(s) in public (%) — a table owner is EXEMPT '
                    'from ENABLE-only row level security, so every policy in 0009_tenant_rls would '
                    'be a no-op for the application role. Reassign ownership to the migration role '
                    '(ALTER TABLE ... OWNER TO <owner>) before applying this migration.',
                    owned_by_app;
            END IF;

            -- Base privileges. RLS restricts WHICH rows a policy-protected
            -- table shows this role; it is not a substitute for the GRANT —
            -- without these the role could not read or write anything at all.
            GRANT USAGE ON SCHEMA public TO coralswarm_tenant_app;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO coralswarm_tenant_app;
            GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO coralswarm_tenant_app;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
                GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO coralswarm_tenant_app;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
                GRANT USAGE, SELECT ON SEQUENCES TO coralswarm_tenant_app;

            -- ── EXECUTE: an explicit allowlist, NOT schema-wide ─────────────
            -- The pre-existing tenant-tree `SECURITY DEFINER` resolvers
            -- (0003's `resolve_graph_account_id`/`resolve_atoms_account_id`,
            -- 0006's `resolve_tenant_reachable_graphs`/
            -- `resolve_tenant_graph_reachable_by_user`, 0007's two share
            -- resolvers) all `REVOKE EXECUTE ... FROM PUBLIC` in their own
            -- migrations, explicitly "in advance of any future lower-privilege
            -- tenant-DB role". This is that role, and it calls all six from
            -- live request paths — `routes::shares::resolve_share_by_token`,
            -- `reachability::tenant_list_reachable_graphs`,
            -- `Store::graph_account_id`, `workers::embed::resolve_dek_for_atoms`
            -- — so it needs EXECUTE back.
            --
            -- An earlier revision did this schema-wide
            -- (`GRANT EXECUTE ON ALL FUNCTIONS ... ` plus an `ALTER DEFAULT
            -- PRIVILEGES ... GRANT EXECUTE ON FUNCTIONS`), on the reasoning
            -- that a future paired resolver should not 42501 the first time an
            -- org request hits it. CodeRabbit flagged that as a MAJOR and it is
            -- right: nearly every function in this schema is `SECURITY
            -- DEFINER`, i.e. it executes with the OWNER's privileges and is
            -- therefore exempt from every policy in this migration. A
            -- schema-wide default-privileges grant means ANY such function
            -- added to `public` later becomes callable by the low-privilege
            -- role the moment it is created — a privilege-escalation path that
            -- appears with no migration reviewing it, no policy governing it,
            -- and no test that would notice. "Convenient for a future author"
            -- is a bad trade against "silently reachable RLS bypass".
            --
            -- So: an explicit signature allowlist, and NO default-privileges
            -- entry. A future resolver 42501s on first use, which is the
            -- correct outcome — it sends whoever added it back here to grant it
            -- deliberately, exactly as 0003/0006/0007 each had to think about
            -- their own `REVOKE ... FROM PUBLIC`.
            --
            -- The three `tenant_rls_*` helpers this migration defines are
            -- granted further down, next to their own definitions.
            GRANT EXECUTE ON FUNCTION resolve_graph_account_id(uuid) TO coralswarm_tenant_app;
            GRANT EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) TO coralswarm_tenant_app;
            GRANT EXECUTE ON FUNCTION resolve_tenant_reachable_graphs(uuid, uuid) TO coralswarm_tenant_app;
            GRANT EXECUTE ON FUNCTION resolve_tenant_graph_reachable_by_user(uuid, uuid, uuid) TO coralswarm_tenant_app;
            GRANT EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) TO coralswarm_tenant_app;
            GRANT EXECUTE ON FUNCTION resolve_session_share_account_id(text) TO coralswarm_tenant_app;

            -- ── ...and close the hole the allowlist alone does NOT close ────
            -- Dropping the schema-wide grant is necessary but NOT sufficient,
            -- which only became visible once the allowlist was covered by a
            -- test (`execute_is_an_allowlist_not_a_schema_wide_grant`): a
            -- freshly created SECURITY DEFINER function was STILL executable by
            -- `coralswarm_tenant_app` afterwards. Postgres grants EXECUTE on
            -- every new function to PUBLIC by default, and the app role is a
            -- member of PUBLIC — so the default ACL, not this migration's
            -- grant, is what actually makes a future helper reachable.
            --
            -- That is exactly why 0003, 0006 and 0007 each end with a
            -- hand-written `REVOKE EXECUTE ... FROM PUBLIC` on their own two
            -- functions. Doing it once, as a default, means the next author
            -- cannot forget: a new function in `public` starts with no PUBLIC
            -- EXECUTE at all, and reaching it from the app role requires an
            -- explicit grant in the migration that adds it.
            --
            -- NOTE THE MISSING `IN SCHEMA public`, IT IS NOT AN OVERSIGHT.
            -- `ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON
            -- FUNCTIONS FROM PUBLIC` is accepted by Postgres, reports
            -- `ALTER DEFAULT PRIVILEGES`, writes NO `pg_default_acl` row, and
            -- has NO effect — a newly created function is still PUBLIC-
            -- executable afterwards. Verified directly on PG 17 while writing
            -- this. The reason is that PUBLIC's EXECUTE default lives in the
            -- schema-INDEPENDENT default (`defaclnamespace = 0`); an
            -- `IN SCHEMA` clause creates a per-schema override and revoking a
            -- grant that is not present in that override is a no-op, leaving
            -- the global default untouched. The schema-less form below writes
            -- `{owner=X/owner}` and actually works.
            --
            -- This is worth the paragraph because the broken form is the one
            -- that looks right, matches the `IN SCHEMA public` style of every
            -- other statement in this block, and fails silently — precisely the
            -- shape of defect this whole migration exists to backstop. It was
            -- caught only because
            -- `execute_is_an_allowlist_not_a_schema_wide_grant` asserts the
            -- OUTCOME (a new SECURITY DEFINER function is not callable) rather
            -- than asserting that the statement was written.
            --
            -- Scope and limits, since this default outlives the migration:
            --   * Applies only to objects created FROM NOW ON. Existing
            --     functions — including every pgvector operator in the baseline
            --     — keep the PUBLIC grant they already have.
            --   * Applies only to objects created BY THIS ROLE (no `FOR ROLE`
            --     clause). A future migration run under a DIFFERENT owner role
            --     would create functions with the PUBLIC default intact. That
            --     residual is why the test above asserts the outcome directly
            --     instead of trusting this statement — the test is
            --     role-agnostic, this statement is not.
            --   * If a future `CREATE EXTENSION` / `ALTER EXTENSION ... UPDATE`
            --     runs under this owner, its new functions also start without
            --     PUBLIC EXECUTE and the app role would 42501 until granted.
            --     Loud, and caught by any smoke test — whereas the failure this
            --     replaces is silent and is an RLS bypass.
            ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

            -- ── Narrow the broad grant where it is cheap and unambiguous ────
            -- `tenant_meta` holds this database's org fingerprint — the single
            -- row `registry::TenantRegistry` checks before it will publish a
            -- pool ("swapped-DSN / two-orgs-one-DB defense"). An app-layer
            -- compromise that could REWRITE it would defeat that check. Every
            -- Rust reference is a read (`SELECT org_id FROM tenant_meta` in
            -- `registry.rs`, `migrate.rs`, `bin/export_member_ocean.rs`);
            -- the only writer is `scripts/onboard_tenant.sh`, running as the
            -- owner. SELECT stays; INSERT/UPDATE/DELETE do not.
            REVOKE INSERT, UPDATE, DELETE ON tenant_meta FROM coralswarm_tenant_app;

            -- `_sqlx_migrations` is sqlx's own ledger — written only by
            -- `sqlx migrate run` / the `migrate_tenants` orchestrator, under
            -- their own owner connection. The live app never touches it.
            REVOKE ALL ON _sqlx_migrations FROM coralswarm_tenant_app;

            -- `supersession_edge_cross_ocean_deletions` (migration 0008, PR
            -- #349) — the forensic record of cross-ocean supersession edges
            -- that PR deleted. See §6(f): it stores BOTH endpoints' node ids
            -- AND both endpoints' graph ids by design, which on this plane are
            -- member-private ocean identifiers, and identifier disclosure
            -- across this boundary is exactly what #348 had to close. 0008's
            -- header places it "in the same category as `_sqlx_migrations`" —
            -- no application reader on any plane, read by a human (i.e. on the
            -- owner connection, which is exempt from RLS) during an incident —
            -- so the app role needs no access at all, and the schema-wide
            -- GRANT above must not be allowed to hand it one.
            --
            -- Revoked rather than policy'd deliberately: 0008 asks that a
            -- future application reader "must get a policy in the same PR", and
            -- a revoke is what makes that self-enforcing. A policy would let
            -- such a reader silently receive filtered rows and never discover
            -- the question had been asked; a revoke gives it a loud 42501 and
            -- forces the decision back to this file.
            REVOKE ALL ON supersession_edge_cross_ocean_deletions FROM coralswarm_tenant_app;

            EXIT; -- success
        EXCEPTION
            WHEN unique_violation           -- 23505: two CREATE ROLEs raced
              OR duplicate_object           -- 42710: same race, other spelling
              OR SQLSTATE 'XX000' THEN      -- tuple concurrently updated
            -- See this section's header for why each of these three is a
            -- shared-catalog race and why the list is not `WHEN OTHERS`.
            attempt := attempt + 1;
            IF attempt >= 10 THEN
                RAISE;
            END IF;
            -- Jittered backoff. A fixed 0.05*attempt ramp makes two racing
            -- sessions retry in lockstep and collide again on the same
            -- iteration; the random component breaks that up.
            PERFORM pg_sleep(0.05 * attempt + random() * 0.05);
        END;
    END LOOP;
END
$$;

-- ── 2. Session-identity helpers ─────────────────────────────────────────────
-- Three tiny functions rather than inline `current_setting` calls in 34
-- policies: the parsing rule (§3's `missing_ok`, empty-string handling) lives
-- in one place, and the reachability predicate itself becomes replaceable by a
-- single `CREATE OR REPLACE FUNCTION` — the same rolling-deploy property
-- 0006's header argues for at length (old and new pods observe one atomic DDL
-- identically; there is no window where they disagree).

-- The acting end user, or NULL when unscoped.
--
-- `NULLIF(..., '')` is not defensive padding: `app.*` is an unregistered
-- "placeholder" GUC class, so once a session has set it ONCE, it does not
-- revert to undefined outside that transaction — it reverts to the empty
-- string, Postgres's session-invented base value for a placeholder. A pooled
-- connection whose previous borrower ran a scoped transaction therefore sees
-- `''`, not "unset", on its next unscoped transaction. Without the NULLIF that
-- would be `''::uuid`, a hard error; with it, it is NULL, which denies exactly
-- the same rows an unset GUC denies. Consistent, and never the PREVIOUS
-- borrower's user id (`set_config(..., true)` is transaction-local, so the real
-- value never survives the commit).
CREATE OR REPLACE FUNCTION tenant_rls_actor_user_id()
RETURNS uuid
LANGUAGE sql
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT NULLIF(current_setting('app.user_id', true), '')::uuid
$$;

-- Whether this transaction is running as the tenant service principal (§4).
-- Any value other than the exact string 'system' — including unset and '' —
-- is false.
CREATE OR REPLACE FUNCTION tenant_rls_is_system()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT COALESCE(NULLIF(current_setting('app.tenant_actor', true), '') = 'system', false)
$$;

-- THE predicate. Every policy in this migration resolves to this set.
--
-- `SECURITY DEFINER` is load-bearing, not decorative (§7.2): it makes the reads
-- of `graphs`/`teams`/`team_members` below run as the function's owner, who is
-- exempt from `ENABLE`-only RLS, which is what breaks the cycle `graphs`'
-- policy -> this function -> `graphs`. Without it Postgres raises "infinite
-- recursion detected in policy for relation graphs" on the first query. It also
-- means this function must never widen: it returns ONLY ids the actor may see,
-- so a caller gains nothing by invoking it directly.
--
-- Mirrors `resolve_tenant_reachable_graphs` (0006) except for `archived_at`
-- (§5) and the account filter — there is exactly one account's rows in this
-- database, and filtering on it would be the §1 no-op.
CREATE OR REPLACE FUNCTION tenant_rls_visible_graph_ids()
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id
    FROM graphs g
    WHERE tenant_rls_is_system()
       OR (
            (g.kind <> 'member' OR g.owner_user_id = tenant_rls_actor_user_id())
        AND (g.kind <> 'team'   OR EXISTS (
                SELECT 1
                FROM teams t
                JOIN team_members tm ON tm.team_id = t.id
                WHERE t.graph_id = g.id
                  AND tm.user_id = tenant_rls_actor_user_id()
                  AND tm.departing_at IS NULL
            ))
       )
$$;

REVOKE EXECUTE ON FUNCTION tenant_rls_actor_user_id() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION tenant_rls_is_system() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION tenant_rls_visible_graph_ids() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tenant_rls_actor_user_id() TO coralswarm_tenant_app;
GRANT EXECUTE ON FUNCTION tenant_rls_is_system() TO coralswarm_tenant_app;
GRANT EXECUTE ON FUNCTION tenant_rls_visible_graph_ids() TO coralswarm_tenant_app;

-- ── 3. RLS + policies ───────────────────────────────────────────────────────
-- `CREATE POLICY` has no `IF NOT EXISTS`; `DROP POLICY IF EXISTS` first is the
-- standard idempotent idiom (same convention as 0083). A bare policy — no
-- `FOR`, no `WITH CHECK` — is `FOR ALL` and reuses `USING` as the `WITH CHECK`
-- expression, so SELECT/INSERT/UPDATE/DELETE are all scoped by one predicate.
-- That is correct for every table here whose predicate anchors to exactly ONE
-- ocean; `graphs` and `node_promotions` are the two exceptions and are written
-- out explicitly.

-- ── 3a. The anchor ──────────────────────────────────────────────────────────
-- `graphs` cannot use the bare form. Its `USING` is a self-referencing subquery
-- (`id IN (visible)`), and reusing that as `WITH CHECK` would evaluate the
-- freshly-INSERTed row's own id against a snapshot of `graphs` — fragile at
-- best. The write rule is expressible directly against the NEW row's columns
-- and is stated that way:
--
--   * a member ocean may only be created/left owned by the acting user;
--   * a team ocean has no owner column to check, and creating one is a
--     legitimate act (the creator becomes its first member via `teams`/
--     `team_members`, which the read predicate then gates);
--   * org/personal oceans are unrestricted within the tenant.
--
-- The read side still gates team oceans on membership — a non-member cannot
-- SEE a team ocean's row, which is what makes every `graph_id`-anchored policy
-- below inherit team gating for free.
ALTER TABLE graphs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON graphs;
CREATE POLICY member_ocean_isolation ON graphs
    USING (id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (
        tenant_rls_is_system()
        OR kind <> 'member'
        OR owner_user_id = tenant_rls_actor_user_id()
    );

-- ── 3b. Direct `graph_id`, NOT NULL, one hop to `graphs` (19) ───────────────
-- The core of the backstop. These tables are read directly by `graph_id` (or by
-- their own primary key) all over the codebase WITHOUT joining `graphs` — RLS
-- is evaluated per TABLE, so `graphs`' policy is never consulted for those
-- reads, and an unprotected `graph_id`-keyed content table is exactly the
-- app-layer-bug leak this exists to catch.

ALTER TABLE atoms ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON atoms;
CREATE POLICY member_ocean_isolation ON atoms
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE nodes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON nodes;
CREATE POLICY member_ocean_isolation ON nodes
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE relationship_edge ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON relationship_edge;
CREATE POLICY member_ocean_isolation ON relationship_edge
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- `supersession_edge.graph_id` arrived in migration 0008 (PR #349) — NOT NULL,
-- `REFERENCES graphs(id) ON DELETE CASCADE`, backfilled from the superseded
-- endpoint after every cross-ocean row was recorded to
-- `supersession_edge_cross_ocean_deletions` and deleted. It is therefore
-- shaped exactly like `relationship_edge.graph_id` and anchored the same way.
-- See §3d for why the endpoint-hop this replaces was strictly weaker.
ALTER TABLE supersession_edge ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON supersession_edge;
CREATE POLICY member_ocean_isolation ON supersession_edge
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE node_merges ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON node_merges;
CREATE POLICY member_ocean_isolation ON node_merges
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- `graph_id` present but carrying no FK to `graphs` ("soft", 0056's header) —
-- the subquery works identically whether or not it is FK-enforced.
ALTER TABLE meeting_transcripts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON meeting_transcripts;
CREATE POLICY member_ocean_isolation ON meeting_transcripts
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- Same "soft" `graph_id` as `meeting_transcripts`.
ALTER TABLE transcript_corrections ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON transcript_corrections;
CREATE POLICY member_ocean_isolation ON transcript_corrections
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE meeting_shares ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON meeting_shares;
CREATE POLICY member_ocean_isolation ON meeting_shares
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON projects;
CREATE POLICY member_ocean_isolation ON projects
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE coding_sessions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON coding_sessions;
CREATE POLICY member_ocean_isolation ON coding_sessions
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE coding_session_shares ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON coding_session_shares;
CREATE POLICY member_ocean_isolation ON coding_session_shares
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- The team ocean's wrapped content key. Anchored on `graph_id`, so a member who
-- cannot see the team ocean's `graphs` row cannot see its key material either.
ALTER TABLE team_keys ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON team_keys;
CREATE POLICY member_ocean_isolation ON team_keys
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE conversation_oceans ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON conversation_oceans;
CREATE POLICY member_ocean_isolation ON conversation_oceans
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- §6(b): the consumer plane anchors `conversations` on `account_id`; here that
-- would be the no-op. `graph_id` is NOT NULL and FK'd, so it is the anchor.
ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON conversations;
CREATE POLICY member_ocean_isolation ON conversations
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE query_history ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON query_history;
CREATE POLICY member_ocean_isolation ON query_history
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE outlook_watched_conversations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON outlook_watched_conversations;
CREATE POLICY member_ocean_isolation ON outlook_watched_conversations
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE outlook_workflows ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON outlook_workflows;
CREATE POLICY member_ocean_isolation ON outlook_workflows
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE teams_watched_resources ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON teams_watched_resources;
CREATE POLICY member_ocean_isolation ON teams_watched_resources
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- §6(b): `teams` also carries `account_id` (the consumer plane's anchor, the
-- no-op here). `teams.graph_id` points AT the team ocean, so anchoring there
-- means a non-member cannot enumerate the ocean ids of teams they do not
-- belong to — the "team-ocean gate" bug class, closed at the row level. Note
-- the visible-graph function reads `teams` itself; that read is inside a
-- SECURITY DEFINER body and is therefore not re-filtered by this policy, which
-- is what keeps it from recursing.
ALTER TABLE teams ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON teams;
CREATE POLICY member_ocean_isolation ON teams
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- ── 3c. Direct `graph_id`, NULLABLE (4) ─────────────────────────────────────
-- §6(c): a NULL anchor yields NULL, i.e. not-true, i.e. the row is hidden.
-- Deliberate and fail-closed; the cutover runbook counts these rows per tenant
-- before cutover so the consequence is measured, not assumed.

ALTER TABLE activity_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON activity_events;
CREATE POLICY member_ocean_isolation ON activity_events
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE llm_usage ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON llm_usage;
CREATE POLICY member_ocean_isolation ON llm_usage
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

ALTER TABLE mcp_tool_calls ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON mcp_tool_calls;
CREATE POLICY member_ocean_isolation ON mcp_tool_calls
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- `op_log` — the sync ledger, and the surface the 2026-07-25 `sync/pull`
-- member-ocean leak came through. NULLABLE `graph_id` AND no FK to `graphs`
-- (verified against 0001's constraint section — the prose elsewhere claiming
-- CASCADE coverage for this table is wrong). See §6(c).
ALTER TABLE op_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON op_log;
CREATE POLICY member_ocean_isolation ON op_log
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- ── 3d. One further hop, then to `graphs` (6) ───────────────────────────────
-- The nested subqueries below run in INVOKER context, so the inner table's own
-- policy also applies — the filtering is consistent, not a fresh policy
-- surface. (Only `tenant_rls_visible_graph_ids()` is SECURITY DEFINER.)

ALTER TABLE node_atoms ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON node_atoms;
CREATE POLICY member_ocean_isolation ON node_atoms
    USING (node_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- `supersession_edge` USED to live here, anchored through `superseded_id ->
-- nodes`. Migration 0008 (PR #349) gave it a direct `graph_id` — NOT NULL,
-- `REFERENCES graphs(id) ON DELETE CASCADE`, backfilled from the superseded
-- endpoint, with the cross-ocean rows recorded and deleted first — so it is now
-- anchored DIRECTLY in §3b above, exactly like `relationship_edge`.
--
-- Worth stating why that is a real upgrade and not a cosmetic one: the hop
-- version was only correct under the assumption that an edge's two endpoints
-- always share a graph, and 0008's own header is explicit that this "was an
-- assumption, never a constraint" — an assumption the supersede route violated
-- in production. Anchoring on a NOT NULL FK'd column instead means the policy
-- no longer depends on an invariant some other code path has to keep. It is
-- also strictly cheaper: one column read instead of a nested subquery on
-- `nodes` (itself RLS'd) per row.

-- `non_supersession_pair` still needs the hop: it has no `graph_id` of its own
-- (0008 gave one only to `supersession_edge`). Its two endpoints ARE same-graph
-- by construction here — unlike `supersession_edge`, its only producer is the
-- same-graph "mark these two as not-a-supersession" path — but note this is the
-- same class of assumption 0008 just had to retire on the sibling table. Giving
-- it a real `graph_id` column is the obvious follow-up; anchoring on
-- `node_a_id` is correct in the meantime because `nodes` is itself RLS'd, so
-- the subquery cannot see an endpoint in an unreachable ocean.
ALTER TABLE non_supersession_pair ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON non_supersession_pair;
CREATE POLICY member_ocean_isolation ON non_supersession_pair
    USING (node_a_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- `conversations` is itself ocean-anchored (3b), so one hop is enough.
ALTER TABLE conversation_messages ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON conversation_messages;
CREATE POLICY member_ocean_isolation ON conversation_messages
    USING (conversation_id IN (
        SELECT id FROM conversations
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

ALTER TABLE conversation_surfaced_edges ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON conversation_surfaced_edges;
CREATE POLICY member_ocean_isolation ON conversation_surfaced_edges
    USING (conversation_id IN (
        SELECT id FROM conversations
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON team_members;
CREATE POLICY member_ocean_isolation ON team_members
    USING (team_id IN (
        SELECT id FROM teams
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- Per-member wrapped copies of a team key. Anchored two hops back to the team
-- ocean, NOT on `user_id`: the boundary is "can you reach this team's ocean",
-- and a `user_id = me` predicate would additionally break the admin re-wrap
-- path that writes other members' envelopes.
ALTER TABLE team_member_key_envelopes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON team_member_key_envelopes;
CREATE POLICY member_ocean_isolation ON team_member_key_envelopes
    USING (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- ── 3e. Two anchors — READ permissively (either), WRITE strictly (both) ─────
-- §6(e). Identical shape and identical reasoning to 0083's `node_promotions`
-- split, with the boundary swapped from account to ocean: a promotion crosses
-- two oceans, so it should be READABLE from either side, but a bare
-- `USING`-only policy is `FOR ALL` and reuses `USING` as `WITH CHECK`, which
-- would let the owner of the source ocean insert a row whose `target_graph_id`
-- is another member's private ocean — writing into a reef they cannot even
-- read. Four policies, not one.
--
-- Requiring BOTH anchors for writes rejects no legitimate write: an intra-tenant
-- promotion is performed by a user who can reach both ends (that is what the
-- app's own promotion authz checks), and CROSS-tenant promotion is a separate
-- cross-DB service that writes the TARGET database's `node_promotions` with
-- fresh local ids under its own connection.
ALTER TABLE node_promotions ENABLE ROW LEVEL SECURITY;
-- All FIVE names, not just the bare one. `member_ocean_isolation` is dropped
-- because an earlier revision of this migration created a single bare policy
-- here before the command split; the four `_select`/`_insert`/`_update`/
-- `_delete` names are dropped because THIS migration creates them below, and
-- `CREATE POLICY` has no `IF NOT EXISTS` — so without these four lines a
-- re-run (a manual re-apply during recovery, a scratch DB that already has
-- them) fails with "policy already exists" partway through, leaving the table
-- with SELECT policies and no write policies. Every other table in this
-- migration is idempotent by the same DROP-then-CREATE idiom; this was the one
-- place the idiom was applied to only one of the five names.
DROP POLICY IF EXISTS member_ocean_isolation ON node_promotions;
DROP POLICY IF EXISTS member_ocean_isolation_select ON node_promotions;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON node_promotions;
DROP POLICY IF EXISTS member_ocean_isolation_update ON node_promotions;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON node_promotions;

CREATE POLICY member_ocean_isolation_select ON node_promotions
    FOR SELECT
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR target_graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );

CREATE POLICY member_ocean_isolation_insert ON node_promotions
    FOR INSERT
    WITH CHECK (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        AND target_graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );

-- `USING` permissive so an existing legitimate row stays reachable to update
-- from either recorded anchor; `WITH CHECK` strict so it can never be LEFT
-- pointing at an ocean the actor cannot reach.
CREATE POLICY member_ocean_isolation_update ON node_promotions
    FOR UPDATE
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR target_graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    )
    WITH CHECK (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        AND target_graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );

-- DELETE has no new row, so only `USING` applies. Strict here too: the merge /
-- unmerge repoint paths only ever delete rows wholly within reachable oceans.
CREATE POLICY member_ocean_isolation_delete ON node_promotions
    FOR DELETE
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        AND target_graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );

-- ── 3f. Deliberately NOT covered (see §6(f) for each reason) ────────────────
-- account_keys, connector_identities, graph_subscriptions, jobs, oauth_tokens,
-- queries, slack_huddle_captures, slack_integrations, sync_cursors,
-- tenant_meta — no policy added.
