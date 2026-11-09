//! Background job definitions.
//!
//! The items exported here are public API consumed by future worker stories.
//! They are not yet wired to callers in `main.rs`, so we suppress the
//! dead-code lint at the module level.
#![allow(dead_code)]

pub mod queue;
pub mod publication;

pub const STANDING_EXTRACTION_KINDS: &[&str] = &["ExtractStandingSourceAtoms", "ExtractStandingMeetingAtoms"];

/// All job kinds the system can enqueue.
///
/// Serialised with `serde`'s internally-tagged representation so a row in the
/// `jobs` table stores `kind` as a top-level field and `payload` as the
/// content — matching the DB schema (`kind TEXT`, `payload JSONB`).
///
/// Retired kinds (`ExtractEntities`, `ExtractRelationships`, `IngestDoor`,
/// `ConflictDetection`, `InferExpertise`, `ReevaluateKind`, `ExtractThreads`,
/// `ExtractDecisions`) have no variant, so
/// no Rust path can construct or enqueue one, and a payload naming one fails
/// to deserialize. Their historical `jobs` rows stay dead-lettered; nothing
/// dequeues those kind strings. See `managed_history::RETIRED_JOB_KINDS`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum JobKind {
    /// One finite evidence batch for a standing source connection.
    PublishSource { connection_id: uuid::Uuid, atom_ids: Vec<uuid::Uuid> },
    /// Compute and store embeddings for a batch of atoms.
    EmbedAtoms { atom_ids: Vec<uuid::Uuid> },
    /// Compute and store embeddings for a batch of atoms `workers::embed_retry`'s
    /// sweep just atomically claimed (bumped `embed_failed_at` to `now()` as
    /// its claim marker — see that module's doc comment).
    ///
    /// A SEPARATE variant from [`JobKind::EmbedAtoms`], not the same one with
    /// a flag, so every OTHER existing enqueue site is untouched. The reason
    /// it must be distinct at all: `workers::embed::load_pending_atoms`'s
    /// candidate query re-applies the backoff-eligibility check to whatever
    /// atom_ids a job names — correct for an ordinary `EmbedAtoms` job (whose
    /// atom_ids weren't pre-vetted), but wrong for atom_ids the retry sweep
    /// JUST claimed: the claim's own `embed_failed_at = now()` bump makes
    /// those exact atom_ids look freshly-failed, so re-applying the backoff
    /// check to them would reject every one and the job would complete empty
    /// — the atom is claimed but never actually retried, and the next sweep
    /// just repeats the cycle forever (CodeRabbit review, PR #194 round 2).
    /// `workers::embed::process_job` dispatches this variant through the
    /// exact same pipeline as `EmbedAtoms`, with one difference: its
    /// `load_pending_atoms` call bypasses the backoff check for these
    /// specific, already-vetted atom_ids (still requires `embedding IS
    /// NULL` — an atom a racing job already embedded is correctly excluded
    /// either way).
    EmbedAtomsRetry { atom_ids: Vec<uuid::Uuid> },
    /// Embed the statements of one source version's semantic atoms for
    /// scoped Ask's statement-vector leg (`semantic_atom_embeddings`). Sent
    /// only through the managed `cloud_embedding` purpose
    /// (`managed::embedding::process_statements`): every statement is
    /// receipted, under the account's daily cap. Enqueued when extraction
    /// persists atoms (`workers::source_atoms`) and by the backfill sweep
    /// (`workers::statement_embed`), keyed `embed_statements:<version>`.
    ///
    /// The version field is deliberately not `source_version_id`: extraction
    /// code finds a version's live extraction jobs by
    /// `payload->>'source_version_id'` without a kind filter.
    EmbedStatements { graph_id: uuid::Uuid, statement_version_id: uuid::Uuid },
    /// Route a session to the right co-present collaborators.
    RouteCopresence { session_id: uuid::Uuid },
    /// Send a templated outreach email to a user.
    SendOutreach {
        user_id: uuid::Uuid,
        template: String,
    },
    /// Recompute semantic drift metrics for a graph.
    ComputeDrift { graph_id: uuid::Uuid },
    /// Resolve/merge duplicate entity reefs across a graph (ADR-0008).
    EntityResolution { graph_id: uuid::Uuid },
    /// Run the inference-closure pass over a graph's edges (ADR-0008).
    InferRelationships { graph_id: uuid::Uuid },
    /// Re-resolve ID-shaped Slack names (`author_name` / `channel_name` /
    /// `participants`) on previously-ingested atoms once a real display name
    /// becomes available — from the `connector_identities` directory or a
    /// fresh Slack API call (PR #176 follow-up: raw ids must never be
    /// user-visible, but historical atoms persisted them permanently).
    ///
    /// All fields optional so the SAME variant serves two triggers:
    ///   - Natural per-identity repair: enqueued right after
    ///     `Store::upsert_connector_identity` persists a fresh name
    ///     (`routes::slack::ingest_slack_message`), scoped to the one
    ///     `(graph_id, workspace_id, author_id)` tuple that was just resolved.
    ///   - Standalone one-shot backfill: all `None` sweeps every graph/author
    ///     with an ID-shaped name still on record — see the worker module doc
    ///     for the hand-insertable `INSERT INTO jobs` this enables.
    SlackNameBackfill {
        /// Restrict to one ocean. `None` scans across all graphs.
        graph_id: Option<uuid::Uuid>,
        /// Slack team id ("T…") — needed to key `connector_identities` and to
        /// pick the right `slack_integrations` bot token. `None` means "derive
        /// per-atom from `graphs -> accounts -> slack_integrations`" (best
        /// effort; see the worker for the single-workspace-per-account
        /// assumption this makes for the fully-unscoped sweep).
        workspace_id: Option<String>,
        /// Raw Slack user id ("U…"/"W…") to restrict the sweep to a single
        /// author's atoms. `None` matches any author.
        author_id: Option<String>,
        /// Keyset pagination cursor: only atoms with `atom_id` greater than
        /// this are candidates. `None` starts from the beginning. Set by
        /// [`crate::workers::slack_name_backfill`] itself when it re-enqueues
        /// a continuation so a full sweep ALWAYS advances past the current
        /// window — including one that made zero repair progress — instead of
        /// re-selecting the same stuck head-of-line atoms forever. Callers
        /// hand-inserting a one-shot backfill should always pass `null` here.
        after_atom_id: Option<uuid::Uuid>,
    },
    /// One-time bounded sweep repairing `atoms.content_tsv` rows written
    /// under the old per-language (stemming) regconfig instead of 'simple'
    /// — see migration 0062's header and `workers::content_tsv_backfill`'s
    /// module doc for the full bug writeup and why this replaced that
    /// migration's original unbounded, unbatched full-table `UPDATE`
    /// (CodeRabbit review, PR #190).
    ///
    /// Not enqueued automatically by any trigger — an operator hand-inserts
    /// a single unscoped sweep with `after_atom_id: None` after deploying
    /// the write-side fix, mirroring `SlackNameBackfill`'s documented
    /// standalone one-shot pattern.
    /// Generate `atoms.title` (migration 0097/0010) for corals that don't have
    /// one yet — both the historical backlog and anything ingested since.
    ///
    /// Titles are what let date-range recall answer from ~10-word headlines
    /// instead of full bodies, so a "what did we work on today and yesterday?"
    /// answer can cover EVERY coral in the range. See
    /// `services::atom_title` for why they are per-coral rather than
    /// per-session, and `workers::atom_title_backfill` for the sweep.
    ///
    /// Sweep-shaped exactly like [`JobKind::ContentTsvRegconfigRepair`]: an
    /// operator hand-inserts one account-less seed, which fans out a child per
    /// account (each child is what carries the RLS scope), and every child
    /// enqueues its own continuation until its account is drained.
    ///
    /// ```sql
    /// INSERT INTO jobs (kind, payload)
    /// VALUES ('BackfillAtomTitles', '{"after_atom_id": null}'::jsonb);
    /// ```
    ///
    /// Unlike the `content_tsv` repair this is NOT purely a one-time backfill:
    /// the same sweep titles newly-ingested corals, because its candidate
    /// predicate is `title IS NULL` rather than a fixed historical window. It
    /// is safe to re-insert the seed at any time — a coral that already has a
    /// title is never a candidate, so a re-run costs one empty batch per
    /// account.
    ///
    /// Inserting it by hand is now a way to force an IMMEDIATE pass, not the
    /// only thing that ever starts one: the worker re-seeds itself whenever it
    /// finds the queue idle and `AUTO_SEED_INTERVAL` has elapsed. Before that
    /// existed, a chain terminated when it ran out of candidates and nothing
    /// re-armed it, so corals ingested afterwards stayed untitled until an
    /// operator noticed — see `workers::atom_title_backfill`'s module doc.
    BackfillAtomTitles {
        /// Keyset pagination cursor: only atoms with `atom_id` greater than
        /// this are candidates. `None` starts from the beginning. Set by
        /// [`crate::workers::atom_title_backfill`] when it enqueues a
        /// continuation, so a sweep always advances and terminates — same
        /// contract as `ContentTsvRegconfigRepair::after_atom_id`.
        after_atom_id: Option<uuid::Uuid>,
    },
    ContentTsvRegconfigRepair {
        /// Keyset pagination cursor: only atoms with `atom_id` greater than
        /// this are candidates. `None` starts from the beginning. Set by
        /// [`crate::workers::content_tsv_backfill`] itself when it
        /// re-enqueues a continuation, so a full sweep always advances past
        /// the current window and terminates — same reasoning as
        /// `SlackNameBackfill::after_atom_id` above.
        after_atom_id: Option<uuid::Uuid>,
    },
    /// Generate the post-meeting summary (overview + decisions + action items)
    /// for one ingested meeting transcript (cs-uxp3a-meeting-summary). Map-reduce
    /// over the transcript (cheap-tier map → capable-tier reduce). A job whose
    /// owning account cannot be resolved is left in the `blocked` summary
    /// state (no cloud call) until a per-meeting opt-in.
    ///
    /// Enqueued automatically right after `Store::upsert_meeting_transcript`
    /// persists a meeting on the ingest path, and re-enqueued on demand by
    /// `POST /v1/meetings/summary/generate` (Retry / opt-in from `blocked`).
    GenerateMeetingSummary {
        graph_id: uuid::Uuid,
        /// The BASE meeting source id (matches `meeting_transcripts.source_id`).
        source_id: String,
        /// `true` only when a user explicitly opted this one meeting into cloud
        /// synthesis from the "generate anyway?" prompt on a `blocked` summary.
        /// The default (`false`, the ingest path) marks an unresolved-owner
        /// summary `blocked`.
        #[serde(default)]
        allow_cloud_override: bool,
    },
    /// Extract semantic facts/decisions/actions/observations with exact evidence
    /// from one immutable meeting version. Continuations carry only a scalar
    /// position; source text never enters the queue payload.
    ExtractMeetingAtoms {
        graph_id: uuid::Uuid,
        source_version_id: uuid::Uuid,
        #[serde(default)]
        start_char: i64,
    },
    /// Extract exact-evidence semantic atoms from an immutable typed source
    /// version. Its stored extraction_version pins the prompt and continuation.
    ExtractSourceAtoms {
        graph_id: uuid::Uuid,
        source_version_id: uuid::Uuid,
        #[serde(default)]
        start_char: i64,
    },
    ExtractStandingSourceAtoms {
        graph_id: uuid::Uuid,
        source_version_id: uuid::Uuid,
        #[serde(default)]
        start_char: i64,
    },
    ExtractStandingMeetingAtoms {
        graph_id: uuid::Uuid,
        source_version_id: uuid::Uuid,
        #[serde(default)]
        start_char: i64,
    },
    /// Durable, server-enqueued cleanup of preserved source text. Ownership is
    /// stamped before source rows disappear; no public API accepts these keys.
    DeleteSourceBlob {
        graph_id: uuid::Uuid,
        account_id: uuid::Uuid,
        org_id: Option<String>,
        blob_key: String,
    },
    /// Propagate a transcript correction to atom content, its embeddings, and
    /// any reef it fully explained (contract §8.4 / C15,
    /// `services::corrections::propagate`) — `workers::propagate_corrections`
    /// runs it.
    ///
    /// Enqueued via `jobs::queue::enqueue_tx` in the SAME transaction as the
    /// correction write (`routes::meetings::create_speaker_relabel` /
    /// `set_correction_status`) — CodeRabbit round 2, finding 1, replacing an
    /// earlier bare `tokio::spawn` that was lost on process restart with
    /// nothing left to re-trigger it: a correction could commit `applied`
    /// while its atoms were never rewritten, permanently. Durably queuing the
    /// propagation atomically with the correction itself closes that gap the
    /// same way every other content-mutation-adjacent job in this tree does
    /// (e.g. `EmbedAtoms` after `routes::ingest`'s atom insert).
    ///
    /// `account_id` (the graph's OWNING account, not necessarily the
    /// requesting user's active account) is carried on `jobs.account_id`
    /// (org-tenancy Phase W.1), not in this payload — same convention as
    /// every other job kind here.
    PropagateCorrections {
        graph_id: uuid::Uuid,
        /// The BASE (un-chunked) meeting/source id — matches
        /// `transcript_corrections.source_id` and
        /// `CreateRelabelBody::source_id`.
        source_id: String,
    },
    /// One-time bounded sweep re-embedding every atom whose stored vector
    /// predates the currently-active `chunker_version`/`embed_model` —
    /// concretely, every pre-header (`chunker_version = 1`) atom left behind
    /// when `coralswarm_core::chunker::CHUNKER_VERSION` was bumped 1 → 2 for
    /// contextual chunk headers (PR #198), plus any atom stamped by a
    /// since-retired embedding model.
    ///
    /// Unlike `ContentTsvRegconfigRepair` (a cheap in-place SQL rewrite),
    /// this runs the REAL embedding path — see `workers::reembed`'s module
    /// doc for the full design: candidate selection, account-scoped DEK
    /// resolution (mirrors `workers::embed_retry`), best-effort contextual
    /// chunk-header reconstruction for pre-header meeting/transcript atoms
    /// (title from `meeting_transcripts` when available, participants from
    /// `atoms.participants`, date always from `atoms.timestamp`), and the
    /// content-mutation → `content_tsv` → oplog ordering that keeps a
    /// partial failure self-healing on the next sweep.
    ///
    /// Not enqueued automatically — an operator hand-inserts a single
    /// unscoped sweep with `after_atom_id: None` after deploying this PR,
    /// mirroring `ContentTsvRegconfigRepair` / `SlackNameBackfill`'s
    /// documented standalone one-shot pattern.
    ReembedOutdatedChunks {
        /// Keyset pagination cursor: only atoms with `atom_id` greater than
        /// this are candidates. `None` starts from the beginning. Set by
        /// [`crate::workers::reembed`] itself when it re-enqueues a
        /// continuation, so a full sweep always advances past the current
        /// window and terminates — same reasoning as
        /// `ContentTsvRegconfigRepair::after_atom_id` above.
        after_atom_id: Option<uuid::Uuid>,
    },
    /// Ingest one Slack huddle's AI-notes canvas (huddle capture rewrite —
    /// `docs/connectors/slack.md` §3.1/§3.3 were measured wrong, see
    /// `backend/tests/fixtures/slack_huddle/README.md`).
    ///
    /// Enqueued by `routes::slack_events::slack_events` the moment the
    /// trigger fires (`huddle_capture_trigger`: `subtype ==
    /// "message_changed"` AND `event.message.room.transcript_file_id` is
    /// present) — durably, via this queue, rather than a bare
    /// `tokio::spawn`, because the trigger fires EXACTLY ONCE per huddle:
    /// there is no natural Slack retry, so a dropped in-process task would
    /// lose the huddle's content forever. `workers::huddle::process_job` is
    /// the consumer; a retryable failure (Slack API blip, DB error) goes
    /// through `jobs::queue::fail`'s bounded backoff exactly like every
    /// other worker, and only a TERMINAL outcome (ingested, or a decided
    /// skip) is recorded in `slack_huddle_captures` (now a terminal-outcome
    /// ledger, not a queue — migration `0102_slack_huddle_captures_ledger` /
    /// tenant `0014`).
    ///
    /// Every field the worker needs is captured here at enqueue time rather
    /// than re-derived from Slack later: the webhook event is the only
    /// moment this data is guaranteed available (a later canvas/API read
    /// could race a further edit), and durability is the entire point of
    /// going through the queue instead of an in-memory spawn.
    SlackHuddleCapture {
        /// Resolved once, in the webhook, from the Slack `team_id` via
        /// `routes::slack::resolve_slack_account_id` — every real caller
        /// already knows this, so (unlike the account-less unscoped sweeps
        /// above) this job is ALWAYS stamped with `account_id` at enqueue
        /// time (see `jobs::queue::enqueue`'s `account_id` parameter).
        account_id: uuid::Uuid,
        /// Slack `team_id` — needed by
        /// `connectors::slack::resolve_channel_destinations`, which keys
        /// `slack_watched_channels` on `(account_id, workspace_id,
        /// channel_id)`.
        workspace_id: String,
        channel_id: String,
        /// `room.thread_root_ts`, falling back to `event.message.ts` — the
        /// huddle's own identity for the `(account_id, channel_id,
        /// huddle_thread_ts)` idempotency key `slack_huddle_captures` still
        /// enforces.
        huddle_thread_ts: String,
        /// The **canvas** file id — `room.transcript_file_id` (a measured
        /// Slack naming trap: it is NOT the transcript, see this variant's
        /// module-level context). `files.info` on this id resolves a fresh
        /// `url_private` to download.
        canvas_file_id: String,
        /// `room.participant_history` — the authoritative attendee list,
        /// captured at trigger time. Empty when Slack sent none; the worker
        /// then falls back to parsing raw `@U…` ids out of the canvas's own
        /// Attendees section.
        participant_history: Vec<String>,
        /// `room.date_start` / `room.date_end` (unix seconds) — the
        /// huddle's actual start/end, used for atom metadata (duration).
        /// `None` when Slack omitted either.
        date_start: Option<i64>,
        date_end: Option<i64>,
    },

    /// Reconcile team-ocean seats against the CURRENT roster of every
    /// seat-bearing Slack channel — the backstop for everything the live
    /// `member_joined_channel` / `member_left_channel` events miss.
    ///
    /// Three gaps this closes, none of which the event path can:
    ///   * **Channels routed before seat grants existed.** Their members hold
    ///     seats with no provenance, so `seats::revoke_seat_grant`'s gate
    ///     correctly refuses to ever revoke them. A sweep re-asserts the
    ///     grants and makes them revocable.
    ///   * **Channels the bot joined before the destination picker shipped.**
    ///     No prompt ever fired, so their members were never discovered.
    ///   * **Events missed while the backend was down**, or that Slack never
    ///     delivered.
    ///
    /// Safe to hand-insert in prod, same shape as `SlackNameBackfill`:
    ///
    /// ```sql
    /// INSERT INTO jobs (kind, payload)
    /// VALUES ('SlackSeatReconcile', '{"account_id": null, "channel_id": null}'::jsonb);
    /// ```
    SlackSeatReconcile {
        /// Restrict to one account. `None` sweeps every account with a
        /// seat-bearing channel — the unscoped one-shot above.
        account_id: Option<uuid::Uuid>,
        /// Restrict to one channel. `None` sweeps every seat-bearing channel
        /// in scope.
        channel_id: Option<String>,
    },

    /// "Email the unmatched" (chat-per-user plan PR 6,
    /// `docs/plans/chat-per-user-ingest-and-in-chat-ask.md` §8 row 6): walk
    /// one Slack channel's roster and nudge every `Unmatched` member's
    /// verified email — never `Unavailable`/`Bot`/`Matched` — to create a
    /// CoralSwarm account. `workers::slack_unmatched_email::discover_and_
    /// notify` (via `connectors::slack_unmatched_email::discover_unmatched_
    /// recipients`) is the consumer.
    ///
    /// Enqueued at the SAME gate event that already posts the destination
    /// and backfill prompts — `routes::slack_events::handle_member_joined_
    /// for_account`'s "bot added to channel" branch — because that IS the
    /// spec's step 3 ("we tell them it works by adding the agent to a
    /// container") and this is step 5 ("we email the people ... who have not
    /// linked yet"); both fire off the identical observable act (contract
    /// I1). One job per (account, channel) self-join, so a channel with a
    /// large roster does not block the webhook's own response.
    ///
    /// Idempotency is NOT handled at the queue layer here (unlike
    /// `RegenerateDigest`'s `digest:{node_id}` key) — the persisted,
    /// atomically-claimed per-recipient cooldown
    /// (`services::unmatched_recipient_email::claim_notice_slot`,
    /// `slack_unmatched_email_notices`) is what makes a duplicate job (a
    /// redelivered event, a re-run backfill) harmless: every recipient it
    /// discovers still only gets emailed once per cooldown window, however
    /// many times this job kind runs against the same channel.
    EmailUnmatchedSlackChannelMembers {
        account_id: uuid::Uuid,
        /// Slack `team_id`.
        workspace_id: String,
        channel_id: String,
    },

    /// Chat invite pass (org-onboarding plan item 22, amended by D21).
    /// One job per member connect: walk the spaces that member's own token
    /// can see and email the three audiences, but only after
    /// `directory.readonly` is on the grant and `people.listDirectoryPeople`
    /// returns a real directory. Enqueued from the Google Chat OAuth
    /// callback (`workers::google_chat_invite::enqueue_on_connect`). The
    /// per-recipient cooldown on `slack_unmatched_email_notices` is what
    /// makes a second connect in the same window harmless.
    ChatInvite {
        account_id: uuid::Uuid,
        /// The member who just connected. Their `google_chat` token is the
        /// reader. The elected space-reader capture path is not used here.
        owner_user_id: uuid::Uuid,
    },

    /// Reconcile team-ocean seats against the CURRENT roster of every
    /// seat-bearing Microsoft Teams channel — the Teams analog of
    /// `SlackSeatReconcile` above (m365 Train 2 PR 2.4, decision #3). Unlike
    /// Slack, Teams has no live channel-roster-membership event at all, so
    /// this worker is the ONLY seating mechanism, not merely a backstop for
    /// one — see `workers::teams_seat_reconcile`'s own module doc, including
    /// why its discovery query is deliberately empty in production today (no
    /// Teams destination picker yet routes a channel into a named team
    /// ocean).
    ///
    /// Safe to hand-insert in prod, same shape as `SlackSeatReconcile`:
    ///
    /// ```sql
    /// INSERT INTO jobs (kind, payload)
    /// VALUES ('TeamsSeatReconcile', '{"account_id": null, "channel_id": null}'::jsonb);
    /// ```
    TeamsSeatReconcile {
        /// Restrict to one account. `None` sweeps every account with a
        /// seat-bearing channel — the unscoped one-shot above.
        account_id: Option<uuid::Uuid>,
        /// Restrict to one channel. `None` sweeps every seat-bearing channel
        /// in scope.
        channel_id: Option<String>,
    },

    /// Walk one Microsoft team's roster on a connected member's delegated
    /// token and email the three invite audiences. `workers::teams_unmatched_
    /// email` is the consumer. Enqueued when a member connects and again
    /// from `workers::teams_seat_reconcile` after a later roster walk — the
    /// connect-time pass (`microsoft::email_unmatched_joined_teams`) is not
    /// the only trigger.
    ///
    /// There is no app-token fallback. A missing member token fails the job.
    /// The notice claim uses `provider = 'microsoft'`.
    ///
    /// Idempotency is the pending-only key `teams-unmatched:{account}:{team}`.
    /// The per-recipient cooldown still blocks a duplicate send after the
    /// job completes and a later walk enqueues it again.
    EmailUnmatchedTeamsMembers {
        account_id: uuid::Uuid,
        /// AAD tenant id. Empty is a blank home tenant: foreign rows are not
        /// treated as guests. The worker still walks the team.
        tenant_id: String,
        /// Graph team id.
        team_id: String,
    },

    /// (Re)generate one reef's digest — `nodes.body.digest.{summary,
    /// excerpts}` — via `workers::digest` (graph-temporal-memory plan,
    /// Workstream A, §2.5).
    ///
    /// Enqueued ONLY by the `nodes_maybe_enqueue_digest_regeneration` SQL
    /// trigger (migration 0109/0019), never from application code — see
    /// that migration's header for why the enqueue decision itself lives in
    /// the database rather than at a hand-found set of `Store` call sites.
    /// The trigger stamps `payload = {"node_id": ..., "graph_id": ...}` with
    /// `jsonb_build_object`, which is why this variant's field NAMES and
    /// TYPES must stay exactly `node_id: Uuid` / `graph_id: Uuid` — a
    /// mismatch would silently fail every enqueue's deserialization inside
    /// `process_job`, not at compile time.
    ///
    /// Idempotency key is the constant `digest:{node_id}` (§2.5), scoped to
    /// PENDING jobs via the existing partial-unique-index dedup pattern
    /// (migration 0050) — a burst of corals landing on the same reef
    /// coalesces into one pending job rather than one per write.
    RegenerateDigest {
        node_id: uuid::Uuid,
        graph_id: uuid::Uuid,
    },

    /// Ingest one Slack channel message as atom(s) — the durable tenant
    /// hand-off (org-slack step 2, `docs/plans/org-connectors-and-channel-
    /// ingest.md` §9c). `routes::slack_events::slack_events` no longer calls
    /// `routes::slack_events::ingest_slack_message` inline from its
    /// background spawn: it resolves every `connector_installs` account this
    /// workspace matches, decides ONE plane per account (consumer, or a
    /// specific org's tenant database — `routes::slack_events::
    /// enqueue_slack_message_ingest`'s doc comment has the full routing
    /// decision table), and enqueues THIS job onto that plane's own `jobs`
    /// table. `workers::slack_ingest`'s tenant-looped worker is the consumer,
    /// dequeuing from whichever plane the job landed on and calling the SAME
    /// `ingest_slack_message` the webhook used to call directly — see that
    /// function's own doc comment for why it is plane-explicit now.
    ///
    /// **Content-at-rest decision (PR-ready commit message has the full
    /// reasoning):** `text` is the raw Slack message body, landing in this
    /// job's `payload` column (JSONB, PLAINTEXT) rather than going straight
    /// into an encrypted atom blob — a NEW at-rest location for message
    /// content that did not exist before this hand-off, and `jobs` rows are
    /// retained indefinitely once `done` (migration `0050`'s own header:
    /// "retained (status='done')", no purge worker exists for this table).
    /// Mitigated at the worker: `jobs::queue::complete_with_payload` is used
    /// instead of the plain `complete()` on a successful `SlackMessageIngest`
    /// run, overwriting `payload` with a redacted marker (`text` cleared,
    /// everything else kept for observability) so the plaintext does not
    /// persist past this job's own processing. A `dead`-lettered job (ingest
    /// permanently failed) is NOT scrubbed — an operator may need the body to
    /// diagnose or hand-replay it — an accepted, narrow residual, unlike the
    /// unbounded default every OTHER terminal state would otherwise carry.
    ///
    /// Every field mirrors what `ingest_slack_message` already took as
    /// positional arguments — captured at enqueue time (the webhook event is
    /// the only moment this data is guaranteed available), same rationale as
    /// [`JobKind::SlackHuddleCapture`]'s own doc comment.
    SlackMessageIngest {
        /// Resolved once per `connector_installs` match, in the webhook —
        /// unlike `SlackHuddleCapture`, this is NOT necessarily the ONLY
        /// account this workspace resolves to: a fan-out enqueues one job
        /// PER matching account, each on that account's own resolved plane.
        account_id: uuid::Uuid,
        /// Slack `team_id`.
        workspace_id: String,
        channel_id: String,
        user_id: String,
        ts: String,
        thread_ts: Option<String>,
        text: String,
        /// The Slack `bot_id` (`B…`) for a bot-authored message, `None` for a
        /// human one. Present ⇒ `ingest_slack_message` attributes the atom to
        /// the APP rather than resolving `user_id` through `users.info`.
        ///
        /// `#[serde(default)]` (like `bot_name` below) so a job already
        /// `pending` in production when this field shipped still
        /// deserialises: `workers::slack_ingest` reads every queued payload
        /// with the CURRENT struct, and a bare `Option<String>` without the
        /// attribute would make every pre-existing row a hard
        /// "malformed SlackMessageIngest payload" failure.
        #[serde(default)]
        bot_id: Option<String>,
        /// The app's display name (`bot_profile.name`, else the legacy
        /// `bot_message` `username`, else `"bot"`), resolved at enqueue time
        /// from the raw event — which is the only place it exists, since the
        /// event itself does not survive into this payload.
        #[serde(default)]
        bot_name: Option<String>,
    },

    /// Ingest one Microsoft Graph connector item (Teams message, Outlook
    /// watched-thread reply, or a meeting transcript) — the durable tenant
    /// hand-off for the Graph webhook funnel (m365 Train 2 PR 2.3), mirroring
    /// [`JobKind::SlackMessageIngest`]'s shape exactly (see that variant's own
    /// doc comment for the parallel design).
    ///
    /// **Personal vs org plane.** `routes::graph_webhook::ingest_message`/
    /// `ingest_message_with` resolve the account's plane
    /// (`routes::slack::account_plane`) on EVERY call now: a `Personal`
    /// account keeps the pre-PR-2.3 synchronous in-process path — this job
    /// kind is never enqueued for it. An `Org` account is handed off through
    /// this job instead, onto whichever plane's own `jobs` table the account's
    /// tenant-routing status resolves to (the org's own tenant pool once
    /// `Active`, or the shared consumer pool if the org has never been
    /// tenant-migrated — same three-way decision table `routes::slack_events::
    /// resolve_org_plane` uses for Slack, mirrored for Graph in
    /// `routes::graph_webhook`).
    ///
    /// **`graph_id`/`user_id` are captured at enqueue time, not re-derived by
    /// the worker.** Unlike Slack (which re-resolves the destination fresh
    /// inside `ingest_slack_message`), the Teams/Outlook watched-resource
    /// tables this funnel reads (`teams_watched_resources`,
    /// `outlook_watched_conversations`) are still control-plane-only as of
    /// this PR (m365 Train 2 PR 2.4 is what makes THEIR own resolution
    /// plane-aware) — so `graph_id`/`user_id` are whatever the existing
    /// (unchanged) control-pool lookup already produced by the time
    /// `ingest_message`/`ingest_message_with` is called, exactly as they were
    /// before this PR. This job kind only fixes WHICH POOL the resulting atom
    /// is written to, not where the destination graph itself is discovered —
    /// see `workers::graph_ingest`'s own module doc for the forward-looking
    /// note on what PR 2.4 changes.
    ///
    /// **Content-at-rest**: `content` is the plaintext connector body,
    /// landing in this job's `payload` column exactly like
    /// `SlackMessageIngest::text` — see that variant's doc comment for the
    /// full reasoning. `workers::graph_ingest` applies the identical
    /// redact-on-success mitigation.
    GraphMessageIngest {
        account_id: uuid::Uuid,
        /// The member this content is attributed to (`teams_watched_resources.
        /// user_id` / `outlook_watched_conversations`' opting-in user / the
        /// transcript's resolved account owner) — never a verified acting
        /// caller, but the same value the pre-PR-2.3 synchronous path already
        /// stamped onto `UserContext.user_id`.
        user_id: uuid::Uuid,
        /// The destination ocean, already resolved by the caller (see this
        /// variant's own doc comment for why the worker does not re-derive
        /// it).
        graph_id: uuid::Uuid,
        /// Human-readable Teams channel/chat label, already resolved once by
        /// the caller (`microsoft::teams_channel_name_by_container`) so the
        /// worker never re-queries it per message. `None` for non-Teams
        /// sources.
        channel_name: Option<String>,
        source_type: String,
        source_id: String,
        content: String,
        participants: Vec<String>,
        timestamp: chrono::DateTime<chrono::Utc>,
        scope: Option<String>,
        // Deliberately no `thread_id` field: `NormalizedItem::thread_id`
        // (Outlook `conversationId`) has never had a corresponding
        // `IngestAtomRequest` field — the pre-PR-2.3 synchronous path already
        // dropped it here too (thread-watch registration is a separate write,
        // `microsoft::upsert_watched_conversation`, done by the caller before
        // ever reaching `ingest_message`/this job). Carrying it through the
        // job payload would only be dead data.
        channel_id: Option<String>,
        author_id: Option<String>,
        author_name: Option<String>,
    },
    /// The batched, backgrounded tail of `Store::promote_node_scoped`
    /// (async-promote PR): copy `atom_ids` from the source ocean into
    /// `target_graph_id`, link each copy to `target_node_id`, and enqueue
    /// native derivation (`ExtractEntities`/`ExtractRelationships`) for
    /// whatever actually landed — exactly the work `promote_node_scoped`
    /// used to do inline, per-atom, inside the caller's HTTP request.
    ///
    /// **Why the route/MCP tool still runs synchronously up to here.**
    /// Everything that decides WHETHER this promotion is allowed —
    /// reachability of both the source reef and the target ocean, the
    /// same-database (never cross-plane) boundary, `target.kind IN
    /// ('team','org')`, and `coral_ids` validated against the source reef's
    /// actual attached atoms — needs the caller's request-scoped identity
    /// and session, which a job dequeued later by an unrelated worker
    /// process does not have and must not try to reconstruct. So the route
    /// keeps every one of those gates exactly as before, publishes the
    /// target node + `node_promotions` row itself (a single O(1)
    /// `INSERT ... ON CONFLICT`, not the part that ever scaled badly), and
    /// hands this job an ALREADY-VALIDATED, already-resolved `atom_ids`
    /// list — never `None`/"all attached corals", so this job never has to
    /// re-derive that set under a possibly-stale scope.
    ///
    /// **Idempotent, from-scratch-safe on retry**, same convention as
    /// `bin/migrate_personal_to_member_ocean`'s atoms phase: every atom copy
    /// is `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING` and
    /// every `node_atoms` link is `ON CONFLICT DO NOTHING`, so re-running
    /// this job's full `atom_ids` list after a partial failure (this job's
    /// own `fail()`/backoff ladder, or a worker restart mid-run) costs one
    /// no-op statement per already-landed atom rather than corrupting
    /// anything. There is deliberately no separate persisted watermark: see
    /// `workers::promote`'s module doc for why a full rescan is cheap enough
    /// here (a handful of batched statements, not one round trip per atom)
    /// to make the extra state not worth having.
    PromoteNode {
        /// The published copy in the target ocean — already exists (the
        /// route/MCP tool creates it synchronously before enqueuing this
        /// job) — this job only copies/links atoms onto it.
        target_node_id: uuid::Uuid,
        /// The target ocean. Also `atoms.graph_id` for every row this job
        /// inserts.
        target_graph_id: uuid::Uuid,
        /// The source reef, carried for logging/audit only — this job never
        /// reads `nodes`/`node_atoms` for the SOURCE graph; every atom it
        /// needs is already named in `atom_ids`.
        source_node_id: uuid::Uuid,
        /// The user whose promote request created this job — threaded to
        /// `tenancy::scope::begin_tenant_scoped`/`begin_account_scoped` so
        /// this job's writes run under the same RLS scope the route's own
        /// transaction would have used.
        promoted_by: uuid::Uuid,
        /// The FINAL, already-validated set of source atom ids to copy —
        /// resolved once by the route/MCP tool (either the caller's explicit
        /// `coral_ids` or every atom currently attached to the source reef),
        /// never re-derived here. Always non-empty (the route rejects an
        /// empty/coral-less promotion before this job is ever enqueued).
        atom_ids: Vec<uuid::Uuid>,
    },
    /// G3 ocean-health check (knowledge-graph-connectivity §12.3). One job
    /// per ocean, self-scheduled by `workers::ocean_health` at most every
    /// six hours. Computes the rolling 7-day window and, on a paging alarm,
    /// writes [`JobKind::HealthViolation`] rows.
    OceanHealth { graph_id: uuid::Uuid },
    /// G3 paging alarm. Enqueued by the OceanHealth check (one per graph /
    /// alarm / ISO week) and completed immediately so it is an audit row
    /// (`status = 'done'`), not a stuck pending job. Operators:
    /// `SELECT * FROM jobs WHERE kind = 'HealthViolation'`.
    HealthViolation { graph_id: uuid::Uuid, alarm: String },

    /// Provider-agnostic history backfill (chat-per-user-ingest plan §4, PR
    /// 2) — durable replacement for the bare `tokio::spawn(backfill_channel(
    /// ..))` the Slack interactivity buttons used to fire. One job = one
    /// (provider, container, target user) unit, writing into exactly ONE
    /// already-linked member's own ocean, never a container-derived fan-out.
    ///
    /// **Why `target_graph_id`/`target_user_id` are captured here, not
    /// re-derived by the worker.** The enqueuer (today: `routes::
    /// slack_interactivity`'s backfill-window dispatch; tomorrow: the §4.2
    /// web-app offer surface) already knows exactly who this backfill is
    /// FOR — the confirmed CoralSwarm identity behind whichever platform
    /// member asked for it (`connectors::slack::resolve_verified_slack_
    /// identity`, the SAME matcher every other C5 routing path uses) — and
    /// has already resolved their own private ocean
    /// (`routes::slack::resolve_primary_graph`, `IngestCaller::
    /// Attributable`). Re-deriving either inside the worker would mean the
    /// worker re-doing an identity match with no request-time context to
    /// bind it to; capturing the ALREADY-authorized target here mirrors
    /// `JobKind::PromoteNode`'s `target_node_id`/`target_graph_id` (also
    /// resolved once, synchronously, before the job exists) rather than
    /// `JobKind::GraphMessageIngest`'s "control-plane-only lookup, unchanged
    /// for now" shape.
    ///
    /// **This is what lifts the org refusal.** The old `backfill_channel`
    /// forwarded every message to `routes::slack_events::ingest_slack_message`,
    /// whose first read is `slack_integrations` off the CONTENT pool — for an
    /// org account that is the tenant pool, which no write path has ever
    /// populated (Slack OAuth connect is still control-plane-only), so every
    /// org backfill attempt failed that lookup on every message. This job
    /// kind's worker instead builds an `IngestAtomRequest` and calls
    /// `routes::ingest::ingest_atom` directly (the same "construct a
    /// synthetic `Tenant`/`UserContext` and call the route from a worker"
    /// shape `connectors::google_meet::ingest_transcript_for_attendee`
    /// already uses) — no `slack_integrations` read, no container-destination
    /// lookup, so an org account is no longer structurally refused.
    ///
    /// **Window cap (§4.1): ours, not the platform's, and unconditional.**
    /// `window_days` is the RAW value the button/offer asked for (`None` =
    /// "all history the platform still holds"); `connectors::history_
    /// backfill::HISTORY_BACKFILL_MAX_DAYS` (90) is applied inside the
    /// engine on every run, including `None`, so the cap can be changed in
    /// one place without re-enqueuing already-queued jobs with a stale
    /// value baked in.
    ///
    /// **No secrets in this payload.** Unlike `SlackMessageIngest`'s
    /// plaintext `text` (mitigated there by scrubbing on completion), this
    /// job carries no message content and no bot token at all — the worker
    /// re-resolves the bot token fresh from `oauth_tokens` per run, the same
    /// way `ingest_slack_message` does. `jobs.payload` for this kind is
    /// therefore safe to retain indefinitely unscrubbed, same as
    /// `PromoteNode`'s.
    ///
    /// **Durability contract (mandatory — see `connectors::history_backfill`'s
    /// own module doc for the full rule, copied from
    /// `connectors::google_meet::fan_out_and_count`'s documented fix):** the
    /// worker completes this job ONLY when every item in the window landed
    /// or was a deterministic (422) skip. A transient failure on any one
    /// item — including a `conversations.history` page-fetch failure, which
    /// halts pagination outright — fails the WHOLE job via `jobs::queue::
    /// fail`, never `complete`; the retry re-runs the full window, safe
    /// because `atoms`' `(graph_id, source_type, source_id)` `ON CONFLICT DO
    /// NOTHING` makes every prior successful write idempotent.
    HistoryBackfill {
        /// Only `"slack"` is implemented (this PR); shaped so a Teams/Google
        /// Chat arm can be added to `connectors::history_backfill` later
        /// without a new job kind. An unrecognised provider is a structural
        /// failure the worker dead-letters immediately (retrying would hit
        /// the identical "no such arm" wall every time).
        provider: String,
        account_id: uuid::Uuid,
        /// Provider workspace/team id (Slack `team_id`).
        workspace_id: String,
        /// Provider container id (Slack `channel_id`).
        container_id: String,
        /// The ONE ocean this backfill writes into — already resolved by the
        /// enqueuer, never re-derived.
        target_graph_id: uuid::Uuid,
        /// The CoralSwarm user this backfill is FOR — already resolved
        /// (confirmed identity match) by the enqueuer. Threaded into the
        /// synthetic `UserContext`/`TenantContext` the worker builds so
        /// `ingest::submit`'s reachability check (which requires this
        /// exact user for an org member ocean) passes.
        target_user_id: uuid::Uuid,
        /// `Some(org_id)` for an org account, `None` for personal — needed
        /// to rebuild the org `TenantContext` (`org_id`/`account_id`/
        /// `user_id`/`pool`) `ingest::submit` requires for DEK resolution
        /// and R2 blob-key prefixing on the org plane. Captured at enqueue
        /// time rather than re-derived, mirroring `SlackHuddleCapture`'s
        /// "every field captured when it's guaranteed available" rationale.
        clerk_org_id: Option<String>,
        /// Raw requested window in days; `None` = "as much as the platform
        /// still holds". See this variant's own doc comment for why the
        /// mandatory 90-day ceiling is applied inside the engine, not here.
        window_days: Option<u32>,
    },

    /// GitHub connector (PR A, docs/connectors/github.md §3.6) — one job per
    /// repo selected `active` (`routes::github::put_repos`), running the
    /// long-running, resumable, code-first-then-narrative-newest-to-oldest
    /// backfill for that repo. **Not yet implemented** — this PR (sync
    /// plumbing) registers the variant and enqueues it on every newly-active
    /// selection so the queue carries real, inspectable rows from day one;
    /// `workers::github_backfill` is a STUB consumer (drains the queue,
    /// logs, completes) until PR B ("code") lands the real worker, so the
    /// queue is never stuck waiting on unbuilt code. `repo_row_id` is
    /// `github_watched_repos.id` — the worker resolves everything else
    /// (`owner`/`name`/`installation_id`/cursors) from that row rather than
    /// carrying a payload the row itself could go stale against.
    GithubBackfill { repo_row_id: uuid::Uuid },

    /// GitHub connector (PR A, §3.7) — the durable webhook hand-off for
    /// every event `routes::github::github_events` does NOT handle
    /// synchronously (`installation`/`installation_repositories` are
    /// handled inline — see that route's own doc comment for why those two
    /// are safe to do so; every other event type — `push`, `pull_request`,
    /// `issues`, `issue_comment`, `pull_request_review`,
    /// `pull_request_review_comment`, `release` — lands here instead).
    /// Mirrors [`JobKind::SlackMessageIngest`]'s shape: captured at enqueue
    /// time (the webhook delivery is the only moment this payload is
    /// guaranteed available), enqueued onto the SAME plane's `jobs` table
    /// the account's tenant-routing status resolves to.
    ///
    /// **Content-at-rest:** `payload` is the raw webhook body (issue/PR
    /// text, review comments, commit messages, author emails). `jobs` rows
    /// are retained indefinitely once `done`. Mitigated at the worker:
    /// `jobs::queue::complete_with_payload` overwrites `payload` with a
    /// redacted marker on success, same as [`JobKind::SlackMessageIngest`].
    /// A `dead`-lettered job is NOT scrubbed — an operator may need the body
    /// to diagnose or hand-replay it.
    GithubEventIngest {
        account_id: uuid::Uuid,
        installation_id: i64,
        /// `X-GitHub-Delivery` — GitHub's own idempotency id for this
        /// specific delivery attempt.
        delivery_id: String,
        /// `X-GitHub-Event` (`"push"`, `"pull_request"`, …).
        event: String,
        /// The raw, already-`serde_json`-parsed webhook payload.
        payload: serde_json::Value,
    },

    /// The receive stage of ADR-0016's two-stage ingest: one verified webhook
    /// delivery, written by the webhook before it answers and drained by
    /// `workers::ingest_receive`, which fetches the full content and calls
    /// `ingest::submit()`. The webhook makes no provider call. Lands on the
    /// consumer pool with no `account_id`: which accounts a delivery reaches
    /// is only resolved when it is processed.
    ///
    /// **Content-at-rest:** a Workspace event's `message.data` can carry
    /// message content, a Gmail push the mailbox address, and a Graph
    /// notification the subscription's `clientState`. The worker scrubs them
    /// when the job settles, on a `done` row and on a `dead` one alike (like
    /// [`JobKind::SlackMessageIngest`]); only a row still awaiting a retry
    /// keeps its payload, since the retry runs from it.
    IngestReceive(ReceivedEvent),
}

impl JobKind {
    /// How many attempts a job of this kind gets before the queue
    /// dead-letters it (`jobs.max_attempts`, written by `queue::enqueue`).
    /// Every kind takes the table default ([`queue::DEFAULT_MAX_ATTEMPTS`],
    /// three attempts, about three minutes on the backoff ladder) except:
    ///
    /// * [`JobKind::IngestReceive`]: nine. A held webhook delivery has no
    ///   backstop (a Graph `deleted` notification is never re-sent and no
    ///   sweep rediscovers it), so its budget must outlast a realistic
    ///   outage of the provider or of a tenant plane. The ladder doubles
    ///   from 60 s, so nine attempts wait 60 + 120 + 240 + 480 + 960 + 1920
    ///   + 3840 + 7680 s between them: 15 300 s, four hours and fifteen
    ///   minutes, after the first failure. See `workers::ingest_receive`'s
    ///   module doc.
    pub fn max_attempts(&self) -> i32 {
        match self {
            JobKind::IngestReceive(_) => 9,
            _ => queue::DEFAULT_MAX_ATTEMPTS,
        }
    }
}

/// One webhook delivery held by [`JobKind::IngestReceive`], per provider.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum ReceivedEvent {
    /// A Gmail `users.watch` push: the mailbox that changed. A trigger only;
    /// the worker diffs from the stored history cursor.
    GmailPush { email_address: String },
    /// A verified Pub/Sub push for the Chat, Chat app and Meet
    /// subscriptions, as the raw envelope.
    GoogleWorkspaceEvent { envelope: serde_json::Value },
    /// One Microsoft Graph change or lifecycle notification, raw. The
    /// webhook checked its `clientState` against the stored subscription
    /// before holding it; the worker checks it again on the row it acts on.
    GraphNotification { notification: serde_json::Value },
}
