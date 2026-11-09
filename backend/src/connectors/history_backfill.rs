//! Provider-agnostic history backfill engine
//! (`docs/plans/chat-per-user-ingest-and-in-chat-ask.md` §4, PR 2 — "[core]
//! Backfill engine").
//!
//! # What changed, and why it is one change and not three
//!
//! The Slack-only predecessor of this module (`routes::slack_interactivity::
//! backfill_channel`, now removed) forwarded every fetched message to
//! `routes::slack_events::ingest_slack_message`, which:
//!
//!   * resolves the **container's** destination(s)
//!     (`connectors::slack::resolve_channel_destinations`) — so a backfill
//!     could never target one specific person's ocean, only whatever the
//!     channel's destination happened to be;
//!   * reads `slack_integrations` off the CONTENT pool as its first
//!     statement — for an org account that is the tenant pool, which no
//!     write path has ever populated (Slack OAuth connect is still
//!     control-plane-only), so this refused outright for every org account;
//!   * ran as a bare `tokio::spawn` — lost on process restart.
//!
//! All three fall out of routing around `ingest_slack_message` entirely.
//! This engine takes an explicit, already-resolved target `graph_id` +
//! `user_id` and calls `routes::ingest::ingest_atom` directly — the same
//! "construct a synthetic `Tenant`/`UserContext` and call the generic ingest
//! route from a worker" shape `connectors::google_meet::
//! ingest_transcript_for_attendee` already uses. No container-destination
//! lookup (retarget), no `slack_integrations` read (org refusal lifted), and
//! the caller (`workers::history_backfill`) is a durable queue worker, not a
//! spawn.
//!
//! # What the org refusal actually blocked (verified, not assumed)
//!
//! The bullet above — and `backfill_channel`'s own removed doc comment —
//! named the blocker as `ingest_slack_message`'s `slack_integrations` read
//! running off the CONTENT pool. **That premise was already stale by the
//! time it was written.** Reading `routes::slack_events::
//! ingest_slack_message` directly (not the comment describing it) shows its
//! `slack_integrations` read runs on `control_pool` via
//! `begin_tenant_system_scoped(&control_pool, account_id)` — a fix that
//! landed specifically because the CONTENT-pool version was a real incident:
//! "every message from that workspace fell into the 'no integration found'
//! arm and was silently discarded" on a genuine org tenant (see that
//! function's own comment, still in place, for the fix this describes). So
//! by the time `backfill_channel` unconditionally refused `AccountPlane::
//! Org`, the specific failure it named would NOT have occurred.
//!
//! What WOULD have: `backfill_channel` called `ingest_slack_message` with
//! `tenant: crate::tenancy::TenantId::Consumer` **hardcoded**, while handing
//! it the caller's real content pool (`tenant_pool` — for an org account,
//! the org's genuine tenant DB). `ingest_slack_message` builds its bot-token
//! `DekPlane` as `DekPlane::from_tenant(&tenant, pool.clone(), account_id)` —
//! with `tenant` forced to `Consumer` but `pool` a genuine tenant DB, this
//! produces a `DekPlane` that is internally inconsistent: a Consumer-shaped
//! plane resolves its DEK via `accounts.dek_wrapped`, and `accounts` does
//! not exist on a tenant database at all (`migrations_tenant` never creates
//! it — confirmed by this module's own org pg test hitting the exact tenant/
//! control table-shape mismatch class, `relation "account_keys" does not
//! exist`, while iterating on its fixture). That lookup would have failed on
//! every message, and `ingest_slack_message`'s bot-token-resolution arm logs
//! a warning and returns `Ok(())` (skip, don't crash) — so an org backfill
//! would have silently dropped every single message, the same visible
//! symptom the removed comment predicted, just via a different mechanism
//! (a Consumer/tenant plane mismatch, not a `slack_integrations` miss).
//!
//! **Why this now holds instead.** This engine never calls
//! `ingest_slack_message`. When `target.plane == AccountPlane::Org`,
//! [`synthetic_ingest_identity`] builds a REAL
//! `TenancyDecision::Org(TenantContext { org_id, account_id, user_id, pool:
//! tenant_pool })` — `tenant`/`pool` are never split the way the hardcode
//! risked. The worker (`workers::history_backfill::process_job`) resolves
//! the bot token the same correct way: `DekPlane::from_tenant(&t.tenant,
//! t.pool.clone(), account_id)`, where `t.tenant`/`t.pool` are the SAME
//! plane `workers::tenant_harness::run_multi_tenant` resolved this job's own
//! `jobs` table from — never independently guessed or hardcoded.
//!
//! # The `workspace_id` trade (read, don't assume)
//!
//! `ingest_slack_message` writes `atoms.workspace_id` via a bespoke `INSERT
//! INTO atoms`. `IngestAtomRequest` has no `workspace_id` field — it has
//! `channel_id`/`channel_name`/`author_id`/`author_name` instead, which this
//! module populates. A NULL `workspace_id` atom is survivable: the capture
//! query in `connectors::slack_onboarding` (`fetch_capture_status`,
//! `fetch_atom_stats`) already matches `(a.workspace_id = $N OR
//! (a.workspace_id IS NULL AND a.source_type = 'slack'))` — confirmed by
//! reading that query, not assumed — so a backfilled atom still counts
//! toward the account's Slack capture total on both surfaces that report it.
//! What this path gains over the bespoke insert: `ingest::submit` derives
//! `excerpt`/`title`/`title_kind`/`fts_language`, none of which the bespoke
//! insert ever wrote.
//!
//! # The durability rule (mandatory, copied from `connectors::google_meet`)
//!
//! Mirrors `google_meet::fan_out_and_count`'s documented fix exactly (see
//! that function's doc comment for the incident it closes):
//!
//!   1. Accumulate rather than short-circuit — one message's failure must
//!      not stop the rest of its page from being attempted.
//!   2. Three outcomes per message: `Ok(true)` written (or idempotently
//!      already present), `Ok(false)` a deterministic 422 rejection
//!      (consumed, never retried), `Err` anything else (transient).
//!   3. A resolution failure (own-bot-identity lookup, a
//!      `conversations.history` page fetch) is a hard failure too — the
//!      **page fetch** specifically halts pagination outright (there is no
//!      cursor to continue from), so it is surfaced immediately rather than
//!      accumulated silently.
//!   4. The caller (`workers::history_backfill::process_job`) marks the job
//!      `done` ONLY when [`run_slack`] returns `Ok` — i.e. the hard-failure
//!      list was empty for the entire window.
//!   5. Whole-unit retry is safe: `atoms`' `UNIQUE (graph_id, source_type,
//!      source_id)` with `ON CONFLICT DO NOTHING` makes every previously
//!      landed message idempotent, so re-running the full window after a
//!      partial failure costs one no-op per already-landed message.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::ingest::atom::IngestAtomRequest;
use crate::routes::slack::AccountPlane;
use crate::store::PostgresStore;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::AppState;

/// Absolute ceiling on how much history a backfill will ever request or
/// ingest, in days — plan §4.1: "State the window as the platform's, not
/// ours... Cap our side at 90 days regardless, for cost." Applied
/// UNCONDITIONALLY, including when the caller asked for "all history" —
/// there is no larger window this engine will ever attempt, regardless of
/// how much the platform itself still retains.
pub(crate) const HISTORY_BACKFILL_MAX_DAYS: u32 = 90;

/// Page cap and inter-page sleep carried over unchanged from the predecessor
/// `backfill_channel` (`docs/plans/chat-per-user-ingest-and-in-chat-ask.md`
/// PR 2 instructions: "keep the page cap and 2s inter-page sleep").
const BACKFILL_MAX_PAGES: usize = 10;
const BACKFILL_PAGE_SLEEP_SECS: u64 = 2;

/// One provider-agnostic backfill unit: exactly one container, exactly one
/// target ocean, bounded to at most [`HISTORY_BACKFILL_MAX_DAYS`]. Built
/// once by the enqueuer (`routes::slack_interactivity`'s backfill-window
/// dispatch today; the §4.2 web-app offer surface tomorrow) and carried
/// verbatim into [`JobKind::HistoryBackfill`](crate::jobs::JobKind::HistoryBackfill).
#[derive(Debug, Clone)]
pub(crate) struct BackfillTarget {
    pub(crate) account_id: Uuid,
    pub(crate) workspace_id: String,
    pub(crate) container_id: String,
    pub(crate) target_graph_id: Uuid,
    pub(crate) target_user_id: Uuid,
    pub(crate) plane: AccountPlane,
    /// `Some` only for `AccountPlane::Org` — see [`JobKind::HistoryBackfill`]
    /// (crate::jobs::JobKind::HistoryBackfill)'s own doc comment for why this
    /// travels with the target rather than being re-derived.
    pub(crate) clerk_org_id: Option<String>,
    pub(crate) window_days: Option<u32>,
}

impl BackfillTarget {
    /// The window this run will actually use — never `None`, never above
    /// [`HISTORY_BACKFILL_MAX_DAYS`], regardless of what was requested.
    fn effective_window_days(&self) -> u32 {
        self.window_days.map(|d| d.min(HISTORY_BACKFILL_MAX_DAYS)).unwrap_or(HISTORY_BACKFILL_MAX_DAYS)
    }

    /// Slack `conversations.history`'s `oldest` param: a unix-seconds cutoff
    /// derived from [`Self::effective_window_days`].
    fn oldest_unix(&self) -> f64 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
        now - (self.effective_window_days() as f64 * 86400.0)
    }
}

/// What one backfill run accomplished — only ever produced on the `Ok` path;
/// see this module's durability-rule doc for why an `Err` carries no partial
/// count (the whole unit did not finish, and the caller must not treat it as
/// done).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct BackfillOutcome {
    /// Messages that produced (or idempotently already held) an atom.
    pub(crate) ingested: usize,
    /// Messages `ingest_atom` deterministically rejected (422) — consumed,
    /// never retried, same posture as `google_meet::ingest_transcript_for_
    /// attendee`'s `Ok(false)` arm.
    pub(crate) skipped_deterministic: usize,
    /// `true` if [`BACKFILL_MAX_PAGES`] was hit before the platform ran out
    /// of history — logged, not an error (no-silent-caps rule).
    pub(crate) truncated: bool,
}

/// Build the synthetic `(Tenant, UserContext)` pair `ingest_atom` needs to
/// land content in `target.target_graph_id` as `target.target_user_id`,
/// mirroring `google_meet::ingest_transcript_for_attendee`'s
/// `Tenant::legacy`/`UserContext` construction for Personal, and building a
/// real `TenancyDecision::Org` for Org — that function only ever runs
/// Personal, so it had no need for the Org half this backfill engine must
/// support (lifting the org refusal is the whole point of this PR).
///
/// `tenant_pool` is the CONTENT pool for this account's plane: `state.db`
/// for Personal (control/consumer), the org's own tenant pool for Org.
fn synthetic_ingest_identity(tenant_pool: &sqlx::PgPool, target: &BackfillTarget) -> Result<(Tenant, UserContext), String> {
    let user_ctx = UserContext {
        user_id: target.target_user_id,
        account_id: target.account_id,
        role: "member".into(),
        device_id: String::new(),
    };
    let store: Arc<dyn crate::store::Store> = Arc::new(PostgresStore::new(tenant_pool.clone()));
    let decision = match target.plane {
        AccountPlane::Personal => None,
        AccountPlane::Org => {
            let org_id = target
                .clerk_org_id
                .clone()
                .ok_or_else(|| "history backfill: org account with no clerk_org_id".to_string())?;
            // `TenantContext.user_id` MUST equal the request's own
            // `user_ctx.user_id` — `ReachabilityResolver::
            // graph_reachable_by_user`'s `require_matching_user` refuses any
            // mismatch (org-tenancy PR 1.4 P1 fix), which is exactly the
            // check that makes "this backfill can only ever land in the
            // target user's OWN member ocean" a real guarantee rather than
            // a convention.
            Some(TenancyDecision::Org(TenantContext {
                org_id,
                account_id: target.account_id,
                user_id: target.target_user_id,
                pool: tenant_pool.clone(),
                page_pool: None,
            }))
        }
    };
    let tenant = Tenant {
        store: Some(store),
        pool: Some(tenant_pool.clone()),
        decision,
        class: crate::route_class::RouteClass::Other,
    };
    Ok((tenant, user_ctx))
}

/// Ingest one already-admitted Slack message into `target.target_graph_id`.
/// `Ok(true)` written, `Ok(false)` a deterministic 422 (consumed), `Err`
/// anything else (transient) — the exact three-way split this module's
/// durability-rule doc names.
#[allow(clippy::too_many_arguments)]
async fn ingest_one_slack_message(
    state: &Arc<AppState>,
    tenant_pool: &sqlx::PgPool,
    target: &BackfillTarget,
    source_id: String,
    content: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    channel_name: Option<String>,
    author_id: String,
    author_name: String,
    permalink: Option<String>,
) -> Result<bool, String> {
    let (tenant, user_ctx) = synthetic_ingest_identity(tenant_pool, target)?;

    let req = IngestAtomRequest {
        graph_id: target.target_graph_id,
        source_type: "slack".to_string(),
        source_id,
        content,
        participants: vec![author_name.clone()],
        timestamp,
        scope: None,
        fts_language: None,
        channel_id: Some(target.container_id.clone()),
        channel_name,
        author_id: Some(author_id),
        author_name: Some(author_name),
        project: None,
        mcp_client_name: None,
        raw_content: None,
        duration_ms: None,
        segments: None,
        session_id: None,
        session_kind: None,
        session_title: None,
        repo_path: None,
        remote: None,
        hostname: None,
        harness_version: None,
        model: None,
        branch: None,
        agent_name: None,
        platform: None,
        task: None,
        parent_session_id: None,
        trusted_user_agent: None,
        client_info: None,
        code_path: None,
    };

    let origin = crate::source_origin::SourceOrigin::native("slack","message","slack_history_backfill")?
        .with_container("channel",&target.container_id,&format!("slack:workspace:{}",target.workspace_id),req.channel_name.as_deref())?
        .with_oauth_grant(target.account_id,"slack",&target.workspace_id,None)?;
    let hints = crate::ingest::capture::CaptureHints { url: permalink, ..Default::default() };
    let resp = crate::ingest::submit(crate::ingest::IngestRequest {
        origin: Some(&origin),
        ..crate::ingest::IngestRequest::private(crate::ingest::Member {
            hints,
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })
    }).await;

    match resp {
        Ok(_) => Ok(true),
        // Deterministic: this message can never be stored as sent, so it is
        // a skip, not a failure that holds the window.
        Err(refused) if refused.is_invalid() => Ok(false),
        Err(refused) => Err(format!("ingest_atom failed: {refused}")),
    }
}

/// Run one Slack backfill unit end to end: paginate `conversations.history`
/// (bounded by [`BACKFILL_MAX_PAGES`] / [`BACKFILL_PAGE_SLEEP_SECS`], with
/// 429 retry via [`send_with_retry`]), admit each message through the SAME
/// rule the live webhook uses (`connectors::slack_message_text::
/// admit_slack_message` — one copy, three callers), and ingest every
/// admitted message into `target.target_graph_id` as `target.target_user_id`
/// — the one ocean this whole run writes into.
///
/// See this module's doc comment for the durability rule this function's
/// `Result` implements: `Ok` only when every message in the window landed or
/// was a deterministic skip; `Err` otherwise, listing every hard failure
/// hit, with the caller expected to fail (not complete) the job so the whole
/// window is retried.
pub(crate) async fn run_slack(
    state: &Arc<AppState>,
    // Control-plane pool: `resolve_bot_identity` records a stored-token
    // Slack death onto `oauth_tokens` (control-only). Other reads/writes go
    // through `tenant_pool`/`ingest_atom`.
    control_pool: &sqlx::PgPool,
    tenant_pool: &sqlx::PgPool,
    http: &reqwest::Client,
    bot_token: &str,
    target: &BackfillTarget,
) -> Result<BackfillOutcome, String> {
    let oldest = target.oldest_unix();

    // Own-bot identity, resolved lazily on the first bot message (same
    // "no auth.test call at all for a channel with no bot messages"
    // optimisation the predecessor `backfill_channel` used) and cached for
    // the rest of this run. `None` ⇒ every bot message this run sees is
    // dropped (fail closed) rather than risk re-ingesting our own posts —
    // this is a best-effort self-exclusion, not a hard failure: it degrades
    // the same way the live webhook's own resolution failure does.
    let own_bot: tokio::sync::OnceCell<Option<crate::routes::slack_events::BotIdentity>> =
        tokio::sync::OnceCell::new();

    let mut cursor: Option<String> = None;
    let mut pages_fetched = 0usize;
    let mut ingested = 0usize;
    let mut skipped_deterministic = 0usize;
    let mut truncated = false;
    let mut hard_failures: Vec<String> = Vec::new();

    loop {
        let mut params: Vec<(&str, String)> =
            vec![("channel", target.container_id.clone()), ("limit", "200".to_owned())];
        if let Some(ref c) = cursor {
            params.push(("cursor", c.clone()));
        }
        params.push(("oldest", format!("{oldest:.0}")));

        // A page-fetch failure halts pagination outright (there is no
        // cursor to continue from without it) — surfaced immediately as a
        // hard failure rather than accumulated, per this module's
        // durability-rule doc point 3.
        let json = send_with_retry(http, bot_token, "conversations.history", &params)
            .await
            .map_err(|e| format!("conversations.history fetch failed: {e}"))?;

        let messages = match json.get("messages").and_then(|m| m.as_array()) {
            Some(m) => m.clone(),
            None => {
                tracing::warn!(
                    container_id = %target.container_id,
                    "history backfill: conversations.history returned no messages array"
                );
                break;
            }
        };
        pages_fetched += 1;

        for msg in &messages {
            let Some(admitted) = crate::connectors::slack_message_text::admit_slack_message(msg) else {
                continue;
            };
            let is_bot_message = admitted.is_bot_message();
            let crate::connectors::slack_message_text::AdmittedMessage { text, bot_id: msg_bot_id, bot_name } =
                admitted;

            if is_bot_message {
                let own = own_bot
                    .get_or_init(|| async {
                        match crate::routes::slack_events::resolve_bot_identity(
                            http,
                            control_pool,
                            target.account_id,
                            &target.workspace_id,
                            bot_token,
                        )
                        .await
                        {
                            Ok(id) => Some(id),
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    account_id = %target.account_id,
                                    container_id = %target.container_id,
                                    "history backfill: cannot resolve this install's own bot \
                                     identity — bot-authored messages will be skipped for this \
                                     run (fail closed)"
                                );
                                None
                            }
                        }
                    })
                    .await;
                match own {
                    Some(own) => {
                        if crate::routes::slack_events::is_our_own_bot_message(
                            own,
                            msg_bot_id.as_deref(),
                            msg.get("user").and_then(Value::as_str),
                        ) {
                            continue;
                        }
                    }
                    None => continue,
                }
            }

            let msg_user = msg.get("user").and_then(Value::as_str).unwrap_or("unknown").to_owned();
            let msg_ts = msg.get("ts").and_then(Value::as_str).unwrap_or("0").to_owned();
            let thread_ts = msg.get("thread_ts").and_then(Value::as_str).map(str::to_owned);

            let source_id = match &thread_ts {
                Some(tts) => format!("{}/{}/{}", target.container_id, tts, msg_ts),
                None => format!("{}/{}", target.container_id, msg_ts),
            };
            let timestamp: chrono::DateTime<chrono::Utc> = {
                let secs: i64 = msg_ts.split('.').next().unwrap_or("0").parse().unwrap_or(0);
                chrono::DateTime::from_timestamp(secs, 0).unwrap_or_else(chrono::Utc::now)
            };

            let author_id = crate::connectors::slack_message_text::author_id_for(
                &crate::connectors::slack_message_text::AdmittedMessage {
                    text: String::new(),
                    bot_id: msg_bot_id.clone(),
                    bot_name: bot_name.clone(),
                },
                &msg_user,
            );
            // Best-effort display name — a live Slack call, cached
            // per-process (`routes::slack::resolve_user_name`), same as the
            // predecessor. Deliberately does NOT consult or write
            // `connector_identities`: that directory's persistence +
            // `SlackNameBackfill` repair job are the live webhook's job
            // (`routes::slack_events::ingest_slack_message`), not this
            // backfill's — a backfilled atom's author_name is best-effort
            // provenance, upgraded automatically the moment any live
            // message from the same author lands.
            let author_name = if is_bot_message {
                bot_name.clone().unwrap_or_else(|| "bot".to_string())
            } else {
                crate::routes::slack::resolve_user_name(http, target.account_id, &msg_user, bot_token).await
            };
            let channel_name =
                Some(crate::routes::slack::resolve_channel_name(http, target.account_id, &target.container_id, bot_token).await);
            // Workspace origin cached per workspace (`auth.test`), so the
            // permalink costs no Slack call per message.
            let permalink = crate::routes::slack::resolve_workspace_url(http, target.account_id, &target.workspace_id, bot_token)
                .await
                .and_then(|origin| {
                    crate::routes::slack::message_permalink(&origin, &target.container_id, &msg_ts, thread_ts.as_deref())
                });

            match ingest_one_slack_message(
                state,
                tenant_pool,
                target,
                source_id,
                text,
                timestamp,
                channel_name,
                author_id,
                author_name,
                permalink,
            )
            .await
            {
                Ok(true) => ingested += 1,
                Ok(false) => skipped_deterministic += 1,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        container_id = %target.container_id,
                        target_graph_id = %target.target_graph_id,
                        "history backfill: transient failure ingesting one message — \
                         accumulating, not short-circuiting; the whole run will not be marked \
                         done"
                    );
                    hard_failures.push(e);
                }
            }
        }

        cursor = json
            .pointer("/response_metadata/next_cursor")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        if cursor.is_none() {
            break;
        }
        if pages_fetched >= BACKFILL_MAX_PAGES {
            truncated = true;
            tracing::warn!(
                container_id = %target.container_id,
                pages_fetched,
                BACKFILL_MAX_PAGES,
                "history backfill: reached page cap — history may be incomplete"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(BACKFILL_PAGE_SLEEP_SECS)).await;
    }

    if !hard_failures.is_empty() {
        return Err(format!(
            "{} of {} message(s) hit a hard failure — not marking this backfill done; the whole \
             window retries next attempt: {}",
            hard_failures.len(),
            hard_failures.len() + ingested + skipped_deterministic,
            hard_failures.join("; ")
        ));
    }

    Ok(BackfillOutcome { ingested, skipped_deterministic, truncated })
}

/// Send a GET to a Slack API method, retrying once on HTTP 429. Carried over
/// unchanged from the predecessor `backfill_channel` (module doc: "keep...
/// the 429 `send_with_retry`").
async fn send_with_retry(
    http: &reqwest::Client,
    bot_token: &str,
    method: &str,
    params: &[(&str, String)],
) -> Result<Value, String> {
    for attempt in 0..2usize {
        let mut req = http
            .get(format!("{}/{method}", crate::connectors::slack::slack_api_base()))
            .bearer_auth(bot_token);
        for (k, v) in params {
            req = req.query(&[(k, v.as_str())]);
        }

        let resp = req.send().await.map_err(|e| format!("HTTP error on {method}: {e}"))?;
        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            tracing::warn!(method, retry_after, attempt, "history backfill: 429, retrying after backoff");
            if attempt == 0 {
                tokio::time::sleep(std::time::Duration::from_secs(retry_after)).await;
                continue;
            }
            return Err(format!("{method} rate-limited twice in a row"));
        }

        let json: Value = resp.json().await.map_err(|e| format!("JSON parse on {method}: {e}"))?;
        if json.get("ok").and_then(Value::as_bool) != Some(true) {
            let err = json.get("error").and_then(Value::as_str).unwrap_or("unknown");
            return Err(format!("{method} Slack API error: {err}"));
        }
        return Ok(json);
    }
    Err(format!("{method} exhausted retries"))
}

#[cfg(test)]
mod history_backfill_pg_tests {
    use std::sync::Arc;

    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    fn set_test_kek() {
        // SAFETY: every test in this process sets `MASTER_KEK_HEX` to the
        // SAME constant (same idiom as `connectors::google_meet`'s own pg
        // tests) — a concurrent write from another test is benign.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn seed_personal_account_with_dek(pool: &PgPool, owner: Uuid) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&wrapped)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }
    async fn seed_personal_primary_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    fn state_for(pool: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }
    async fn atom_source_ids(pool: &PgPool, graph_id: Uuid) -> Vec<String> {
        sqlx::query_scalar("SELECT DISTINCT source_id FROM atoms WHERE graph_id = $1 ORDER BY source_id")
            .bind(graph_id)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// A single-page mock `conversations.history` responding with `messages`
    /// fixed at construction and no further pages.
    async fn spawn_mock_slack_one_page(messages: Vec<serde_json::Value>) -> String {
        use axum::{extract::Query, routing::get, Json, Router};
        use std::collections::HashMap;
        use std::sync::Arc as StdArc;

        let messages = StdArc::new(messages);

        async fn history(
            axum::extract::State(messages): axum::extract::State<StdArc<Vec<serde_json::Value>>>,
            Query(_q): Query<HashMap<String, String>>,
        ) -> Json<serde_json::Value> {
            Json(serde_json::json!({ "ok": true, "messages": messages.as_slice() }))
        }
        async fn users_info(Query(q): Query<HashMap<String, String>>) -> Json<serde_json::Value> {
            let user = q.get("user").map(String::as_str).unwrap_or("");
            Json(serde_json::json!({"ok": true, "user": {"id": user, "profile": {}}}))
        }
        async fn conversations_info() -> Json<serde_json::Value> {
            Json(serde_json::json!({"ok": true, "channel": {"name": "general"}}))
        }
        async fn auth_test() -> Json<serde_json::Value> {
            Json(serde_json::json!({"ok": true, "user_id": "UOURBOT", "bot_id": "BOURBOT"}))
        }

        let app = Router::new()
            .route("/conversations.history", get(history))
            .route("/users.info", get(users_info))
            .route("/conversations.info", get(conversations_info))
            .route("/auth.test", get(auth_test))
            .with_state(messages);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock slack");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    fn slack_message(user: &str, ts: &str, text: &str) -> serde_json::Value {
        serde_json::json!({"type": "message", "user": user, "ts": ts, "text": text})
    }

    /// (a) The engine writes to the target ocean and no other — a decoy
    /// personal ocean on a SECOND account, never named by this run, must
    /// stay empty.
    #[tokio::test]
    async fn run_slack_writes_only_to_the_target_graph_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping run_slack_writes_only_to_the_target_graph_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "orgt_history_backfill_target_only";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_personal_account_with_dek(&pool, owner).await;
        let target_graph = seed_personal_primary_graph(&pool, account_id).await;

        let decoy_owner = seed_user(&pool).await;
        let decoy_account = seed_personal_account_with_dek(&pool, decoy_owner).await;
        let decoy_graph = seed_personal_primary_graph(&pool, decoy_account).await;

        let base_url = spawn_mock_slack_one_page(vec![
            slack_message("U1", "1700000000.000100", "hello there"),
            slack_message("U1", "1700000001.000200", "second message"),
        ])
        .await;
        // CodeRabbit round 1 (#1005, Thread 24): held for this test's whole
        // body, alongside every other in-process test that mutates this
        // SAME process-global var (see `SLACK_API_BASE_ENV_LOCK`'s own doc).
        let _slack_api_base_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base_url);
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();

        let target = BackfillTarget {
            account_id,
            workspace_id: "T1".to_string(),
            container_id: "C1".to_string(),
            target_graph_id: target_graph,
            target_user_id: owner,
            plane: AccountPlane::Personal,
            clerk_org_id: None,
            window_days: Some(7),
        };
        let state = state_for(pool.clone());
        let outcome = run_slack(&state, &pool, &pool, &http, "xoxb-test", &target)
            .await
            .expect("run_slack must succeed against a healthy mock Slack");
        assert_eq!(outcome.ingested, 2, "both admitted messages must land: {outcome:?}");

        let target_ids = atom_source_ids(&pool, target_graph).await;
        assert_eq!(target_ids.len(), 2, "the target ocean must hold both backfilled atoms: {target_ids:?}");
        let decoy_ids = atom_source_ids(&pool, decoy_graph).await;
        assert!(decoy_ids.is_empty(), "a graph never named by this run must stay untouched: {decoy_ids:?}");
        let origins:Vec<(String,serde_json::Value)>=sqlx::query_as("SELECT provider_namespace,origin FROM source_items WHERE graph_id=$1")
            .bind(target_graph).fetch_all(&pool).await.unwrap();
        assert_eq!(origins.len(),2);
        for (namespace,origin) in origins {
            assert_eq!(namespace,format!("declared:user:{owner}"));
            assert_eq!(origin["service_id"],"slack");
            assert_eq!(origin["capture_surface"],"slack_history_backfill");
            assert_eq!(origin["container"]["provider_namespace"],"slack:workspace:T1");
            assert!(origin["connection"]["id"].as_str().unwrap().starts_with("grant-key-sha256:"));
        }
        run_slack(&state,&pool,&pool,&http,"xoxb-test",&target).await.unwrap();
        let versions:i64=sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(target_graph).fetch_one(&pool).await.unwrap();
        assert_eq!(versions,2,"historical recapture retains source identity and snapshot count");

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);
        teardown(&base, db, pool).await;
    }

    /// (b) An org account is no longer refused. The predecessor
    /// `backfill_channel` returned a hard `Err("backfill is not yet
    /// supported for organization accounts...")` before ever touching
    /// Slack; this asserts the run actually attempts and lands content in
    /// the target member's own ocean instead.
    #[tokio::test]
    async fn run_slack_no_longer_refuses_an_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping run_slack_no_longer_refuses_an_org_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        // Genuinely TWO separate databases — the same "two genuinely
        // separate pools" shape `connectors::slack::matches_and_persists_
        // across_two_genuinely_separate_pools_pg` already uses, and load-
        // bearing here for a reason a single shared DB cannot exercise:
        // `account_keys` (the org-plane DEK table this run's bot-token/atom
        // encryption reads) is a TENANT-tree-only table
        // (`migrations_tenant/0002_tenant_meta_and_account_keys.sql`) —
        // `migrations_control` never creates it. A single DB migrated with
        // only `migrations_control` (this test's first cut) fails at the
        // `INSERT INTO account_keys` seed with `relation "account_keys"
        // does not exist`, which is exactly the cross-plane distinction this
        // whole PR's org support depends on getting right.
        let control_db = "orgt_history_backfill_org_lifted_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "orgt_history_backfill_org_lifted_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let control_conn = recreate(&base, control_db).await;
        control_conn.close().await.ok();
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let tenant_conn = recreate(&base, tenant_db).await;
        tenant_conn.close().await.ok();
        let control_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, control_db)).await.unwrap();
        let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();

        // CONTROL: `users`/`accounts`/`account_members` — none of which
        // `migrations_tenant` creates at all (see `matches_and_persists_
        // across_two_genuinely_separate_pools_pg`'s own doc comment for the
        // same point made about `users`).
        let member = seed_user(&control_pool).await;
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(member)
        .bind("org_history_backfill_test")
        .fetch_one(&control_pool)
        .await
        .unwrap();
        // PR F (seat lifecycle): `account_members_default_seat_tier` refuses
        // an org member insert with no `seat_tier_id` supplied — same fixture
        // shape `cross_tenant_probe_pg.rs::seed_membership` already uses.
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(member)
        .execute(&control_pool)
        .await
        .unwrap();

        // TENANT: `account_keys` (org DEK — `crypto::resolve_dek_for_tenant`
        // reads `account_keys.dek_wrapped` on the TENANT pool) and `graphs`
        // (the member ocean this run must land in).
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(&wrapped)
            .execute(&tenant_pool)
            .await
            .unwrap();
        let member_ocean: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) RETURNING id",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();

        let base_url = spawn_mock_slack_one_page(vec![slack_message("U1", "1700000002.000300", "org message")]).await;
        // CodeRabbit round 1 (#1005, Thread 24): see `SLACK_API_BASE_ENV_LOCK`'s
        // own doc comment.
        let _slack_api_base_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&base_url);
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();

        let target = BackfillTarget {
            account_id,
            workspace_id: "T-ORG".to_string(),
            container_id: "C-ORG".to_string(),
            target_graph_id: member_ocean,
            target_user_id: member,
            plane: AccountPlane::Org,
            clerk_org_id: Some("org_history_backfill_test".to_string()),
            window_days: Some(30),
        };
        // AppState supplies CONTROL membership/revocation reads; the Org
        // Tenant constructed by run_slack supplies the separate content pool.
        // Fresh capture admission must check the real CONTROL membership
        // seeded above, never look for accounts in the tenant content DB.
        let state = state_for(control_pool.clone());
        let outcome = run_slack(&state, &control_pool, &tenant_pool, &http, "xoxb-test", &target)
            .await
            .expect(
                "an org account must no longer be refused — the old backfill_channel returned a \
                 hard error here before ever calling Slack",
            );
        assert_eq!(outcome.ingested, 1);
        let ids = atom_source_ids(&tenant_pool, member_ocean).await;
        assert_eq!(ids.len(), 1, "the org member's own ocean must hold the backfilled atom: {ids:?}");

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);
        teardown(&base, control_db, control_pool).await;
        teardown(&base, tenant_db, tenant_pool).await;
    }

    /// (c) A transient failure must not be silently swallowed as success —
    /// a `conversations.history` page-fetch failure on the SECOND page
    /// (after the first page's message already landed) must surface as
    /// `Err`, so the caller (`workers::history_backfill::process_job`)
    /// fails rather than completes the job, and the whole window is
    /// retried. The first page's atom must still be present (idempotent
    /// partial progress), proving nothing already-landed is lost even
    /// though the run itself is not "done".
    #[tokio::test]
    async fn run_slack_page_fetch_failure_is_not_swallowed_as_success_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping run_slack_page_fetch_failure_is_not_swallowed_as_success_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "orgt_history_backfill_transient";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_personal_account_with_dek(&pool, owner).await;
        let target_graph = seed_personal_primary_graph(&pool, account_id).await;

        // A two-page mock: page 1 succeeds and carries a `next_cursor`; page
        // 2 (any request WITH a `cursor` param) answers a Slack-shaped
        // error, forcing `send_with_retry` to return `Err` and halting
        // pagination.
        use axum::{extract::Query, routing::get, Json, Router};
        use std::collections::HashMap;
        async fn history(Query(q): Query<HashMap<String, String>>) -> Json<serde_json::Value> {
            if q.contains_key("cursor") {
                Json(serde_json::json!({"ok": false, "error": "internal_error"}))
            } else {
                Json(serde_json::json!({
                    "ok": true,
                    "messages": [
                        {"type": "message", "user": "U1", "ts": "1700000010.000100", "text": "page one"}
                    ],
                    "response_metadata": {"next_cursor": "page2cursor"}
                }))
            }
        }
        async fn users_info(Query(q): Query<HashMap<String, String>>) -> Json<serde_json::Value> {
            let user = q.get("user").map(String::as_str).unwrap_or("");
            Json(serde_json::json!({"ok": true, "user": {"id": user, "profile": {}}}))
        }
        async fn conversations_info() -> Json<serde_json::Value> {
            Json(serde_json::json!({"ok": true, "channel": {"name": "general"}}))
        }
        let app = Router::new()
            .route("/conversations.history", get(history))
            .route("/users.info", get(users_info))
            .route("/conversations.info", get(conversations_info));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock slack");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        // CodeRabbit round 1 (#1005, Thread 24): see `SLACK_API_BASE_ENV_LOCK`'s
        // own doc comment.
        let _slack_api_base_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _slack_api_base_guard = crate::connectors::slack::SlackApiBaseTestGuard::set(&format!("http://{addr}"));
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();

        let target = BackfillTarget {
            account_id,
            workspace_id: "T1".to_string(),
            container_id: "C1".to_string(),
            target_graph_id: target_graph,
            target_user_id: owner,
            plane: AccountPlane::Personal,
            clerk_org_id: None,
            window_days: Some(7),
        };
        let state = state_for(pool.clone());
        let err = run_slack(&state, &pool, &pool, &http, "xoxb-test", &target)
            .await
            .expect_err("a page-2 fetch failure must surface as Err, never as a completed outcome");
        assert!(err.contains("conversations.history fetch failed"), "unexpected error text: {err}");

        // The first page's message, already ingested before the failure,
        // must still be present — idempotent partial progress, not lost.
        let ids = atom_source_ids(&pool, target_graph).await;
        assert_eq!(ids.len(), 1, "page one's already-landed atom must survive the later failure: {ids:?}");

        drop(_slack_api_base_guard);
        drop(_slack_api_base_lock);
        teardown(&base, db, pool).await;
    }
}
