use super::*;

#[derive(Default)]
pub(super) struct EmbeddingWorkers {
    pub(super) embed_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) reembed_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) propagate_corrections_handle: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn embedding(
    state: &Arc<AppState>,
    shutdown: &shutdown::Shutdown,
    cloud_embedder: &Option<Arc<dyn coralswarm_core::embed::EmbedProvider + Send + Sync>>,
) -> EmbeddingWorkers {
    let worker_runtime = state.tenant_runtime.clone();
    let worker_activity = state.worker_activity.clone();
    let embedder = state.embedder.clone();
    // ── Embed worker (tenant loop) ────────────────────────────────────────────
    // Requires both the pool (jobs queue, DEK resolution, atom load, FTS
    // update, embed_failed marking) and the Store (vector write + sync-op
    // emission — review H12) — per tenant, both provided by the harness. The
    // queue-depth backpressure state (`last_depth_check`) is owned here and
    // threaded into every tick (consumer-plane-only check — see `embed::tick`).
    let embed_handle = if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let r2_clone = state.r2.clone();
        let emb_clone = embedder.clone();
        let cloud_clone = cloud_embedder.clone();
        let control_pool = state.db.clone();
        let depth_state = Arc::new(tokio::sync::Mutex::new(None::<std::time::Instant>));
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "embed",
                workers::embed::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let emb = emb_clone.clone();
                let cloud = cloud_clone.clone();
                let control = control_pool.clone();
                let r2 = r2_clone.clone();
                let depth = depth_state.clone();
                async move {
                    workers::embed::tick(
                        &t,
                        emb.as_ref(),
                        cloud.as_deref(),
                        control.as_ref(),
                        r2.as_deref(),
                        &depth,
                    )
                    .await
                }
            },
        ));
        tracing::info!("embed worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Re-embed backfill worker (leftover missing-cloud / chunker stamps) ──
    // Tenant loop: each plane's leftover gap is seeded onto that pool when
    // the queue is empty (see `workers::reembed::tick`). Same four
    // dependencies as the primary embed worker (pool, Store, embedder,
    // optional R2), plus the control pool so an org seed can stamp
    // `account_id` from `tenant_databases`.
    let reembed_handle = if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone())
    {
        let r2_clone = state.r2.clone();
        let emb_clone = embedder.clone();
        let cloud_clone = cloud_embedder.clone();
        let control_pool = state.db.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "reembed",
                workers::reembed::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let emb = emb_clone.clone();
                let cloud = cloud_clone.clone();
                let control = control_pool.clone();
                let r2 = r2_clone.clone();
                async move {
                    workers::reembed::tick(
                        &t,
                        emb.as_ref(),
                        cloud.as_deref(),
                        r2.as_deref(),
                        control.as_ref(),
                    )
                    .await
                }
            },
        ));
        tracing::info!("reembed backfill worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Correction propagation worker (contract §8.4 / C15) ──────────────────
    // Consumer-plane only, same shape as `reembed_handle` above — see
    // `workers::propagate_corrections`' module doc. Needs only the pool (job
    // queue, `services::corrections::propagate`'s own scoped writes) and,
    // optionally, R2 for blob-backed atoms; no `Store` needed
    // (`store::CLOUD_DEVICE_ID` is used directly for the sync-op device id,
    // matching every other cloud-only worker's device identity).
    let propagate_corrections_handle = if let Some(pool) = state.db.clone() {
        let r2_clone = state.r2.clone();
        let handle = tokio::spawn(workers::propagate_corrections::run(
            pool,
            r2_clone,
            shutdown.clone(),
        ));
        tracing::info!("corrections propagation worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Embed retry sweep worker ──────────────────────────────────────────────
    // Periodically re-enqueues atoms whose embedding previously failed but are
    // now backoff-eligible (see `workers::embed_retry`'s module doc for why a
    // separate sweep is needed on top of `workers::embed`'s own retry-aware
    // candidate query — partial job failures and dead-lettered jobs otherwise
    // never revisit those atom_ids again). Needs only the pool.
    //
    // Sweep worker on the tenant loop (each tenant's `atoms`/`jobs` need the
    // same repair pass); the harness owns shutdown-aware scheduling — same
    // "periodic sweeper, no grace window needed" reasoning as before, so it's
    // intentionally NOT joined in the "Drain shutdown-aware job workers" step
    // further down (see `workers::embed_retry::tick`'s doc comment). Its tick
    // still takes `shutdown` directly for the mid-sweep per-account enqueue
    // race.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let tick_shutdown = shutdown.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "embed_retry",
                workers::embed_retry::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let sd = tick_shutdown.clone();
                async move { workers::embed_retry::tick(&t, &sd).await }
            },
        ));
        tracing::info!("embed retry sweep worker spawned");
    }

    // Library evidence belongs to every plane. The harness gates tenant
    // schema versions; each tick backfills or drains one bounded graph batch.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "library_evidence",
                workers::library_evidence::INTERVAL,
            ),
            shutdown.clone(),
            |t| async move { workers::library_evidence::tick(&t).await },
        ));
    }

    // ── Statement embedding backfill sweep ───────────────────────────────────
    // Enqueues `EmbedStatements` for versions whose statements lack a current
    // cloud vector (scoped Ask's statement leg); the embed worker above runs
    // them on the managed `cloud_embedding` purpose. See
    // `workers::statement_embed`.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let tick_shutdown = shutdown.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "statement_embed_sweep",
                workers::statement_embed::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let sd = tick_shutdown.clone();
                async move { workers::statement_embed::sweep_tick(&t, &sd).await }
            },
        ));
        tracing::info!("statement embedding sweep worker spawned");
    }

    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "managed_receipt_reaper",
                std::time::Duration::from_secs(60),
            ),
            shutdown.clone(),
            |t| async move { managed::reap_tick(&t).await },
        ));
        tracing::info!("managed receipt reaper spawned");
    }

    EmbeddingWorkers {
        embed_handle,
        reembed_handle,
        propagate_corrections_handle,
    }
}

#[derive(Default)]
pub(super) struct SemanticWorkers {
    pub(super) digest_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) ocean_health_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) summary_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) source_atoms_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) source_blob_cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn semantics(
    state: &Arc<AppState>,
    shutdown: &shutdown::Shutdown,
) -> SemanticWorkers {
    let worker_runtime = state.tenant_runtime.clone();
    let worker_activity = state.worker_activity.clone();
    // ── Reef digest regeneration worker (graph-temporal-memory §2.5) ─────────
    // Drains `RegenerateDigest` jobs, enqueued by the
    // `nodes_maybe_enqueue_digest_regeneration` SQL trigger (migration
    // 0109/0019), by deletion invalidation (control 0338 / tenant 0171) and
    // by an on-demand `GET /v1/nodes/{id}/digest` miss, all deduped on
    // `digest:{node_id}`. Managed enrichment (source-agnostic PR 8): claims
    // only with DIGESTS_ENABLED=true and ENRICHMENT_PROCESSING_ENABLED=true. Needs pool (jobs queue + coral counters),
    // Store, an embedder (§2.4's title-similarity ranking), and an LLM.
    // R2 is threaded in (not required to spawn) because every coral written
    // after the 2026-08-04 cutover is R2-only — see `workers::digest`'s
    // module doc. Without it the worker can still digest all-inline reefs
    // and fails loudly, never silently, on the rest.
    let digest_handle = if let (Some(pool), Some(store), Some(llm)) =
        (state.db.clone(), state.store.clone(), state.llm.clone())
    {
        let embedder = state.embedder.clone();
        let r2_clone = state.r2.clone();
        // The managed gate's control pool (credits, price sheet, budget).
        let digest_control = pool.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "digest",
                workers::digest::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let embedder = embedder.clone();
                let llm = llm.clone();
                let r2 = r2_clone.clone();
                let control = digest_control.clone();
                async move {
                    workers::digest::tick(&t, &control, embedder.as_ref(), llm.as_ref(), r2.as_deref()).await
                }
            },
        ));
        tracing::info!("digest worker spawned");
        Some(handle)
    } else {
        tracing::warn!("digest worker NOT spawned — requires database, store, and LLM provider");
        None
    };

    // ── Ocean health (G3 / knowledge-graph-connectivity §12.3) ───────────────
    // Self-schedules one OceanHealth job per ocean at most every six hours.
    // Paging alarms write HealthViolation rows. Needs pool + store (tenant
    // loop); no LLM.
    let ocean_health_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "ocean_health",
                    workers::ocean_health::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                |t| async move { workers::ocean_health::tick(&t).await },
            ));
            tracing::info!("ocean-health worker spawned");
            Some(handle)
        } else {
            tracing::warn!("ocean-health worker NOT spawned — requires database and store");
            None
        };

    // ── Meeting-summary worker (cs-uxp3a-meeting-summary) ─────────────────────
    // Map-reduce post-meeting summary (cheap map → capable reduce), egress-gated.
    // Needs pool (jobs queue + egress read + DEK), Store (transcript + summary
    // columns), an LLM, and (optionally) R2 for the raw-text fallback path.
    // Tenant loop; `summary::tick` additionally takes the CONTROL pool for the
    // egress-policy read (`account_prefs` is control-plane only).
    let summary_handle = if let (Some(pool), Some(store), Some(llm)) =
        (state.db.clone(), state.store.clone(), state.llm.clone())
    {
        let r2_clone = state.r2.clone();
        let control_pool = pool.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "summary",
                workers::summary::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let llm = llm.clone();
                let r2 = r2_clone.clone();
                let control = control_pool.clone();
                async move { workers::summary::tick(&t, &control, llm.as_ref(), r2.as_deref()).await }
            },
        ));
        tracing::info!("meeting-summary worker spawned");
        Some(handle)
    } else {
        tracing::warn!(
            "meeting-summary worker NOT spawned — requires database, store, and LLM provider"
        );
        None
    };

    // Semantic source extraction shares the summary worker's tenant harness
    // and control-plane egress gate, but preserves exact versioned evidence.
    let source_atoms_handle = if let (Some(pool), Some(store), Some(llm)) =
        (state.db.clone(), state.store.clone(), state.llm.clone())
    {
        let r2_clone = state.r2.clone();
        let control_pool = pool.clone();
        let source_atoms_shutdown = shutdown.clone();
        Some(tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "source_atoms",
                workers::source_atoms::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let llm = llm.clone();
                let r2 = r2_clone.clone();
                let control = control_pool.clone();
                let stop = source_atoms_shutdown.clone();
                async move {
                    workers::source_atoms::tick_refilling(&t, &control, llm.as_ref(), r2.as_deref(), &stop).await
                }
            },
        )))
    } else {
        tracing::warn!(
            "source-atoms worker NOT spawned — requires database, store, and LLM provider"
        );
        None
    };

    // Fact supersession judge (source-agnostic plan PR 9): ambiguous pairs from
    // the deterministic rules, judged by Jev under managed_enrichment. Idle
    // unless FACT_SUPERSESSION_MODE=live and ENRICHMENT_PROCESSING_ENABLED=true.
    if let (Some(pool), Some(store), Some(llm)) = (state.db.clone(), state.store.clone(), state.llm.clone()) {
        let control = pool.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "fact_supersession_judge",
                crate::fact_supersession::judge::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let llm = llm.clone();
                let control = control.clone();
                async move { crate::fact_supersession::judge::tick(&t, &control, llm.as_ref()).await }
            },
        ));
        tracing::info!("fact supersession judge sweep spawned");
    }

    // Cleanup must continue even when model processing is disabled or no model
    // provider is configured. Missing blob transport remains a retryable job.
    let source_blob_cleanup_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let r2 = state.r2.clone();
            Some(tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "source_blob_cleanup",
                    workers::source_blob_cleanup::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |tenant| {
                    let r2 = r2.clone();
                    async move {
                        workers::source_blob_cleanup::tick(
                            &tenant,
                            r2.as_deref()
                                .map(|r| r as &dyn services::blob_store::PromotionBlobStore),
                        )
                        .await
                    }
                },
            )))
        } else {
            None
        };

    // ── Relationship-graph workers (ADR-0008, tenant loop) ────────────────────
    // Entity resolution plus the model-free inference closure (pool + Store + LLM). These previously had no
    // shutdown wiring at all; riding the harness makes them stop claiming new
    // jobs on shutdown like the others (an improvement, not a regression).
    if let (Some(pool), Some(store), Some(llm)) =
        (state.db.clone(), state.store.clone(), state.llm.clone())
    {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool.clone(),
            store.clone(),
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "inference_closure",
                workers::inference_closure::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| async move { workers::inference_closure::tick(&t).await },
        ));
        tracing::info!("inference-closure worker spawned");
        {
            let llm = llm.clone();
            // The managed gate's control pool (credits, price sheet, budget).
            let er_control = pool.clone();
            tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool.clone(),
                store.clone(),
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "entity_resolution",
                    workers::entity_resolution::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let llm = llm.clone();
                    let control = er_control.clone();
                    async move { workers::entity_resolution::tick(&t, &control, llm.as_ref()).await }
                },
            ));
            tracing::info!("entity-resolution worker spawned");
        }
    } else {
        tracing::warn!(
            "relationship-graph workers NOT spawned — require database, store, and LLM provider"
        );
    }

    SemanticWorkers {
        digest_handle,
        ocean_health_handle,
        summary_handle,
        source_atoms_handle,
        source_blob_cleanup_handle,
    }
}

#[derive(Default)]
pub(super) struct CollaborationWorkers {
    pub(super) slack_name_backfill_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) promote_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) slack_seat_reconcile_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) slack_unmatched_email_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) chat_invite_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) content_tsv_backfill_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) atom_title_backfill_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) huddle_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) teams_seat_reconcile_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) teams_unmatched_email_handle: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn collaboration(
    state: &Arc<AppState>,
    shutdown: &shutdown::Shutdown,
) -> CollaborationWorkers {
    let worker_runtime = state.tenant_runtime.clone();
    let worker_activity = state.worker_activity.clone();
    // ── Slack name backfill worker (PR #176 follow-up; org-slack step 4) ─────
    // Tenant loop: the natural-repair enqueue in `routes::slack_events::
    // persist_identity_and_enqueue_backfill` moved planes in the SAME commit
    // (see that function's doc comment), so `SlackNameBackfill` jobs land on
    // WHICHEVER plane the identity write resolved — the consumer `jobs`
    // table, or a specific org's own tenant `jobs` table — and this worker's
    // dequeue must follow (see the decision table in `workers::
    // tenant_harness` and this worker's own module doc).
    // Re-resolves ID-shaped author_name/channel_name/participants on stored
    // Slack atoms once a real name is available (connector_identities
    // directory or a fresh Slack API call). `control_pool` is threaded
    // through the same way `member_ocean_reconciler`/`slack_ingest` above
    // thread theirs — needed ONLY by the account-less seed job's fan-out;
    // every other read/write runs on `t.pool`/`t.store`.
    let slack_name_backfill_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let slack_name_backfill_control_pool = pool.clone();
            let slack_name_backfill_http = state.http_client.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "slack_name_backfill",
                    workers::slack_name_backfill::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = slack_name_backfill_control_pool.clone();
                    let http = slack_name_backfill_http.clone();
                    async move { workers::slack_name_backfill::tick(&t, &control, &http).await }
                },
            ));
            tracing::info!("slack-name-backfill worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── Promote worker (async-promote PR) ────────────────────────────────────
    // The batched, backgrounded tail of `Store::promote_node_scoped` — see
    // `workers::promote`'s module doc for the production incident (a
    // 3,129-coral promote that never completed) this exists to fix.
    // `promote_r2` threads the R2 client through the same way `huddle_r2`
    // above does — the worker re-uploads each R2-backed coral's content
    // under the target's own correctly-scoped key (see that module's doc
    // comment) rather than only copying rows.
    //
    // Handle retained (unlike the connector sweeps discussed above) and
    // joined into the shutdown drain below: a claimed `PromoteNode` job can
    // hold `copy_atoms_to_graph_scoped`'s transaction open mid R2-re-upload,
    // and dropping the task instead of draining it would abort that
    // transaction (rolling it back) while leaving the job row `running`
    // until `workers::reaper` eventually resets it — a needless delay for
    // something the drain window can usually avoid outright.
    let promote_handle = if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone())
    {
        let promote_r2 = state.r2.clone();
        let publication_control = pool.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "promote",
                workers::promote::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let r2 = promote_r2.clone();
                let control = publication_control.clone();
                async move {
                    // Both ticks run, THEN the error is reported (CodeRabbit
                    // round 1 on #871). `?` on the source-publication tick used
                    // to skip topic promotion entirely for this tenant on this
                    // pass — and that tick returns `Err` on any queue or
                    // `source_connections` read failure, so a persistent fault
                    // in the NEW publication path would have stalled the
                    // pre-existing `PromoteNode` queue behind it. They share a
                    // worker, not a fate.
                    let source_tick =
                        services::source_publication::tick(&t, &control, r2.as_deref()).await;
                    let topic_tick = workers::promote::tick(&t, r2.as_deref()).await;
                    let source_processed = match source_tick {
                        Ok(tick) => tick.processed,
                        Err(e) => {
                            tracing::warn!(error = %e, "source publication tick failed; topic promotion still ran");
                            0
                        }
                    };
                    let processed = source_processed + topic_tick?.processed;
                    // `Tick::worked(0)` would suppress the harness's idle
                    // backoff, keeping a dormant tenant database on the 500ms
                    // fast poll forever. Both ticks can legitimately have
                    // nothing to do.
                    Ok(if processed == 0 {
                        workers::tenant_harness::Tick::idle()
                    } else {
                        workers::tenant_harness::Tick::worked(processed)
                    })
                }
            },
        ));
        tracing::info!("promote worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Slack seat reconciler (org-slack step 4) ─────────────────────────────
    // The backstop half of seat revocation: the live `member_joined_channel`
    // / `member_left_channel` handlers keep seats correct going forward, and
    // this reconciles what they cannot reach — channels routed before
    // `team_seat_grants` existed (seats with no provenance, therefore
    // permanently un-revocable until a sweep records one), channels the bot
    // joined before the destination picker shipped, and any membership event
    // missed while this process was down. Job-driven, not timer-driven: a
    // sweep is hand-insertable in prod exactly like `SlackNameBackfill` (see
    // `jobs::JobKind::SlackSeatReconcile`'s doc comment for the SQL) — onto
    // whichever plane's `jobs` table the operator targets.
    //
    // Tenant loop: each plane's `jobs` table is invisible to a dequeuer
    // scoped to any other one, same reasoning as `reaper`/`tombstone_purge`
    // above. `control_pool` resolves the account roster and the Slack bot
    // token (control-only, `oauth_tokens` — see this worker's module doc);
    // every seat read/write runs on `t.pool`.
    let slack_seat_reconcile_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let slack_seat_reconcile_control_pool = pool.clone();
            let slack_seat_reconcile_http = state.http_client.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "slack_seat_reconcile",
                    workers::slack_seat_reconcile::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = slack_seat_reconcile_control_pool.clone();
                    let http = slack_seat_reconcile_http.clone();
                    async move { workers::slack_seat_reconcile::tick(&t, &control, &http).await }
                },
            ));
            tracing::info!("slack-seat-reconcile worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── "Email the unmatched" worker (chat-per-user plan PR 6) ───────────────
    // Enqueued at the same channel self-join gate event as the destination/
    // backfill prompts (`routes::slack_events::handle_member_joined_for_
    // account`) — see `jobs::JobKind::EmailUnmatchedSlackChannelMembers`'s
    // own doc comment. Tenant-looped for the identical reason `slack_seat_
    // reconcile` above is: each plane's `jobs` table is invisible to a
    // dequeuer scoped to any other one. Takes the full `state` Arc (not just
    // `http_client`, unlike its sibling above) because sending the actual
    // email needs `state.resend_api_key` and `state.org_web_base_url`/
    // `personal_web_base_url` — see `workers::slack_unmatched_email::tick`'s
    // own doc comment.
    let slack_unmatched_email_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let slack_unmatched_email_control_pool = pool.clone();
            let slack_unmatched_email_http = state.http_client.clone();
            let slack_unmatched_email_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "slack_unmatched_email",
                    workers::slack_unmatched_email::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = slack_unmatched_email_control_pool.clone();
                    let http = slack_unmatched_email_http.clone();
                    let app_state = slack_unmatched_email_state.clone();
                    async move {
                        workers::slack_unmatched_email::tick(&t, &app_state, &control, &http).await
                    }
                },
            ));
            tracing::info!("slack-unmatched-email worker spawned");
            Some(handle)
        } else {
            None
        };

    // Chat invite (org-onboarding item 22). Same tenant-looped queue shape
    // as slack_unmatched_email: the job is enqueued onto the account's own
    // jobs table from the Chat OAuth callback.
    let chat_invite_handle = if let (Some(pool), Some(store)) =
        (state.db.clone(), state.store.clone())
    {
        let chat_invite_control_pool = pool.clone();
        let chat_invite_http = state.http_client.clone();
        let chat_invite_state = state.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "chat_invite",
                workers::google_chat_invite::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let control = chat_invite_control_pool.clone();
                let http = chat_invite_http.clone();
                let app_state = chat_invite_state.clone();
                async move { workers::google_chat_invite::tick(&t, &app_state, &control, &http).await }
            },
        ));
        tracing::info!("chat-invite worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Content-tsv regconfig repair sweep worker (PR #190 follow-up) ────────
    // CONSUMER-ONLY (org-tenancy PR 2.1 decision): one-shot repair of
    // migration-0062-era rows, which exist only on the shared plane (see the
    // decision table in `workers::tenant_harness`).
    // One-time bounded backfill for `atoms.content_tsv` rows written under
    // the old per-language (stemming) regconfig instead of 'simple' —
    // replaces migration 0062's original unbounded full-table `UPDATE`
    // (CodeRabbit review) with the same bounded-sweep shape as
    // `workers::slack_name_backfill`. Needs only the pool (candidate atom
    // selection + rewrite, jobs queue) — no Store/LLM/HTTP client required.
    // Not triggered by anything automatically; an operator hand-inserts the
    // sweep job after deploying this fix (see the worker's module doc for
    // the exact `INSERT INTO jobs` statement).
    //
    // Wired to the same `shutdown` signal as embed/extract/
    // slack_name_backfill above (stop claiming new jobs, bounded grace
    // window to finish anything already claimed).
    let content_tsv_backfill_handle = state.db.clone().map(|pool| {
        let handle = tokio::spawn(workers::content_tsv_backfill::run(pool, shutdown.clone()));
        tracing::info!("content-tsv-backfill worker spawned");
        handle
    });

    // ── Atom-title sweep (tenant loop) ────────────────────────────────────────
    // Generates `atoms.title` (migration 0097/0010) for corals that lack one
    // or only carry `routes::ingest`'s synchronous fallback (`title_kind =
    // 'fallback'`, migration 0212/0080), which is what lets date-range recall
    // answer from real LLM headlines instead of bodies. Same seed → per-
    // account-fan-out shape as `workers::slack_name_backfill` — see
    // `workers::atom_title_backfill`'s module doc for why this moved off the
    // control-pool-only `run` loop (verified in prod 2026-09-09: zero
    // `BackfillAtomTitles` jobs had EVER run on the tenant plane) and onto the
    // tenant harness. Takes the LLM because titling is an aux-tier call; a
    // `None` LLM (or an egress-blocked account) degrades to the deterministic
    // fallback title rather than skipping the coral. Takes R2 (optional, like
    // `extract`/`summary`) so R2-only atoms — the overwhelming majority in
    // prod since the 2026-08-04 R2 cutover — can actually be fetched and
    // titled instead of silently excluded from the sweep's old
    // `content_inline`-only candidate predicate.
    let atom_title_seed_clock = workers::atom_title_backfill::SeedClock::new();
    let atom_title_backfill_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let llm = state.llm.clone();
            let r2_clone = state.r2.clone();
            let control_pool = pool.clone();
            let seed_clock = atom_title_seed_clock.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "atom_title_backfill",
                    workers::atom_title_backfill::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let llm = llm.clone();
                    let r2 = r2_clone.clone();
                    let control = control_pool.clone();
                    let seed_clock = seed_clock.clone();
                    async move {
                        // `Send + Sync` is needed to hold the LLM across the
                        // spawn; `tick` only needs the bare trait object.
                        let llm_ref: Option<&dyn crate::llm::LlmProvider> =
                            llm.as_deref().map(|l| l as &dyn crate::llm::LlmProvider);
                        workers::atom_title_backfill::tick(
                            &t,
                            &control,
                            llm_ref,
                            r2.as_deref(),
                            &seed_clock,
                        )
                        .await
                    }
                },
            ));
            tracing::info!("atom-title-backfill worker spawned");
            Some(handle)
        } else {
            tracing::warn!("atom-title-backfill worker NOT spawned — requires database and store");
            None
        };

    // ── Huddle capture worker (org-slack step 4) ──────────────────────────────
    // Tenant loop: `enqueue_huddle_capture` (`routes::slack_events`) moved
    // planes in the SAME commit as this worker (see that function's doc
    // comment) — `JobKind::SlackHuddleCapture` jobs land on WHICHEVER plane
    // the huddle-capture trigger resolved, and this worker's dequeue must
    // follow, same reasoning as `slack_ingest`/`reaper`/`tombstone_purge`
    // above (see the decision table in `workers::tenant_harness` and this
    // worker's own module doc).
    // Downloads the Slack AI notes canvas and ingests it (or records a
    // decided skip). `control_pool` is threaded through the same way
    // `member_ocean_reconciler`/`slack_ingest`/`slack_name_backfill` above
    // thread theirs — needed for the Slack bot token (`oauth_tokens`,
    // control-only) and the account's personal-vs-org plane; every content
    // read/write runs on `t.pool`/`t.tenant`.
    //
    // Handle captured and drained below (unlike `slack_ingest`/`reaper`
    // above): a tick here does a canvas download, DEK/crypto work, and a
    // blob write per job, so on SIGTERM it deserves the same bounded grace
    // window as `slack_name_backfill`/`slack_seat_reconcile` (its sibling
    // queue workers) rather than being dropped instantly.
    let huddle_handle = if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let huddle_control_pool = pool.clone();
        let huddle_http = state.http_client.clone();
        let huddle_r2 = state.r2.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "huddle",
                workers::huddle::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let control = huddle_control_pool.clone();
                let http = huddle_http.clone();
                let r2 = huddle_r2.clone();
                async move { workers::huddle::tick(&t, &control, &http, r2.as_deref()).await }
            },
        ));
        tracing::info!("huddle capture worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Graph subscription renewal + backstop worker (m365 Train 2 PR 2.2) ───
    // Tenant loop: the last consumer-only straggler on `workers::
    // tenant_harness`'s decision table (`huddle` moved earlier, in the Slack
    // train). Two registrations, not one, because renewal/ensure-recreate and
    // the backstop resync run on different native cadences and
    // `HarnessConfig::sweep_worker` carries exactly one — see `workers::
    // graph_subscriptions`'s own module doc for the full two-pool-split,
    // account-enumeration, and org-tenant-backstop-deferral rationale.
    // `graph_subs_control_pool` is the shared control/consumer pool, needed
    // for the Microsoft OAuth token (`oauth_tokens`, control-only); every
    // subscription/watched-resource read/write runs on `t.pool`.
    // `graph_subs_backstop_state` is a full `Arc<AppState>` clone — the
    // backstop tick's content-ingest step (consumer plane only, for now)
    // still goes through the canonical `routes::graph_webhook::ingest_*`
    // path, same reasoning `workers::slack_ingest`'s own registration
    // documents for its `state` clone.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let graph_subs_control_pool = pool.clone();
        let graph_subs_http = state.http_client.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool.clone(),
            store.clone(),
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "graph_subscriptions_renew",
                workers::graph_subscriptions::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = graph_subs_control_pool.clone();
                let http = graph_subs_http.clone();
                async move { workers::graph_subscriptions::tick_renew(&t, &control, &http).await }
            },
        ));
        tracing::info!("graph subscription renewal worker spawned");

        let graph_subs_backstop_control_pool = pool.clone();
        let graph_subs_backstop_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool.clone(),
            store.clone(),
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "graph_subscriptions_backstop",
                workers::graph_subscriptions::BACKSTOP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = graph_subs_backstop_control_pool.clone();
                let state = graph_subs_backstop_state.clone();
                async move { workers::graph_subscriptions::tick_backstop(&t, &control, &state).await }
            },
        ));
        tracing::info!("graph subscription backstop resync worker spawned");

        // Polling doorbells (slot taken, or no eligible owner) re-list about
        // every 45 minutes. Live doorbells stay on the 6-hour backstop above.
        // Mail is not part of this tick.
        let graph_subs_poll_control_pool = pool.clone();
        let graph_subs_poll_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool.clone(),
            store.clone(),
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "graph_doorbell_polling",
                workers::graph_subscriptions::DOORBELL_POLLING_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = graph_subs_poll_control_pool.clone();
                let state = graph_subs_poll_state.clone();
                async move {
                    workers::graph_subscriptions::tick_doorbell_polling(&t, &control, &state).await
                }
            },
        ));
        tracing::info!("teams doorbell polling worker spawned");

        // m365 Train 2 PR 2.6: resumes any backfill (watched Outlook
        // conversation, watched Teams resource, or Outlook workflow candidate
        // scan) a previous pass left incomplete under a per-call page budget
        // — see `workers::graph_subscriptions::tick_backfill_continue`'s own
        // module doc. Same `Arc<AppState>` clone as the backstop tick, same
        // two-pool-split reasoning.
        let graph_subs_backfill_control_pool = pool.clone();
        let graph_subs_backfill_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "graph_subscriptions_backfill_continue",
                workers::graph_subscriptions::BACKFILL_CONTINUE_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = graph_subs_backfill_control_pool.clone();
                let state = graph_subs_backfill_state.clone();
                async move {
                    workers::graph_subscriptions::tick_backfill_continue(&t, &control, &state).await
                }
            },
        ));
        tracing::info!("graph subscription backfill-continuation worker spawned");
    }

    // ── Google watch/channel renewal worker (gsuite Train G1 PR 2) ───────────
    // Tenant-loop from birth (no legacy single-loop version to migrate away
    // from — see workers::google_watch_renewal's own module doc). Two
    // registrations for the same reason graph_subscriptions has two:
    // renewal and the backstop alert sweep run on different native cadences
    // and `HarnessConfig::sweep_worker` carries exactly one.
    // `google_watch_control_pool` is the shared control/consumer pool,
    // needed for the account's Google OAuth token (`oauth_tokens`,
    // control-only); every `google_watch_channels` read/write runs on
    // `t.pool`.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let google_watch_control_pool = pool.clone();
        let google_watch_http = state.http_client.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool.clone(),
            store.clone(),
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "google_watch_renew",
                workers::google_watch_renewal::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = google_watch_control_pool.clone();
                let http = google_watch_http.clone();
                async move { workers::google_watch_renewal::tick_renew(&t, &control, &http).await }
            },
        ));
        tracing::info!("google watch renewal worker spawned");

        let google_watch_backstop_control_pool = pool.clone();
        let google_watch_backstop_http = state.http_client.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "google_watch_backstop",
                workers::google_watch_renewal::BACKSTOP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = google_watch_backstop_control_pool.clone();
                let http = google_watch_backstop_http.clone();
                async move { workers::google_watch_renewal::tick_backstop(&t, &control, &http).await }
            },
        ));
        tracing::info!("google watch backstop alert worker spawned");
    }

    // ── Teams seat reconciler (m365 Train 2 PR 2.4, decision #3) ─────────────
    // The Microsoft analog of `slack_seat_reconcile` above — see that
    // registration's comment for the shared rationale (job-driven, hand-
    // insertable exactly like `SlackNameBackfill`/`SlackSeatReconcile`; see
    // `jobs::JobKind::TeamsSeatReconcile`'s doc comment for the SQL). Unlike
    // Slack, Teams has no live channel-roster-membership event at all, so
    // this worker is the ONLY seating mechanism for Teams, not merely a
    // backstop for one — and its discovery query is deliberately empty in
    // production today (no Teams destination picker yet routes a channel
    // into a named team ocean) — see `workers::teams_seat_reconcile`'s own
    // module doc for the full "shipped ahead of its own picker" rationale.
    // `control_pool` resolves the account roster and the CONTROL-only halves
    // of the roster-identity match (`connectors::microsoft::
    // resolve_team_roster_members`); every seat read/write runs on `t.pool`.
    let teams_seat_reconcile_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let teams_seat_reconcile_control_pool = pool.clone();
            let teams_seat_reconcile_http = state.http_client.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "teams_seat_reconcile",
                    workers::teams_seat_reconcile::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = teams_seat_reconcile_control_pool.clone();
                    let http = teams_seat_reconcile_http.clone();
                    async move { workers::teams_seat_reconcile::tick(&t, &control, &http).await }
                },
            ));
            tracing::info!("teams-seat-reconcile worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── Teams unmatched-invite worker (org-onboarding plan item 19) ──────────
    // Dequeues `EmailUnmatchedTeamsMembers`. The roster read is a connected
    // member's delegated token inside `resolve_team_roster_members`; this
    // registration only supplies the HTTP client and the control pool the
    // notice claim writes to.
    let teams_unmatched_email_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let teams_unmatched_control_pool = pool.clone();
            let teams_unmatched_http = state.http_client.clone();
            let teams_unmatched_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "teams_unmatched_email",
                    workers::teams_unmatched_email::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = teams_unmatched_control_pool.clone();
                    let http = teams_unmatched_http.clone();
                    let app_state = teams_unmatched_state.clone();
                    async move {
                        workers::teams_unmatched_email::tick(&t, &app_state, &control, &http).await
                    }
                },
            ));
            tracing::info!("teams-unmatched-email worker spawned");
            Some(handle)
        } else {
            None
        };

    CollaborationWorkers {
        slack_name_backfill_handle,
        promote_handle,
        slack_seat_reconcile_handle,
        slack_unmatched_email_handle,
        chat_invite_handle,
        content_tsv_backfill_handle,
        atom_title_backfill_handle,
        huddle_handle,
        teams_seat_reconcile_handle,
        teams_unmatched_email_handle,
    }
}

#[derive(Default)]
pub(super) struct ConnectorWorkers {
    pub(super) slot_reservations_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) member_connect_nudge_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) org_join_request_digest_handle: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn connectors(state: &Arc<AppState>, shutdown: &shutdown::Shutdown) -> ConnectorWorkers {
    let worker_runtime = state.tenant_runtime.clone();
    let worker_activity = state.worker_activity.clone();
    // ── Microsoft Calendar sync worker (both planes) ─────────────────────────
    // Per-member calendar default-on for org members (amended decision #1,
    // m365 enterprise train PR 3.8) AND the sweep of every personal
    // account-wide Microsoft connection on the consumer entry (discovered
    // from `oauth_tokens` via `resolve_microsoft_calendar_personal_accounts`,
    // least-recently-attempted first). Rides the SAME tenant-loop harness
    // `graph_subscriptions` uses, registered the same shape as that worker's
    // own sweep registrations above: `control_pool` for the Microsoft OAuth
    // token + discovery (`oauth_tokens`, control-only); `state` (not just
    // `http`) because ingest goes through the canonical
    // `routes::ingest::ingest_atom` path, not a bare Graph fetch — see
    // `workers::microsoft_calendar_sync`'s own module doc.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let ms_calendar_control_pool = pool.clone();
        let ms_calendar_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "microsoft_calendar_sync",
                workers::microsoft_calendar_sync::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = ms_calendar_control_pool.clone();
                let state = ms_calendar_state.clone();
                async move { workers::microsoft_calendar_sync::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("microsoft calendar sync worker spawned");
    }

    // ── Google Calendar sync worker (both planes) ────────────────────────────
    // The Google twin of the Microsoft calendar worker above: per-member
    // catch-up sweep for ORG members (gated per org on the `google_calendar`
    // registry flag, `ORG_GOOGLE_CALENDAR_ENABLED`, default off) AND the
    // sweep of every personal account-wide Calendar connection on the
    // consumer entry (discovered from `oauth_tokens`, so a connection whose
    // connect-time `events.watch` failed still gets its channel + backfill).
    // The push webhook is the low-latency path; this is the backstop. See
    // `workers::google_calendar_sync`'s module doc.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let g_calendar_control_pool = pool.clone();
        let g_calendar_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "google_calendar_sync",
                workers::google_calendar_sync::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = g_calendar_control_pool.clone();
                let state = g_calendar_state.clone();
                async move { workers::google_calendar_sync::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("google calendar sync worker spawned");
    }

    // Explicit meeting consent has its own retry clock, independent of calendar capture.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let meeting_control = pool.clone();
        let meeting_state = state.clone();
        let mut meeting_cfg = workers::tenant_harness::HarnessConfig::sweep_worker(
            "teams_meeting_sync",
            workers::teams_meeting_sync::POLL_INTERVAL,
        );
        // New meeting consent and Outlook schedule intents may be due before
        // the currently-known work deadline; both routes signal note_enqueue.
        meeting_cfg.wake_on_enqueue = true;
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            meeting_cfg,
            shutdown.clone(),
            move |t| {
                let control = meeting_control.clone();
                let state = meeting_state.clone();
                async move { workers::teams_meeting_sync::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("selected Teams meeting sync worker spawned");
    }

    // ── Outlook everything-mode sync worker (m365 enterprise train PR 3.4) ───
    // The whole-mailbox pull half of the contract's everything-mode carve-out
    // — a deliberate SIBLING of the calendar worker above, not a merge into
    // it (see `workers::outlook_everything_mode`'s own module doc for why).
    // Both planes are genuinely reachable here (unlike the calendar worker's
    // consumer no-op): a personal-plane user can enable everything mode for
    // their own AccountWide-token mailbox exactly as an org member can for
    // their own Member-token one.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let everything_mode_control_pool = pool.clone();
        let everything_mode_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "outlook_everything_mode",
                workers::outlook_everything_mode::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = everything_mode_control_pool.clone();
                let state = everything_mode_state.clone();
                async move { workers::outlook_everything_mode::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("outlook everything-mode sync worker spawned");
    }

    // ── Gmail everything-mode sync worker (gsuite enterprise train PR G2.4) ──
    // The whole-mailbox pull half of the contract's everything-mode carve-out,
    // consuming Outlook's own worker above as its structural + mechanical
    // precedent (see `workers::gmail_everything_mode`'s own module doc for
    // what genuinely differs). Both planes are genuinely reachable here, same
    // as the Outlook worker — Gmail tokens are ALWAYS per-member on both
    // planes (unlike Outlook's AccountWide personal-plane arm).
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let gmail_everything_mode_control_pool = pool.clone();
        let gmail_everything_mode_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "gmail_everything_mode",
                workers::gmail_everything_mode::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = gmail_everything_mode_control_pool.clone();
                let state = gmail_everything_mode_state.clone();
                async move { workers::gmail_everything_mode::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("gmail everything-mode sync worker spawned");
    }

    // ── Google Chat spaces sweep (gsuite Train G4 PR 4) ──────────────────────
    // Backfill + catch-up + roster reconcile for routed Chat SPACES. On the
    // TENANT loop, not the consumer loop: `CONNECTOR_CONTRACT.md` §5.5 is
    // explicit that a pull sweep registered on the single consumer loop
    // *silently never runs for org tenants* — the exact failure
    // `workers::graph_subscriptions` had to be converted away from. Chat
    // starts tenant-native, like `workers::google_watch_renewal` above.
    // D1 keeps the org GATE closed until G4.6, so the org arm finds nothing
    // in production today; the registration and the code path are real so
    // that G4.6 is a gate flip rather than a worker rewrite. `control_pool`
    // is threaded through for `oauth_tokens`/`accounts`/`account_members`
    // (control-only); every space/atom/graph read-write runs on `t.pool`.
    //
    // NOT in the shutdown drain below, and that is the established shape
    // here, not an oversight: of this function's `run_multi_tenant` spawns
    // only a named subset retains a `JoinHandle` for `drain_one`, and every
    // connector sweep — `google_watch_renewal`, `gmail_everything_mode`,
    // `outlook_everything_mode`, `graph_subscriptions` — drops its handle
    // exactly like this one. What makes that safe for THIS worker
    // specifically is that a pass aborted mid-flight costs nothing: the
    // cursor is committed only after every destination and every item
    // succeeds (`google_chat_sweep::sync_space_once`'s own doc), so an
    // interrupted pass re-walks the identical range on the next tick, and
    // both the atom write and the deletion are idempotent. Draining this one
    // alone would buy a shorter re-walk at the cost of being the only
    // connector worker in the file that behaves differently.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let chat_sweep_control_pool = pool.clone();
        let chat_sweep_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "google_chat_sweep",
                workers::google_chat_sweep::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = chat_sweep_control_pool.clone();
                let state = chat_sweep_state.clone();
                async move { workers::google_chat_sweep::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("google chat spaces sweep worker spawned");
    }

    // Chat cold-start repeat (item 25). The first post is on ADDED_TO_SPACE.
    // This sweep is the daily follow-up. Control-only; requestId makes a
    // replay of a post Google already accepted a no-op, so dropping the
    // handle matches the other connector sweeps.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let cold_start_control = pool.clone();
        let cold_start_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "chat_cold_start",
                workers::chat_cold_start::interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = cold_start_control.clone();
                let state = cold_start_state.clone();
                async move { workers::chat_cold_start::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("chat cold-start worker spawned");
    }

    // ── Google Meet sweep worker (gsuite Train G3 PR 2) ───────────────────────
    // Pull-only backstop (no push subscription for Meet in this PR — see
    // `connectors::google_meet`'s own module doc, "Delivery"). The consumer
    // tick keeps its cursor/ledger on the control plane; the org tick (Meet
    // org-plane lift, a no-op unless the org's `google_meet` flag is on) keeps
    // them on the org's own tenant pool (`t.pool`) — see
    // `workers::google_meet_sweep`'s own module doc. Not in the shutdown drain, same reasoning as every other connector
    // sweep here (google_watch_renewal, gmail_everything_mode,
    // outlook_everything_mode, graph_subscriptions, google_chat_sweep): a pass
    // aborted mid-flight costs nothing — the watermark only advances past a
    // conference record that fully succeeded, and re-walking the same range is
    // idempotent (`atoms`' own `(graph_id, source_type, source_id)` uniqueness
    // plus the `google_meet_ingested_conferences` ledger).
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let meet_sweep_control_pool = pool.clone();
        let meet_sweep_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "google_meet_sweep",
                workers::google_meet_sweep::POLL_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = meet_sweep_control_pool.clone();
                let state = meet_sweep_state.clone();
                async move { workers::google_meet_sweep::tick(&t, &control, &state).await }
            },
        ));
        tracing::info!("google meet sweep worker spawned");
    }

    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let composio_control = pool.clone();
        let composio_state = state.clone();
        let composio_worker = workers::composio_capture::Worker::from_env(shutdown.clone());
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "composio_capture",
                workers::composio_capture::poll_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = composio_control.clone();
                let state = composio_state.clone();
                let worker = composio_worker.clone();
                async move { worker.tick(&t, &control, &state).await }
            },
        ));
    }

    // ── Tombstone purge worker (ADR-0005) ─────────────────────────────────────
    // Daily sweep of expired soft-delete tombstones (op_log delete-ops older
    // than TOMBSTONE_RETENTION_DAYS), which otherwise accumulate forever.
    // Tenant loop: `op_log` is a tenant-tree table (tenant Stores emit sync
    // ops), so each tenant DB needs the same daily purge. The harness polls a
    // newly-seen tenant immediately, preserving the old fire-once-at-boot
    // interval behavior. (Store is constructed from `db` unconditionally
    // above, so gating on both doesn't narrow availability.)
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "tombstone_purge",
                workers::tombstone_purge::PURGE_INTERVAL,
            ),
            shutdown.clone(),
            move |t| async move { workers::tombstone_purge::tick(&t).await },
        ));
        tracing::info!("tombstone purge worker spawned");
    }

    // ── MCP task TTL sweep (MCP spec 2026-07-28 PR 2, task-based ask_ocean) ──
    // Hourly reclamation of expired `mcp_tasks` rows (24h TTL) — pure storage
    // reclamation, not a correctness dependency (`get_task_result` already
    // treats an expired-but-unswept row as `not_found` at read time). Tenant
    // loop: `mcp_tasks` rows live on whichever plane created them (the
    // consumer pool for personal/flag-off callers, an org's own tenant pool
    // for org callers — same tenancy routing as every other MCP tool), so
    // every active tenant needs its own sweep, same shape as the tombstone
    // purge above.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "mcp_task_sweep",
                workers::mcp_task_sweep::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| async move { workers::mcp_task_sweep::tick(&t).await },
        ));
        tracing::info!("mcp task sweep worker spawned");
    }

    // ── Review-deadline sweep (high-signal review, Stage 1 PR 3) ─────────────
    // Hourly: resolves review-queue items older than REVIEW_DEADLINE_DAYS with
    // the machine's best guess, so the review badge is structurally incapable
    // of unbounded growth. Tenant-looped for the same reason as the tombstone
    // purge above (relationship_edge/nodes are tenant-tree tables, and the
    // consumer plane sweeps loop-per-account under RLS).
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "review_deadline",
                workers::review_deadline::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| async move { workers::review_deadline::tick(&t).await },
        ));
        tracing::info!("review-deadline sweep worker spawned");
    }

    // ── Access-log purge worker (platform-admin-metrics PR 1 of 7) ───────────
    // Daily sweep of `user_access_log` rows older than
    // `workers::access_log_purge::ACCESS_LOG_RETENTION_DAYS` (30 days — see
    // migration 0095_platform_metrics's header for the raw-IP retention
    // rationale). CONTROL-ONLY, not tenant-looped: `user_access_log` has no
    // tenant-tree counterpart and exists exactly once, in the control DB, so
    // this worker owns a small standalone loop against `state.db` directly
    // instead of registering with `workers::tenant_harness` (see the
    // worker's own module doc for the full "why not the harness" reasoning).
    // Needs only the pool — no Store/LLM/HTTP client required.
    if let Some(pool) = state.db.clone() {
        tokio::spawn(workers::access_log_purge::run(pool, shutdown.clone()));
        tracing::info!("access-log purge worker spawned");
    }

    // ── OAuth state jti replay-store sweep (foundation train PR 1.4) ─────────
    // Purges `oauth_state_jti` rows once they age past their own retention
    // window (the signed state token's `STATE_TTL_SECS` plus a clock-skew
    // buffer — see `workers::oauth_state_jti_sweep`'s own module doc for why
    // that is always safe). CONTROL-ONLY, not tenant-looped, for the exact
    // same reason as the access-log purge just above: OAuth `state` tokens
    // are minted/verified entirely against the control-plane `accounts`
    // table, so this table has no tenant-tree counterpart. Needs only the
    // pool.
    if let Some(pool) = state.db.clone() {
        tokio::spawn(workers::oauth_state_jti_sweep::run(pool, shutdown.clone()));
        tracing::info!("oauth-state-jti purge worker spawned");
    }

    // ── Platform rollup worker (platform-admin-metrics PR 5 of 7) ────────────
    // Nightly (env-configurable via `PLATFORM_ROLLUP_INTERVAL_SECS`) cross-tenant
    // rollup into `platform_metrics_daily`/`platform_active_users` — see
    // `workers::platform_rollup`'s module doc for the full consumer/org
    // scoping split and the per-tenant failure-isolation contract. Tenant
    // loop: an org's atom/reef/session/token counts live in that org's own
    // tenant DB, so every active tenant needs its own daily pass, same shape
    // as `tombstone_purge`/`reaper` above.
    //
    // `rollup_control_pool` is a SEPARATE clone of the control pool, captured
    // by this closure specifically so `tick` always has a handle to the
    // control-plane pool to WRITE `platform_metrics_daily`/
    // `platform_active_users` into — those tables are control-only and do not
    // exist on an org tenant's own database, so the harness's per-tenant
    // `t.pool` (that org's tenant DB) is only ever used to COLLECT stats, never
    // to write the rollup row. See `workers::platform_rollup::tick`'s doc
    // comment for the full read-vs-write pool split.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let rollup_control_pool = pool.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "platform_rollup",
                workers::platform_rollup::rollup_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = rollup_control_pool.clone();
                async move { workers::platform_rollup::tick(&t, &control).await }
            },
        ));
        tracing::info!("platform rollup worker spawned");
    }

    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let control = pool.clone();
        let config = managed::standing::execution::harness_config();
        tracing::info!(enabled=managed::standing::execution::rollout_enabled(), "standing rollout requires STANDING_PROCESSING_ENABLED=true");
        tracing::info!(enabled=managed::enrichment::enabled(), "managed_enrichment requires ENRICHMENT_PROCESSING_ENABLED=true; no job kind is admitted yet");
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool, store, worker_runtime.clone(), worker_activity.clone(), config, shutdown.clone(),
            move |t| {
                let control = control.clone();
                async move { managed::standing::execution::sweep_tick(&t, &control).await }
            },
        ));
    }

    // ── Capture admission health (COR-71) ────────────────────────────────────
    // Measures capture-to-Atom lag and stalled captures on every plane and
    // writes each plane's report to the control table that chain-health
    // reads. Same two-pool shape as platform_rollup above.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let health_control_pool = pool.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "capture_admission_health",
                workers::capture_admission_health::INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = health_control_pool.clone();
                async move { workers::capture_admission_health::tick(&t, &control).await }
            },
        ));
        tracing::info!("capture admission health worker spawned");
    }

    // ── Pipeline stage health ────────────────────────────────────────────────
    // Per-stage, per-source-type flow on every plane (embedding, extraction,
    // linking, reefs, ER, edges, journal, titles, Jev, dead letters, managed
    // spend and holds). Writes each plane's report to the control table the
    // `/internal/admin/pipeline-stage-health` route reads, and pages with
    // `alert = true` when a stage that should flow has stopped.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let stage_control_pool = pool.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "pipeline_stage_health",
                workers::pipeline_stage_health::INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = stage_control_pool.clone();
                async move { workers::pipeline_stage_health::tick(&t, &control).await }
            },
        ));
        tracing::info!("pipeline stage health worker spawned");
    }

    // ── Billing derivation worker (usage-metering PR9 of 13) ─────────────────
    // Turns raw `llm_usage` rows into `credit_ledger` debits + the
    // materialized `credit_balances` row — see `workers::billing_derivation`'s
    // module doc for the full three-axis (job/query/journal) design, the
    // per-account cursor placement, and the refuse-to-write-on-partial-
    // failure discipline it borrows from `platform_rollup::consumer_tick`.
    // Same two-pool shape as platform_rollup immediately above: tenant
    // content is COLLECTED from `t.pool`, the ledger is WRITTEN to a
    // separately-captured clone of the control pool, because
    // `credit_ledger`/`credit_balances` are control-only tables with no
    // tenant-tree counterpart at all.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let billing_control_pool = pool.clone();
        let billing_runtime = worker_runtime.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "billing_derivation",
                workers::billing_derivation::derivation_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = billing_control_pool.clone();
                let runtime = billing_runtime.clone();
                async move { workers::billing_derivation::tick_with_runtime(&t, &control, runtime.as_deref()).await }
            },
        ));
        tracing::info!("billing derivation worker spawned");
    }

    // ── Slot-reservation expiry sweep (CS-435, org-onboarding plan §3 item
    // 15, D9) ──────────────────────────────────────────────────────────────
    // Same `tenant_harness::sweep_worker` registration shape as
    // `billing_derivation` immediately above, but `workers::
    // slot_reservations::tick` is a no-op on every `TenantId::Org` pass —
    // `slot_reservations` is control-only with no tenant-tree twin (its own
    // migration header) — and needs no separately-captured `control` pool at
    // all: on the ONE pass it does real work (`TenantId::Consumer`), `t.pool`
    // already IS the control/consumer pool. See that module's own doc
    // comment.
    //
    // Drained below (unlike most one-indexed-`UPDATE` sweeps in this file,
    // which the `journal_sweep` registration's own comment names as "not
    // worth waiting on"): this sweep directly gates what
    // `billing::slot_pool::SlotPool::available()` reports to an admin
    // mid-purchase and to a future auto-add caller (org-onboarding plan §3
    // item 9, not yet built) deciding whether to fall back to approval — a
    // SIGTERM mid-pass leaving a handful of already-expired reservations
    // unswept for one extra interval is low-stakes on its own, but giving an
    // in-flight pass a bounded chance to finish keeps that number small
    // rather than deferring the whole thing silently until the process comes
    // back up.
    let slot_reservations_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::sweep_worker(
                    "slot_reservations",
                    workers::slot_reservations::sweep_interval(),
                ),
                shutdown.clone(),
                move |t| async move { workers::slot_reservations::tick(&t).await },
            ));
            tracing::info!("slot-reservation expiry sweep worker spawned");
            Some(handle)
        } else {
            tracing::warn!(
                "slot-reservation expiry sweep worker NOT spawned — requires database and store"
            );
            None
        };

    // ── Member-connect nudge worker (CS-436, org-onboarding plan §3 item 49,
    // D21) ─────────────────────────────────────────────────────────────────
    // "Connect your integration" email for an org member who was approved
    // but has not connected any platform with their own grant — the third
    // audience item 49 adds, and the admin-surfacing counterpart to
    // `routes::teams::list_org_members`'s `connected_providers`/
    // `connect_nudged_at` fields (same PR). See `workers::
    // member_connect_nudge`'s own module doc for the full candidate
    // predicate, the `slack_unmatched_email_notices` reuse under a new
    // `provider` value, and why this worker's claim/send ordering
    // deliberately diverges from `slack_unmatched_email`'s.
    //
    // Same `tenant_harness::sweep_worker` registration shape as
    // `billing_derivation` immediately above, but every table this worker
    // touches is control-plane-only (see that module doc's "Shape"
    // section), so `control` is the only pool its `tick` ever reads or
    // writes — `t.pool` is unused for anything but identifying WHICH org
    // this tick is for.
    //
    // Handle RETAINED and drained below (CodeRabbit round 1, CS-436,
    // Stability & Availability) — an earlier version of this registration
    // discarded the `tokio::spawn` handle the same way `billing_derivation`
    // immediately above does, on the reasoning that a periodic idempotent
    // sweep has no queue claim to abandon mid-tick. That reasoning covers
    // LOST WORK (the next tick just re-selects an untouched candidate) but
    // not this worker's own real external side effect: `process_account`
    // holds `send_connect_nudge_email`'s cooldown claim open across the
    // Resend call and commits only after a successful send (module doc's
    // own "claim vs send order" section) — if the process is torn down
    // in the narrow window AFTER Resend accepts the email but BEFORE that
    // commit lands, the claim never persists, and the SAME candidate is
    // nudged again on the very next tick: a real duplicate send, not a
    // merely-deferred one. Retaining the handle and joining it through the
    // existing bounded `grace` drain (`history_backfill_handle`'s own
    // registration is the identical shape) lets an in-flight tick's
    // current candidate finish its commit before the process exits,
    // closing that window for everything but a hard kill past `grace`.
    let member_connect_nudge_handle = if let (Some(pool), Some(store)) =
        (state.db.clone(), state.store.clone())
    {
        let member_connect_nudge_control_pool = pool.clone();
        let member_connect_nudge_state = state.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "member_connect_nudge",
                workers::member_connect_nudge::nudge_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = member_connect_nudge_control_pool.clone();
                let app_state = member_connect_nudge_state.clone();
                async move { workers::member_connect_nudge::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("member connect nudge worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Join-request digest worker (CS-438, org-onboarding plan §3 item 5,
    // D13) ─────────────────────────────────────────────────────────────────
    // The batched "N people asked to join <org> on CoralSwarm" admin email
    // for CS-437's join-request queue — one email per admin per day, only
    // on days with new requests. See `workers::org_join_request_digest`'s
    // own module doc for the full claim/idempotency design.
    //
    // Same `tenant_harness::sweep_worker` registration shape as
    // `member_connect_nudge` immediately above, and every table this
    // worker touches is control-plane-only for the identical reason (that
    // module doc's own "Shape" section), so `control` is the only pool its
    // `tick` ever reads or writes.
    //
    // Handle RETAINED and drained below — same reasoning as `member_
    // connect_nudge_handle` immediately above: this worker holds a claim
    // open across its own Resend call and commits only on a successful
    // send, so a hard kill in that narrow window risks a genuine duplicate
    // send on the next tick, not merely a deferred one.
    let org_join_request_digest_handle = if let (Some(pool), Some(store)) =
        (state.db.clone(), state.store.clone())
    {
        let org_join_request_digest_control_pool = pool.clone();
        let org_join_request_digest_state = state.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "org_join_request_digest",
                workers::org_join_request_digest::digest_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = org_join_request_digest_control_pool.clone();
                let app_state = org_join_request_digest_state.clone();
                async move { workers::org_join_request_digest::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("join-request digest worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Auto-add drain sweep worker (CS-449, org-onboarding plan §3 item
    //    38, D11's "ask once") ─────────────────────────────────────────────
    // The safety net behind the two synchronous drain calls
    // (`clerk_provision::remove_org_membership` and `routes::org_slots::
    // purchase_slots`) — see `workers::auto_add_drain_sweep`'s own module
    // doc for why a sweep is needed in addition to those two, not instead
    // of them. Same `tenant_harness::sweep_worker` registration shape as
    // `org_join_request_digest` immediately above, including the extra
    // `&Arc<AppState>` `tick` takes (auto-add provisions a real Clerk
    // membership, same as the digest's own `send_digest_email`/`state`
    // dependency).
    //
    // Handle NOT retained (unlike the digest's own): a drain has nothing
    // in flight across an external call that a hard kill mid-tick would
    // leave half-sent — every write `drain_org` makes is a single guarded
    // statement (`reserve_slot`, `mark_decided`, `patch_open_request`),
    // each already safe to interrupt on its own terms (the same reasoning
    // `slot_reservations`'s own sweep gives for needing no advisory lock).
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let auto_add_drain_control_pool = pool.clone();
        let auto_add_drain_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "auto_add_drain_sweep",
                workers::auto_add_drain_sweep::sweep_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = auto_add_drain_control_pool.clone();
                let app_state = auto_add_drain_state.clone();
                async move { workers::auto_add_drain_sweep::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("auto-add drain sweep worker spawned");
    }

    // ── Billing notices worker (settings launch plan PR 3.6) ──────────────────
    // Claims and delivers the extra-usage limit/depleted/member-cap emails.
    // See `workers::billing_notices` and `billing::notices` for the design.
    // Handle not retained: nothing is held open across the Resend call, and a
    // hard kill leaves a lease that expires plus an idempotent retry.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let billing_notices_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "billing_notices",
                workers::billing_notices::sweep_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let app_state = billing_notices_state.clone();
                async move { workers::billing_notices::tick(&t, &app_state).await }
            },
        ));
        tracing::info!("billing notices worker spawned");
    }

    // ── Org trials worker (self-serve signup PR C′) ───────────────────────────
    // Abandons unpaid Checkouts after 24 h, claims the trial emails, and
    // converts a trial whose credits ran out (or whose 90 days are over) by
    // charging the seats. See `workers::org_trials` and `billing::org_trial`.
    // Handle not retained: no transaction spans a Stripe call and every step
    // is state-guarded and idempotency-keyed, so a hard kill loses nothing.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let org_trials_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "org_trials",
                workers::org_trials::sweep_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let app_state = org_trials_state.clone();
                async move { workers::org_trials::tick(&t, &app_state).await }
            },
        ));
        tracing::info!("org trials worker spawned");
    }

    // ── Self-serve org cleanup sweep (signup PR A2) ───────────────────────────
    // Deletes the Clerk org and accounts row of a sign-up whose Checkout was
    // abandoned, and the Clerk org of an unpaid org whose provisioning
    // failed (requeueing instead if it turns out to be paid). Workers
    // process group only (this function runs only where
    // `backend_role.runs_workers()`). Handle not retained: every step is
    // claimed with an atomic state change and re-checked before it deletes,
    // and a claim a hard kill leaves behind is taken over after 15 minutes.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let cleanup_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "org_selfserve_cleanup",
                workers::org_selfserve_cleanup::sweep_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let app_state = cleanup_state.clone();
                async move { workers::org_selfserve_cleanup::tick(&t, &app_state).await }
            },
        ));
        tracing::info!("org self-serve cleanup worker spawned");
    }

    // ── Auto-reload worker (settings launch plan PR 3.5) ──────────────────────
    // Charges saved cards off-session to top up extra usage, and reconciles
    // stuck reloads. See `workers::auto_reload` and `billing::extra_usage::reload`.
    // Handle not retained: every step is idempotent (stable Stripe key, row
    // state machine), so a hard kill is recovered by the reaper.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let auto_reload_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "auto_reload",
                workers::auto_reload::sweep_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let app_state = auto_reload_state.clone();
                async move { workers::auto_reload::tick(&t, &app_state).await }
            },
        ));
        tracing::info!("auto-reload worker spawned");
    }

    // ── Usage reconciliation worker (usage-metering PR11 of 13) ──────────────
    // Daily `llm_usage` vs `query_history` cross-check plus deduped
    // unknown-model alerting into `usage_reconciliation_daily` — see
    // `workers::usage_reconciliation`'s module doc for the full design and
    // its relationship to `billing_derivation` (PR9) immediately above. Same
    // two-pool shape: tenant content is COLLECTED from `t.pool`, the summary
    // row is WRITTEN to a separately-captured clone of the control pool,
    // because `usage_reconciliation_daily` is control-only with no
    // tenant-tree counterpart, same as `platform_metrics_daily`/
    // `credit_ledger`.
    //
    // `unknown_model_alert_tracker` is created ONCE here, wrapped in a
    // `Mutex`, and captured by the closure below — the ONE process-wide,
    // long-lived instance `billing::unknown_model_alert::
    // UnknownModelAlertTracker`'s own doc requires: it must persist across
    // every tenant's tick and every day's tick for its escalate-once-then-
    // quiet dedup to mean anything, exactly like `workers::tenant_harness`'s
    // own `pool_failures` counter persists across that harness's retry loop.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let reconciliation_control_pool = pool.clone();
        let unknown_model_alert_tracker = std::sync::Arc::new(std::sync::Mutex::new(
            billing::unknown_model_alert::UnknownModelAlertTracker::new(),
        ));
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "usage_reconciliation",
                workers::usage_reconciliation::reconciliation_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = reconciliation_control_pool.clone();
                let tracker = unknown_model_alert_tracker.clone();
                async move { workers::usage_reconciliation::tick(&t, &control, &tracker).await }
            },
        ));
        tracing::info!("usage reconciliation worker spawned");
    }

    // ── Waitlist sync worker (platform-admin-metrics PR 5 of 7) ──────────────
    // Daily sync of the marketing site's Cloudflare D1 waitlist into the
    // control-only `waitlist_snapshot` table (migration
    // `0096_waitlist_snapshot`). CONTROL-ONLY, not tenant-looped — same "no
    // tenant dimension, owns a small standalone loop" shape as
    // `workers::access_log_purge` above (see `workers::waitlist_sync`'s
    // module doc for the full reasoning). Needs the pool and the shared HTTP
    // client; degrades to a no-op with a logged warning (never a panic, never
    // a failed request) if `CF_ACCOUNT_ID`/`CF_D1_DATABASE_ID`/`CF_API_TOKEN`
    // aren't all set.
    if let Some(pool) = state.db.clone() {
        tokio::spawn(workers::waitlist_sync::run(
            pool,
            state.http_client.clone(),
            shutdown.clone(),
        ));
        tracing::info!("waitlist-sync worker spawned");
    }

    // ── Account-deletion worker (settings launch PR 2.4) ──────────────────
    // Drives `account_deletions` rows (migration 0370) through
    // `services::account_deletion`'s resumable steps. CONTROL-ONLY and
    // standalone like the access-log purge above. Not drained at shutdown: a
    // SIGTERM mid-step leaves a lease that expires, and the next worker
    // resumes at the first unrecorded step.
    if state.db.is_some() {
        tokio::spawn(workers::account_deletion::run(state.clone(), shutdown.clone()));
        tracing::info!("account-deletion worker spawned");
    }

    // ── Org-deletion worker (control 0387) ────────────────────────────────
    // Drives `org_deletions` through `services::org_deletion`, plus the
    // unpaid / Clerk-dashboard entries and the 30-day export retention.
    // Idles while ORG_DELETION_ENABLED is unset. Not drained at shutdown,
    // for the same reason as account deletion: a lease that expires and a
    // checkpointed, idempotent step list.
    if state.db.is_some() {
        tokio::spawn(workers::org_deletion::run(state.clone(), shutdown.clone()));
        tracing::info!("org-deletion worker spawned");
    }

    ConnectorWorkers {
        slot_reservations_handle,
        member_connect_nudge_handle,
        org_join_request_digest_handle,
    }
}

#[derive(Default)]
pub(super) struct IngestionWorkers {
    pub(super) history_backfill_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) github_backfill_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) github_event_ingest_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) ingest_receive_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) github_reconcile_sweep_handle: Option<tokio::task::JoinHandle<()>>,
    pub(super) journal_sweep_handle: Option<tokio::task::JoinHandle<()>>,
}

pub(super) fn ingestion(state: &Arc<AppState>, shutdown: &shutdown::Shutdown) -> IngestionWorkers {
    let worker_runtime = state.tenant_runtime.clone();
    let worker_activity = state.worker_activity.clone();
    // Read-model queue: existing separate worker pool, schema gate, wake bridge,
    // idle backoff and per-tenant failure isolation; workers process only.
    if let (Some(pool),Some(store))=(state.db.clone(),state.store.clone()) {
        let control=pool.clone();
        let model_r2=state.r2.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,store,worker_runtime.clone(),worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker("read_model_refresh",workers::read_model_refresh::BASE_POLL),
            shutdown.clone(),move |t|{let control=control.clone();let r2=model_r2.clone();async move{workers::read_model_refresh::tick_with_blobs(&t,&control,r2.as_deref()).await}}
        ));
    }
    // ── Job reaper ─────────────────────────────────────────────────────────────
    // Backstop for orphaned `running` jobs — see `jobs::queue`'s module docs
    // and `workers::reaper` for the full incident writeup this closes.
    // Needs only the pool; runs even when Store/LLM aren't configured, which
    // is exactly when a stuck queue is hardest to notice otherwise.
    //
    // Tenant loop: every tenant DB has its own `jobs` queue that can orphan
    // `running` rows, so the reaper sweeps them all. The harness owns
    // shutdown-aware scheduling (stop starting new sweeps promptly) — see
    // `workers::reaper::tick`'s doc comment for the exact contract. Its
    // handle is intentionally NOT joined in the "Drain shutdown-aware job
    // workers" section below: a sweep is one indexed `UPDATE` bounded by the
    // reaper's own 30s internal timeout, not a multi-step job, so there is
    // nothing worth waiting on beyond it simply stopping. (Store is
    // constructed from `db` unconditionally above, so gating on both doesn't
    // narrow the "runs even when LLM/R2 are unconfigured" property.)
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let stale_after = workers::reaper::stale_after();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "reaper",
                workers::reaper::REAP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| async move { workers::reaper::tick(&t, stale_after).await },
        ));
        tracing::info!("job reaper worker spawned");
    }

    // ── `llm_usage_pending` orphan sweeper (usage-metering plan PR6) ─────────
    // Reclaims `llm_usage_pending` rows a reserve/finalize call left behind —
    // a provider error, a killed process, or (the COMMON case) a client
    // disconnecting mid-stream before `finalize_for_stream` ever runs. See
    // `workers::usage_pending_sweep`'s module doc for the full `kind='sync'`
    // (~15min) vs `kind='batch'` (~25h, matching the Batches API's ~24h
    // window) floor rationale. Tenant loop: `llm_usage_pending` exists on
    // BOTH planes (paired migration control `0149`/tenant `0055`), same shape
    // as `reaper`/`tombstone_purge` above.
    //
    // No `JoinHandle` captured, so this is intentionally absent from the
    // "Drain shutdown-aware job workers" step below — a sweep is one bounded
    // DB round-trip (or, on the consumer plane, a handful of them), not a
    // multi-step job worth waiting on; see `workers::usage_pending_sweep`'s
    // own "Shutdown" doc section for why copying `platform_rollup`'s spawn
    // site would NOT have proven that (it discards its handle too, but isn't
    // in the drain list for an unrelated reason).
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "usage_pending_sweep",
                workers::usage_pending_sweep::SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| async move { workers::usage_pending_sweep::tick(&t).await },
        ));
        tracing::info!("usage-pending sweep worker spawned");
    }

    if !crate::admission::env_flag(workers::jobs_retention::ENABLED_ENV) {
        tracing::info!("jobs-retention: disabled");
    } else {
        match workers::jobs_retention::Policy::from_env() {
            Err(e) => tracing::error!(error = %e, "jobs-retention: disabled, invalid configuration"),
            Ok(policy) => {
                if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
                    tokio::spawn(workers::tenant_harness::run_multi_tenant(
                        pool,
                        store,
                        worker_runtime.clone(),
                        worker_activity.clone(),
                        workers::jobs_retention::harness_config(),
                        shutdown.clone(),
                        move |t| async move { workers::jobs_retention::tick(&t, policy).await },
                    ));
                    tracing::info!(
                        done_days = policy.done_days,
                        dead_days = policy.dead_days,
                        "jobs-retention: enabled"
                    );
                }
            }
        }
    }

    // ── Member-ocean reconciler (member-private-oceans PR 3, provision arm;
    //    PR 8, purge arm) ──
    // Sweep-worker trigger 3 from docs/plans/member-private-oceans.md §2.4:
    // for each org tenant, diff `account_members` (control-plane) against
    // that tenant's `graphs WHERE kind='member'` and create any missing
    // primary member oceans. Behind `MEMBER_OCEANS_ENABLED` (default off,
    // checked inside the tick itself so flipping it takes effect on the next
    // scheduled tick with no redeploy) — inert while off. Tenant loop only
    // (member oceans are an org-tenant-only concept; the tick no-ops on the
    // consumer plane). `control_pool` is threaded through the same way
    // `workers::summary::tick` threads its own control-plane read — see that
    // worker's spawn site just above for the identical pattern.
    //
    // `state.r2.clone()` is ALSO threaded through, purely for the purge arm
    // (PR 8, `MEMBER_OCEAN_PURGE_ENABLED` — its own flag, default off,
    // independent of `MEMBER_OCEANS_ENABLED`): every other arm in that file
    // ignores it. `None` (no R2 configured, e.g. local dev with no R2_*
    // secrets) is a legitimate value here — the purge arm treats "blobs to
    // delete but no R2 client" as an abort-and-retry-next-tick condition for
    // that one graph, never a panic.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let control_pool = pool.clone();
        let member_ocean_r2 = state.r2.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            // Keeps Clerk-deleted orgs scheduled so the archive arm can
            // revoke their share tokens (#1396); see `harness_config`.
            workers::member_ocean_reconciler::harness_config(),
            shutdown.clone(),
            move |t| {
                let control = control_pool.clone();
                let r2 = member_ocean_r2.clone();
                async move {
                    let blobs = r2
                        .as_deref()
                        .map(|r| r as &dyn workers::member_ocean_reconciler::PurgeBlobDeleter);
                    workers::member_ocean_reconciler::tick(&t, &control, blobs).await
                }
            },
        ));
        tracing::info!("member-ocean reconciler worker spawned");
    }

    // ── Slack message ingest worker (org-slack step 2) ────────────────────────
    // Drains `JobKind::SlackMessageIngest` off whichever plane `routes::
    // slack_events::enqueue_slack_message_ingest` resolved and enqueued
    // onto — the durable replacement for that handler's old inline
    // `tokio::spawn(ingest_slack_message(...))`. Tenant loop: each plane's
    // `jobs` table is invisible to a dequeuer scoped to any other one, same
    // reasoning as `reaper`/`tombstone_purge` above. `control_pool` is
    // threaded through the same way `member_ocean_reconciler`'s just above
    // does — needed only to resolve each job's personal-vs-org
    // `AccountPlane` (`accounts.clerk_org_id`, control-only). `state` is a
    // full clone purely so `ingest_slack_message` gets its plane-agnostic
    // shared clients (`http_client`, `r2`, `dek_cache`) — the SAME state
    // clone shape `workers::graph_subscriptions::tick_backstop`'s own
    // registration uses (org-tenancy m365 Train 2 PR 2.2).
    //
    // Huddle capture is NOT wired through this worker — it has its own
    // tenant-looped `workers::huddle::tick`, spawned separately above (org-
    // slack step 4), since its trigger/payload shape and ledger
    // (`slack_huddle_captures`) are distinct from a plain message ingest.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let slack_ingest_control_pool = pool.clone();
        let slack_ingest_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "slack_ingest",
                workers::slack_ingest::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let control = slack_ingest_control_pool.clone();
                let app_state = slack_ingest_state.clone();
                async move { workers::slack_ingest::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("slack ingest worker spawned");
    }

    // Slack per-member history continuation (Phase G wave 3, item 46b).
    // Connect pulls the first page; this sweep continues one page per due
    // channel on that member's own slack_user token.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let member_capture_control = pool.clone();
        let member_capture_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "slack_member_capture",
                workers::slack_member_capture::INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let control = member_capture_control.clone();
                let app_state = member_capture_state.clone();
                async move { workers::slack_member_capture::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("slack member capture backfill worker spawned");
    }

    // ── History backfill worker (chat-per-user-ingest plan §4, PR 2) ─────────
    // Drains `JobKind::HistoryBackfill` off whichever plane `routes::
    // slack_interactivity`'s backfill-window dispatch enqueued it onto — the
    // durable replacement for that handler's old inline `tokio::spawn(
    // backfill_channel(..))`. Tenant loop, same reasoning as `slack_ingest`
    // just above: each plane's `jobs` table is invisible to a dequeuer
    // scoped to any other one, and this is precisely what makes an ORG
    // backfill reachable at all — the job sits on the SAME tenant pool its
    // `target_graph_id` lives on. `control_pool` is threaded through only to
    // resolve the bot token (`oauth_tokens` is control-plane-only for every
    // provider); `state` is a full clone so `routes::ingest::ingest_atom`
    // gets its plane-agnostic shared clients, same shape `slack_ingest`
    // above uses for `ingest_slack_message`.
    // Handle RETAINED and drained below (CodeRabbit round 1, #1005,
    // Stability & Availability) — an earlier version discarded
    // `tokio::spawn`'s handle here, so `run()`'s shutdown drain never
    // touched this worker: `run_multi_tenant` stops claiming NEW jobs on
    // shutdown but lets its current tick finish, and with no handle to join,
    // `#[tokio::main]` could return and terminate that in-flight tick before
    // `process_job` ever called `complete`/`fail`. The claimed row is then
    // stuck `running` until `workers::reaper` resets it, and the FULL
    // backfill window (up to 90 days) re-runs from scratch — same
    // duplicate-work shape `slack_unmatched_email_handle` below is already
    // drained specifically to avoid.
    let history_backfill_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let history_backfill_control_pool = pool.clone();
            let history_backfill_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "history_backfill",
                    workers::history_backfill::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let control = history_backfill_control_pool.clone();
                    let app_state = history_backfill_state.clone();
                    async move { workers::history_backfill::tick(&t, &control, &app_state).await }
                },
            ));
            tracing::info!("history backfill worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── GitHub backfill / event-ingest workers (PR A, sync plumbing —
    // docs/connectors/github.md) ──────────────────────────────────────────────
    // Tenant loop, same reasoning as the Slack worker above:
    // `github_watched_repos` is a tenant-tree table, so a job for an org
    // account lands on that org's own `jobs` table. Handles RETAINED and
    // drained below for the same reason `history_backfill_handle` just above
    // is: a backfill tick can run for minutes, and a dropped handle would let
    // `run()` return mid-tick, leaving the claimed row `running` until the
    // reaper resets it and the whole unit re-runs from scratch.
    let github_backfill_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let github_backfill_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "github_backfill",
                    workers::github_backfill::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let app_state = github_backfill_state.clone();
                    async move { workers::github_backfill::tick(&t, &app_state).await }
                },
            ));
            tracing::info!("github backfill worker spawned");
            Some(handle)
        } else {
            None
        };

    let github_event_ingest_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let github_event_ingest_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "github_event_ingest",
                    workers::github_event_ingest::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let app_state = github_event_ingest_state.clone();
                    async move { workers::github_event_ingest::tick(&t, &app_state).await }
                },
            ));
            tracing::info!("github event-ingest worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── Ingest receive worker (ADR-0016's receive stage) ──────────────────────
    // Drains the `JobKind::IngestReceive` jobs the provider webhooks write
    // before they answer: fetches the content and stores it
    // through `ingest::submit()`. Consumer-only (see `workers::
    // ingest_receive`'s module doc); retained and drained so an in-flight
    // fetch finishes rather than waiting for the reaper.
    let ingest_receive_handle =
        if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
            let ingest_receive_state = state.clone();
            let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
                pool,
                store,
                worker_runtime.clone(),
                worker_activity.clone(),
                workers::tenant_harness::HarnessConfig::queue_worker(
                    "ingest_receive",
                    workers::ingest_receive::EMPTY_POLL_SLEEP,
                ),
                shutdown.clone(),
                move |t| {
                    let app_state = ingest_receive_state.clone();
                    async move { workers::ingest_receive::tick(&t, &app_state).await }
                },
            ));
            tracing::info!("ingest receive worker spawned");
            Some(handle)
        } else {
            None
        };

    // ── GitHub reconcile sweep (docs/connectors/github.md §3.7, phase 2) ─────
    // The periodic backstop for whatever the webhook path misses. Tenant
    // loop, same reasoning as the two workers above. Handle RETAINED and
    // drained below (per this PR's own brief — unlike `google_chat_sweep`'s
    // dropped handle, whose own comment explains why THAT worker's every
    // pass is cheaply re-walkable from a durable cursor with no partial-tick
    // cost: this sweep shares that same idempotent-resume property, but is
    // kept retained here anyway so a mid-pass `since`/`head_tree_sha` write
    // always finishes rather than racing process exit).
    let github_reconcile_sweep_handle = if let (Some(pool), Some(store)) =
        (state.db.clone(), state.store.clone())
    {
        let github_reconcile_sweep_control_pool = pool.clone();
        let github_reconcile_sweep_state = state.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "github_reconcile_sweep",
                workers::github_reconcile_sweep::poll_interval(),
            ),
            shutdown.clone(),
            move |t| {
                let control = github_reconcile_sweep_control_pool.clone();
                let app_state = github_reconcile_sweep_state.clone();
                async move { workers::github_reconcile_sweep::tick(&t, &control, &app_state).await }
            },
        ));
        tracing::info!("github reconcile sweep worker spawned");
        Some(handle)
    } else {
        None
    };

    // ── Graph message ingest worker (m365 Train 2 PR 2.3) ─────────────────────
    // Drains `JobKind::GraphMessageIngest` off whichever plane
    // `routes::graph_webhook::resolve_graph_ingest_target` resolved and
    // enqueued onto — the durable replacement for that funnel's old
    // unconditional `tenancy::extractor::Tenant::legacy(state)` hardcode.
    // Tenant loop, same reasoning as the Slack worker just above: each
    // plane's `jobs` table is invisible to a dequeuer scoped to any other
    // one. No `control_pool` clone needed here (unlike the Slack worker) —
    // see `workers::graph_ingest`'s own module doc for why the plane
    // decision doesn't need re-deriving at dequeue time. `state` is a full
    // clone so the worker can call the same `routes::ingest::ingest_atom`
    // the personal-plane synchronous path already uses.
    if let (Some(pool), Some(store)) = (state.db.clone(), state.store.clone()) {
        let graph_ingest_state = state.clone();
        tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::queue_worker(
                "graph_ingest",
                workers::graph_ingest::EMPTY_POLL_SLEEP,
            ),
            shutdown.clone(),
            move |t| {
                let app_state = graph_ingest_state.clone();
                async move { workers::graph_ingest::tick(&t, &app_state).await }
            },
        ));
        tracing::info!("graph ingest worker spawned");
    }

    // ── Journal sweep worker (Today-journal PR B) ─────────────────────────────
    // Hourly-journal compile sweep — see `workers::journal_sweep`'s module doc
    // for the full lock/epoch/budget/failure contract. `journal_rollups`
    // exists on BOTH planes (migrations `0113`/`0024`), so this joins the
    // tenant loop exactly like `tombstone_purge`/`reaper`/`platform_rollup`
    // above; needs pool + Store (to reach `journal_dirty_hours`/
    // `journal_hour_inputs`/`upsert_journal_rollup`) + an LLM provider (one
    // compile call per dirty hour), same trio as the relationship-graph
    // workers just above.
    // Drained below (unlike the other sweep workers, which are one indexed
    // `UPDATE` and not worth waiting on — see the reaper comment above
    // `embed_handle`): one pass here is up to `MAX_COMPILES_PER_PLANE` (12)
    // sequential Anthropic calls, so a SIGTERM mid-pass can silently drop
    // an already-completed LLM call whose `upsert_journal_rollup` hadn't
    // run yet — that spend is gone and the hour stays dirty for the next
    // pass. Binding the handle and joining it under the same `grace` window
    // as `summary` (a comparable cost profile) gives an in-flight pass a
    // bounded chance to finish and persist before the process exits.
    let journal_sweep_handle = if let (Some(pool), Some(store), Some(llm)) =
        (state.db.clone(), state.store.clone(), state.llm.clone())
    {
        // The managed gate's control pool (credits, price sheet, budget).
        let journal_control = pool.clone();
        let handle = tokio::spawn(workers::tenant_harness::run_multi_tenant(
            pool,
            store,
            worker_runtime.clone(),
            worker_activity.clone(),
            workers::tenant_harness::HarnessConfig::sweep_worker(
                "journal_sweep",
                workers::journal_sweep::JOURNAL_SWEEP_INTERVAL,
            ),
            shutdown.clone(),
            move |t| {
                let llm = llm.clone();
                let control = journal_control.clone();
                async move { workers::journal_sweep::tick(&t, &control, llm.as_ref()).await }
            },
        ));
        tracing::info!("journal sweep worker spawned");
        Some(handle)
    } else {
        tracing::warn!(
            "journal sweep worker NOT spawned — requires database, store, and LLM provider"
        );
        None
    };

    IngestionWorkers {
        history_backfill_handle,
        github_backfill_handle,
        github_event_ingest_handle,
        ingest_receive_handle,
        github_reconcile_sweep_handle,
        journal_sweep_handle,
    }
}

/// Self-serve tenant provisioning worker (org signup A1). `None` when a
/// dependency is missing; see `workers::tenant_provisioning::deps_from_env`.
pub(super) fn tenant_provisioning(
    state: &Arc<AppState>,
    shutdown: &shutdown::Shutdown,
    tenant_secrets: Arc<dyn crate::tenancy::secrets::SecretResolver>,
) -> Option<tokio::task::JoinHandle<()>> {
    match (state.db.clone(), state.tenant_runtime.as_ref()) {
        (Some(control), Some(rt)) => {
            let deps = workers::tenant_provisioning::deps_from_env(control, rt, tenant_secrets)?;
            Some(tokio::spawn(workers::tenant_provisioning::run(deps, shutdown.clone())))
        }
        (None, _) => {
            tracing::info!("tenant provisioning worker not spawned: no control pool");
            None
        }
        (_, None) => {
            tracing::info!("tenant provisioning worker not spawned: TENANT_APEX_DOMAIN unset (no tenant runtime)");
            None
        }
    }
}
