//! Self-knowledge context for the ask path.
//!
//! The synthesis path retrieves company *content* atoms and answers from them.
//! But many real questions aren't about content — they're about CoralSwarm
//! itself: "what is this?", "what can you connect to?", "what's connected?",
//! "which channels do you ingest?", "how fresh is your data?". None of that
//! lives in the vector index, so those questions used to bottom out in
//! "I don't have enough context".
//!
//! This module assembles a compact `SYSTEM CONTEXT` block — product
//! self-knowledge plus this workspace's live operational state — that the caller
//! injects ahead of the retrieved atoms (see [`crate::synthesis`]). It is small
//! (a few hundred tokens), never cited, and explicitly fenced off from company
//! facts by the synthesis prompt.
//!
//! It is additive and best-effort: any failing sub-build degrades that section
//! and never errors the query. With no Postgres pool the operational
//! section is simply omitted; the product section still answers "what is
//! CoralSwarm?".
//!
//! # Consumer-plane RLS scoping (org-tenancy OPS-4 phase B.5)
//!
//! Exactly ONE query here reaches an RLS-covered table (migration
//! `0083_consumer_rls`): the ocean-metadata block, whose `Store::ocean_stats`
//! aggregates `nodes` / `relationship_edge` / `atoms`. It now runs through
//! [`crate::store::Store::ocean_stats_scoped`] (PR #290), which is the one
//! `_scoped` variant that does NOT take a caller-supplied transaction — it is
//! a pure-read fan-out of four independent aggregates that deliberately opens
//! its own `begin_tenant_scoped` transaction per aggregate so they can still
//! run CONCURRENTLY (a shared transaction would serialize them on one
//! connection and regress this path's latency). It is scoped to the OCEAN'S
//! OWNING account, which is `account_id` here — a caller resolves it from the
//! queried ocean via the 0084 `graph_account_id` bootstrap, not from the
//! caller's session. Tenant RLS Phase 2 additionally
//! threads this function's own `user_id` parameter (the gate-verified acting
//! caller, already required for the reachable-graphs union above) into that
//! same call, so a member ocean's stats stay readable under
//! `migrations_tenant/0008_tenant_rls` once Phase 3 cuts over.
//!
//! Everything else in this module is outside the roster or a documented
//! carve-out: the identity block reads `users`/`account_members` (CONTROL
//! plane, no 0083 policy); the workspace-state block delegates to
//! `connectors::state::build_operational_state`, a separately-tracked
//! conversion unit in the rls-coverage manifest; and the reachable-graphs
//! union goes through `ReachabilityResolver`, the permanent multi-account
//! Model B carve-out.
//!
//! Because `ocean_stats_scoped` self-scopes, this file contains no literal
//! `begin_account_scoped` call and therefore carries no rls-coverage manifest
//! entry — the gate's Store-method detection stops matching it once the call
//! moves to the `_scoped` name (that script's header records this exact
//! exception).

use sqlx::PgPool;
use uuid::Uuid;

/// Authored product self-knowledge — what CoralSwarm is and what it does. Kept
/// terse and factual (not marketing copy) so it grounds "what is this / what can
/// you do / what do you connect to" without bloating the prompt or leaking into
/// company answers. The ocean lexicon (coral/reef/ocean/current) is included so
/// the model can explain its own vocabulary when asked.
const PRODUCT_OVERVIEW: &str = "\
ABOUT CORALSWARM:
- CoralSwarm is a knowledge engine that ingests a team's work communications and \
answers questions from them with cited sources. You are its answering layer.
- Vocabulary (the \"ocean\" lexicon): a coral is one atom of captured content (a \
Slack message, email, meeting-transcript chunk, etc.); a reef is a curated entity \
or topic node grouping related corals (a person, project, decision); an ocean is \
a knowledge graph / scope; currents are the typed relationships and activity \
between reefs.
- Sources it can connect: Slack (live message ingestion) and Microsoft (Outlook \
email by explicit per-thread opt-in, Teams channels/chats, and calendar). Gmail, \
Google Drive, Notion, Zoom and others are planned, not yet live. The connected \
set for THIS workspace is in the WORKSPACE STATE section below, when present.
- How answers work: your question is embedded and matched against the ocean's \
corals; the most relevant are synthesized into a short answer with inline \
citations to the underlying corals. Relationships between reefs (e.g. who manages \
whom) are tracked as reviewable typed edges.
- Privacy posture: a user has a personal primary ocean plus any team/org oceans \
they belong to. A personal ocean is reachable only by its owner; team and org \
oceans are reachable by the people in them. All oceans are processed \
server-side. Promotion of content from a personal to a shared ocean is one-way \
and copies, never moves.";

/// Build the `SYSTEM CONTEXT` block for one (account, ocean), or `None` when
/// there's nothing useful to add. The product section is always present, so this
/// returns `Some` whenever the block is non-empty (effectively always); the
/// operational section is appended only in the cloud (a Postgres pool is
/// required) and only when the workspace actually has connections or coverage.
///
/// Plane note (org-tenancy PR 2.3): `store` is the caller's CONTENT-plane
/// store and `decision` its tenancy decision (threaded into the
/// reachable-graphs union so an org tenant's graph list resolves on the
/// TENANT pool); `pool` stays the CONTROL/consumer pool — the identity block
/// reads `users`/`account_members`, which exist only there. The operational
/// (connector/coverage) section reads `pool` too: connectors are per-user,
/// personal-plane state, so an org-context call simply renders no coverage
/// block — an empty section, never a cross-plane read of another account's
/// rows.
pub async fn build_system_context(
    pool: Option<&PgPool>,
    store: &dyn crate::store::Store,
    decision: Option<&crate::tenancy::runtime::TenancyDecision>,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    user_role: &str,
) -> Option<String> {
    let mut sections: Vec<String> = vec![PRODUCT_OVERVIEW.to_string()];

    // Reachable oceans (the D9 union) — fetched once and shared by the identity
    // section and the current-ocean name. Store-backed.
    let graphs = crate::tenancy::reachability::ReachabilityResolver::list_reachable_graphs(
        decision, store, user_id,
    )
    .await
    .unwrap_or_default();
    let current_name = graphs
        .iter()
        .find(|g| g.id == graph_id)
        .map(|g| g.name.clone());

    // Identity — who's asking. Reachable oceans from the store; email/role (cloud).
    if let Some(block) = build_identity(pool, account_id, graph_id, user_id, user_role, &graphs).await
    {
        sections.push(block);
    }

    // Operational state is a cloud (Postgres) concept: connectors + oauth + atom
    // coverage by source. No pool omits it cleanly.
    if let Some(p) = pool {
        let state = crate::connectors::state::build_operational_state(p, account_id, graph_id).await;
        if let Some(block) = crate::connectors::state::render_state_block(&state) {
            sections.push(format!(
                "WORKSPACE STATE (CoralSwarm's own configuration & coverage for THIS \
                 workspace — what is connected, which channels/threads are ingested, \
                 how much and how fresh):\n{block}"
            ));
        }
    }

    // Ocean metadata (reef/relationship/coral counts) — store-backed.
    //
    // RLS-scoped to the OCEAN'S owning account (org-tenancy OPS-4 phase B.5;
    // see the module doc). Same dispatch shape as
    // `routes::graphs::get_ocean_stats`: a Postgres plane takes the
    // self-scoping variant, and a pool-less call keeps the plain call.
    // `content_pool` is the ORG TENANT pool under an
    // Org decision and the consumer/control pool otherwise — the same pool the
    // `store` handed to this function resolves on.
    let content_pool = match decision {
        Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)) => Some(&ctx.pool),
        _ => pool,
    };
    let stats_res = if content_pool.is_some() {
        store.ocean_stats_scoped(account_id, user_id, graph_id).await
    } else {
        store.ocean_stats(graph_id).await
    };
    if let Ok(stats) = stats_res {
        if let Some(block) = render_ocean_metadata(
            current_name.as_deref(),
            &stats.reef_counts,
            stats.relationship_count,
            stats.atom_count,
            &stats.breakdowns,
        ) {
            sections.push(block);
        }
    }

    if sections.is_empty() {
        return None;
    }

    Some(format!(
        "SYSTEM CONTEXT (facts about CoralSwarm itself and this workspace's \
         configuration — use this to answer questions about what CoralSwarm is, \
         what it can do, what is connected, which channels/sources it ingests, how \
         fresh or how much data it holds, how many reefs/relationships/corals this \
         ocean has, and aggregate/ranking questions (most active channel or person, \
         corals by source/scope/month, busiest period, relationships by type or \
         review status, largest reefs) from the WORKSPACE KNOWLEDGE counts, and who \
         the asker is / what oceans they can access; also use it \
         to self-qualify what you can and cannot see \
         when the retrieved context falls short. NEVER attach a citation to it, and \
         NEVER use it to assert facts about the company's content):\n\n{}",
        sections.join("\n\n")
    ))
}

/// Identity of the person asking — their email/role and the oceans they can
/// reach — so the ask path can answer "who am I?", "what's my role?", "which
/// oceans can I see?". Reachable oceans come from the store (the D9 union);
/// email and the per-ocean role need the cloud pool.
async fn build_identity(
    pool: Option<&PgPool>,
    account_id: Uuid,
    graph_id: Uuid,
    user_id: Uuid,
    user_role: &str,
    graphs: &[crate::store::Graph],
) -> Option<String> {
    let email: Option<String> = match pool {
        Some(p) => sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(p)
            .await
            .ok()
            .flatten(),
        None => None,
    };

    // Role in the queried ocean's account (most accurate); fall back to the
    // caller-supplied active-account role.
    let fallback = non_empty(user_role);
    let role: Option<String> = match pool {
        Some(p) => sqlx::query_scalar(
            "SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .fetch_optional(p)
        .await
        .ok()
        .flatten()
        .or(fallback),
        None => fallback,
    };

    let current = graphs
        .iter()
        .find(|g| g.id == graph_id)
        .map(|g| (g.name.clone(), g.kind.clone()));
    let reachable: Vec<(String, String)> = graphs
        .iter()
        .map(|g| (g.name.clone(), g.kind.clone()))
        .collect();

    render_identity(
        email.as_deref(),
        role.as_deref(),
        current.as_ref().map(|(n, k)| (n.as_str(), k.as_str())),
        &reachable,
    )
}

fn non_empty(s: &str) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// Friendly label for a `graphs.kind` value.
fn ocean_kind_label(kind: &str) -> &str {
    match kind {
        // A member ocean inside an org tenant is the same thing to a user as
        // a consumer-plane personal one: owner-only. Kept in step with
        // `web/lib/profile.ts`'s `oceanKindLabel`, so an answer to "which
        // oceans can I see?" cannot use a word the UI never shows.
        "personal" | "private" | "member" => "personal",
        "team" => "team",
        "org" => "org",
        other => other,
    }
}

/// Render the identity block, or `None` when nothing about the asker is known.
fn render_identity(
    email: Option<&str>,
    role: Option<&str>,
    current_ocean: Option<(&str, &str)>,
    reachable: &[(String, String)],
) -> Option<String> {
    if email.is_none() && current_ocean.is_none() && reachable.is_empty() {
        return None;
    }

    let mut lines = vec![
        "WHO YOU ARE TALKING TO (the person asking — answer \"who am I?\", \"what's \
         my role?\", \"which oceans can I see?\" from this; this is the asker, not a \
         company entity, and is never citable):"
            .to_string(),
    ];

    if let Some(e) = email {
        let with_role = match role {
            Some(r) => format!(" (role: {r})"),
            None => String::new(),
        };
        lines.push(format!("- Signed in as {e}{with_role}."));
    } else if let Some(r) = role {
        lines.push(format!("- Role: {r}."));
    }

    if let Some((name, kind)) = current_ocean {
        lines.push(format!(
            "- Current ocean: \"{name}\" ({}).",
            ocean_kind_label(kind)
        ));
    }

    if !reachable.is_empty() {
        const CAP: usize = 12;
        let shown: Vec<String> = reachable
            .iter()
            .take(CAP)
            .map(|(n, k)| format!("\"{n}\" ({})", ocean_kind_label(k)))
            .collect();
        let mut line = format!("- Oceans you can access: {}", shown.join(", "));
        if reachable.len() > CAP {
            line.push_str(&format!(", +{} more", reachable.len() - CAP));
        }
        line.push('.');
        lines.push(line);
    }

    Some(lines.join("\n"))
}

/// Human-facing label for each `node.type` in the reef breakdown.
fn reef_type_label(t: &str) -> &'static str {
    match t {
        "entity" => "entities",
        "decision" => "decisions",
        "thread" => "threads",
        "reference" => "references",
        _ => "other",
    }
}

/// Render the ocean-metadata block, or `None` when the ocean is effectively
/// empty (no reefs, no relationships, no corals) — nothing worth stating.
fn render_ocean_metadata(
    name: Option<&str>,
    reef_counts: &[(String, i64)],
    rel_count: i64,
    atom_count: i64,
    breakdowns: &[crate::store::dimensions::DimensionBreakdown],
) -> Option<String> {
    let reef_total: i64 = reef_counts.iter().map(|(_, c)| *c).sum();
    if reef_total == 0 && rel_count == 0 && atom_count == 0 {
        return None;
    }

    let named = match name {
        Some(n) if !n.trim().is_empty() => format!(" — \"{}\"", n.trim()),
        _ => String::new(),
    };
    let mut lines = vec![format!(
        "WORKSPACE KNOWLEDGE (what this ocean{named} currently holds):"
    )];

    // Reefs, with a by-type breakdown ordered for stable, readable output.
    if reef_total > 0 {
        let mut sorted: Vec<&(String, i64)> = reef_counts.iter().filter(|(_, c)| *c > 0).collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let breakdown: Vec<String> = sorted
            .iter()
            .map(|(t, c)| format!("{c} {}", reef_type_label(t)))
            .collect();
        lines.push(format!(
            "- Reefs (curated entities, decisions, and topics): {reef_total} total — {}.",
            breakdown.join(", ")
        ));
    } else {
        lines.push("- Reefs (curated entities, decisions, and topics): none yet.".to_string());
    }

    lines.push(format!(
        "- Relationships (typed edges between reefs): {rel_count}."
    ));
    lines.push(format!(
        "- Corals (atoms of captured content): {atom_count}."
    ));

    // Per-dimension breakdowns (most active channels/people, corals by
    // source/scope/month, relationships by type/status, largest reefs). These are
    // the precomputed counts that let synthesis answer "how many / most active /
    // busiest" without (mis)counting the retrieved top-K atom sample.
    for bd in breakdowns {
        if let Some(line) = render_breakdown(bd) {
            lines.push(line);
        }
    }

    Some(lines.join("\n"))
}

/// Render one breakdown as a single `- <lead-in>: <name> (<count>), ...` line,
/// or `None` when it has no entries. Entries arrive pre-sorted and pre-capped
/// (top-N) from the store.
fn render_breakdown(bd: &crate::store::dimensions::DimensionBreakdown) -> Option<String> {
    if bd.entries.is_empty() {
        return None;
    }
    let parts: Vec<String> = bd
        .entries
        .iter()
        .map(|(label, count)| format!("{label} ({count})"))
        .collect();
    Some(format!("- {}: {}.", bd.lead_in, parts.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::store::dimensions::DimensionBreakdown;

    fn bd(key: &'static str, lead_in: &'static str, entries: &[(&str, i64)]) -> DimensionBreakdown {
        DimensionBreakdown {
            key,
            lead_in,
            entries: entries.iter().map(|(l, c)| ((*l).to_string(), *c)).collect(),
        }
    }

    #[test]
    fn metadata_renders_name_breakdown_and_totals() {
        let block = render_ocean_metadata(
            Some("Primary"),
            &[
                ("entity".into(), 30),
                ("decision".into(), 8),
                ("thread".into(), 4),
            ],
            17,
            1351,
            &[],
        )
        .expect("non-empty ocean → block");
        assert!(block.contains("this ocean — \"Primary\" currently holds"));
        assert!(block.contains("42 total — 30 entities, 8 decisions, 4 threads."));
        assert!(block.contains("Relationships (typed edges between reefs): 17."));
        assert!(block.contains("Corals (atoms of captured content): 1351."));
    }

    #[test]
    fn metadata_orders_breakdown_by_count_desc() {
        let block = render_ocean_metadata(
            None,
            &[("entity".into(), 2), ("decision".into(), 9)],
            0,
            5,
            &[],
        )
        .unwrap();
        // Higher count first regardless of input order; no ocean name when absent.
        assert!(block.contains("9 decisions, 2 entities"));
        assert!(!block.contains('"'));
    }

    #[test]
    fn metadata_renders_dimension_breakdowns() {
        let block = render_ocean_metadata(
            Some("Primary"),
            &[("entity".into(), 3)],
            0,
            120,
            &[
                bd(
                    "channel",
                    "Most active channels (by message volume)",
                    &[("#general", 80), ("#eng", 30), ("#random", 10)],
                ),
                bd("author", "Most active people (by message volume)", &[("Sara", 60), ("Amir", 40)]),
                // Empty breakdowns are skipped, not rendered as bare lead-ins.
                bd("status", "Relationships by review status", &[]),
            ],
        )
        .expect("non-empty ocean → block");
        assert!(block.contains(
            "- Most active channels (by message volume): #general (80), #eng (30), #random (10)."
        ));
        assert!(block.contains("- Most active people (by message volume): Sara (60), Amir (40)."));
        assert!(!block.contains("Relationships by review status"));
    }

    #[test]
    fn identity_full_renders_email_role_current_and_reachable() {
        let reachable = vec![
            ("Primary".into(), "personal".into()),
            ("Acme".into(), "org".into()),
            ("Eng".into(), "team".into()),
        ];
        let block = render_identity(
            Some("Alex@example.com"),
            Some("owner"),
            Some(("Primary", "personal")),
            &reachable,
        )
        .expect("known asker → block");
        assert!(block.contains("Signed in as Alex@example.com (role: owner)."));
        assert!(block.contains("Current ocean: \"Primary\" (personal)."));
        assert!(block.contains("Oceans you can access: \"Primary\" (personal), \"Acme\" (org), \"Eng\" (team)."));
    }

    #[test]
    fn identity_no_pool_no_email_still_lists_oceans() {
        // No pool → no email/role, but reachable oceans from the store.
        let reachable = vec![("Local".into(), "personal".into())];
        let block = render_identity(None, None, Some(("Local", "personal")), &reachable).unwrap();
        assert!(!block.contains("Signed in as"));
        assert!(block.contains("Current ocean: \"Local\" (personal)."));
        assert!(block.contains("Oceans you can access: \"Local\" (personal)."));
    }

    #[test]
    fn identity_nothing_known_is_none() {
        assert!(render_identity(None, None, None, &[]).is_none());
    }

    #[test]
    fn identity_caps_long_ocean_list() {
        let reachable: Vec<(String, String)> = (0..20)
            .map(|i| (format!("O{i}"), "team".to_string()))
            .collect();
        let block = render_identity(Some("a@b.c"), None, None, &reachable).unwrap();
        assert!(block.contains("+8 more."));
    }

    #[test]
    fn metadata_empty_ocean_is_none() {
        assert!(render_ocean_metadata(Some("Empty"), &[], 0, 0, &[]).is_none());
    }

    #[test]
    fn metadata_present_with_only_atoms() {
        // Corals but no reefs/edges yet (fresh ingest) still yields a block.
        let block = render_ocean_metadata(Some("New"), &[], 0, 12, &[]).unwrap();
        assert!(block.contains("Reefs (curated entities, decisions, and topics): none yet."));
        assert!(block.contains("Corals (atoms of captured content): 12."));
    }
}
