//! Jev inside scoped Ask: an optional pack pass and citation check over the
//! evidence the scoped pipeline already admitted and receipted.
//!
//! Jev only judges cards built from `candidates`, and the result is applied by
//! filtering and reordering those same arrays, so it can never add evidence.
//! The catalog scan is not ported: it seats reef digests and corals the Library
//! read never admitted, and node context is not model evidence here.
//! Every call runs under the asker's `interactive_ask` budget and fails open.
//!
//! The pack pass is opt-in (`JEV_ASK_PACK=on`, see [`pack_enabled`]); the
//! citation check runs whenever `JEV_ASK_PATH_MODE` is active.
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::decision::{self, AskBudget, AtomJudgment, CiteVerdict, DigestCard, JevAskPathMode, PackRole};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    /// Not attempted: `JEV_ASK_PATH_MODE` is off, the turn sends nothing, or
    /// (pack only) `JEV_ASK_PACK` is not on.
    #[default]
    Off,
    /// Mode is on but the turn had nothing for this phase to judge.
    Skipped,
    /// Ran and logged; the turn is unchanged.
    Shadow,
    Applied,
    /// No provider, a budget hold, a timeout or an error: the turn is unchanged.
    FailOpen,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct Report {
    pub pack: Phase,
    pub cite: Phase,
    pub pack_ms: u64,
    pub cite_ms: u64,
}

/// `JEV_ASK_PACK`: whether the pack pass runs. Off unless `on`/`live`/
/// `true`/`1`. Measured on the 2026-10-20 quality-eval run (107 Asks, the
/// coralswarm org): pack sends ~7.7k input tokens and needs 600-1,200 output
/// tokens, so it ran past the 8 s Ask budget on 96 turns and failed open on
/// 105. It added ~8.25 s to the p50 Ask and changed 2 turns. Ask latency is
/// the binding bound, so pack stays off until it fits the budget.
pub(crate) fn pack_enabled_from(raw: Option<&str>) -> bool {
    raw.is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "on" | "live" | "true" | "1"))
}

pub(crate) fn pack_enabled() -> bool {
    pack_enabled_from(std::env::var("JEV_ASK_PACK").ok().as_deref())
}

const EVIDENCE: [(&str, &str); 2] = [("outcomes", "semantic_atom_id"), ("legacy_evidence", "atom_id")];

#[derive(Debug, Clone, PartialEq)]
struct Item {
    id: Uuid,
    graph: Uuid,
    kind: String,
    body: String,
    superseded: bool,
}

fn uuid(value: &Value, field: &str) -> Option<Uuid> {
    value.get(field)?.as_str()?.parse().ok()
}

fn text(value: &Value, field: &str) -> String {
    value.get(field).and_then(Value::as_str).unwrap_or_default().to_owned()
}

/// Model evidence in prompt order: each outcome's statement and first quote,
/// then each unmapped legacy chunk's content.
fn items(candidates: &Value) -> Vec<Item> {
    let mut out = Vec::new();
    for (key, id_field) in EVIDENCE {
        for value in candidates.get(key).and_then(Value::as_array).into_iter().flatten() {
            let (Some(id), Some(graph)) = (uuid(value, id_field), uuid(value, "graph_id")) else { continue };
            let body = if key == "outcomes" {
                let quote = value["citations"].get(0).map(|c| text(c, "quote")).unwrap_or_default();
                format!("{}\n{quote}", text(value, "statement"))
            } else {
                text(value, "content")
            };
            out.push(Item {
                id,
                graph,
                kind: if key == "outcomes" { text(value, "kind") } else { "legacy_chunk".into() },
                body,
                superseded: value.get("supersession").is_some(),
            });
        }
    }
    out
}

/// The kept evidence in its new order, or `None` when Jev would keep nothing.
/// Unjudged items stay as evidence. Superseded outcomes still rank after
/// current ones (the `fact_supersession` read-path order); within that, Jev's
/// evidence, conflict and historical roles, then the original rank.
fn compose(items: &[Item], judgments: &HashMap<Uuid, AtomJudgment>) -> Option<Vec<(Uuid, PackRole)>> {
    let mut kept: Vec<(bool, std::cmp::Reverse<PackRole>, usize, Uuid)> = items
        .iter()
        .enumerate()
        .filter_map(|(rank, item)| {
            let role = judgments.get(&item.id).map_or(PackRole::Evidence, |j| j.role);
            (role != PackRole::Drop).then_some((item.superseded, std::cmp::Reverse(role), rank, item.id))
        })
        .collect();
    if kept.is_empty() {
        return None;
    }
    kept.sort();
    Some(kept.into_iter().map(|(_, role, _, id)| (id, role.0)).collect())
}

/// Filters and reorders the evidence arrays in place. Nothing is added.
fn apply(candidates: &mut Value, order: &[(Uuid, PackRole)]) {
    let rank: HashMap<Uuid, usize> = order.iter().enumerate().map(|(i, (id, _))| (*id, i)).collect();
    for (key, id_field) in EVIDENCE {
        let Some(array) = candidates.get_mut(key).and_then(Value::as_array_mut) else { continue };
        array.retain(|v| uuid(v, id_field).is_some_and(|id| rank.contains_key(&id)));
        array.sort_by_key(|v| uuid(v, id_field).map(|id| rank[&id]));
    }
}

/// Packs the admitted evidence. Returns each kept item's role for the prompt
/// copy; empty when the turn is unchanged. `fence` runs right before anything
/// is sent; `Err` means access changed and the answer must be withheld.
pub(crate) async fn pack(
    candidates: &mut Value,
    ask: &str,
    retrieval_query: &str,
    budget: Option<AskBudget>,
    fence: impl std::future::Future<Output = bool>,
    report: &mut Report,
) -> Result<HashMap<Uuid, PackRole>, ()> {
    let mode = JevAskPathMode::from_env();
    if !mode.is_active() || !pack_enabled() {
        return Ok(HashMap::new());
    }
    let Some(provider) = decision::decision_provider() else {
        tracing::warn!(mode = mode.as_str(), "scoped ask: no Jev provider configured, pack fails open");
        report.pack = Phase::FailOpen;
        return Ok(HashMap::new());
    };
    let items = items(candidates);
    if items.is_empty() {
        report.pack = Phase::Skipped;
        return Ok(HashMap::new());
    }
    if !fence.await {
        return Err(());
    }
    let cards: Vec<DigestCard> = items
        .iter()
        .map(|item| DigestCard {
            atom_id: item.id,
            reef_title: String::new(),
            reef_type: item.kind.clone(),
            digest: decision::digest_first_last(&item.body, decision::PACK_DIGEST_CHARS),
            reef_id: Some(item.graph),
        })
        .collect();
    let started = Instant::now();
    let judged = decision::with_ask_budget(
        budget,
        decision::evaluate_pack_with_retry(provider.as_ref(), ask, retrieval_query, &cards),
    )
    .await;
    report.pack_ms = started.elapsed().as_millis() as u64;
    let order = match judged {
        Ok(judgments) => compose(&items, &judgments),
        Err(error) => {
            tracing::warn!(%error, elapsed_ms = report.pack_ms, "scoped ask: Jev pack failed open");
            report.pack = Phase::FailOpen;
            return Ok(HashMap::new());
        }
    };
    let Some(order) = order else {
        tracing::info!(cards = cards.len(), "scoped ask: Jev dropped every card, pack fails open");
        report.pack = Phase::FailOpen;
        return Ok(HashMap::new());
    };
    tracing::info!(mode = mode.as_str(), elapsed_ms = report.pack_ms, cards = cards.len(), kept = order.len(),
        "scoped ask: Jev pack complete");
    if !mode.is_live() {
        report.pack = Phase::Shadow;
        return Ok(HashMap::new());
    }
    apply(candidates, &order);
    report.pack = Phase::Applied;
    Ok(order.into_iter().collect())
}

/// The prompt's `[n]` targets in order: outcome citations, then legacy chunks.
fn numbered(candidates: &mut Value) -> Vec<&mut Value> {
    let Some(object) = candidates.as_object_mut() else { return Vec::new() };
    let mut outcomes = Vec::new();
    let mut legacy = Vec::new();
    for (key, value) in object.iter_mut() {
        let Some(array) = value.as_array_mut() else { continue };
        match key.as_str() {
            "outcomes" => outcomes.extend(
                array.iter_mut().filter_map(|o| o.get_mut("citations").and_then(Value::as_array_mut)).flatten(),
            ),
            "legacy_evidence" => legacy.extend(array.iter_mut()),
            _ => {}
        }
    }
    outcomes.extend(legacy);
    outcomes
}

/// What the answer model saw for one `[n]`: a passage's quote under its
/// source title (answers quote PR and issue titles), or a legacy chunk.
fn target(value: &Value) -> Option<(Uuid, String)> {
    match uuid(value, "passage_id") {
        Some(id) => {
            let (title, quote) = (text(value, "title"), text(value, "quote"));
            Some((id, if title.trim().is_empty() { quote } else { format!("{title}\n{quote}") }))
        }
        None => Some((uuid(value, "atom_id")?, text(value, "content"))),
    }
}

/// Checks each `[n]` the answer cites against its receipted passage or chunk
/// and marks it with `jev_verdict`. `fence` runs right before anything is sent;
/// `Err` means access changed and the answer must be withheld.
pub(crate) async fn cite(
    candidates: &mut Value,
    answer: &str,
    budget: Option<AskBudget>,
    fence: impl std::future::Future<Output = bool>,
    report: &mut Report,
) -> Result<(), ()> {
    let mode = JevAskPathMode::from_env();
    if !mode.is_active() {
        return Ok(());
    }
    let targets: Vec<(Uuid, String)> = numbered(candidates).into_iter().map(|v| target(v).unwrap_or_default()).collect();
    let rendered: Vec<Uuid> = targets.iter().map(|(id, _)| *id).collect();
    let claims = decision::extract_numbered_cites(answer, &rendered);
    if claims.is_empty() {
        report.cite = Phase::Skipped;
        return Ok(());
    }
    let Some(provider) = decision::decision_provider() else {
        tracing::warn!(mode = mode.as_str(), "scoped ask: no Jev provider configured, cite fails open");
        report.cite = Phase::FailOpen;
        return Ok(());
    };
    if !fence.await {
        return Err(());
    }
    let desk: HashSet<Uuid> = rendered.iter().copied().collect();
    let bodies: HashMap<Uuid, String> = targets.into_iter().collect();
    let started = Instant::now();
    let checked = decision::with_ask_budget(
        budget,
        decision::verify_citations(provider.as_ref(), answer, &rendered, &[], &desk, &bodies, true),
    )
    .await;
    report.cite_ms = started.elapsed().as_millis() as u64;
    let composed = match checked {
        Ok(composed) => {
            report.cite = if mode.is_live() { Phase::Applied } else { Phase::Shadow };
            composed
        }
        Err(error) => {
            tracing::warn!(%error, elapsed_ms = report.cite_ms, "scoped ask: Jev cite failed open, cites unverified");
            report.cite = Phase::FailOpen;
            decision::unverified_compose(&claims, &desk)
        }
    };
    tracing::info!(mode = mode.as_str(), elapsed_ms = report.cite_ms, cites = composed.results.len(),
        failed = composed.results.iter().filter(|r| r.verdict.is_failure()).count(), "scoped ask: Jev cite complete");
    if !mode.is_live() {
        return Ok(());
    }
    let verdicts: HashMap<usize, CiteVerdict> =
        composed.results.iter().filter_map(|r| Some((r.n?, r.verdict))).collect();
    for (i, value) in numbered(candidates).into_iter().enumerate() {
        if let Some(verdict) = verdicts.get(&(i + 1)) {
            value["jev_verdict"] = json!(verdict.as_str());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn judged(id: Uuid, role: PackRole) -> (Uuid, AtomJudgment) {
        (id, AtomJudgment { atom_id: id, role, score: role as i32 as f64 })
    }

    fn outcome(id: Uuid, graph: Uuid, statement: &str, superseded: bool) -> Value {
        let mut v = json!({"graph_id":graph,"semantic_atom_id":id,"kind":"decision","statement":statement,
            "citations":[{"passage_id":Uuid::new_v4(),"quote":format!("{statement} quote")}]});
        if superseded {
            v["supersession"] = json!({"status":"superseded","superseded_by":[]});
        }
        v
    }

    #[test]
    fn pack_only_filters_and_reorders_admitted_evidence() {
        let graph = Uuid::new_v4();
        let [current, stale, off_topic, legacy, foreign] = [(); 5].map(|_| Uuid::new_v4());
        let mut candidates = json!({
            "outcomes":[outcome(stale,graph,"old plan",true),outcome(off_topic,graph,"lunch",false),outcome(current,graph,"new plan",false)],
            "legacy_evidence":[{"graph_id":graph,"atom_id":legacy,"content":"legacy note"}],
            "legacy_headlines":[{"atom_id":Uuid::new_v4()}],
        });
        let items = items(&candidates);
        assert_eq!(items.iter().map(|i| i.id).collect::<Vec<_>>(), vec![stale, off_topic, current, legacy]);
        // Jev rates the superseded outcome above the current one, drops one card
        // and names an id it was never shown.
        let judgments: HashMap<_, _> = [
            judged(stale, PackRole::Evidence),
            judged(off_topic, PackRole::Drop),
            judged(current, PackRole::Historical),
            judged(foreign, PackRole::Evidence),
        ]
        .into();
        let order = compose(&items, &judgments).unwrap();
        assert_eq!(order, vec![(legacy, PackRole::Evidence), (current, PackRole::Historical), (stale, PackRole::Evidence)],
            "unjudged stays evidence, superseded still ranks last, the foreign id never appears");
        apply(&mut candidates, &order);
        let ids = |key: &str, field: &str| -> Vec<Uuid> {
            candidates[key].as_array().unwrap().iter().map(|v| uuid(v, field).unwrap()).collect()
        };
        assert_eq!(ids("outcomes", "semantic_atom_id"), vec![current, stale]);
        assert_eq!(ids("legacy_evidence", "atom_id"), vec![legacy]);
        assert_eq!(candidates["legacy_headlines"].as_array().unwrap().len(), 1, "headlines are not model evidence");
    }

    #[test]
    fn pack_is_opt_in() {
        for off in [None, Some(""), Some("off"), Some("shadow"), Some("no")] {
            assert!(!pack_enabled_from(off), "{off:?}");
        }
        for on in ["on", "live", " TRUE ", "1"] {
            assert!(pack_enabled_from(Some(on)), "{on}");
        }
    }

    #[test]
    fn dropping_every_card_keeps_the_turn() {
        let graph = Uuid::new_v4();
        let id = Uuid::new_v4();
        let items = items(&json!({"outcomes":[outcome(id,graph,"plan",false)]}));
        assert_eq!(compose(&items, &[judged(id, PackRole::Drop)].into()), None);
    }

    #[test]
    fn citation_targets_follow_prompt_numbering() {
        let graph = Uuid::new_v4();
        let legacy = Uuid::new_v4();
        let mut candidates = json!({
            "legacy_evidence":[{"graph_id":graph,"atom_id":legacy,"content":"legacy note"}],
            "outcomes":[outcome(Uuid::new_v4(),graph,"plan",false)],
        });
        let passage = uuid(&candidates["outcomes"][0]["citations"][0], "passage_id").unwrap();
        let targets: Vec<_> = numbered(&mut candidates).into_iter().map(|v| target(v).unwrap()).collect();
        assert_eq!(targets, vec![(passage, "plan quote".to_string()), (legacy, "legacy note".to_string())]);
    }

    #[test]
    fn a_cited_passage_is_checked_under_its_title() {
        let passage = Uuid::new_v4();
        let cited = json!({"passage_id":passage,"quote":"Copies the model.","title":"PR #9: fix(promote): copy embed_model"});
        assert_eq!(target(&cited), Some((passage, "PR #9: fix(promote): copy embed_model\nCopies the model.".to_string())));
        let untitled = json!({"passage_id":passage,"quote":"Copies the model.","title":null});
        assert_eq!(target(&untitled), Some((passage, "Copies the model.".to_string())));
    }
}
