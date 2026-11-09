//! Deterministic relationship inference (ADR-0008, Phase 2).
//!
//! Composes stored edges via the predicate registry's [`CompositionRule`]s to
//! derive new edges. Rule semantics: `a(X,Y) ∧ b(Y,Z) ⇒ yields(X,Z)`, joined on
//! the shared middle node `Y`.
//!
//! This module is **pure** — it takes a slice of input edges and returns derived
//! candidates. It performs no dedup against existing edges and no confidence
//! gating; the inference-closure worker owns that persistence policy. Keeping the
//! core pure makes the headline behaviour ("wife + my-daughter ⇒ likely mother")
//! unit-testable without a database.

use uuid::Uuid;

use super::predicates;
use crate::llm::{model_config, CompletionRequest, LlmError, LlmProvider, Message, Role, ToolSchema};

/// Defeasibility-review prompt for derived relationship edges (audit H2).
///
/// This is the false-positive guard for the headline "spouse-of-a-parent ⇒
/// parent?" case: after a defeasible edge is *derived* (purely, by [`infer`]),
/// an LLM is asked whether the relationship actually holds and the candidate's
/// confidence is adjusted (or the edge dropped) accordingly. It runs **only**
/// when [`infer_review_enabled`] is true, so default behaviour is unchanged.
const INFER_REVIEW_SYSTEM_PROMPT: &str =
    include_str!("../../prompts/infer_relationship_review_v1.txt");

/// An input edge fed to the inference pass.
#[derive(Debug, Clone)]
pub struct EdgeInput {
    pub edge_id: Uuid,
    pub src: Uuid,
    pub predicate: String,
    pub dst: Uuid,
    pub confidence: f32,
}

/// A derived edge candidate produced by composing two input edges.
#[derive(Debug, Clone)]
pub struct InferredEdge {
    pub src: Uuid,
    pub predicate: &'static str,
    pub dst: Uuid,
    pub confidence: f32,
    pub defeasible: bool,
    /// Human-readable derivation id, e.g. `"spouse_of∘parent_of→parent_of"`.
    pub rule: String,
    /// The two edges (by id) that were composed.
    pub source_edge_ids: Vec<Uuid>,
}

/// Apply every composition rule once over `edges`, producing derived candidates.
///
/// Single pass (one composition level) over the input set — enough for the
/// headline kinship inferences (spouse+parent⇒parent, parent+parent⇒grandparent).
/// The derived edge's confidence is the rule prior dampened by the weaker of the
/// two source edges: `base_confidence * min(c1, c2)`.
pub fn infer(edges: &[EdgeInput]) -> Vec<InferredEdge> {
    let mut out = Vec::new();
    for rule in predicates::compositions() {
        for e1 in edges.iter().filter(|e| e.predicate == rule.a) {
            // b(Y,Z) where Y == e1.dst
            for e2 in edges
                .iter()
                .filter(|e| e.predicate == rule.b && e.src == e1.dst)
            {
                // Need a genuine X ≠ Z (no self-edges).
                if e1.src == e2.dst {
                    continue;
                }
                let confidence = rule.base_confidence * e1.confidence.min(e2.confidence);
                out.push(InferredEdge {
                    src: e1.src,
                    predicate: rule.yields,
                    dst: e2.dst,
                    confidence,
                    defeasible: rule.defeasible,
                    rule: format!("{}∘{}→{}", rule.a, rule.b, rule.yields),
                    source_edge_ids: vec![e1.edge_id, e2.edge_id],
                });
            }
        }
    }
    out
}

// ── Defeasibility review (audit H2) ─────────────────────────────────────────────
//
// `infer` above is pure and untouched: with `INFER_REVIEW_ENABLED` unset/false
// the call path below is never invoked, so existing callers see byte-for-byte
// identical behaviour (no LLM call, no latency, no cost, no signature change).
//
// When enabled, a caller resolves the candidate's reef names + source-edge
// descriptions and calls `review_inferred_edge`, then folds the verdict back in
// with `apply_review`. This keeps the inference module decoupled from the store:
// it never does I/O of its own; the name lookup is the caller's job.

/// Whether the gated defeasibility review runs. Env: `INFER_REVIEW_ENABLED`
/// (default **false**). Truthy values: `1`, `true`, `yes`, `on` (case-insensitive).
pub fn infer_review_enabled() -> bool {
    match std::env::var("INFER_REVIEW_ENABLED") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// Human-readable context an edge needs to be reviewed: the candidate's subject
/// / object names and the descriptions of the source edges it was composed from.
/// The caller (which has the store + the reef titles) fills this in.
#[derive(Debug, Clone)]
pub struct ReviewContext {
    /// Display name of the derived edge's subject (`src` reef).
    pub subject: String,
    /// Display name of the derived edge's object (`dst` reef).
    pub object: String,
    /// Human-readable source-edge lines, e.g. `"spouse_of(Lena, Marcus)"`.
    pub source_edges: Vec<String>,
    /// Optional known facts that could strengthen or defeat the inference.
    pub context: Vec<String>,
}

/// The reviewer's verdict on whether a derived edge holds. Parsed leniently from
/// the model's JSON, mirroring `entity_resolution::parse_verdict`.
#[derive(Debug, Clone, PartialEq)]
pub enum Holds {
    /// Strongly entailed (e.g. structural closures).
    True,
    /// Probable but defeasible.
    Likely,
    /// Context defeats the inference.
    False,
}

impl Holds {
    fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "true" => Some(Holds::True),
            "likely" => Some(Holds::Likely),
            "false" => Some(Holds::False),
            _ => None,
        }
    }
}

/// Parsed reviewer verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct ReviewVerdict {
    pub holds: Holds,
    pub confidence: f32,
    pub caveat: String,
}

/// Robustly extract the verdict JSON from a possibly fenced / prose-wrapped model
/// response (strip fences, take the first `{` to the last `}`), mirroring the
/// lenient parser in `entity_resolution`.
fn parse_review_verdict(raw: &str) -> Result<ReviewVerdict, String> {
    let cleaned = raw
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim();
    let start = cleaned
        .find('{')
        .ok_or_else(|| format!("no JSON object in response: {cleaned}"))?;
    let end = cleaned
        .rfind('}')
        .ok_or_else(|| format!("no closing brace in response: {cleaned}"))?;
    if end < start {
        return Err(format!("malformed JSON object in response: {cleaned}"));
    }
    let value: serde_json::Value = serde_json::from_str(&cleaned[start..=end])
        .map_err(|e| format!("verdict parse error: {e} — raw: {}", &cleaned[start..=end]))?;

    let holds = value
        .get("holds")
        .and_then(|v| {
            // Accept the string form ("true"/"likely"/"false") or a raw JSON
            // boolean (true ⇒ True, false ⇒ False) for robustness.
            v.as_str()
                .and_then(Holds::parse)
                .or_else(|| v.as_bool().map(|b| if b { Holds::True } else { Holds::False }))
        })
        .ok_or_else(|| format!("missing or invalid `holds` in verdict: {value}"))?;

    // `confidence` is optional & clamped; default to a neutral mid value rather
    // than failing the parse on a missing field.
    let confidence = value
        .get("confidence")
        .and_then(|v| v.as_f64())
        .map(|c| c.clamp(0.0, 1.0) as f32)
        .unwrap_or(0.5);

    let caveat = value
        .get("caveat")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    Ok(ReviewVerdict { holds, confidence, caveat })
}

/// Build the user prompt for the review of one derived edge.
fn build_review_prompt(edge: &InferredEdge, ctx: &ReviewContext) -> String {
    let rule = predicates::compositions()
        .iter()
        .find(|r| format!("{}∘{}→{}", r.a, r.b, r.yields) == edge.rule);
    let (base_confidence, note) = rule
        .map(|r| (r.base_confidence, r.note))
        .unwrap_or((edge.confidence, ""));

    let mut s = String::new();
    s.push_str("## Candidate inferred edge\n");
    s.push_str(&format!("subject: {}\n", ctx.subject));
    s.push_str(&format!("predicate: {}\n", edge.predicate));
    s.push_str(&format!("object: {}\n\n", ctx.object));
    s.push_str("## Derivation\n");
    s.push_str(&format!("rule: {}\n", edge.rule));
    s.push_str(&format!("base_confidence: {base_confidence}\n"));
    s.push_str(&format!("defeasible: {}\n", edge.defeasible));
    s.push_str(&format!("note: {note}\n"));
    s.push_str("source edges:\n");
    if ctx.source_edges.is_empty() {
        s.push_str("  - (none provided)\n");
    } else {
        for e in &ctx.source_edges {
            s.push_str(&format!("  - {e}\n"));
        }
    }
    s.push_str("context (optional):\n");
    if ctx.context.is_empty() {
        s.push_str("  - (none)\n");
    } else {
        for c in &ctx.context {
            s.push_str(&format!("  - {c}\n"));
        }
    }
    s
}

/// Ask the LLM whether a derived defeasible edge actually holds.
///
/// Reuses the judge-tier model (`model_config::resolve_model`) and the resolve
/// token cap — no new env var. Returns the parsed verdict; parse/LLM failures
/// surface as `Err` so the caller can choose to leave the edge untouched.
/// JSON Schema for the forced-tool inference-review verdict (audit H3). Mirrors
/// what `parse_review_verdict` accepts: `holds` (string `true`/`likely`/`false`
/// or a JSON boolean), optional `confidence`, optional `caveat`. When honored,
/// `resp.content` is guaranteed-valid JSON; otherwise the brace-slice fallback in
/// `parse_review_verdict` runs.
fn review_verdict_tool_schema() -> ToolSchema {
    ToolSchema {
        strict: false,
        name: "emit_inference_review".to_string(),
        schema: serde_json::json!({
            "type": "object",
            "properties": {
                "holds": {
                    "type": ["string", "boolean"],
                    "description": "one of \"true\", \"likely\", \"false\", or a JSON boolean"
                },
                "confidence": { "type": "number" },
                "caveat": { "type": "string" }
            },
            "required": ["holds"]
        }),
    }
}

/// Usage-metering PR4 note: this function (and its only caller,
/// [`review_inferred_edges`]) has NO live caller anywhere in this codebase
/// today — `review_inferred_edges` is `pub` but unreferenced, so this is dead
/// code that still ships in the compiled binary. Deleting it is out of this
/// train's scope, and leaving a direct `.complete()` here would defeat the
/// choke point's whole guarantee (the negative-control grep for surviving
/// direct provider calls would have to special-case it forever). So it is
/// wired into [`crate::llm::usage::metered_complete`] like every live site,
/// with `usage_ctx` added as an explicit parameter rather than hardcoded to
/// `None` — there is no real request/job context to source one from at a
/// dead call site, and forcing a future caller to pass one (or explicitly
/// `None`) is safer than silently defaulting to permanently-unmetered the
/// day this does get wired up.
pub async fn review_inferred_edge(
    llm: &dyn LlmProvider,
    edge: &InferredEdge,
    ctx: &ReviewContext,
    usage_ctx: Option<&crate::llm::usage::UsageContext>,
) -> Result<ReviewVerdict, LlmError> {
    let prompt = build_review_prompt(edge, ctx);
    let resp = crate::llm::usage::metered_complete(
        llm,
        CompletionRequest {
            messages: vec![Message {
                role: Role::User,
                content: prompt,
            }],
            model: model_config::resolve_model(),
            max_tokens: model_config::resolve_max_tokens(),
            system: Some(INFER_REVIEW_SYSTEM_PROMPT.to_string()),
            // Audit H3: force schema-valid JSON; `parse_review_verdict` (brace
            // slice) remains the fallback for providers that ignore this field.
            tool_schema: Some(review_verdict_tool_schema()),
            effort: None,
            reasoning: crate::llm::ReasoningSetting::Off,
        },
        usage_ctx,
        "infer_relationship_review",
    )
    .await?;
    // A parse failure of a 200 body is bad model output, not transient.
    parse_review_verdict(&resp.content).map_err(LlmError::Parse)
}

/// Fold a reviewer verdict back into a derived edge in place.
///
/// Returns `true` if the edge should be **kept** (and has had its confidence
/// adjusted) and `false` if it should be **dropped** (`holds == false`). The
/// adjusted confidence is the minimum of the rule-derived confidence and the
/// reviewer's, so the LLM can only *lower* trust, never inflate a weak chain.
pub fn apply_review(edge: &mut InferredEdge, verdict: &ReviewVerdict) -> bool {
    match verdict.holds {
        Holds::False => false,
        Holds::True | Holds::Likely => {
            edge.confidence = edge.confidence.min(verdict.confidence);
            true
        }
    }
}

/// Convenience: run the gated review over a batch of derived edges, dropping any
/// the reviewer defeats and lowering confidence on the rest. Only defeasible
/// edges are reviewed; structural (non-defeasible) closures and the
/// already-cheap path are left untouched. `resolve` maps an `InferredEdge` to
/// its [`ReviewContext`] (the caller owns reef-name lookup). A per-edge review
/// error leaves that edge unchanged.
///
/// This function is **not** called by default; it only runs behind the
/// `INFER_REVIEW_ENABLED` flag at the call site (it no-ops and returns the input
/// unchanged when the flag is off, so wiring it in is itself side-effect-free).
pub async fn review_inferred_edges<F>(
    llm: &dyn LlmProvider,
    edges: Vec<InferredEdge>,
    mut resolve: F,
    usage_ctx: Option<&crate::llm::usage::UsageContext>,
) -> Vec<InferredEdge>
where
    F: FnMut(&InferredEdge) -> Option<ReviewContext>,
{
    if !infer_review_enabled() {
        return edges;
    }
    let mut out = Vec::with_capacity(edges.len());
    for mut edge in edges {
        if !edge.defeasible {
            out.push(edge);
            continue;
        }
        let Some(ctx) = resolve(&edge) else {
            // No context to review against — keep the edge as-is.
            out.push(edge);
            continue;
        };
        match review_inferred_edge(llm, &edge, &ctx, usage_ctx).await {
            Ok(verdict) => {
                if apply_review(&mut edge, &verdict) {
                    out.push(edge);
                }
                // holds == false ⇒ dropped.
            }
            Err(e) => {
                tracing::warn!(
                    rule = %edge.rule,
                    error = %e,
                    "inference review failed — keeping derived edge unchanged"
                );
                out.push(edge);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relationships::predicates::{
        AUNT_UNCLE_OF, CHILD_OF, NIECE_NEPHEW_OF, PARENT_OF, SIBLING_OF, SPOUSE_OF,
    };

    /// Serializes the two tests below that mutate the process-global
    /// `INFER_REVIEW_ENABLED` env var: `cargo test`'s default in-binary
    /// thread parallelism means `review_flag_defaults_off` and
    /// `review_flag_truthy_values` running concurrently could otherwise race
    /// a `set_var` in one against a `remove_var`/read in the other — same
    /// pattern as `synthesis`'s `ENV_GUARD` / `routes::queries`'s
    /// `SUPERSESSION_ENV_GUARD`.
    static ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn edge(src: Uuid, predicate: &str, dst: Uuid) -> EdgeInput {
        EdgeInput {
            edge_id: Uuid::new_v4(),
            src,
            predicate: predicate.to_string(),
            dst,
            confidence: 0.95,
        }
    }

    /// The Alex / Harper / Casey / Jordan / Kacey family, exactly as stored
    /// (with inverse/symmetric edges materialized, as the extraction worker does).
    fn family() -> (Vec<EdgeInput>, [Uuid; 5]) {
        let Alex = Uuid::new_v4();
        let harper = Uuid::new_v4();
        let Casey = Uuid::new_v4();
        let Jordan = Uuid::new_v4();
        let kacey = Uuid::new_v4();

        let edges = vec![
            // "Harper is my wife" (author Alex) — spouse_of is symmetric.
            edge(Alex, SPOUSE_OF, harper),
            edge(harper, SPOUSE_OF, Alex),
            // "Casey is my daughter" — parent_of + child_of inverse.
            edge(Alex, PARENT_OF, Casey),
            edge(Casey, CHILD_OF, Alex),
            // "Jordan is the mother of Kacey" — the COUSIN, not Casey.
            edge(Jordan, PARENT_OF, kacey),
            edge(kacey, CHILD_OF, Jordan),
        ];
        (edges, [Alex, harper, Casey, Jordan, kacey])
    }

    /// THE HEADLINE: wife + my-daughter ⇒ Harper is (likely) Casey's mother.
    #[test]
    fn infers_harper_is_Caseys_parent() {
        let (edges, [_Alex, harper, Casey, _Jordan, _kacey]) = family();
        let inferred = infer(&edges);

        let hit = inferred
            .iter()
            .find(|e| e.predicate == PARENT_OF && e.src == harper && e.dst == Casey)
            .expect("must infer parent_of(Harper, Casey) from spouse_of∘parent_of");

        assert!(hit.defeasible, "the wife⇒parent inference must be flagged defeasible");
        assert!(
            hit.confidence > 0.0 && hit.confidence < 0.85,
            "defeasible inference should land below the auto-confirm gate (got {})",
            hit.confidence
        );
        assert!(hit.rule.contains("spouse_of"));
    }

    /// THE CONFLATION BUG MUST STAY DEAD: nothing links Jordan to Casey, so the
    /// engine must NOT derive Jordan as Casey's parent. (Jordan is Kacey's mother.)
    #[test]
    fn never_infers_Jordan_is_Caseys_parent() {
        let (edges, [_Alex, _harper, Casey, Jordan, _kacey]) = family();
        let inferred = infer(&edges);

        assert!(
            !inferred
                .iter()
                .any(|e| e.predicate == PARENT_OF && e.src == Jordan && e.dst == Casey),
            "must NOT infer parent_of(Jordan, Casey) — that was the original bug"
        );
    }

    /// REGRESSION (prod, 2026-06): the `family()` fixture has no parent-sibling
    /// edge, so the buggy `child_of∘sibling_of⇒child_of` rule never fired in tests
    /// — but prod has "Jordan is my older sister", and the rule then mis-yielded
    /// `child_of(Casey, Jordan)`, reviving the conflation (Jordan marked as Casey's
    /// PARENT, confirmed @0.88). A parent's sibling is an aunt/uncle, never a parent.
    #[test]
    fn parents_sibling_is_aunt_not_parent() {
        let Casey = Uuid::new_v4();
        let Alex = Uuid::new_v4();
        let Jordan = Uuid::new_v4();
        let edges = vec![
            // Casey is Alex's daughter.
            edge(Casey, CHILD_OF, Alex),
            edge(Alex, PARENT_OF, Casey),
            // Alex and Jordan are siblings — the edge that triggered the bug.
            edge(Alex, SIBLING_OF, Jordan),
            edge(Jordan, SIBLING_OF, Alex),
        ];
        let out = infer(&edges);

        // MUST NOT make the aunt a parent.
        assert!(
            !out.iter().any(|e| e.predicate == CHILD_OF && e.src == Casey && e.dst == Jordan),
            "child_of∘sibling_of must NOT make a parent's sibling a parent (Jordan is Casey's aunt)"
        );
        // MUST derive the correct niece/nephew + aunt/uncle relations.
        assert!(
            out.iter().any(|e| e.predicate == NIECE_NEPHEW_OF && e.src == Casey && e.dst == Jordan),
            "Casey should be inferred as Jordan's niece"
        );
        assert!(
            out.iter().any(|e| e.predicate == AUNT_UNCLE_OF && e.src == Jordan && e.dst == Casey),
            "Jordan should be inferred as Casey's aunt"
        );
    }

    /// Companion direction: the sibling of someone who IS a child of Z is also
    /// Z's child (shared parents) — not Z's niece/nephew.
    #[test]
    fn sibling_of_a_child_is_also_that_parents_child() {
        let alde = Uuid::new_v4(); // the sibling
        let bex = Uuid::new_v4(); // the known child
        let parent = Uuid::new_v4();
        let edges = vec![
            edge(alde, SIBLING_OF, bex),
            edge(bex, SIBLING_OF, alde),
            edge(bex, CHILD_OF, parent),
            edge(parent, PARENT_OF, bex),
        ];
        let out = infer(&edges);

        assert!(
            out.iter().any(|e| e.predicate == CHILD_OF && e.src == alde && e.dst == parent),
            "sibling of a child is also that parent's child"
        );
        assert!(
            !out.iter().any(|e| e.predicate == NIECE_NEPHEW_OF && e.src == alde && e.dst == parent),
            "must not mislabel a child as a niece/nephew"
        );
    }

    /// Inference must not invent self-edges.
    #[test]
    fn no_self_edges() {
        let (edges, _) = family();
        for e in infer(&edges) {
            assert_ne!(e.src, e.dst, "inference produced a self-edge");
        }
    }

    // ── Defeasibility-review (audit H2) unit tests ──────────────────────────────

    #[test]
    fn review_flag_defaults_off() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let prior = std::env::var("INFER_REVIEW_ENABLED").ok();

        // Guard: the env var is virtually never set in CI/tests, so the gate is
        // off and the review path is dormant by default.
        std::env::remove_var("INFER_REVIEW_ENABLED");
        assert!(!infer_review_enabled());

        match prior {
            Some(v) => std::env::set_var("INFER_REVIEW_ENABLED", v),
            None => std::env::remove_var("INFER_REVIEW_ENABLED"),
        }
    }

    #[test]
    fn review_flag_truthy_values() {
        let _guard = ENV_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let prior = std::env::var("INFER_REVIEW_ENABLED").ok();

        for v in ["1", "true", "TRUE", "Yes", "on"] {
            std::env::set_var("INFER_REVIEW_ENABLED", v);
            assert!(infer_review_enabled(), "{v} should enable review");
        }
        for v in ["0", "false", "no", "off", ""] {
            std::env::set_var("INFER_REVIEW_ENABLED", v);
            assert!(!infer_review_enabled(), "{v} should keep review off");
        }

        match prior {
            Some(v) => std::env::set_var("INFER_REVIEW_ENABLED", v),
            None => std::env::remove_var("INFER_REVIEW_ENABLED"),
        }
    }

    #[test]
    fn parse_verdict_central_likely_case() {
        let raw = r#"{"holds":"likely","confidence":0.6,"caveat":"assumes step or biological child"}"#;
        let v = parse_review_verdict(raw).unwrap();
        assert_eq!(v.holds, Holds::Likely);
        assert!((v.confidence - 0.6).abs() < 1e-6);
        assert!(v.caveat.contains("child"));
    }

    #[test]
    fn parse_verdict_strips_fences_and_prose() {
        let raw = "```json\n{\"holds\":\"true\",\"confidence\":0.95,\"caveat\":\"structural\"}\n```";
        let v = parse_review_verdict(raw).unwrap();
        assert_eq!(v.holds, Holds::True);

        let raw2 = "Sure — here you go: {\"holds\":\"false\",\"confidence\":0.2,\"caveat\":\"prior marriage\"} done";
        let v2 = parse_review_verdict(raw2).unwrap();
        assert_eq!(v2.holds, Holds::False);
        assert!((v2.confidence - 0.2).abs() < 1e-6);
    }

    #[test]
    fn parse_verdict_accepts_bool_holds_and_defaults_confidence() {
        let raw = r#"{"holds":true}"#;
        let v = parse_review_verdict(raw).unwrap();
        assert_eq!(v.holds, Holds::True);
        // Missing confidence defaults to neutral 0.5 rather than failing.
        assert!((v.confidence - 0.5).abs() < 1e-6);
        assert_eq!(v.caveat, "");
    }

    #[test]
    fn parse_verdict_clamps_out_of_range_confidence() {
        let v = parse_review_verdict(r#"{"holds":"likely","confidence":1.7}"#).unwrap();
        assert!((v.confidence - 1.0).abs() < 1e-6);
        let v2 = parse_review_verdict(r#"{"holds":"likely","confidence":-0.4}"#).unwrap();
        assert!((v2.confidence - 0.0).abs() < 1e-6);
    }

    #[test]
    fn parse_verdict_rejects_garbage() {
        assert!(parse_review_verdict("not json at all").is_err());
        assert!(parse_review_verdict(r#"{"confidence":0.5}"#).is_err()); // no holds
    }

    #[test]
    fn apply_review_drops_on_false() {
        let mut edge = InferredEdge {
            src: Uuid::new_v4(),
            predicate: PARENT_OF,
            dst: Uuid::new_v4(),
            confidence: 0.6,
            defeasible: true,
            rule: "spouse_of∘parent_of→parent_of".into(),
            source_edge_ids: vec![],
        };
        let keep = apply_review(
            &mut edge,
            &ReviewVerdict { holds: Holds::False, confidence: 0.2, caveat: "x".into() },
        );
        assert!(!keep, "holds=false must drop the edge");
    }

    #[test]
    fn apply_review_only_lowers_confidence() {
        let mut edge = InferredEdge {
            src: Uuid::new_v4(),
            predicate: PARENT_OF,
            dst: Uuid::new_v4(),
            confidence: 0.6,
            defeasible: true,
            rule: "spouse_of∘parent_of→parent_of".into(),
            source_edge_ids: vec![],
        };
        // Reviewer is more confident — we must NOT inflate.
        assert!(apply_review(
            &mut edge,
            &ReviewVerdict { holds: Holds::Likely, confidence: 0.9, caveat: "".into() },
        ));
        assert!((edge.confidence - 0.6).abs() < 1e-6, "confidence must not inflate");

        // Reviewer is less confident — we lower to the reviewer's value.
        assert!(apply_review(
            &mut edge,
            &ReviewVerdict { holds: Holds::Likely, confidence: 0.3, caveat: "".into() },
        ));
        assert!((edge.confidence - 0.3).abs() < 1e-6, "confidence must lower to reviewer");
    }

    #[test]
    fn build_review_prompt_includes_rule_metadata() {
        let edge = InferredEdge {
            src: Uuid::new_v4(),
            predicate: PARENT_OF,
            dst: Uuid::new_v4(),
            confidence: 0.57,
            defeasible: true,
            rule: "spouse_of∘parent_of→parent_of".into(),
            source_edge_ids: vec![],
        };
        let ctx = ReviewContext {
            subject: "Harper".into(),
            object: "Casey".into(),
            source_edges: vec![
                "spouse_of(Harper, Alex)".into(),
                "parent_of(Alex, Casey)".into(),
            ],
            context: vec![],
        };
        let p = build_review_prompt(&edge, &ctx);
        assert!(p.contains("subject: Harper"));
        assert!(p.contains("object: Casey"));
        assert!(p.contains("predicate: parent_of"));
        // base_confidence + note must come from the registered rule, not the edge.
        assert!(p.contains("base_confidence: 0.6"), "prompt: {p}");
        assert!(p.contains("spouse of a parent"), "prompt should carry the rule note");
        assert!(p.contains("spouse_of(Harper, Alex)"));
    }
}
