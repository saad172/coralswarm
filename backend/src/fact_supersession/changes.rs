//! Stated changes (extraction v3) as supersession facts.
//!
//! Extraction v3 records, on a semantic atom, a change that ONE statement
//! makes: "deploy.yml previously fired on CI and is now manual dispatch"
//! (`semantic_atoms.stated_changes`, `workers::source_atoms::v3`). A pair of
//! facts from two sources cannot express that, so each change becomes two
//! facts on the change's subject and state key:
//!
//! - the **new** fact: the new value, from the effective date when one is
//!   stated. It knows the value it replaces ([`StatedChange::New`]).
//! - the **old** fact: the replaced value, ending at the effective date
//!   ([`StatedChange::Old`]). Read only for the change's own version, never
//!   as a prior of another one, and compared only with its own new fact.
//!
//! The rule ([`stated_replaces`], `Rule::StatedChange`) is deterministic: the
//! new fact supersedes its own old fact, and any earlier fact on the same
//! subject and key whose value the change names as the old one (by reef, or
//! by the same words). It holds on many-valued keys too: the evidence names
//! the value it replaces. Writing stays in `fact_supersession`
//! (`decide_pair` / `upsert_edge`), the only writer of
//! `fact_supersession_edges`.
//!
//! A change's own pair has the same atom on both sides. Readers never treat
//! that as the atom being superseded (`outcome_supersession_sql`,
//! `superseded_sql`): the atom also states the current value.
//!
//! Reef merges: the recorded reefs follow merges ([`current_reef`]), and a
//! merge's reconciliation (`reconcile_tx`) re-derives the change facts of
//! the atoms it moved ([`load_for_atoms`]), so a change lands on the keeper
//! and an undo puts it back.
//!
//! Temporal evidence: the new fact asserts its value; a dated old fact
//! asserts the old value and withdraws it at the effective date, and in live
//! mode the decision chains a transition. An undated old fact records no
//! evidence (it has no end to state), only the decision.

use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{truncate, Fact, FactFilter, MAX_VALUE_CHARS, PRIOR_LIMIT, STATED_SOURCE_SQL, VERSION_FACT_LIMIT};
use crate::workers::source_atoms::EXTRACTION_VERSION_V3;

/// A fact's role in a stated change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatedChange {
    /// The new value; `old_value` / `old_node` name what it replaces.
    New { old_value: Option<String>, old_node: Option<Uuid> },
    /// The replaced value, from the same statement.
    Old,
}

/// `y` (a new fact) supersedes `x` by its own statement. Pure.
pub fn stated_replaces(y: &Fact, x: &Fact) -> bool {
    let Some(StatedChange::New { old_value, old_node }) = &y.change else { return false };
    if y.subject != x.subject || y.state_key != x.state_key {
        return false;
    }
    let named = match (old_node, x.value_node) {
        (Some(old), Some(value)) if *old == value => true,
        _ => old_value.as_deref().is_some_and(|o| o.trim().eq_ignore_ascii_case(x.value.trim())),
    };
    if !named {
        return false;
    }
    match x.change {
        // Its own old value.
        Some(StatedChange::Old) => x.atom_id == y.atom_id,
        // An earlier statement of the value it names. Never a later one
        // (the value may have come back), and never one that starts after
        // the change took effect.
        _ => {
            x.atom_id != y.atom_id
                && x.observed_at <= y.observed_at
                && !matches!((x.valid_from, y.valid_from), (Some(a), Some(b)) if a > b)
        }
    }
}

/// An old fact is only ever compared with the new fact of its own change.
pub fn comparable(a: &Fact, b: &Fact) -> bool {
    match (&a.change, &b.change) {
        (Some(StatedChange::Old), _) => stated_replaces(b, a),
        (_, Some(StatedChange::Old)) => stated_replaces(a, b),
        _ => true,
    }
}

/// Two facts from one atom are compared only when they are one change's
/// old and new values.
pub fn same_change(a: &Fact, b: &Fact) -> bool {
    a.atom_id == b.atom_id && (stated_replaces(a, b) || stated_replaces(b, a))
}

#[derive(sqlx::FromRow)]
struct ChangeRow {
    semantic_atom_id: Uuid,
    source_version_id: Uuid,
    source_item_id: Uuid,
    subject: Uuid,
    state_key: String,
    new_value: String,
    new_node: Option<Uuid>,
    new_title: Option<String>,
    old_value: Option<String>,
    old_node: Option<Uuid>,
    old_title: Option<String>,
    effective_from: Option<DateTime<Utc>>,
    effective_quote: Option<String>,
    passage_id: Uuid,
    quote: String,
    observed_at: DateTime<Utc>,
    linked: bool,
}

/// `<alias>.change->>'key'` as a uuid, or NULL when it is not one.
fn uuid_of(alias: &str, key: &str) -> String {
    format!("(CASE WHEN {alias}.change->>'{key}' ~ '^[0-9a-fA-F-]{{36}}$' THEN ({alias}.change->>'{key}')::uuid END)")
}

/// The reef `id` names now. The recorded reef while it exists and is not
/// the one a merge in progress is deleting (`gone`, bind `$<gone>`);
/// otherwise followed through standing merges (`node_merges`, not undone) to
/// the first such reef. A merge records its row before its reconciliation
/// runs; an undo restores the reef before its own runs, while its row still
/// reads as standing, which is why an existing reef is never redirected.
fn current_reef(id: &str, gone: u8) -> String {
    format!(
        "(WITH RECURSIVE r(id,depth) AS (SELECT {id},0 \
            UNION ALL SELECT m.keep_id,r.depth+1 FROM r JOIN node_merges m ON m.graph_id=$1 AND m.dup_id=r.id AND m.undone_at IS NULL \
             WHERE r.depth<8 AND (r.id=${gone} OR NOT EXISTS(SELECT 1 FROM nodes n WHERE n.node_id=r.id AND n.graph_id=$1))) \
          SELECT r.id FROM r JOIN nodes n ON n.node_id=r.id AND n.graph_id=$1 WHERE r.id IS DISTINCT FROM ${gone} \
           ORDER BY r.depth LIMIT 1)"
    )
}

/// Which change facts to read.
enum Select<'a> {
    Version(Uuid),
    /// New facts only (an old fact is never a prior).
    Subject { node: Uuid, key: &'a str },
    /// A merge's reconciliation: every change on these atoms, with the reef
    /// the merge is deleting.
    Atoms(&'a [Uuid], Option<Uuid>),
}

/// The change facts `filter` selects. For a version, its new and old facts;
/// for a (subject, key), only new facts.
///
/// The subject is the reef recorded at extraction as it is now (followed
/// through merges), or, when that reef is gone, the reef now linked to the
/// atom whose title or alias is the subject mention. Value reefs follow
/// merges the same way; a value reef that is gone falls back to the words.
pub(super) async fn load_facts(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    filter: &FactFilter,
) -> Result<Vec<Fact>, sqlx::Error> {
    let select = match filter {
        FactFilter::Version(v) => Select::Version(*v),
        FactFilter::Subject { node, key } => Select::Subject { node: *node, key },
    };
    Ok(load(tx, graph_id, select).await?.0)
}

/// Every change fact on `atoms` (merge reconciliation), resolving a subject
/// recorded on `gone` (the reef the merge deletes) to its keeper. `None` when
/// the read reaches its limit (the set would be incomplete).
pub(super) async fn load_for_atoms(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    atoms: &[Uuid],
    gone: Option<Uuid>,
) -> Result<Option<Vec<Fact>>, sqlx::Error> {
    if atoms.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let (facts, complete) = load(tx, graph_id, Select::Atoms(atoms, gone)).await?;
    Ok(complete.then_some(facts))
}

/// Facts, and whether the read stayed under its row limit.
async fn load(tx: &mut Transaction<'_, Postgres>, graph_id: Uuid, select: Select<'_>) -> Result<(Vec<Fact>, bool), sqlx::Error> {
    let limit = match select {
        Select::Version(_) => VERSION_FACT_LIMIT,
        Select::Subject { .. } => PRIOR_LIMIT,
        Select::Atoms(..) => super::RECONCILE_FACT_LIMIT,
    };
    // `gone`'s bind position: after the filter's own binds.
    let gone = if matches!(select, Select::Subject { .. }) { 6 } else { 5 };
    let stored_subject = current_reef(&uuid_of("x", "subject_node_id"), gone);
    let sql = format!(
        "WITH c AS ( \
           SELECT a.semantic_atom_id,a.source_version_id,v.source_item_id,a.graph_id,x.change,x.ordinal, \
                  COALESCE(sn.node_id,ln.node_id) subject,COALESCE(v.occurred_at,v.captured_at) observed_at \
             FROM semantic_atoms a \
             JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id \
             JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted' \
             CROSS JOIN LATERAL jsonb_array_elements(a.stated_changes) WITH ORDINALITY x(change,ordinal) \
             LEFT JOIN LATERAL (SELECT {stored_subject} node_id) sn ON true \
             LEFT JOIN LATERAL (SELECT l.node_id FROM node_semantic_atoms l \
                  JOIN nodes n ON n.node_id=l.node_id AND n.graph_id=l.graph_id \
                 WHERE sn.node_id IS NULL AND l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id \
                   AND (lower(n.title)=lower(x.change->>'subject') OR jsonb_exists(n.body->'aliases',lower(x.change->>'subject'))) \
                 ORDER BY l.node_id LIMIT 1) ln ON true \
            WHERE a.graph_id=$1 AND a.extraction_version=$2 AND a.stated_changes IS NOT NULL \
              AND jsonb_typeof(a.stated_changes)='array' AND {filter} AND {STATED_SOURCE_SQL}) \
         SELECT c.semantic_atom_id,c.source_version_id,c.source_item_id,c.subject, \
                c.change->>'state_key' state_key, \
                c.change->>'new_value' new_value,nn.node_id new_node,nn.title new_title, \
                NULLIF(c.change->>'old_value','') old_value,onode.node_id old_node,onode.title old_title, \
                NULLIF(c.change->>'effective_from','')::timestamptz effective_from,NULLIF(c.change->>'effective_quote','') effective_quote, \
                p.passage_id,p.quote,c.observed_at, \
                EXISTS(SELECT 1 FROM node_semantic_atoms l WHERE l.graph_id=c.graph_id AND l.node_id=c.subject AND l.semantic_atom_id=c.semantic_atom_id) linked \
           FROM c \
           LEFT JOIN nodes nn ON nn.node_id={new_id} AND nn.graph_id=c.graph_id \
           LEFT JOIN nodes onode ON onode.node_id={old_id} AND onode.graph_id=c.graph_id \
           JOIN LATERAL (SELECT p.passage_id,p.quote FROM semantic_atom_passages p \
                WHERE p.graph_id=c.graph_id AND p.source_version_id=c.source_version_id AND p.semantic_atom_id=c.semantic_atom_id \
                ORDER BY p.start_char,p.passage_id LIMIT 1) p ON true \
          WHERE c.subject IS NOT NULL AND c.change->>'state_key' ~ '^[a-z][a-z0-9_.-]{{0,63}}$' \
            AND length(COALESCE(c.change->>'new_value','')) BETWEEN 1 AND 512 \
          ORDER BY c.observed_at DESC,c.semantic_atom_id,c.ordinal LIMIT $4",
        filter = match select {
            Select::Version(_) => "a.source_version_id=$3",
            Select::Subject { .. } => "x.change->>'state_key'=$5 AND COALESCE(sn.node_id,ln.node_id)=$3",
            Select::Atoms(..) => "a.semantic_atom_id=ANY($3)",
        },
        new_id = current_reef(&uuid_of("c", "new_node_id"), gone),
        old_id = current_reef(&uuid_of("c", "old_node_id"), gone),
    );
    let query = sqlx::query_as::<_, ChangeRow>(&sql).bind(graph_id).bind(EXTRACTION_VERSION_V3);
    let query = match select {
        Select::Version(v) => query.bind(v).bind(limit).bind(None::<Uuid>),
        Select::Subject { node, key } => query.bind(node).bind(limit).bind(key).bind(None::<Uuid>),
        Select::Atoms(atoms, gone) => query.bind(atoms).bind(limit).bind(gone),
    };
    let rows = query.fetch_all(&mut **tx).await?;
    let complete = (rows.len() as i64) < limit;
    let with_old = !matches!(select, Select::Subject { .. });
    let mut facts = Vec::with_capacity(rows.len() * 2);
    for r in rows {
        let relational = r.state_key.starts_with("rel.");
        // A relational change keys on its new value's reef.
        if relational && r.new_node.is_none() {
            continue;
        }
        let value_of = |title: Option<&String>, words: &str| truncate(title.map(String::as_str).unwrap_or(words), MAX_VALUE_CHARS);
        let new_value = value_of(r.new_title.as_ref(), &r.new_value);
        if new_value.is_empty() || (relational && r.new_node == Some(r.subject)) {
            continue;
        }
        let old_value = r.old_value.as_deref().map(|o| value_of(r.old_title.as_ref(), o)).filter(|o| !o.is_empty());
        let fact = |value: String, value_node: Option<Uuid>, change: StatedChange, from, until, linked| Fact {
            atom_id: r.semantic_atom_id,
            version_id: r.source_version_id,
            source_item_id: r.source_item_id,
            subject: r.subject,
            state_key: r.state_key.clone(),
            value,
            value_node,
            valid_from: from,
            valid_until: until,
            validity_quote: r.effective_quote.clone().filter(|_| r.effective_from.is_some()),
            passage_id: r.passage_id,
            quote: r.quote.clone(),
            observed_at: r.observed_at,
            linked,
            change: Some(change),
        };
        let new_node = if relational { r.new_node } else { None };
        let old_node = if relational { r.old_node } else { None };
        facts.push(fact(
            new_value,
            new_node,
            StatedChange::New { old_value: r.old_value.clone(), old_node },
            r.effective_from,
            None,
            r.linked,
        ));
        // The old fact belongs to its own version only. A relational old
        // value whose reef is gone groups with nothing; the new fact still
        // names it for earlier facts.
        if let (true, Some(old)) = (with_old, old_value) {
            if !relational || old_node.is_some() {
                let dated = r.effective_from.is_some();
                facts.push(fact(old, old_node, StatedChange::Old, None, r.effective_from, r.linked && dated));
            }
        }
    }
    Ok((facts, complete))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(day: &str) -> DateTime<Utc> {
        format!("{day}T00:00:00Z").parse().unwrap()
    }

    fn f(atom: u128, value: &str, node: Option<u128>, change: Option<StatedChange>) -> Fact {
        Fact {
            atom_id: Uuid::from_u128(atom),
            version_id: Uuid::from_u128(atom + 100),
            source_item_id: Uuid::from_u128(atom + 200),
            subject: Uuid::from_u128(1),
            state_key: "rel.manager".into(),
            value: value.into(),
            value_node: node.map(Uuid::from_u128),
            valid_from: None,
            valid_until: None,
            validity_quote: None,
            passage_id: Uuid::from_u128(atom + 300),
            quote: "q".into(),
            observed_at: at("2026-05-01"),
            linked: true,
            change,
        }
    }

    fn new(atom: u128, value: &str, node: u128, old: &str, old_node: Option<u128>) -> Fact {
        f(atom, value, Some(node), Some(StatedChange::New { old_value: Some(old.into()), old_node: old_node.map(Uuid::from_u128) }))
    }

    #[test]
    fn a_change_replaces_its_own_old_value_and_the_earlier_fact_it_names() {
        let y = new(5, "Ana Ruiz", 20, "Bo Chen", Some(21));
        let own_old = f(5, "Bo Chen", Some(21), Some(StatedChange::Old));
        assert!(stated_replaces(&y, &own_old) && same_change(&y, &own_old) && comparable(&own_old, &y));
        let mut earlier = f(1, "Bo Chen", Some(21), None);
        earlier.observed_at = at("2026-01-01");
        assert!(stated_replaces(&y, &earlier), "named by reef");
        let mut by_words = f(2, "bo chen", Some(99), None);
        by_words.observed_at = at("2026-01-01");
        assert!(stated_replaces(&y, &by_words), "named by the same words (the reef may have merged)");
        let mut later = earlier.clone();
        later.observed_at = at("2026-09-01");
        assert!(!stated_replaces(&y, &later), "a later statement of the old value is not replaced");
        let mut starts_after = earlier.clone();
        starts_after.valid_from = Some(at("2026-06-01"));
        let mut dated = y.clone();
        dated.valid_from = Some(at("2026-03-01"));
        assert!(!stated_replaces(&dated, &starts_after));
        let other = f(3, "Cy Lo", Some(22), None);
        assert!(!stated_replaces(&y, &other), "an unnamed value is not replaced");
        let mut other_key = earlier.clone();
        other_key.state_key = "rel.owner".into();
        assert!(!stated_replaces(&y, &other_key));
        // Another change's old fact, or the old fact against anything else.
        let foreign_old = f(6, "Bo Chen", Some(21), Some(StatedChange::Old));
        assert!(!stated_replaces(&y, &foreign_old));
        assert!(!comparable(&own_old, &other) && !comparable(&other, &own_old));
        assert!(comparable(&y, &other), "the new fact is an ordinary fact otherwise");
        assert!(!same_change(&y, &f(5, "Cy Lo", Some(22), None)));
    }

    /// Current states (no old value) on one single-valued key give the
    /// existing rules real candidates: the later stated start replaces.
    #[test]
    fn current_states_on_one_key_decide_by_later_start() {
        use super::super::{decide, Rule, Side, Verdict};
        let state = |atom: u128, value: &str, from: &str| {
            let mut s = f(atom, value, None, Some(StatedChange::New { old_value: None, old_node: None }));
            s.state_key = "attr.status".into();
            s.valid_from = Some(at(from));
            s
        };
        let (blocked, done) = (state(1, "blocked", "2026-10-01"), state(2, "done", "2026-10-15"));
        assert_eq!(decide(&blocked, &done), Verdict::Supersede { superseder: Side::B, rule: Rule::LaterStart });
        assert!(!stated_replaces(&done, &blocked), "no old value named: not a stated change");
    }

    #[test]
    fn decide_applies_the_stated_change_on_any_key_before_other_rules() {
        use super::super::{decide, Rule, Side, Verdict};
        // Many-valued: a stated change still decides.
        let mut y = new(5, "Hermes", 30, "Apollo", Some(31));
        y.state_key = "rel.works_on".into();
        let mut old = f(5, "Apollo", Some(31), Some(StatedChange::Old));
        old.state_key = y.state_key.clone();
        assert_eq!(decide(&old, &y), Verdict::Supersede { superseder: Side::B, rule: Rule::StatedChange });
        assert_eq!(decide(&y, &old), Verdict::Supersede { superseder: Side::A, rule: Rule::StatedChange });
        // The old fact against an unrelated fact: never a decision.
        let mut unrelated = f(9, "Zed", Some(40), None);
        unrelated.state_key = y.state_key.clone();
        unrelated.valid_from = Some(at("2026-08-01"));
        old.valid_until = Some(at("2026-03-01"));
        assert_eq!(decide(&old, &unrelated), Verdict::Independent, "explicit_end never fires against a change's old fact");
    }
}
