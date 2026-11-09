//! Operator core: merge duplicate reefs minted per source version by the v1
//! binding ladder (2026-10-19 incident). Thin binary: `dedupe_reefs`.
//!
//! Every merge goes through the existing human-merge path, the same two
//! calls `merge_reefs` and the authenticated merge route make:
//! `Store::write_judgement_scoped` (a `source='human'` POSITIVE
//! `reef_judgement` authored by the operator's `--created-by`), then
//! `Store::merge_nodes_with_judgement_scoped` in the same transaction.
//! `merge_nodes_impl` (`store/mod.rs`) is what preserves the reef's data:
//! it moves `node_atoms` and `node_semantic_atoms`, re-points
//! `relationship_edge` (dropping self-loops and collisions with the keeper's
//! own edges), re-points `supersession_edge` and `node_promotions`, unions
//! `reef_identifiers`, teaches the keeper the duplicate's title and aliases,
//! moves `graphs.self_node_id`, emits the sync ops, and writes the
//! `node_merges` undo row (`POST /v1/nodes/merges/{id}/undo`). It also
//! refuses a merge whose evidence is not fully admitted
//! (`library_permitted_atoms`). This module writes nothing else.
//!
//! ## Policy (explicit and conservative)
//!
//! A **group** is reefs with the same graph, `type='entity'`, `entity_type`,
//! `scope`, `owner_id` and normalized title (`lower`, trimmed, inner
//! whitespace collapsed). Scope and owner are part of the key, so a merge
//! never widens who can see a reef's evidence. Groups never span graphs.
//!
//! * **Scope A (default):** groups with at least one member created at or
//!   after `cutoff` (2026-10-19T03:17Z). Only members created since the
//!   cutoff are merged away. `include_preexisting` also folds the group's
//!   older copies. It does not widen which groups are found: a group whose
//!   copies all predate `cutoff` is never read (`MEMBER_SQL`'s `HAVING
//!   bool_or(created_at>=cutoff)`). To fold long-standing duplicates too,
//!   pass an early `--cutoff` (e.g. `2000-01-01T00:00:00Z`) with
//!   `--include-preexisting`.
//! * **Canonical:** the graph's self reef if it is in the group, else the
//!   OLDEST member (`created_at`, then `node_id`). The oldest reef is the one
//!   people saw before the incident. It carries the judgements, promotions,
//!   bookmarks and MCP references made against its id, and it is stable
//!   across re-runs, so the tool is resumable. "Most corals" would pick
//!   whichever per-version copy happened to see the most text, and it can
//!   flip between runs.
//! * **Type rule:** only named-entity types merge: `person`, `org`, `project`,
//!   `repo`, `tool`, `product` (override with `entity_types`). `topic`
//!   (concepts and generic nouns, such as git branch names like `main`,
//!   table names like `jobs`, or words like `teams`) and an unknown type are
//!   excluded. The same word as a topic often names different things in
//!   different sources.
//! * **Title rule:** excluded if the normalized title is on [`STOPLIST`],
//!   has fewer than 3 characters, or contains no letter.
//! * **Per-pair guards** (skipped, reported): an active NEGATIVE judgement
//!   between keeper and duplicate (any source; a ruling of "different" is
//!   not overridden by a title match), a duplicate with promotion lineage
//!   (`node_promotions` either side, `promoted_from_node_id` set, or another
//!   reef promoted from it), and a duplicate whose decisive identifiers
//!   (email, Slack user, Clerk user) conflict with the keeper's or with a
//!   copy already folded into it this run: two emails are two people,
//!   whatever the title says.
//! * **Group guard:** any `reef_identity_corrections` row on a member skips
//!   the whole group. Corrections cascade with `node_atoms`, so merging
//!   would lose the human's correction.
//!
//! Execute: one transaction per DUPLICATE (a same-title group can hold
//! hundreds of copies; a transaction per group held locks for minutes). Each
//! transaction locks the keeper and that one duplicate, re-checks the group
//! key and every guard, merges, and commits, under `lock_timeout` and
//! `statement_timeout`. A timed-out duplicate is rolled back and retried with
//! backoff, then recorded as failed while the run continues. It is idempotent
//! and resumable: merged duplicates no longer exist, so a re-run finds only
//! what is left.
//!
//! ## Rule `same-title` (#1516, founder 2026-10-27)
//!
//! [`Policy::same_title`] is the one-off merge for the mint rule in
//! `store::door_title_identity_class`: a non-person entity is its title
//! within its title identity class (`store::title_identity_class`: `topic`
//! and `project` share the class `topic_project`, `org` is its own), so
//! every `org` group and every `topic`/`project` group (mixed or not) folds
//! into its keeper (the same keeper `store::door_exact_title_reef_tx` binds
//! new mentions to: self reef, else oldest), whatever its age, title length
//! or genericness. The keeper keeps its own kind; `merge_nodes_impl` never
//! writes `nodes.entity_type`, and the copies' kinds go with their rows (and
//! their `node_merges` snapshots). `person` is never merged by this rule: two people often
//! share a name. Only a HUMAN negative judgement skips a pair; model
//! verdicts that same-title copies differ are what minted the copies. The
//! identity-correction, promotion-lineage, identifier-conflict, scope and
//! owner guards are unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::store::{JudgementSource, JudgementVerdict, NewJudgement, PostgresStore, Store};

/// Reefs minted since extraction v2 was released (#1339).
pub const DEFAULT_CUTOFF: &str = "2026-10-19T03:17:00Z";
/// Named-entity types that merge by default.
pub const DEFAULT_ENTITY_TYPES: &[&str] = &["person", "org", "project", "repo", "tool", "product"];
/// Normalized titles that are too generic to identify one real thing, even
/// within one graph and one entity type: branch/environment names, roles,
/// pronouns, placeholders and generic software words.
pub const STOPLIST: &[&str] = &[
    "main", "master", "dev", "develop", "development", "staging", "stage", "prod", "production",
    "test", "tests", "testing", "release", "releases", "local", "default", "head", "origin",
    "app", "apps", "api", "web", "backend", "frontend", "server", "client", "service", "services",
    "ui", "db", "database", "data", "docs", "readme", "config", "admin", "user", "users", "owner",
    "team", "teams", "bot", "system", "assistant", "agent", "unknown", "none", "null", "n/a",
    "me", "you", "we", "they", "it", "someone", "everyone", "anyone", "author", "speaker",
    "customer", "customers", "company", "org", "project", "repo", "tool", "product", "person",
];
const MEMBER_SQL: &str = "
WITH m AS (
 SELECT n.node_id, n.graph_id, n.type, {CLASS} AS entity_type, {KIND} AS kind, n.scope, n.owner_id,
        n.created_at, n.coral_count::bigint AS coral_count, n.title,
        lower(regexp_replace(btrim(n.title),'\\s+',' ','g')) AS norm_title,
        COALESCE(g.self_node_id=n.node_id,false) AS is_self
   FROM nodes n JOIN graphs g ON g.id=n.graph_id
  WHERE n.type='entity' AND ($1::uuid IS NULL OR n.graph_id=$1)
), k AS (
 SELECT graph_id, entity_type, scope, owner_id, norm_title FROM m
  GROUP BY 1,2,3,4,5 HAVING count(*)>1 AND bool_or(created_at>=$2)
)
SELECT m.node_id, m.graph_id, m.entity_type, m.kind, m.scope, m.owner_id, m.created_at, m.coral_count, m.title, m.norm_title, m.is_self,
  (SELECT count(*) FROM node_atoms a WHERE a.node_id=m.node_id) AS node_atoms,
  (SELECT count(*) FROM node_semantic_atoms a WHERE a.node_id=m.node_id) AS semantic_links,
  (SELECT count(*) FROM relationship_edge e WHERE e.src_node_id=m.node_id OR e.dst_node_id=m.node_id) AS edges,
  EXISTS(SELECT 1 FROM reef_identity_corrections c WHERE c.node_id=m.node_id) AS has_correction,
  (EXISTS(SELECT 1 FROM node_promotions p WHERE p.source_node_id=m.node_id OR p.target_node_id=m.node_id)
   OR EXISTS(SELECT 1 FROM nodes x WHERE x.node_id=m.node_id AND x.promoted_from_node_id IS NOT NULL)
   OR EXISTS(SELECT 1 FROM nodes x WHERE x.promoted_from_node_id=m.node_id)) AS promoted
  FROM m JOIN k ON k.graph_id=m.graph_id AND k.entity_type=m.entity_type AND k.scope=m.scope
   AND k.owner_id IS NOT DISTINCT FROM m.owner_id AND k.norm_title=m.norm_title
 ORDER BY m.graph_id, m.entity_type, m.norm_title, m.scope, m.owner_id NULLS FIRST, m.is_self DESC, m.created_at, m.node_id";

/// One reef of a candidate group.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Member {
    pub node_id: Uuid,
    pub graph_id: Uuid,
    /// The group key's kind: the title identity class under
    /// [`Rule::SameTitle`] (`org`, `topic_project`, ...), the
    /// `nodes.entity_type` column under [`Rule::NamedEntities`].
    pub entity_type: String,
    /// This reef's own kind (equal to `entity_type` under the incident rule).
    pub kind: String,
    pub scope: String,
    pub owner_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub coral_count: i64,
    pub title: String,
    pub norm_title: String,
    pub is_self: bool,
    pub node_atoms: i64,
    pub semantic_links: i64,
    pub edges: i64,
    pub has_correction: bool,
    pub promoted: bool,
}

/// Why a group (or one duplicate) is not merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Exclusion {
    EntityType,
    Stoplist,
    TooShortOrNoLetters,
    IdentityCorrection,
    NegativeJudgement,
    PromotionLineage,
    IdentifierConflict,
    PersonTitle,
}
impl Exclusion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EntityType => "entity_type_not_named_entity",
            Self::Stoplist => "generic_title_stoplist",
            Self::TooShortOrNoLetters => "title_under_3_chars_or_no_letters",
            Self::IdentityCorrection => "identity_correction_on_member",
            Self::NegativeJudgement => "active_negative_judgement",
            Self::PromotionLineage => "promotion_lineage",
            Self::IdentifierConflict => "conflicting_identifiers",
            Self::PersonTitle => "person_title_is_not_identity",
        }
    }
}

/// Which identity rule decides a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// The 2026-10-19 incident policy: named-entity types, stoplist and
    /// short-title exclusions, any active negative judgement skips a pair.
    NamedEntities,
    /// #1516: every non-person kind is its title; see the module doc.
    SameTitle,
}
impl Rule {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NamedEntities => "named-entities",
            Self::SameTitle => "same-title",
        }
    }
}

/// Kinds [`Rule::SameTitle`] merges: every Library kind except `person`.
pub const SAME_TITLE_ENTITY_TYPES: &[&str] = &["org", "project", "topic"];
/// [`Rule::SameTitle`] folds every copy, however old.
pub const SAME_TITLE_CUTOFF: &str = "2000-01-01T00:00:00Z";

/// The merge policy.
#[derive(Debug, Clone)]
pub struct Policy {
    pub graph: Option<Uuid>,
    pub cutoff: DateTime<Utc>,
    pub entity_types: Vec<String>,
    pub include_preexisting: bool,
    pub rule: Rule,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            graph: None,
            cutoff: DEFAULT_CUTOFF.parse().expect("valid cutoff"),
            entity_types: DEFAULT_ENTITY_TYPES.iter().map(|s| s.to_string()).collect(),
            include_preexisting: false,
            rule: Rule::NamedEntities,
        }
    }
}
impl Policy {
    /// The #1516 one-off merge: every non-person same-title group, every age.
    pub fn same_title() -> Self {
        Self {
            graph: None,
            cutoff: SAME_TITLE_CUTOFF.parse().expect("valid cutoff"),
            entity_types: SAME_TITLE_ENTITY_TYPES.iter().map(|s| s.to_string()).collect(),
            include_preexisting: true,
            rule: Rule::SameTitle,
        }
    }
    /// The SQL key a reef `n` is grouped under. The incident rule reads the
    /// `nodes.entity_type` column only (unchanged since #1344). The
    /// same-title rule uses the mint's own title identity class
    /// ([`crate::store::NODE_TITLE_IDENTITY_CLASS_SQL`]), so it groups
    /// exactly the copies `store::door_exact_title_reef_tx` would bind as
    /// one: a `topic` and a `project` of one title are one group.
    fn class_sql(&self) -> String {
        match self.rule {
            Rule::NamedEntities => "COALESCE(n.entity_type,'')".to_string(),
            Rule::SameTitle => format!("COALESCE({},'')", crate::store::NODE_TITLE_IDENTITY_CLASS_SQL),
        }
    }
    /// The SQL kind of reef `n` itself, for reporting and the judgement
    /// evidence.
    fn kind_sql(&self) -> String {
        match self.rule {
            Rule::NamedEntities => "COALESCE(n.entity_type,'')".to_string(),
            Rule::SameTitle => format!("COALESCE({},'')", crate::store::NODE_LIBRARY_KIND_SQL),
        }
    }
    /// Whether a group keyed `key` is one of [`Policy::entity_types`]. Under
    /// [`Rule::SameTitle`] a listed kind selects its whole class, so
    /// `--entity-types topic` also folds a topic's `project` twins.
    fn selects(&self, key: &str) -> bool {
        match self.rule {
            Rule::NamedEntities => self.entity_types.iter().any(|t| t == key),
            Rule::SameTitle => {
                !key.is_empty() && self.entity_types.iter().any(|t| crate::store::title_identity_class(t) == key)
            }
        }
    }
    /// Whether a negative judgement with this `source` keeps a pair apart.
    fn negative_blocks(&self, human: bool) -> bool {
        human || self.rule == Rule::NamedEntities
    }
}

/// Title-only exclusion for a normalized title.
pub fn title_exclusion(norm_title: &str) -> Option<Exclusion> {
    if STOPLIST.contains(&norm_title) {
        return Some(Exclusion::Stoplist);
    }
    if norm_title.chars().count() < 3 || !norm_title.chars().any(char::is_alphabetic) {
        return Some(Exclusion::TooShortOrNoLetters);
    }
    None
}

/// A planned group: canonical keeper plus the duplicates to merge into it.
#[derive(Debug, Clone)]
pub struct Group {
    pub graph_id: Uuid,
    pub entity_type: String,
    pub scope: String,
    pub owner_id: Option<Uuid>,
    pub norm_title: String,
    pub keep: Member,
    pub dups: Vec<Member>,
    /// Members left alone and why (pair guards, or out-of-scope older copies
    /// are not listed here).
    pub skipped: Vec<(Member, Exclusion)>,
    /// Whole group excluded.
    pub excluded: Option<Exclusion>,
    /// Older copies not merged under scope A.
    pub preexisting_left: usize,
}
impl Group {
    /// The distinct kinds of the keeper and the copies merged into it,
    /// sorted and `+`-joined (`project+topic` for a mixed group).
    pub fn kinds(&self) -> String {
        let kinds: std::collections::BTreeSet<&str> =
            std::iter::once(self.keep.kind.as_str()).chain(self.dups.iter().map(|d| d.kind.as_str())).collect();
        kinds.into_iter().collect::<Vec<_>>().join("+")
    }
}

/// Read every candidate group, apply the policy. READ ONLY.
pub async fn plan(pool: &PgPool, policy: &Policy) -> Result<Vec<Group>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
    let members: Vec<Member> = sqlx::query_as(&MEMBER_SQL.replace("{CLASS}", &policy.class_sql()).replace("{KIND}", &policy.kind_sql()))
        .bind(policy.graph)
        .bind(policy.cutoff)
        .fetch_all(&mut *tx)
        .await?;
    let ids: Vec<Uuid> = members.iter().map(|m| m.node_id).collect();
    let negatives: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT node_a_id,node_b_id FROM reef_judgement WHERE annulled_at IS NULL AND verdict='negative' \
          AND node_a_id=ANY($1) AND node_b_id=ANY($1) AND ($2 OR source='human')",
    )
    .bind(&ids)
    .bind(policy.negative_blocks(false))
    .fetch_all(&mut *tx)
    .await?;
    let identifiers: Vec<(Uuid, String, String)> =
        sqlx::query_as("SELECT node_id,kind,value_norm FROM reef_identifiers WHERE node_id=ANY($1) AND kind=ANY($2)")
            .bind(&ids)
            .bind(crate::workers::entity_resolution::DECISIVE_IDENTIFIER_KINDS)
            .fetch_all(&mut *tx)
            .await?;
    tx.rollback().await?;
    let negatives: HashSet<(Uuid, Uuid)> = negatives.into_iter().collect();
    let mut by_node: Identifiers = HashMap::new();
    for (node, kind, value) in identifiers {
        by_node.entry(node).or_default().entry(kind).or_default().insert(value);
    }
    Ok(build_groups(members, &negatives, &by_node, policy))
}

/// Decisive identifiers per reef: kind -> values.
pub type Identifiers = HashMap<Uuid, BTreeMap<String, HashSet<String>>>;

/// Some kind on both sides with no value in common.
fn conflicts(a: &BTreeMap<String, HashSet<String>>, b: &BTreeMap<String, HashSet<String>>) -> bool {
    a.iter().any(|(kind, values)| b.get(kind).is_some_and(|other| values.is_disjoint(other)))
}

/// Pure policy over already-read members (ordered by group key, self first,
/// then oldest). Exposed for unit tests.
pub fn build_groups(members: Vec<Member>, negatives: &HashSet<(Uuid, Uuid)>, identifiers: &Identifiers, policy: &Policy) -> Vec<Group> {
    let mut by_key: BTreeMap<(Uuid, String, String, Option<Uuid>, String), Vec<Member>> = BTreeMap::new();
    for m in members {
        by_key
            .entry((m.graph_id, m.entity_type.clone(), m.norm_title.clone(), m.owner_id, m.scope.clone()))
            .or_default()
            .push(m);
    }
    let mut out = Vec::new();
    for ((graph_id, entity_type, norm_title, owner_id, scope), mut ms) in by_key {
        ms.sort_by(|a, b| b.is_self.cmp(&a.is_self).then(a.created_at.cmp(&b.created_at)).then(a.node_id.cmp(&b.node_id)));
        let keep = ms.remove(0);
        let excluded = if policy.rule == Rule::SameTitle && entity_type == "person" {
            Some(Exclusion::PersonTitle)
        } else if !policy.selects(&entity_type) {
            Some(Exclusion::EntityType)
        } else if let Some(e) = title_exclusion(&norm_title).filter(|_| policy.rule == Rule::NamedEntities) {
            Some(e)
        } else if keep.has_correction || ms.iter().any(|m| m.has_correction) {
            Some(Exclusion::IdentityCorrection)
        } else {
            None
        };
        let mut dups = Vec::new();
        let mut skipped = Vec::new();
        let mut preexisting_left = 0;
        // What the keeper will carry once each accepted copy is folded in.
        let mut folded = identifiers.get(&keep.node_id).cloned().unwrap_or_default();
        for m in ms {
            if m.created_at < policy.cutoff && !policy.include_preexisting {
                preexisting_left += 1;
                continue;
            }
            let pair = if keep.node_id < m.node_id { (keep.node_id, m.node_id) } else { (m.node_id, keep.node_id) };
            if negatives.contains(&pair) {
                skipped.push((m, Exclusion::NegativeJudgement));
            } else if m.promoted || m.is_self {
                skipped.push((m, Exclusion::PromotionLineage));
            } else if identifiers.get(&m.node_id).is_some_and(|own| conflicts(&folded, own)) {
                skipped.push((m, Exclusion::IdentifierConflict));
            } else {
                for (kind, values) in identifiers.get(&m.node_id).cloned().unwrap_or_default() {
                    folded.entry(kind).or_default().extend(values);
                }
                dups.push(m);
            }
        }
        if dups.is_empty() && skipped.is_empty() && excluded.is_none() {
            continue;
        }
        out.push(Group { graph_id, entity_type, scope, owner_id, norm_title, keep, dups, skipped, excluded, preexisting_left });
    }
    out
}

/// Dry-run totals.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub groups_to_merge: usize,
    pub reefs_to_merge: usize,
    pub node_atoms_moved: i64,
    pub semantic_links_moved: i64,
    pub edges_touched: i64,
    /// Group key (identity class under the same-title rule) -> (groups,
    /// reefs merged away).
    pub by_entity_type: BTreeMap<String, (usize, usize)>,
    /// (graph, group key) -> (groups, reefs merged away).
    pub by_graph_type: BTreeMap<(Uuid, String), (usize, usize)>,
    /// The kinds a merged group holds (`project+topic` for a mixed one,
    /// `topic`, `org`, ...) -> (groups, reefs merged away).
    pub by_kinds: BTreeMap<String, (usize, usize)>,
    /// Merged groups by the keeper's kind -> (groups, reefs merged away).
    pub by_keeper_kind: BTreeMap<String, (usize, usize)>,
    /// (graph, kinds) -> (groups, reefs merged away).
    pub by_graph_kinds: BTreeMap<(Uuid, String), (usize, usize)>,
    /// reason -> (groups, reefs that would otherwise merge)
    pub excluded: BTreeMap<&'static str, (usize, usize)>,
    /// (entity_type, title, reason, copies)
    pub excluded_titles: Vec<(String, String, &'static str, usize)>,
    pub preexisting_left: usize,
}

pub fn report(groups: &[Group], policy: &Policy) -> Report {
    let mut r = Report::default();
    let mut titles: HashMap<(String, String, &'static str), usize> = HashMap::new();
    for g in groups {
        let candidates = g.dups.len() + g.skipped.len();
        if let Some(e) = g.excluded {
            let slot = r.excluded.entry(e.as_str()).or_default();
            slot.0 += 1;
            // What scope A would have merged had the group been allowed.
            let would = g.dups.len() + g.skipped.len();
            slot.1 += would;
            if would > 0 {
                *titles.entry((g.entity_type.clone(), g.norm_title.clone(), e.as_str())).or_default() += would;
            }
            continue;
        }
        for (_, e) in &g.skipped {
            let slot = r.excluded.entry(e.as_str()).or_default();
            slot.0 += 1;
            slot.1 += 1;
        }
        if g.dups.is_empty() {
            continue;
        }
        let _ = candidates;
        r.groups_to_merge += 1;
        r.reefs_to_merge += g.dups.len();
        r.preexisting_left += if policy.include_preexisting { 0 } else { g.preexisting_left };
        let e = r.by_entity_type.entry(g.entity_type.clone()).or_default();
        e.0 += 1;
        e.1 += g.dups.len();
        let e = r.by_graph_type.entry((g.graph_id, g.entity_type.clone())).or_default();
        e.0 += 1;
        e.1 += g.dups.len();
        let kinds = g.kinds();
        let e = r.by_kinds.entry(kinds.clone()).or_default();
        e.0 += 1;
        e.1 += g.dups.len();
        let e = r.by_graph_kinds.entry((g.graph_id, kinds)).or_default();
        e.0 += 1;
        e.1 += g.dups.len();
        let e = r.by_keeper_kind.entry(g.keep.kind.clone()).or_default();
        e.0 += 1;
        e.1 += g.dups.len();
        for d in &g.dups {
            r.node_atoms_moved += d.node_atoms;
            r.semantic_links_moved += d.semantic_links;
            r.edges_touched += d.edges;
        }
    }
    let mut t: Vec<_> = titles.into_iter().map(|((et, title, why), n)| (et, title, why, n)).collect();
    t.sort_by(|a, b| b.3.cmp(&a.3).then(a.1.cmp(&b.1)));
    r.excluded_titles = t;
    r
}

/// Outcome of an execute pass.
///
/// Accounting is per group and per duplicate. A group is `groups_merged` only
/// when every planned duplicate merged. `groups_partial` counts groups where
/// some, but not all, duplicates merged (the rest changed since planning or
/// failed); `groups_changed` counts groups where nothing merged because every
/// duplicate changed since planning; `groups_failed` counts groups with at
/// least one duplicate that failed after retries (a partial group can be both
/// partial and failed). A re-run picks up whatever is left.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Executed {
    pub groups_merged: usize,
    pub groups_partial: usize,
    pub reefs_merged: usize,
    pub corals_moved: u64,
    pub groups_failed: usize,
    pub groups_changed: usize,
    /// Duplicates skipped because they no longer matched the plan (gone,
    /// regrouped, newly guarded or identity-conflicting).
    pub reefs_changed: usize,
    /// Duplicates that failed after all retries.
    pub reefs_failed: usize,
    pub failures: Vec<String>,
    /// Set when the run stopped early (too many consecutive connection
    /// failures). Everything merged before it is committed.
    pub aborted: Option<String>,
    /// Planned groups never reached because the run aborted.
    pub groups_not_attempted: usize,
}

/// Bounds for one duplicate's merge transaction and the retry loop around it.
#[derive(Debug, Clone)]
pub struct ExecOptions {
    /// `SET LOCAL lock_timeout`: yield rather than queue behind other traffic.
    pub lock_timeout_ms: u64,
    /// `SET LOCAL statement_timeout`, per statement. The slowest statement of
    /// a merge is `merge_nodes_impl`'s admission scan over the graph's
    /// `library_permitted_atoms`; the whole old per-group transaction (hundreds
    /// of duplicates) measured over two minutes in prod, so one duplicate's
    /// slowest statement is seconds at most. 60s leaves an order of magnitude
    /// of headroom while still bounding how long a merge can hold the keeper.
    pub statement_timeout_ms: u64,
    /// Attempts per duplicate for lock/statement timeouts, deadlocks and
    /// connection errors.
    pub max_attempts: u32,
    /// First retry delay; doubles each attempt.
    pub retry_backoff_ms: u64,
    /// Pause between duplicate merges (throttle). 0 = none.
    pub pause_ms: u64,
    /// Abort the run after this many duplicates in a row failed on a
    /// connection error.
    pub max_consecutive_connection_failures: u32,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            lock_timeout_ms: 2_000,
            statement_timeout_ms: 60_000,
            max_attempts: 4,
            retry_backoff_ms: 250,
            pause_ms: 0,
            max_consecutive_connection_failures: 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrKind {
    /// lock_timeout, statement_timeout, deadlock, serialization failure.
    Contention,
    /// The connection or pool is unusable right now.
    Connection,
    Other,
}

fn classify(e: &crate::store::StoreError) -> ErrKind {
    let crate::store::StoreError::Db(inner) = e else { return ErrKind::Other };
    let Some(e) = inner.downcast_ref::<sqlx::Error>() else { return ErrKind::Other };
    match e {
        sqlx::Error::Database(d) => match d.code().as_deref() {
            Some("55P03" | "57014" | "40001" | "40P01") => ErrKind::Contention,
            Some(c) if c.starts_with("08") || matches!(c, "57P01" | "57P02" | "57P03") => ErrKind::Connection,
            _ => ErrKind::Other,
        },
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed | sqlx::Error::Protocol(_) => {
            ErrKind::Connection
        }
        _ => ErrKind::Other,
    }
}

enum DupOutcome {
    Merged(u64),
    /// The duplicate no longer matches the plan; nothing written.
    Changed(&'static str),
}

struct DupFailure {
    msg: String,
    connection: bool,
}

/// Plan, then merge, with default bounds. `max_groups` bounds the number of
/// groups attempted (a rehearsal).
pub async fn execute(
    pool: &PgPool,
    policy: &Policy,
    created_by: Uuid,
    max_groups: Option<usize>,
) -> Result<Executed, sqlx::Error> {
    execute_with(pool, policy, created_by, max_groups, &ExecOptions::default()).await
}

pub async fn execute_with(
    pool: &PgPool,
    policy: &Policy,
    created_by: Uuid,
    max_groups: Option<usize>,
    opts: &ExecOptions,
) -> Result<Executed, sqlx::Error> {
    let groups = plan(pool, policy).await?;
    Ok(execute_groups(pool, &groups, policy, created_by, max_groups, opts).await)
}

/// Merge planned groups, ONE DUPLICATE PER TRANSACTION. A same-title group can
/// hold hundreds of copies; one transaction per group held the keeper and
/// many rows locked for minutes (2026-10-28: it blocked the Library listing
/// worker for up to 29s, 47 times in 15 minutes). Each transaction here locks
/// only the keeper and one duplicate, is bounded by `lock_timeout` and
/// `statement_timeout`, and re-checks every guard against the keeper as it
/// stands after the earlier merges. Any per-duplicate error is recorded and
/// the run continues; only repeated connection failures stop it (cleanly,
/// with `aborted` set).
pub async fn execute_groups(
    pool: &PgPool,
    groups: &[Group],
    policy: &Policy,
    created_by: Uuid,
    max_groups: Option<usize>,
    opts: &ExecOptions,
) -> Executed {
    let store = PostgresStore::new(pool.clone());
    let mut out = Executed::default();
    let todo: Vec<&Group> =
        groups.iter().filter(|g| g.excluded.is_none() && !g.dups.is_empty()).take(max_groups.unwrap_or(usize::MAX)).collect();
    let mut consecutive_conn = 0u32;
    let mut attempted_any = false;
    for (gi, g) in todo.iter().enumerate() {
        let (mut merged, mut changed, mut failed) = (0usize, 0usize, 0usize);
        let mut abort = false;
        for d in &g.dups {
            if attempted_any && opts.pause_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(opts.pause_ms)).await;
            }
            attempted_any = true;
            match merge_dup_with_retry(pool, &store, g, d, policy, created_by, opts).await {
                Ok(DupOutcome::Merged(moved)) => {
                    merged += 1;
                    out.corals_moved += moved;
                    consecutive_conn = 0;
                }
                Ok(DupOutcome::Changed(_)) => {
                    changed += 1;
                    consecutive_conn = 0;
                }
                Err(f) => {
                    failed += 1;
                    out.failures.push(format!(
                        "graph={} {}:{:?} keep={} dup={}: {}",
                        g.graph_id, g.entity_type, g.norm_title, g.keep.node_id, d.node_id, f.msg
                    ));
                    if f.connection {
                        consecutive_conn += 1;
                        if consecutive_conn >= opts.max_consecutive_connection_failures {
                            out.aborted = Some(format!("{consecutive_conn} consecutive connection failures; last: {}", f.msg));
                            abort = true;
                        }
                    } else {
                        consecutive_conn = 0;
                    }
                }
            }
            if abort {
                break;
            }
        }
        out.reefs_merged += merged;
        out.reefs_changed += changed;
        out.reefs_failed += failed;
        if failed > 0 {
            out.groups_failed += 1;
        }
        if merged == g.dups.len() {
            out.groups_merged += 1;
        } else if merged > 0 {
            out.groups_partial += 1;
        } else if failed == 0 && changed > 0 {
            out.groups_changed += 1;
        }
        if abort {
            out.groups_not_attempted = todo.len() - gi - 1;
            break;
        }
    }
    out
}

async fn merge_dup_with_retry(
    pool: &PgPool,
    store: &PostgresStore,
    g: &Group,
    d: &Member,
    policy: &Policy,
    created_by: Uuid,
    opts: &ExecOptions,
) -> Result<DupOutcome, DupFailure> {
    let attempts = opts.max_attempts.max(1);
    let mut attempt = 1;
    loop {
        match merge_dup_once(pool, store, g, d, policy, created_by, opts).await {
            Ok(o) => return Ok(o),
            Err(e) => {
                let kind = classify(&e);
                if kind == ErrKind::Other || attempt >= attempts {
                    return Err(DupFailure {
                        msg: format!("{e} (attempt {attempt}/{attempts}, rolled back)"),
                        connection: kind == ErrKind::Connection,
                    });
                }
                let delay = opts.retry_backoff_ms.saturating_mul(1u64 << (attempt - 1).min(10));
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}

async fn merge_dup_once(
    pool: &PgPool,
    store: &PostgresStore,
    g: &Group,
    d: &Member,
    policy: &Policy,
    created_by: Uuid,
    opts: &ExecOptions,
) -> Result<DupOutcome, crate::store::StoreError> {
    let mut tx = pool.begin().await?;
    match merge_dup_tx(store, &mut tx, g, d, policy, created_by, opts).await {
        Ok(DupOutcome::Merged(moved)) => {
            tx.commit().await?;
            Ok(DupOutcome::Merged(moved))
        }
        Ok(changed) => {
            tx.rollback().await.ok();
            Ok(changed)
        }
        Err(e) => {
            tx.rollback().await.ok();
            Err(e)
        }
    }
}

/// One duplicate into the keeper, in the caller's transaction. Locks the
/// keeper and this duplicate only (stable order, like `merge_nodes_impl`),
/// re-checks the group key, the correction and negative-judgement guards, and
/// the identifier-conflict guard for the pair. The keeper's identifiers
/// include everything folded in by earlier merges (`merge_nodes_impl` unions
/// them), so a copy that conflicts with the keeper as it now stands is skipped
/// exactly as the group-level check did.
async fn merge_dup_tx(
    store: &PostgresStore,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    g: &Group,
    d: &Member,
    policy: &Policy,
    created_by: Uuid,
    opts: &ExecOptions,
) -> Result<DupOutcome, crate::store::StoreError> {
    sqlx::query("SELECT set_config('lock_timeout',$1,true), set_config('statement_timeout',$2,true)")
        .bind(format!("{}ms", opts.lock_timeout_ms))
        .bind(format!("{}ms", opts.statement_timeout_ms))
        .execute(&mut **tx)
        .await?;
    let mut ids = vec![d.node_id, g.keep.node_id];
    ids.sort();
    let rows: Vec<(Uuid, Uuid, String, String, String, Option<Uuid>)> = sqlx::query_as(&format!(
        "SELECT n.node_id,n.graph_id,{},lower(regexp_replace(btrim(n.title),'\\s+',' ','g')),n.scope,n.owner_id \
           FROM nodes n WHERE n.node_id=ANY($1) AND n.type='entity' ORDER BY n.node_id FOR UPDATE",
        policy.class_sql()
    ))
    .bind(&ids)
    .fetch_all(&mut **tx)
    .await?;
    let same = rows.len() == ids.len()
        && rows.iter().all(|r| r.1 == g.graph_id && r.2 == g.entity_type && r.3 == g.norm_title && r.4 == g.scope && r.5 == g.owner_id);
    if !same {
        return Ok(DupOutcome::Changed("group key changed or reef gone"));
    }
    let guarded: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reef_identity_corrections WHERE node_id=ANY($1)) \
             OR EXISTS(SELECT 1 FROM reef_judgement WHERE annulled_at IS NULL AND verdict='negative' AND graph_id=$2 \
                        AND ($4 OR source='human') \
                        AND ((node_a_id=$3 AND node_b_id=$5) OR (node_b_id=$3 AND node_a_id=$5)))",
    )
    .bind(&ids)
    .bind(g.graph_id)
    .bind(g.keep.node_id)
    .bind(policy.negative_blocks(false))
    .bind(d.node_id)
    .fetch_one(&mut **tx)
    .await?;
    if guarded {
        return Ok(DupOutcome::Changed("correction or negative judgement added"));
    }
    if crate::workers::entity_resolution::identities_conflict_tx(tx, &ids).await? {
        return Ok(DupOutcome::Changed("identifier conflict with the keeper"));
    }
    let judgement = store
        .write_judgement_scoped(
            tx,
            NewJudgement {
                graph_id: g.graph_id,
                node_a_id: g.keep.node_id,
                node_b_id: d.node_id,
                verdict: JudgementVerdict::Positive,
                source: JudgementSource::Human,
                confidence: None,
                model_id: None,
                prompt_version: None,
                evidence: json!({
                    "route": "ops_dedupe_reefs_cli",
                    "rule": policy.rule.as_str(),
                    "policy": "same graph, entity type, scope, owner and normalized title",
                    "normalized_title": g.norm_title,
                    "entity_type": g.entity_type,
                    "keep_kind": g.keep.kind,
                    "dup_kind": d.kind,
                    "cutoff": policy.cutoff.to_rfc3339(),
                    "incident": match policy.rule {
                        Rule::NamedEntities => "2026-10-19 v1 standing per-version reef minting",
                        Rule::SameTitle => "#1516 per-atom identity scope minted same-title non-person copies",
                    },
                }),
                created_by: Some(created_by),
                expires_at: None,
            },
        )
        .await?;
    let moved = store.merge_nodes_with_judgement_scoped(tx, g.keep.node_id, d.node_id, judgement).await?;
    Ok(DupOutcome::Merged(moved))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(graph: u8, et: &str, title: &str, minutes_after_cutoff: i64, is_self: bool) -> Member {
        let cutoff: DateTime<Utc> = DEFAULT_CUTOFF.parse().unwrap();
        Member {
            node_id: Uuid::new_v4(),
            graph_id: Uuid::from_bytes([graph; 16]),
            entity_type: et.into(),
            kind: et.into(),
            scope: "private".into(),
            owner_id: None,
            created_at: cutoff + chrono::Duration::minutes(minutes_after_cutoff),
            coral_count: 1,
            title: title.into(),
            norm_title: title.to_lowercase(),
            is_self,
            node_atoms: 1,
            semantic_links: 1,
            edges: 0,
            has_correction: false,
            promoted: false,
        }
    }

    /// A member as the same-title rule reads it: grouped by title identity
    /// class, carrying its own kind.
    fn classed(graph: u8, et: &str, title: &str, minutes_after_cutoff: i64, is_self: bool) -> Member {
        Member { entity_type: crate::store::title_identity_class(et).into(), ..member(graph, et, title, minutes_after_cutoff, is_self) }
    }

    #[test]
    fn errors_classify_into_retry_connection_and_fatal() {
        use crate::store::StoreError;
        let db = |e: sqlx::Error| StoreError::Db(Box::new(e));
        assert_eq!(classify(&db(sqlx::Error::PoolTimedOut)), ErrKind::Connection);
        assert_eq!(classify(&db(sqlx::Error::PoolClosed)), ErrKind::Connection);
        assert_eq!(classify(&db(sqlx::Error::Io(std::io::Error::other("reset")))), ErrKind::Connection);
        assert_eq!(classify(&db(sqlx::Error::RowNotFound)), ErrKind::Other);
        assert_eq!(classify(&StoreError::Conflict("merge identity support unavailable".into())), ErrKind::Other);
        assert_eq!(classify(&StoreError::Db("plain string error".into())), ErrKind::Other);
    }

    #[test]
    fn exec_options_bound_every_merge() {
        let o = ExecOptions::default();
        assert!(o.lock_timeout_ms > 0 && o.lock_timeout_ms <= 5_000, "yield to live traffic quickly");
        assert!(o.statement_timeout_ms > o.lock_timeout_ms && o.statement_timeout_ms <= 120_000);
        assert!(o.max_attempts >= 2 && o.max_consecutive_connection_failures >= 1 && o.pause_ms == 0);
    }

    #[test]
    fn stoplist_and_short_titles_are_excluded() {
        assert_eq!(title_exclusion("main"), Some(Exclusion::Stoplist));
        assert_eq!(title_exclusion("ab"), Some(Exclusion::TooShortOrNoLetters));
        assert_eq!(title_exclusion("1234"), Some(Exclusion::TooShortOrNoLetters));
        assert_eq!(title_exclusion("example-founder"), None);
        assert_eq!(title_exclusion("coralswarm/coralswarm"), None);
    }

    #[test]
    fn oldest_preexisting_is_canonical_and_only_new_copies_merge() {
        let old = member(1, "person", "example-founder", -60, false);
        let older = member(1, "person", "example-founder", -120, false);
        let new1 = member(1, "person", "example-founder", 10, false);
        let new2 = member(1, "person", "example-founder", 20, false);
        let groups = build_groups(vec![new2.clone(), old.clone(), new1.clone(), older.clone()], &HashSet::new(), &Identifiers::new(), &Policy::default());
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].keep.node_id, older.node_id);
        let dups: HashSet<Uuid> = groups[0].dups.iter().map(|d| d.node_id).collect();
        assert_eq!(dups, HashSet::from([new1.node_id, new2.node_id]));
        assert_eq!(groups[0].preexisting_left, 1);
        let all = build_groups(vec![new2, old.clone(), new1, older], &HashSet::new(), &Identifiers::new(), &Policy { include_preexisting: true, ..Policy::default() });
        assert!(all[0].dups.iter().any(|d| d.node_id == old.node_id));
    }

    #[test]
    fn self_reef_wins_and_cross_graph_never_groups() {
        let old = member(1, "person", "Alex", -60, false);
        let me = member(1, "person", "Alex", 30, true);
        let copy = member(1, "person", "Alex", 45, false);
        let other_graph = member(2, "person", "Alex", 40, false);
        let groups = build_groups(vec![old.clone(), copy.clone(), me.clone(), other_graph], &HashSet::new(), &Identifiers::new(), &Policy::default());
        assert_eq!(groups.len(), 1, "graph 2's lone copy forms no group");
        assert_eq!(groups[0].graph_id, Uuid::from_bytes([1; 16]));
        assert_eq!(groups[0].keep.node_id, me.node_id, "the self reef is canonical even when it is not the oldest");
        assert_eq!(groups[0].dups.iter().map(|d| d.node_id).collect::<Vec<_>>(), vec![copy.node_id]);
        assert_eq!(groups[0].preexisting_left, 1, "the older copy is pre-existing: scope A leaves it");
    }

    #[test]
    fn topics_and_negative_pairs_are_excluded() {
        let a = member(1, "topic", "postgres", -60, false);
        let b = member(1, "topic", "postgres", 5, false);
        let groups = build_groups(vec![a, b], &HashSet::new(), &Identifiers::new(), &Policy::default());
        assert_eq!(groups[0].excluded, Some(Exclusion::EntityType));
        let keep = member(1, "org", "composio", -60, false);
        let dup = member(1, "org", "composio", 5, false);
        let pair = if keep.node_id < dup.node_id { (keep.node_id, dup.node_id) } else { (dup.node_id, keep.node_id) };
        let groups = build_groups(vec![keep, dup], &HashSet::from([pair]), &Identifiers::new(), &Policy::default());
        assert!(groups[0].dups.is_empty());
        assert_eq!(groups[0].skipped[0].1, Exclusion::NegativeJudgement);
    }

    #[test]
    fn same_title_rule_folds_generic_non_person_titles_and_never_people() {
        let policy = Policy::same_title();
        let topic = [classed(1, "topic", "org", -900, false), classed(1, "topic", "org", -60, false), classed(1, "topic", "org", 30, false)];
        let short = [classed(1, "org", "r2", -60, false), classed(1, "org", "r2", 5, false)];
        let people = [classed(1, "person", "alex kim", -60, false), classed(1, "person", "alex kim", 5, false)];
        let mut all: Vec<Member> = topic.iter().chain(short.iter()).chain(people.iter()).cloned().collect();
        all.reverse();
        let groups = build_groups(all, &HashSet::new(), &Identifiers::new(), &policy);
        let by = |et: &str| groups.iter().find(|g| g.entity_type == et).unwrap();
        assert_eq!(by("topic_project").keep.node_id, topic[0].node_id, "oldest copy keeps, however old");
        assert_eq!(by("topic_project").dups.len(), 2, "a stoplisted title folds under the same-title rule");
        assert_eq!(by("org").dups.len(), 1, "a short title folds too");
        assert_eq!(by("person").excluded, Some(Exclusion::PersonTitle));
        assert!(by("person").dups.is_empty() || by("person").excluded.is_some());
        let r = report(&groups, &policy);
        assert_eq!((r.groups_to_merge, r.reefs_to_merge), (2, 3));
        assert_eq!(r.excluded.get(Exclusion::PersonTitle.as_str()), Some(&(1, 1)));
        // Even when a caller names person, the rule refuses it.
        let forced = Policy { entity_types: vec!["person".into()], ..Policy::same_title() };
        let groups = build_groups(people.to_vec(), &HashSet::new(), &Identifiers::new(), &forced);
        assert_eq!(groups[0].excluded, Some(Exclusion::PersonTitle));
        assert!(Policy::same_title().negative_blocks(true) && !Policy::same_title().negative_blocks(false));
        assert!(Policy::default().negative_blocks(false), "the old rule keeps every negative ruling");
    }

    #[test]
    fn same_title_rule_folds_topic_and_project_into_the_oldest_kind_but_not_org() {
        let policy = Policy::same_title();
        let old_project = classed(1, "project", "apollo", -600, false);
        let topic = classed(1, "topic", "apollo", -60, false);
        let project = classed(1, "project", "apollo", 30, false);
        let org = classed(1, "org", "apollo", -900, false);
        let org_copy = classed(1, "org", "apollo", 40, false);
        let lone_topic = classed(1, "topic", "zephyr", 10, false);
        let lone_project = classed(1, "project", "zephyr", 20, false);
        let all = vec![project.clone(), org_copy.clone(), topic.clone(), lone_project.clone(), org.clone(), old_project.clone(), lone_topic.clone()];
        let groups = build_groups(all, &HashSet::new(), &Identifiers::new(), &policy);
        assert_eq!(groups.len(), 3, "apollo topic+project, apollo org, zephyr topic+project");
        let apollo = groups.iter().find(|g| g.entity_type == "topic_project" && g.norm_title == "apollo").unwrap();
        assert_eq!(apollo.keep.node_id, old_project.node_id, "the oldest reef keeps, whatever its kind");
        assert_eq!(apollo.keep.kind, "project");
        assert_eq!(apollo.dups.iter().map(|d| d.node_id).collect::<HashSet<_>>(), HashSet::from([topic.node_id, project.node_id]));
        assert_eq!(apollo.kinds(), "project+topic");
        let org_group = groups.iter().find(|g| g.entity_type == "org").unwrap();
        assert_eq!((org_group.keep.node_id, org_group.dups.len()), (org.node_id, 1), "an org never folds into a topic or project");
        let zephyr = groups.iter().find(|g| g.norm_title == "zephyr").unwrap();
        assert_eq!((zephyr.keep.kind.as_str(), zephyr.dups[0].kind.as_str()), ("topic", "project"));
        let r = report(&groups, &policy);
        assert_eq!((r.groups_to_merge, r.reefs_to_merge), (3, 4));
        assert_eq!(r.by_entity_type.get("topic_project"), Some(&(2, 3)));
        assert_eq!(r.by_kinds.get("project+topic"), Some(&(2, 3)));
        assert_eq!(r.by_kinds.get("org"), Some(&(1, 1)));
        assert_eq!(r.by_keeper_kind.get("project"), Some(&(1, 2)));
        assert_eq!(r.by_keeper_kind.get("topic"), Some(&(1, 1)));
        // Naming one kind of the class selects the whole class.
        let topics_only = Policy { entity_types: vec!["topic".into()], ..Policy::same_title() };
        let groups = build_groups(vec![lone_topic, lone_project], &HashSet::new(), &Identifiers::new(), &topics_only);
        assert_eq!((groups[0].excluded, groups[0].dups.len()), (None, 1));
        let orgs_only = Policy { entity_types: vec!["org".into()], ..Policy::same_title() };
        let groups = build_groups(vec![topic, old_project], &HashSet::new(), &Identifiers::new(), &orgs_only);
        assert_eq!(groups[0].excluded, Some(Exclusion::EntityType));
    }

    #[test]
    fn conflicting_identifiers_never_fold_together() {
        let keep = member(1, "person", "alex kim", -60, false);
        let same_email = member(1, "person", "alex kim", 5, false);
        let no_email = member(1, "person", "alex kim", 6, false);
        let other_email = member(1, "person", "alex kim", 7, false);
        let ids = |pairs: &[(&Member, &str)]| -> Identifiers {
            pairs.iter().map(|(m, e)| (m.node_id, BTreeMap::from([("email".to_string(), HashSet::from([e.to_string()]))]))).collect()
        };
        let policy = Policy { include_preexisting: true, ..Policy::default() };
        let identifiers = ids(&[(&keep, "a@x.test"), (&same_email, "a@x.test"), (&other_email, "b@x.test")]);
        let groups = build_groups(vec![keep.clone(), same_email.clone(), no_email.clone(), other_email.clone()], &HashSet::new(), &identifiers, &policy);
        let dups: HashSet<Uuid> = groups[0].dups.iter().map(|d| d.node_id).collect();
        assert_eq!(dups, HashSet::from([same_email.node_id, no_email.node_id]));
        assert_eq!(groups[0].skipped.iter().map(|(m, e)| (m.node_id, *e)).collect::<Vec<_>>(), vec![(other_email.node_id, Exclusion::IdentifierConflict)]);
        // The keeper has none: the first copy's email is folded in, and a
        // later copy with another email is still two people.
        let identifiers = ids(&[(&same_email, "a@x.test"), (&other_email, "b@x.test")]);
        let groups = build_groups(vec![keep, same_email.clone(), no_email, other_email.clone()], &HashSet::new(), &identifiers, &policy);
        assert!(groups[0].dups.iter().any(|d| d.node_id == same_email.node_id));
        assert_eq!(groups[0].skipped.iter().map(|(m, e)| (m.node_id, *e)).collect::<Vec<_>>(), vec![(other_email.node_id, Exclusion::IdentifierConflict)]);
    }
}
