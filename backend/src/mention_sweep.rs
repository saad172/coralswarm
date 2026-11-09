//! Deferred resolution of the entity mentions the binder left `ambiguous`
//! (`semantic_atoms.entities[*]`, `status: "unresolved"`, `reason:
//! "ambiguous"`). Thin binary: `resolve_ambiguous_mentions`.
//!
//! The binder (`workers::source_atoms::bind_v2`) leaves a mention unlinked
//! when the door's ladder returns `NearMatch` (a consequential reef with a
//! similar title) and Jev gives no answer. Many of those near matches are
//! not identity competitors: the mention names something narrower than the
//! reef ("Atlas scale redesign" next to "Atlas"). The sweep re-asks the
//! same ladder against today's graph and decides with no model call:
//!
//! * `Resolved` binds, `Rejected` stays.
//! * `NearMatch` stays unresolved when any candidate competes for the
//!   mention's identity (see [`rival`]): people, orgs, the self reef, a
//!   same-token or broader name, a spelling variant. COR-36: an ambiguous
//!   identity is never decided by a title guess.
//! * Otherwise, and for `Unknown`, the binder's exact-title rule
//!   (`v2::exact_title_tx`) binds an existing reef. That rule refuses to
//!   choose among same-title copies holding conflicting identifiers, and
//!   for people among copies recorded as distinct (#1516: a non-person
//!   title within its identity class is one reef; topic and project share
//!   a class), and so does the sweep. With no same-title reef
//!   the sweep mints one, through the door's `mint_reef_scoped`, unless the
//!   title is generic ([`reef_dedupe::title_exclusion`]). Later mentions of
//!   that title bind to it, so there is at most one minted reef per title
//!   and entity type in a graph.
//!
//! A swept mention records `how` (`ladder`, `exact_title` or `minted`, the
//! binder's own labels) and `swept: RULE`. A minted reef carries
//! `body.born.worker = WORKER`. Re-running finds only the mentions that
//! still compete and writes nothing for them.
//!
//! Each atom is decided in the caller's transaction ([`sweep_atom_tx`]),
//! which takes the binder's lock order: source item, then the atom row.

use std::collections::{BTreeSet, HashMap};

use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

pub use crate::co_occurrence::Write;
use crate::reef_dedupe;
use crate::store::{NearCandidate, ReefRefCtx, ReefResolution, Store, StoreError};
use crate::workers::source_atoms::{map_node_type, v2};

pub const RULE: &str = "mention_sweep_v1";
pub const WORKER: &str = "mention_sweep";
pub const DEFAULT_PAGE: i64 = 200;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Counts {
    pub atoms: usize,
    pub atoms_changed: usize,
    pub mentions: usize,
    pub linked_ladder: usize,
    pub linked_exact_title: usize,
    pub minted: usize,
    pub kept_competing: usize,
    pub kept_same_title_distinct: usize,
    pub kept_generic_title: usize,
    pub kept_rejected: usize,
}

impl Counts {
    pub fn add(&mut self, o: &Counts) {
        self.atoms += o.atoms;
        self.atoms_changed += o.atoms_changed;
        self.mentions += o.mentions;
        self.linked_ladder += o.linked_ladder;
        self.linked_exact_title += o.linked_exact_title;
        self.minted += o.minted;
        self.kept_competing += o.kept_competing;
        self.kept_same_title_distinct += o.kept_same_title_distinct;
        self.kept_generic_title += o.kept_generic_title;
        self.kept_rejected += o.kept_rejected;
    }

    pub fn linked(&self) -> usize {
        self.linked_ladder + self.linked_exact_title + self.minted
    }
}

/// Where a swept mention lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Target {
    Reef(Uuid),
    /// Dry run: the reef a mint of this title would create (index into the
    /// run's planned titles).
    Planned(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keep {
    Competing(Rival),
    SameTitleDistinct,
    GenericTitle,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Link { target: Target, how: &'static str },
    Keep(Keep),
}

/// One decided mention, for the operator's projection file. No text.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Decision {
    pub graph_id: Uuid,
    pub semantic_atom_id: Uuid,
    pub source_type: String,
    pub extraction_version: String,
    pub outcome: &'static str,
    pub reef: Option<Uuid>,
    pub planned: Option<usize>,
}

/// Per-graph state across atoms: the titles a dry run would mint, keyed by
/// the binder's title key (`lower(trim)`) and entity type.
#[derive(Debug, Default)]
pub struct Run {
    planned: HashMap<(String, String), usize>,
    pub decisions: Option<Vec<Decision>>,
}

impl Run {
    pub fn recording() -> Self {
        Self { decisions: Some(Vec::new()), ..Self::default() }
    }
}

fn tokens(s: &str) -> BTreeSet<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

/// Why a near-match candidate competes for the mention's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rival {
    /// People and orgs: a partial or reordered name is the ambiguity
    /// COR-36 keeps unresolved.
    PersonOrOrg,
    SelfReef,
    /// A name holding every token of the mention: the mention may be its
    /// short form.
    Broader,
    /// A name whose tokens are all in the mention (a prefix, suffix or
    /// subset: `queue::could_not_do` for `jobs::queue::could_not_do`, a
    /// shorter path, "c2980b" for "c2980b CI"): the mention may be its long
    /// form. The v3.4.3 gate found 16–29% of mints for such near matches
    /// duplicated the existing reef, so they are never minted.
    Narrower,
    /// The title reached the door's near-match trigram bar without either
    /// name containing the other: a spelling variant.
    Variant,
    /// No name of the candidate relates to the mention by tokens or title
    /// similarity, so nothing shows it is a different thing.
    Unexplained,
}

impl Rival {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PersonOrOrg => "person_or_org",
            Self::SelfReef => "self_reef",
            Self::Broader => "broader",
            Self::Narrower => "narrower",
            Self::Variant => "variant",
            Self::Unexplained => "unexplained",
        }
    }
}

/// Whether, and why, a near-match candidate competes for the mention's
/// identity. A project or topic candidate competes when any of its names is
/// broader or narrower than the mention by tokens (one's tokens all in the
/// other's), or its title is a spelling variant. It does not compete when
/// its names only overlap the mention, like a sibling ("Atlas billing"
/// against "Atlas scale redesign").
pub fn rival(entity_type: &str, mention: &str, c: &NearCandidate, near_trgm: f64) -> Option<Rival> {
    if c.is_self {
        return Some(Rival::SelfReef);
    }
    if matches!(entity_type, "person" | "org") {
        return Some(Rival::PersonOrOrg);
    }
    let m = tokens(mention);
    let names = std::iter::once((c.title.as_str(), f64::from(c.similarity) >= near_trgm))
        .chain(c.aliases.iter().map(|a| (a.as_str(), false)));
    let mut related = false;
    for (name, similar) in names {
        let n = tokens(name);
        if !n.is_empty() && n.len() < m.len() && n.is_subset(&m) {
            return Some(Rival::Narrower);
        } else if !m.is_empty() && m.is_subset(&n) {
            return Some(Rival::Broader);
        } else if similar {
            return Some(Rival::Variant);
        } else if !n.is_disjoint(&m) {
            related = true;
        }
    }
    (!related).then_some(Rival::Unexplained)
}

/// How a `NearMatch` is decided, by the binder and the sweep alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NearDecision {
    /// A rival competes and no exact-title reef settles it: unresolved
    /// (unless Jev answers, in the binder).
    Keep(Rival),
    /// The reef with the mention's exact title and type.
    ExactTitle(Uuid),
    /// No rival: the caller may mint (subject to [`mint_refusal_tx`]).
    Open,
}

/// A person, org or self-reef rival keeps the mention unresolved outright
/// (COR-36). Otherwise an existing reef with the mention's exact title binds
/// (`v2::exact_title_tx`); failing that, any other rival keeps it
/// unresolved, and only a near match with no rival is open to a mint.
pub(crate) async fn decide_near_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    node_type: &str,
    mention: &str,
    entity_type: &str,
    candidates: &[NearCandidate],
) -> Result<NearDecision, sqlx::Error> {
    let near_trgm = crate::store::door_near_trgm();
    let rivals: Vec<Rival> = candidates.iter().filter_map(|c| rival(entity_type, mention, c, near_trgm)).collect();
    if let Some(r) = rivals.iter().find(|r| matches!(r, Rival::PersonOrOrg | Rival::SelfReef)) {
        return Ok(NearDecision::Keep(*r));
    }
    if let Some(node_id) = v2::exact_title_tx(tx, graph_id, node_type, mention, entity_type, version_id).await? {
        return Ok(NearDecision::ExactTitle(node_id));
    }
    Ok(rivals.first().map_or(NearDecision::Open, |r| NearDecision::Keep(*r)))
}

/// Atoms in `graph` with at least one ambiguous mention, from permitted
/// sources, after `after` in id order.
pub async fn pending_atoms_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    after: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT a.semantic_atom_id FROM semantic_atoms a \
           JOIN source_versions v ON v.graph_id = a.graph_id AND v.version_id = a.source_version_id \
           JOIN source_items s ON s.graph_id = v.graph_id AND s.source_item_id = v.source_item_id \
          WHERE a.graph_id = $1 AND s.access_state = 'permitted' \
            AND ($2::uuid IS NULL OR a.semantic_atom_id > $2) \
            AND EXISTS (SELECT 1 FROM jsonb_array_elements(a.entities) e \
                         WHERE e->>'status' = 'unresolved' AND e->>'reason' = 'ambiguous') \
          ORDER BY a.semantic_atom_id LIMIT $3",
    )
    .bind(graph_id)
    .bind(after)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
}

fn is_ambiguous(e: &Value) -> bool {
    e.get("status").and_then(Value::as_str) == Some("unresolved")
        && e.get("reason").and_then(Value::as_str) == Some("ambiguous")
}

/// Decide, and under `Apply` write, every ambiguous mention of one atom.
/// Dry runs must pass a READ ONLY transaction: nothing here locks or writes
/// unless `write` is `Apply`.
pub async fn sweep_atom_tx(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    semantic_atom_id: Uuid,
    write: Write,
    run: &mut Run,
) -> Result<Counts, StoreError> {
    let mut counts = Counts::default();
    let lock = if write == Write::Apply { " FOR UPDATE OF s" } else { "" };
    let parent: Option<(Uuid, String)> = sqlx::query_as(&format!(
        "SELECT s.source_item_id, v.source_type FROM semantic_atoms a \
           JOIN source_versions v ON v.graph_id = a.graph_id AND v.version_id = a.source_version_id \
           JOIN source_items s ON s.graph_id = v.graph_id AND s.source_item_id = v.source_item_id \
          WHERE a.graph_id = $1 AND a.semantic_atom_id = $2 AND s.access_state = 'permitted'{lock}"
    ))
    .bind(graph_id)
    .bind(semantic_atom_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((_, source_type)) = parent else { return Ok(counts) };
    let lock = if write == Write::Apply { " FOR UPDATE" } else { "" };
    let row: Option<(Uuid, Value, String, String)> = sqlx::query_as(&format!(
        "SELECT source_version_id, entities, model, extraction_version FROM semantic_atoms \
          WHERE graph_id = $1 AND semantic_atom_id = $2{lock}"
    ))
    .bind(graph_id)
    .bind(semantic_atom_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((version_id, mut entities, model, extraction_version)) = row else { return Ok(counts) };
    counts.atoms = 1;
    let mut links = BTreeSet::new();
    let Some(items) = entities.as_array_mut() else { return Ok(counts) };
    for e in items.iter_mut() {
        if !is_ambiguous(e) {
            continue;
        }
        let (Some(mention), Some(entity_type)) = (
            e.get("mention").and_then(Value::as_str).map(str::to_string),
            e.get("type").and_then(Value::as_str).map(str::to_string),
        ) else {
            continue;
        };
        counts.mentions += 1;
        let node_type = map_node_type(&entity_type);
        let ctx = ReefRefCtx {
            atom_id: Some(version_id),
            entity_type: Some(entity_type.clone()),
            worker: WORKER,
            model: Some(model.clone()),
            surface_form: Some(mention.clone()),
            ..Default::default()
        };
        let resolution = store.resolve_reef_ref_scoped(tx, graph_id, node_type, &mention, &ctx).await?;
        let outcome = match resolution {
            ReefResolution::Resolved { node_id, .. } => Outcome::Link { target: Target::Reef(node_id), how: "ladder" },
            ReefResolution::Rejected(_) => Outcome::Keep(Keep::Rejected),
            ReefResolution::NearMatch { candidates } => {
                match decide_near_tx(tx, graph_id, version_id, node_type, &mention, &entity_type, &candidates).await? {
                    NearDecision::Keep(r) => Outcome::Keep(Keep::Competing(r)),
                    NearDecision::ExactTitle(node_id) => Outcome::Link { target: Target::Reef(node_id), how: "exact_title" },
                    NearDecision::Open => {
                        by_title(store, tx, graph_id, version_id, node_type, &mention, &entity_type, &ctx, write, run).await?
                    }
                }
            }
            ReefResolution::Unknown => {
                by_title(store, tx, graph_id, version_id, node_type, &mention, &entity_type, &ctx, write, run).await?
            }
        };
        match outcome {
            Outcome::Keep(k) => match k {
                Keep::Competing(_) => counts.kept_competing += 1,
                Keep::SameTitleDistinct => counts.kept_same_title_distinct += 1,
                Keep::GenericTitle => counts.kept_generic_title += 1,
                Keep::Rejected => counts.kept_rejected += 1,
            },
            Outcome::Link { target, how } => {
                match how {
                    "ladder" => counts.linked_ladder += 1,
                    "minted" => counts.minted += 1,
                    _ => counts.linked_exact_title += 1,
                }
                if let Target::Reef(node_id) = target {
                    if write == Write::Apply {
                        store.record_entity_type_hint_scoped(tx, node_id, &entity_type).await?;
                    }
                    links.insert(node_id);
                }
                e["status"] = json!("linked");
                e["reason"] = Value::Null;
                e["how"] = json!(how);
                e["swept"] = json!(RULE);
            }
        }
        if let Some(log) = run.decisions.as_mut() {
            let (outcome_label, reef, planned) = match outcome {
                Outcome::Link { target: Target::Reef(n), how } => (how, Some(n), None),
                Outcome::Link { target: Target::Planned(i), how } => (how, None, Some(i)),
                Outcome::Keep(Keep::Competing(r)) => (r.as_str(), None, None),
                Outcome::Keep(Keep::SameTitleDistinct) => ("kept_same_title_distinct", None, None),
                Outcome::Keep(Keep::GenericTitle) => ("kept_generic_title", None, None),
                Outcome::Keep(Keep::Rejected) => ("kept_rejected", None, None),
            };
            log.push(Decision {
                graph_id,
                semantic_atom_id,
                source_type: source_type.clone(),
                extraction_version: extraction_version.clone(),
                outcome: outcome_label,
                reef,
                planned,
            });
        }
    }
    let changed = counts.linked() > 0;
    counts.atoms_changed = usize::from(changed);
    if write == Write::DryRun || !changed {
        return Ok(counts);
    }
    for node_id in &links {
        sqlx::query("INSERT INTO node_semantic_atoms(node_id, semantic_atom_id, graph_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
            .bind(node_id)
            .bind(semantic_atom_id)
            .bind(graph_id)
            .execute(&mut **tx)
            .await?;
    }
    if crate::node_atoms_bridge::enabled() {
        let chunks = crate::node_atoms_bridge::version_chunks_tx(tx, graph_id, version_id).await?;
        if !chunks.is_empty() {
            let bridge: Vec<(Uuid, &[Uuid])> = links.iter().map(|n| (*n, chunks.as_slice())).collect();
            crate::node_atoms_bridge::link_reefs_tx(store, tx, &bridge).await?;
        }
    }
    sqlx::query("UPDATE semantic_atoms SET entities = $3 WHERE graph_id = $1 AND semantic_atom_id = $2")
        .bind(graph_id)
        .bind(semantic_atom_id)
        .bind(&entities)
        .execute(&mut **tx)
        .await?;
    Ok(counts)
}

/// The binder's `Unknown` branch without Jev: an exact-title reef, else one
/// mint per title.
#[allow(clippy::too_many_arguments)]
/// Why a mention with no exact-title reef is not minted a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MintRefusal {
    /// Same-title reefs exist that `exact_title_tx` would not choose among.
    SameTitleDistinct,
    /// The title is generic ([`reef_dedupe::title_exclusion`]).
    GenericTitle,
}

/// Whether a mention that `exact_title_tx` did not bind may be minted as a
/// new reef. Shared by the sweep and the binder's non-competing near match.
pub(crate) async fn mint_refusal_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    node_type: &str,
    mention: &str,
    entity_type: &str,
) -> Result<Option<MintRefusal>, sqlx::Error> {
    // A non-person mention is refused when its title identity class is held
    // (#1516: a topic and a project of one title are one reef), a person
    // when a reef of exactly its type is.
    let same_title: bool = match crate::store::door_title_identity_class(Some(entity_type)).filter(|_| node_type == "entity") {
        Some(class) => sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM nodes n WHERE n.graph_id = $1 AND n.type = $2 AND lower(n.title) = lower($3) \
               AND {} = $4)",
            crate::store::NODE_TITLE_IDENTITY_CLASS_SQL
        ))
        .bind(graph_id)
        .bind(node_type)
        .bind(mention.trim())
        .bind(class)
        .fetch_one(&mut **tx)
        .await?,
        None => sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM nodes n WHERE n.graph_id = $1 AND n.type = $2 AND lower(n.title) = lower($3) \
               AND COALESCE(n.entity_type, n.body->>'entity_type') = $4)",
        )
        .bind(graph_id)
        .bind(node_type)
        .bind(mention.trim())
        .bind(entity_type)
        .fetch_one(&mut **tx)
        .await?,
    };
    if same_title {
        return Ok(Some(MintRefusal::SameTitleDistinct));
    }
    let norm = mention.trim().to_lowercase().split_whitespace().collect::<Vec<_>>().join(" ");
    Ok(reef_dedupe::title_exclusion(&norm).is_some().then_some(MintRefusal::GenericTitle))
}

async fn by_title(
    store: &dyn Store,
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    version_id: Uuid,
    node_type: &str,
    mention: &str,
    entity_type: &str,
    ctx: &ReefRefCtx,
    write: Write,
    run: &mut Run,
) -> Result<Outcome, StoreError> {
    if let Some(node_id) = v2::exact_title_tx(tx, graph_id, node_type, mention, entity_type, version_id).await? {
        return Ok(Outcome::Link { target: Target::Reef(node_id), how: "exact_title" });
    }
    match mint_refusal_tx(tx, graph_id, node_type, mention, entity_type).await? {
        Some(MintRefusal::SameTitleDistinct) => return Ok(Outcome::Keep(Keep::SameTitleDistinct)),
        Some(MintRefusal::GenericTitle) => return Ok(Outcome::Keep(Keep::GenericTitle)),
        None => {}
    }
    // A dry run plans one mint per title and identity class, as the mint does.
    let key = (
        mention.trim().to_lowercase(),
        crate::store::door_title_identity_class(Some(entity_type)).map_or_else(|| entity_type.to_string(), str::to_string),
    );
    if write == Write::DryRun {
        let next = run.planned.len();
        return Ok(match run.planned.get(&key) {
            Some(i) => Outcome::Link { target: Target::Planned(*i), how: "exact_title" },
            None => {
                run.planned.insert(key, next);
                Outcome::Link { target: Target::Planned(next), how: "minted" }
            }
        });
    }
    let node_id = store.mint_reef_scoped(tx, graph_id, node_type, mention.trim(), ctx).await?;
    Ok(Outcome::Link { target: Target::Reef(node_id), how: "minted" })
}

/// Sweep one graph in pages, one transaction per atom under `Apply` and one
/// READ ONLY, rolled-back transaction per page under `DryRun`. `begin`
/// opens a transaction in the caller's scope. After any write, one
/// debounced entity-resolution pass is enqueued for the graph, as the
/// binder does.
pub async fn sweep_graph<F, Fut>(
    store: &dyn Store,
    begin: F,
    graph_id: Uuid,
    write: Write,
    max_atoms: Option<usize>,
    run: &mut Run,
) -> Result<(Counts, Vec<(Uuid, String)>), StoreError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Transaction<'static, Postgres>, StoreError>>,
{
    let mut total = Counts::default();
    let mut failed = Vec::new();
    let mut after = None;
    let mut seen = 0usize;
    loop {
        let page = match max_atoms {
            Some(m) if seen >= m => break,
            Some(m) => DEFAULT_PAGE.min((m - seen) as i64),
            None => DEFAULT_PAGE,
        };
        let mut tx = begin().await?;
        if write == Write::DryRun {
            sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
        }
        let ids = pending_atoms_tx(&mut tx, graph_id, after, page).await?;
        if ids.is_empty() {
            tx.rollback().await?;
            break;
        }
        seen += ids.len();
        after = ids.last().copied();
        if write == Write::DryRun {
            for id in &ids {
                total.add(&sweep_atom_tx(store, &mut tx, graph_id, *id, write, run).await?);
            }
            tx.rollback().await?;
            continue;
        }
        tx.rollback().await?;
        for id in ids {
            let one = async {
                let mut tx = begin().await?;
                let c = sweep_atom_tx(store, &mut tx, graph_id, id, write, run).await?;
                tx.commit().await?;
                Ok::<_, StoreError>(c)
            }
            .await;
            match one {
                Ok(c) => total.add(&c),
                Err(e) => failed.push((id, e.to_string())),
            }
        }
    }
    if write == Write::Apply && total.linked() > 0 {
        let mut tx = begin().await?;
        let account: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM graphs WHERE id = $1")
            .bind(graph_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
        if let Some(account) = account {
            crate::workers::entity_resolution::enqueue_after_binding(&mut *tx, account, graph_id).await?;
        }
        tx.commit().await?;
    }
    Ok((total, failed))
}

#[derive(Debug, Default)]
pub struct BackfillOutcome {
    pub total: Counts,
    pub graphs: usize,
    pub failed: Vec<(Uuid, String)>,
    pub decisions: Vec<Decision>,
}

/// Operator entry: every graph (or one) of a database, as the pool's role.
/// The bin requires the database owner.
pub async fn backfill(
    pool: &PgPool,
    graph_id: Option<Uuid>,
    write: Write,
    max_atoms: Option<usize>,
    record: bool,
) -> Result<BackfillOutcome, sqlx::Error> {
    let graphs: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM graphs WHERE $1::uuid IS NULL OR id = $1 ORDER BY id")
        .bind(graph_id)
        .fetch_all(pool)
        .await?;
    let store = crate::store::PostgresStore::new(pool.clone());
    let mut out = BackfillOutcome { graphs: graphs.len(), ..BackfillOutcome::default() };
    for graph in graphs {
        let mut run = if record { Run::recording() } else { Run::default() };
        let begin = || async {
            let mut tx = pool.begin().await?;
            sqlx::query("SET LOCAL statement_timeout = '120s'").execute(&mut *tx).await?;
            Ok::<_, StoreError>(tx)
        };
        match sweep_graph(&store, begin, graph, write, max_atoms, &mut run).await {
            Ok((c, failed)) => {
                out.total.add(&c);
                out.failed.extend(failed);
            }
            Err(e) => out.failed.push((graph, e.to_string())),
        }
        out.decisions.extend(run.decisions.unwrap_or_default());
    }
    Ok(out)
}

// ── Repair: sweep mints that duplicate an existing reef ─────────────────────

/// One sweep-minted reef and the existing reef it duplicates.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DuplicateMint {
    pub graph_id: Uuid,
    /// The sweep-minted reef, merged away.
    pub dup: Uuid,
    /// The existing reef that survives.
    pub keep: Uuid,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RepairReport {
    /// Sweep-minted project/topic reefs examined.
    pub examined: usize,
    /// Pairs found (merged under `Apply`).
    pub pairs: Vec<DuplicateMint>,
    /// No existing reef relates by tokens: a genuinely new name.
    pub no_match: usize,
    /// More than one existing reef relates: not decided here.
    pub ambiguous: usize,
    /// A human or system negative judgement separates the pair.
    pub judged_distinct: usize,
    pub failed: Vec<(Uuid, String)>,
}

/// Whether two reef titles name the same code entity: equal ignoring case,
/// or one is the other with a leading namespace or path qualification
/// (`jobs::queue::could_not_do` and `queue::could_not_do`,
/// `backend/src/x.rs` and `src/x.rs`). The shorter must itself be code-ish
/// (it holds `::` or `/`, or ends in a file extension) and neither may
/// contain whitespace, so ordinary words never make two names one: "Google
/// Tasks" is not "Google", "MobileNav.tsx" is not "MobileNav", "LLM call" is
/// not "LLM". A shared token is a reason not to mint, never a reason to
/// merge.
pub fn same_entity_title(a: &str, b: &str) -> bool {
    let (a, b) = (a.trim().to_lowercase(), b.trim().to_lowercase());
    if a.is_empty() || b.is_empty() || a.contains(char::is_whitespace) || b.contains(char::is_whitespace) {
        return false;
    }
    if a == b {
        return true;
    }
    let (short, long) = if a.len() < b.len() { (a, b) } else { (b, a) };
    if ubiquitous_basename(&short) {
        return false;
    }
    let has_extension = short.rsplit_once('.').is_some_and(|(stem, ext)| {
        !stem.is_empty() && (1..=5).contains(&ext.len()) && ext.chars().all(|c| c.is_ascii_alphanumeric())
    });
    let code_ish = short.contains("::") || short.contains('/') || has_extension;
    code_ish
        && long.strip_suffix(short.as_str()).is_some_and(|prefix| {
            (prefix.ends_with("::") && prefix.len() > 2) || (prefix.ends_with('/') && prefix.len() > 1)
        })
}

/// Basenames nearly every repository has many of: `widgets/README.md` is
/// not "the" `README.md`, so qualification never makes them one entity.
const UBIQUITOUS_FILES: &[&str] = &[
    "mod.rs", "lib.rs", "main.rs", "build.rs", "tests.rs", "__init__.py", "__main__.py", "main.py", "app.py",
    "setup.py", "conftest.py", "manage.py", "settings.py", "urls.py", "models.py", "views.py", "utils.py",
    "package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "cargo.toml", "cargo.lock",
    "tsconfig.json", "pyproject.toml", "requirements.txt", "setup.cfg", "go.mod", "go.sum", "main.go",
    "dockerfile", "makefile", "docker-compose.yml", "docker-compose.yaml", "fly.toml", "vercel.json",
    ".gitignore", ".gitattributes", ".dockerignore", ".editorconfig", ".npmrc", ".nvmrc", "codeowners",
    "page.tsx", "page.ts", "page.jsx", "page.js", "layout.tsx", "layout.ts", "route.ts", "route.js",
    "loading.tsx", "error.tsx", "not-found.tsx", "app.tsx", "app.jsx", "app.ts", "app.js", "types.ts",
    "utils.ts", "utils.js", "constants.ts", "config.ts", "config.js", "styles.css", "globals.css",
];
/// Prefixes of the same kind (`README.md`, `index.ts`, `LICENSE-MIT`,
/// `.env.local`, `vite.config.ts`).
const UBIQUITOUS_PREFIXES: &[&str] = &[
    "readme", "index.", "license", "licence", "changelog", "contributing", "security.", "code_of_conduct",
    "notice", "authors", ".env", ".eslintrc", ".prettierrc", ".babelrc", "vite.config.", "next.config.",
    "jest.config.", "vitest.config.", "babel.config.", "webpack.config.", "tailwind.config.", "postcss.config.",
    "eslint.config.", "playwright.config.",
];

/// The name's last path or namespace segment is a file every repository
/// has many of ([`UBIQUITOUS_FILES`], [`UBIQUITOUS_PREFIXES`]).
fn ubiquitous_basename(name: &str) -> bool {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let base = base.rsplit("::").next().unwrap_or(base);
    UBIQUITOUS_FILES.contains(&base) || UBIQUITOUS_PREFIXES.iter().any(|p| base.starts_with(p))
}

/// How many entity reefs in the graph carry `short` as a path- or
/// namespace-qualified tail (`…/ocean_health.rs`, `…::could_not_do`).
async fn qualified_forms_tx(pool: &PgPool, graph_id: Uuid, short: &str) -> Result<i64, sqlx::Error> {
    let escaped = short.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
    sqlx::query_scalar(
        "SELECT count(DISTINCT lower(n.title)) FROM nodes n WHERE n.graph_id = $1 AND n.type = 'entity' \
           AND (lower(n.title) LIKE '%/' || $2 OR lower(n.title) LIKE '%::' || $2)",
    )
    .bind(graph_id)
    .bind(escaped)
    .fetch_one(pool)
    .await
}

/// Find, and under `Apply` merge, the reefs this sweep minted
/// (`body.born.worker = WORKER`) that name the same code entity as an
/// existing reef ([`same_entity_title`]: a namespace- or path-qualified form,
/// or a case variant). After the dry run on prod found 19 parent/child
/// pairs a token-subset rule would have merged ("Google Tasks" into
/// "Google"), nothing looser qualifies. Only project and topic reefs;
/// people and orgs are never merged by name (COR-36). The keeper must be
/// the only existing reef of the same type and entity type that qualifies,
/// must not be sweep-minted itself, and no
/// active negative `reef_judgement` may separate the pair. Each merge goes
/// through `Store::merge_nodes_scoped` in its own transaction, so it is
/// recorded in `node_merges` and undoable. `created` bounds the mint time.
pub async fn repair_minted_duplicates(
    pool: &PgPool,
    graph_id: Option<Uuid>,
    created: (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>),
    write: Write,
) -> Result<RepairReport, sqlx::Error> {
    let minted: Vec<(Uuid, Uuid, String, String, String)> = sqlx::query_as(
        "SELECT n.node_id, n.graph_id, n.type, COALESCE(n.entity_type, n.body->>'entity_type'), n.title FROM nodes n \
          WHERE n.body->'born'->>'worker' = $1 AND ($2::uuid IS NULL OR n.graph_id = $2) \
            AND ($3::timestamptz IS NULL OR n.created_at >= $3) AND ($4::timestamptz IS NULL OR n.created_at < $4) \
            AND COALESCE(n.entity_type, n.body->>'entity_type') IN ('project', 'topic') \
          ORDER BY n.graph_id, n.created_at, n.node_id",
    )
    .bind(WORKER)
    .bind(graph_id)
    .bind(created.0)
    .bind(created.1)
    .fetch_all(pool)
    .await?;
    let mut report = RepairReport::default();
    let mut existing: HashMap<(Uuid, String, String), Vec<(Uuid, String)>> = HashMap::new();
    for (dup, graph, node_type, entity_type, title) in minted {
        report.examined += 1;
        let key = (graph, node_type.clone(), entity_type.clone());
        if !existing.contains_key(&key) {
            let rows: Vec<(Uuid, String)> = sqlx::query_as(
                "SELECT n.node_id, n.title FROM nodes n WHERE n.graph_id = $1 AND n.type = $2 \
                   AND COALESCE(n.entity_type, n.body->>'entity_type') = $3 \
                   AND n.body->'born'->>'worker' IS DISTINCT FROM $4 \
                   AND n.body->>'lifecycle' IS DISTINCT FROM 'evidence_retracted'",
            )
            .bind(graph)
            .bind(&node_type)
            .bind(&entity_type)
            .bind(WORKER)
            .fetch_all(pool)
            .await?;
            existing.insert(key.clone(), rows);
        }
        let related: Vec<Uuid> = existing[&key]
            .iter()
            .filter(|(id, t)| *id != dup && same_entity_title(t, &title))
            .map(|(id, _)| *id)
            .collect();
        let keep = match related.as_slice() {
            [] => {
                report.no_match += 1;
                continue;
            }
            [one] => *one,
            _ => {
                report.ambiguous += 1;
                continue;
            }
        };
        // A qualified pair names one file only when the graph knows one path
        // ending in the bare name: two `…/ocean_health.rs` make it ambiguous.
        let keep_title = &existing[&key].iter().find(|(id, _)| *id == keep).expect("keeper listed").1;
        let (a, b) = (title.trim().to_lowercase(), keep_title.trim().to_lowercase());
        if a != b {
            let short = if a.len() < b.len() { a } else { b };
            if qualified_forms_tx(pool, graph, &short).await? > 1 {
                report.ambiguous += 1;
                continue;
            }
        }
        let distinct: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM reef_judgement j WHERE j.graph_id = $1 AND j.verdict = 'negative' AND j.annulled_at IS NULL \
               AND ((j.node_a_id = $2 AND j.node_b_id = $3) OR (j.node_a_id = $3 AND j.node_b_id = $2)))",
        )
        .bind(graph)
        .bind(keep)
        .bind(dup)
        .fetch_one(pool)
        .await?;
        if distinct {
            report.judged_distinct += 1;
            continue;
        }
        if write == Write::Apply {
            let store = crate::store::PostgresStore::new(pool.clone());
            let merged = async {
                let mut tx = pool.begin().await?;
                sqlx::query("SET LOCAL statement_timeout = '120s'").execute(&mut *tx).await?;
                store.merge_nodes_scoped(&mut tx, keep, dup).await?;
                tx.commit().await?;
                Ok::<_, StoreError>(())
            }
            .await;
            if let Err(e) = merged {
                report.failed.push((dup, e.to_string()));
                continue;
            }
        }
        report.pairs.push(DuplicateMint { graph_id: graph, dup, keep });
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(title: &str, aliases: &[&str], similarity: f32) -> NearCandidate {
        NearCandidate {
            node_id: Uuid::nil(),
            title: title.into(),
            aliases: aliases.iter().map(|s| s.to_string()).collect(),
            coral_count: 100,
            degree: 0,
            is_self: false,
            similarity,
        }
    }

    #[test]
    fn sibling_project_and_topic_candidates_do_not_compete() {
        assert_eq!(rival("project", "Atlas scale redesign", &cand("Atlas billing", &[], 0.4), 0.5), None, "a sibling");
        assert_eq!(rival("topic", "deploy pipeline config", &cand("pipeline audit", &["unrelated deploy"], 0.3), 0.5), None, "overlap only");
    }

    /// The prod dry run's wrong merges: a specific thing is not its generic
    /// parent. Only namespace/path qualification and case make one name.
    #[test]
    fn only_qualified_code_names_are_the_same_entity() {
        for (a, b) in [
            ("Google Tasks", "Google"), ("from_email_name", "email"), ("managed_embedding_held", "embedding"),
            ("superseded_by_newer_version", "superseded"), ("voter message", "message"), ("MobileNav.tsx", "MobileNav"),
            ("Comal VoterProfiles", "Comal"), ("LLM call", "LLM"), ("OAuth writer", "OAuth"), ("macOS stopgap", "macOS"),
            ("pinned graph ids", "Graph"), ("c2980b CI", "c2980b"), ("search_path outage", "2026-10-14 search_path outage"),
            ("xjobs::queue", "jobs::queue"), ("backend/src/x.rs", "rc/x.rs"), ("src/x.rs", "src/y.rs"), ("a.rs", "b.a.rs"),
            // Ubiquitous basenames: a qualified one is never "the" bare one.
            ("widgets/README.md", "README.md"), ("docs/readme", "readme"), ("backend/src/lib.rs", "lib.rs"),
            ("src/workers/mod.rs", "workers/mod.rs"), ("web/package.json", "package.json"), ("web/app/page.tsx", "page.tsx"),
            ("backend/Cargo.toml", "Cargo.toml"), ("web/src/index.ts", "index.ts"), ("app/LICENSE-MIT", "LICENSE-MIT"),
            ("backend/Dockerfile", "Dockerfile"), ("pkg/__init__.py", "__init__.py"), ("web/.env.local", ".env.local"),
        ] {
            assert!(!same_entity_title(a, b) && !same_entity_title(b, a), "{a} / {b}");
        }
        for (a, b) in [
            ("jobs::queue::could_not_do", "queue::could_not_do"), ("backend/src/x.rs", "src/x.rs"),
            ("MobileNav.tsx", "mobilenav.tsx"), ("crate::store::Store", "store::Store"),
            ("docs/design/capture/Policy.dc.html", "Policy.dc.html"), ("backend/src/ocean_health.rs", "ocean_health.rs"),
            ("README.md", "readme.md"),
        ] {
            assert!(same_entity_title(a, b) && same_entity_title(b, a), "{a} / {b}");
        }
    }

    #[test]
    fn identity_competitors_stay_unresolved() {
        assert_eq!(rival("person", "Bea Park", &cand("Bea", &[], 0.4), 0.5), Some(Rival::PersonOrOrg));
        assert_eq!(rival("org", "Acme Labs", &cand("Acme", &[], 0.4), 0.5), Some(Rival::PersonOrOrg));
        assert_eq!(rival("project", "Atlas", &cand("Atlas redesign", &[], 0.6), 0.5), Some(Rival::Broader));
        assert_eq!(rival("topic", "redesign atlas", &cand("Atlas redesign", &[], 0.9), 0.5), Some(Rival::Broader), "same tokens");
        assert_eq!(rival("topic", "coralswarm", &cand("coral swarm", &[], 0.6), 0.5), Some(Rival::Variant));
        assert_eq!(rival("project", "Atlas scale redesign", &cand("Atlas", &["scale redesign atlas v2"], 0.4), 0.5), Some(Rival::Narrower), "a narrower title and a broader alias: a rival either way");
        assert_eq!(rival("project", "Atlas scale", &cand("Zebra", &[], 0.2), 0.5), Some(Rival::Unexplained));
        // v3.4.3 gate: a shorter or longer form of the same name competes
        // (it would have minted a duplicate).
        assert_eq!(rival("project", "Atlas scale redesign", &cand("Atlas", &[], 0.4), 0.5), Some(Rival::Narrower));
        assert_eq!(rival("topic", "jobs::queue::could_not_do", &cand("queue::could_not_do", &[], 0.7), 0.5), Some(Rival::Narrower));
        assert_eq!(rival("project", "backend/src/jobs/queue.rs", &cand("jobs/queue.rs", &[], 0.6), 0.5), Some(Rival::Narrower));
        assert_eq!(rival("topic", "c2980b CI", &cand("c2980b", &[], 0.5), 0.5), Some(Rival::Narrower));
        assert_eq!(rival("topic", "search_path outage", &cand("2026-10-14 search_path outage", &[], 0.6), 0.5), Some(Rival::Broader));
        assert_eq!(rival("project", "Atlas scale redesign", &cand("Atlas billing", &["atlas"], 0.4), 0.5), Some(Rival::Narrower), "a narrower alias");
        let mut me = cand("Atlas", &[], 0.4);
        me.is_self = true;
        assert_eq!(rival("topic", "Atlas scale redesign", &me, 0.5), Some(Rival::SelfReef));
    }
}
