//! Pure source queries shared by interactive Store reads and page model workers.
//! Callers own authorization and the member-scoped read-only snapshot. No pools.
use super::*;

pub async fn list_nodes_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    node_type: Option<&str>,
    page_size: i64,
    offset: i64,
) -> Result<Vec<Node>, StoreError> {
    // COR-113: read only a window of candidate reefs instead of counting
    // every permitted link in the graph. `settle_page` explains why the
    // window gives the canonical page. Deep pages, where the window would
    // cover most of the graph, use the single full-admission statement.
    let page_size = page_size.max(0);
    let offset = offset.max(0);
    let need = offset.saturating_add(page_size);
    sqlx::query(workspace_reads::JIT_OFF_SQL)
        .execute(&mut **tx)
        .await?;
    let mut window = workspace_reads::first_window(need);
    while window <= workspace_reads::CANDIDATE_WINDOW_CEILING {
        let rows: Vec<workspace_reads::CandidateRow> =
            sqlx::query_as(workspace_reads::CANDIDATE_PAGE_SQL)
                .bind(graph_id)
                .bind(node_type)
                .bind(window)
                .fetch_all(&mut **tx)
                .await?;
        let candidates: Vec<workspace_reads::Candidate> =
            rows.into_iter().map(Into::into).collect();
        if workspace_reads::bound_drifted(&candidates) {
            // The window order is only canonical while `coral_count`
            // bounds the permitted count. A visited reef proves it does
            // not, so take the exact statement instead.
            tracing::warn!(%graph_id, "list_nodes_scoped: coral_count below permitted link count; using full admission");
            break;
        }
        if let Some(settled) = workspace_reads::settle_page(&candidates, window, need) {
            let page: Vec<workspace_reads::Candidate> = settled
                .into_iter()
                .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                .collect();
            if page.is_empty() {
                return Ok(Vec::new());
            }
            let ids: Vec<Uuid> = page.iter().map(|c| c.node_id).collect();
            let graphs = vec![graph_id; ids.len()];
            let titles: std::collections::HashMap<Uuid, String> =
                crate::services::journal_activities::project_node_titles(tx, &graphs, &ids)
                    .await?
                    .into_iter()
                    .map(|((_, id), title)| (id, title))
                    .collect();
            return Ok(page
                .into_iter()
                .map(|c| Node {
                    title: titles.get(&c.node_id).cloned().unwrap_or_default(),
                    node_id: c.node_id,
                    node_type: c.node_type,
                    scope: c.scope,
                    created_at: c.created_at,
                    updated_at: c.updated_at,
                    atom_count: c.atom_count,
                })
                .collect());
        }
        window = window.saturating_mul(4);
    }
    let mut nodes = list_nodes_full_admission(tx, graph_id, node_type, page_size, offset).await?;
    let ids: Vec<Uuid> = nodes.iter().map(|n| n.node_id).collect();
    let graphs = vec![graph_id; ids.len()];
    let titles =
        crate::services::journal_activities::project_node_titles(tx, &graphs, &ids).await?;
    for node in &mut nodes {
        node.title = titles
            .get(&(graph_id, node.node_id))
            .cloned()
            .unwrap_or_default();
    }
    Ok(nodes)
}

pub async fn list_atoms_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
) -> Result<Vec<Atom>, StoreError> {
    let rows: Vec<(
            Uuid,
            String,
            String,
            DateTime<Utc>,
            DateTime<Utc>,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT atom_id, source_type, source_id, timestamp, created_at, channel_id, channel_name, author_name \
             FROM (SELECT * FROM atoms WHERE library_atom_permitted(graph_id,atom_id)) atoms \
             WHERE graph_id = $1 \
             ORDER BY created_at DESC \
             LIMIT 100",
        )
        .bind(graph_id)
        .fetch_all(&mut **tx)
        .await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                atom_id,
                source_type,
                source_id,
                timestamp,
                created_at,
                channel_id,
                channel_name,
                author_name,
            )| {
                Atom {
                    atom_id,
                    source_type,
                    source_id,
                    timestamp,
                    created_at,
                    channel_id,
                    channel_name,
                    author_name,
                }
            },
        )
        .collect())
}

pub async fn expand_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    seed_atom_ids: &[Uuid],
    query_embedding: Option<&[f32]>,
) -> Result<Vec<NodeWithAtoms>, StoreError> {
    use std::collections::HashMap;

    // Short-circuit: nothing to expand.
    if seed_atom_ids.is_empty() {
        return Ok(vec![]);
    }

    // ── Query 1: find all distinct nodes any seed atom belongs to ─────────
    #[derive(sqlx::FromRow)]
    struct NodeRow {
        node_id: Uuid,
        node_type: String,
        title: String,
    }

    let mut node_rows: Vec<NodeRow> = sqlx::query_as(
            r#"
            SELECT DISTINCT na.node_id, n.type AS node_type, ''::text AS title
            FROM node_atoms na
            JOIN nodes n ON n.node_id = na.node_id
            JOIN atoms seed ON seed.atom_id=na.atom_id AND seed.graph_id=n.graph_id
            WHERE library_atom_permitted(seed.graph_id,seed.atom_id) AND na.atom_id = ANY($1)
              AND n.graph_id = $2 AND library_node_permitted(n.graph_id,n.node_id)
            "#,
        )
        .bind(seed_atom_ids)
        .bind(graph_id)
        .fetch_all(&mut **tx)
        .await?;

    if node_rows.is_empty() {
        return Ok(vec![]);
    }
    let ids:Vec<Uuid>=node_rows.iter().map(|n|n.node_id).collect();
    let titles=crate::services::journal_activities::project_node_titles(tx,&vec![graph_id;ids.len()],&ids).await?;
    for row in &mut node_rows {row.title=titles.get(&(graph_id,row.node_id)).cloned().unwrap_or_default();}


    let node_ids: Vec<Uuid> = node_rows.iter().map(|r| r.node_id).collect();

    // ── Query 2: seeds-first, similarity-ranked siblings per node (D1) ────
    // See `Store::expand`'s doc comment for the full rationale (identical
    // to the unscoped `expand` impl above, just run inside the caller's
    // scoped transaction).
    //
    // **`JOIN atoms`, not `LEFT JOIN` (m365 enterprise train PR 4.1).**
    // This used to be a `LEFT JOIN` so a `node_atoms` row survived even
    // when its atom had no matching-model embedding (the `NULLS LAST`
    // ranking fallback below). But the row it selects — `na.atom_id` —
    // comes from `node_atoms`, which carries no retrieval window; a
    // `LEFT JOIN` meant a sibling atom OUTSIDE the caller's window
    // (migration `0064_team_seat_grants_history_from`'s windowed `atoms`
    // SELECT policy) still surfaced here, because the join simply
    // produced NULLs for the `atoms` side instead of excluding the row.
    // An inner `JOIN` makes this query consult the one table the window
    // actually lives on: a sibling only appears when it has a real,
    // RLS-VISIBLE `atoms` row. The `NULLS LAST` fallback is unaffected —
    // an atom with a non-matching-model embedding still has a real
    // `atoms` row, so it still joins; only a genuinely absent/
    // out-of-window atom is now excluded, which is the fix.
    #[derive(sqlx::FromRow)]
    struct NodeAtomRow {
        node_id: Uuid,
        atom_id: Uuid,
    }

    let query_vec = query_embedding.map(|e| Vector::from(e.to_vec()));

    let atom_rows: Vec<NodeAtomRow> = sqlx::query_as(
            r#"
            WITH seed_counts AS (
                SELECT na.node_id, COUNT(*) AS seed_count
                FROM node_atoms na
                JOIN atoms a ON a.atom_id=na.atom_id AND a.graph_id=$6
                WHERE library_atom_permitted(a.graph_id,a.atom_id) AND na.node_id = ANY($1)
                  AND na.atom_id = ANY($2)
                GROUP BY na.node_id
            ),
            ranked AS (
                SELECT na.node_id, na.atom_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY na.node_id
                           ORDER BY (na.atom_id = ANY($2)) DESC,
                                    CASE
                                        WHEN $3::vector IS NOT NULL
                                             AND a.embedding_next IS NOT NULL
                                             AND a.embed_model_next = $4
                                        THEN a.embedding_next <=> $3
                                        ELSE NULL
                                    END,
                                    na.atom_id
                       ) AS rn
                FROM node_atoms na
                JOIN (SELECT * FROM atoms WHERE library_atom_permitted(graph_id,atom_id)) a ON a.atom_id = na.atom_id
                WHERE na.node_id = ANY($1) AND a.graph_id=$6
            )
            SELECT ranked.node_id, ranked.atom_id
            FROM ranked
            LEFT JOIN seed_counts sc ON sc.node_id = ranked.node_id
            WHERE ranked.rn <= GREATEST($5::bigint, COALESCE(sc.seed_count, 0))
            ORDER BY ranked.node_id
            "#,
        )
        .bind(node_ids.as_slice())
        .bind(seed_atom_ids)
        .bind(query_vec)
        .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD)
        .bind(MAX_SIBLING_ATOMS_PER_NODE as i64)
        .bind(graph_id)
        .fetch_all(&mut **tx)
        .await?;

    // ── Query 3: load atoms from superseder nodes ─────────────────────────
    // For each superseded node in the seed set, pull atoms from the node that
    // supersedes it. `superseded_id` is the original node we already have, so
    // the atom is surfaced under its original context node.
    //
    // `JOIN atoms` added (m365 enterprise train PR 4.1) — this query
    // previously read `node_atoms` bare with no join to `atoms` at all,
    // which is the same window leak query 2 had: a superseder node's
    // atom, however old, surfaced regardless of the caller's retrieval
    // window. The inner join restricts to atoms the caller can actually
    // see (RLS-enforced), anchored additionally on `graph_id` for the
    // same reason query 1 above is.
    #[derive(sqlx::FromRow)]
    struct SupersederAtomRow {
        superseded_id: Uuid,
        atom_id: Uuid,
    }

    let superseder_atom_rows: Vec<SupersederAtomRow> = sqlx::query_as(
            r#"
            SELECT se.superseded_id, na.atom_id
            FROM node_atoms na
            JOIN supersession_edge se ON se.superseder_id = na.node_id
            JOIN (SELECT * FROM atoms WHERE library_atom_permitted(graph_id,atom_id)) a ON a.atom_id = na.atom_id AND a.graph_id = $2
            WHERE se.superseded_id = ANY($1::uuid[])
            "#,
        )
        .bind(node_ids.as_slice())
        .bind(graph_id)
        .fetch_all(&mut **tx)
        .await?;

    // ── Group atom_ids by node_id ─────────────────────────────────────────
    let mut atom_map: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in atom_rows {
        atom_map.entry(row.node_id).or_default().push(row.atom_id);
    }

    // Inject superseder atoms under the superseded node so callers surface
    // the newer/correct information without knowing about the supersession.
    // Tracked separately in `injected_map` (not just folded into
    // `atom_map`) so the returned `NodeWithAtoms::injected_atom_ids`
    // preserves which entries are borrowed display copies rather than
    // this node's own genuine `node_atoms` membership — the provenance
    // signal `routes::queries::multi_graph_retrieve` needs even when the
    // superseder's own node never separately appears in the expansion.
    let mut injected_map: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in superseder_atom_rows {
        atom_map
            .entry(row.superseded_id)
            .or_default()
            .push(row.atom_id);
        injected_map
            .entry(row.superseded_id)
            .or_default()
            .push(row.atom_id);
    }

    // ── Combine node metadata with atom lists, sorted by node_id ──────────
    let mut results: Vec<NodeWithAtoms> = node_rows
        .into_iter()
        .map(|n| NodeWithAtoms {
            node_id: n.node_id,
            node_type: n.node_type,
            title: n.title,
            atom_ids: atom_map.remove(&n.node_id).unwrap_or_default(),
            injected_atom_ids: injected_map.remove(&n.node_id).unwrap_or_default(),
        })
        .collect();

    results.sort_by_key(|n| n.node_id);

    Ok(results)
}

pub async fn list_cooccurrence_neighbors_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    node_ids: &[Uuid],
    min_shared: i64,
    exclude: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid, i32)>, StoreError> {
    list_cooccurrence_neighbors_via(&mut **tx, graph_id, node_ids, min_shared, exclude).await
}

pub async fn graph_self_node_id_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
) -> Result<Option<Uuid>, StoreError> {
    graph_self_node_id_via(&mut **tx, graph_id).await
}

pub async fn list_decision_anchor_candidates_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    exclude: Option<Uuid>,
) -> Result<Vec<(Uuid, Uuid, i32)>, StoreError> {
    list_decision_anchor_candidates_via(&mut **tx, graph_id, exclude).await
}

pub async fn list_relationship_edges_by_status_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
    status: EdgeStatus,
) -> Result<Vec<RelationshipEdge>, StoreError> {
    let rows: Vec<RelationshipEdgeRow> = sqlx::query_as(
        r#"
            SELECT edge_id, graph_id, src_node_id, predicate, dst_node_id,
                   origin, status, confidence, provenance,
                   created_at, updated_at, version
            FROM relationship_edge
            WHERE graph_id = $1
              AND status = $2
              AND predicate <> 'co_occurs_with'
            "#,
    )
    .bind(graph_id)
    .bind(status.as_str())
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows.into_iter().map(relationship_edge_from_row).collect())
}

pub async fn atom_inline_content_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    atom_ids: &[Uuid],
) -> Result<Vec<(Uuid, Option<String>, String, Option<DateTime<Utc>>, i32)>, StoreError> {
    let rows: Vec<(Uuid, Option<String>, String, Option<DateTime<Utc>>, i32)> = sqlx::query_as(
        r#"
                SELECT atom_id, content_inline, source_type, timestamp,
                       COALESCE(header_len, 0)
                FROM   (SELECT * FROM atoms WHERE library_atom_permitted(graph_id,atom_id)) atoms
                WHERE  atom_id = ANY($1)
                  AND  content_inline IS NOT NULL
                "#,
    )
    .bind(atom_ids)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

pub async fn node_promotion_origins_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: Uuid,
) -> Result<Vec<(Uuid, Uuid)>, StoreError> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT node_id, promoted_from_graph_id \
             FROM nodes \
             WHERE graph_id = $1 AND promoted_from_graph_id IS NOT NULL",
    )
    .bind(graph_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows)
}

pub async fn atom_authors_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    atom_ids: &[Uuid],
) -> Result<Vec<(Uuid, Option<String>, Option<String>)>, StoreError> {
    Ok(sqlx::query_as("SELECT atom_id,author_id,author_name FROM atoms WHERE atom_id=ANY($1) AND (author_id IS NOT NULL OR author_name IS NOT NULL) AND library_atom_permitted(graph_id,atom_id)")
        .bind(atom_ids).fetch_all(&mut **tx).await?)
}

pub async fn get_conversation_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
) -> Result<Option<Conversation>, StoreError> {
    let row: Option<PgConvRow> = sqlx::query_as(
            r#"SELECT id, account_id, graph_id, user_id, library_conversation_title(id) AS title, pinned, shared,
                      mode, created_at, updated_at
               FROM conversations WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.map(pg_conv))
}

pub async fn list_messages_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    conversation_id: Uuid,
    before_seq: Option<i32>,
    limit: i64,
) -> Result<Vec<ConversationMessage>, StoreError> {
    let rows: Vec<PgPageRow> = sqlx::query_as(verdict::CONVERSATION_PAGE_SQL)
        .bind(conversation_id)
        .bind(before_seq)
        .bind(limit)
        .fetch_all(&mut **tx)
        .await?;
    Ok(rows.into_iter().map(pg_page_msg).collect())
}

pub async fn list_conversation_oceans_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    conversation_id: Uuid,
) -> Result<Vec<Uuid>, StoreError> {
    let rows: Vec<(Uuid,)> =
        sqlx::query_as("SELECT graph_id FROM conversation_oceans WHERE conversation_id = $1")
            .bind(conversation_id)
            .fetch_all(&mut **tx)
            .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

pub async fn atom_detail_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    atom_id: Uuid,
    account_id: Uuid,
) -> Result<Option<AtomDetail>, StoreError> {
    // One query gates IDOR via JOIN graphs.account_id; returns None when the
    // atom doesn't exist OR isn't owned by the caller (a single row guard
    // both cases without leaking the 404/403 distinction).
    //
    // A `#[derive(FromRow)]` struct, not a plain tuple: sqlx's built-in
    // `FromRow` impl is only generated for tuples up to 16 elements
    // (`sqlx-core`'s `impl_from_row_for_tuple!` macro invocations), and
    // migration 0072's `project`/`mcp_client_name` columns pushed this
    // query's column count to 17.
    #[derive(sqlx::FromRow)]
    struct AtomRow {
        atom_id: Uuid,
        graph_id: Uuid,
        source_type: String,
        source_id: String,
        content_inline: Option<String>,
        content_blob_key: String,
        participants: Vec<String>,
        timestamp: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
        scope: String,
        fts_language: String,
        channel_id: Option<String>,
        channel_name: Option<String>,
        author_id: Option<String>,
        author_name: Option<String>,
        project: Option<String>,
        mcp_client_name: Option<String>,
        header_len: i32,
        title: Option<String>,
        excerpt: Option<String>,
    }

    let row: Option<AtomRow> = sqlx::query_as(
        "SELECT a.atom_id, a.graph_id, a.source_type, a.source_id, \
                    a.content_inline, a.content_blob_key, a.participants, \
                    a.timestamp, a.created_at, a.scope, \
                    COALESCE(a.fts_language, 'english') AS fts_language, \
                    a.channel_id, a.channel_name, a.author_id, a.author_name, \
                    a.project, a.mcp_client_name, \
                    COALESCE(a.header_len, 0) AS header_len, \
                    a.title, a.excerpt \
             FROM   (SELECT * FROM atoms WHERE library_atom_permitted(graph_id,atom_id)) a \
             JOIN   graphs g ON g.id = a.graph_id \
             WHERE  a.atom_id = $1 AND g.account_id = $2",
    )
    .bind(atom_id)
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(AtomRow {
        atom_id,
        graph_id,
        source_type,
        source_id,
        content_inline,
        content_blob_key,
        participants,
        timestamp,
        created_at,
        scope,
        fts_language,
        channel_id,
        channel_name,
        author_id,
        author_name,
        project,
        mcp_client_name,
        header_len,
        title,
        excerpt,
    }) = row
    else {
        return Ok(None);
    };

    // Linked reefs in a separate query (small fanout — most atoms touch a
    // handful of nodes, capped at MAX_SIBLING_ATOMS_PER_NODE on the
    // expand path).
    let mut reefs: Vec<(Uuid, String, String)> = sqlx::query_as(
        "SELECT n.node_id, n.type, ''::text \
             FROM   node_atoms na \
             JOIN   nodes n ON n.node_id = na.node_id \
             WHERE  na.atom_id = $1 \
             ORDER BY n.created_at",
    )
    .bind(atom_id)
    .fetch_all(&mut **tx)
    .await?;

    let ids:Vec<Uuid>=reefs.iter().map(|n|n.0).collect();
    let titles=crate::services::journal_activities::project_node_titles(tx,&vec![graph_id;ids.len()],&ids).await?;
    for (id,_,title) in &mut reefs {*title=titles.get(&(graph_id,*id)).cloned().unwrap_or_default();}
    let linked_reefs = reefs
        .into_iter()
        .map(|(node_id, node_type, title)| AtomDetailReef {
            node_id,
            node_type,
            title,
        })
        .collect();

    Ok(Some(AtomDetail {
        atom_id,
        graph_id,
        source_type,
        source_id,
        content_inline,
        content_blob_key,
        participants,
        timestamp,
        created_at,
        scope,
        fts_language,
        channel_id,
        channel_name,
        author_id,
        author_name,
        project,
        mcp_client_name,
        header_len,
        linked_reefs,
        title,
        excerpt,
    }))
}

pub async fn list_archived_teams_scoped(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
) -> Result<Vec<ArchivedTeam>, StoreError> {
    let rows: Vec<(Uuid, Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT t.id, g.id, g.name, g.archived_at \
             FROM teams t JOIN graphs g ON g.id = t.graph_id \
             WHERE t.account_id = $1 AND g.archived_at IS NOT NULL \
             ORDER BY g.archived_at DESC",
    )
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(team_id, graph_id, name, archived_at)| ArchivedTeam {
            team_id,
            graph_id,
            name,
            archived_at,
        })
        .collect())
}
