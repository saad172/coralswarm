//! One graph's admitted lexical evidence. Each source is tokenized once and
//! large bodies stream as bounded vectors; no aggregate tsvector can overflow.

pub(super) const QUALIFIED_REEFS_SQL: &str = r#"
WITH q AS MATERIALIZED (
 SELECT to_tsquery('simple',$4) || to_tsquery('english',$4) query
), permitted AS MATERIALIZED (
 SELECT a.* FROM public.library_permitted_atoms($1,NULL::uuid[]) p
 JOIN public.atoms a ON a.atom_id=p.atom_id AND a.graph_id=$1
), graph_nodes AS MATERIALIZED (
 SELECT node_id,graph_id,type,title FROM public.nodes
 WHERE graph_id=$1 AND ($3::text IS NULL OR scope=$3)
), legacy_links AS MATERIALIZED (
 SELECT n.node_id,a.atom_id FROM graph_nodes n
 JOIN public.node_atoms l ON l.node_id=n.node_id
 JOIN permitted a ON a.atom_id=l.atom_id
), semantic_evidence AS MATERIALIZED (
 SELECT a.semantic_atom_id,a.statement,a.entities FROM public.served_semantic_atoms a
 WHERE a.graph_id=$1
), semantic_links AS MATERIALIZED (
 SELECT n.node_id,a.semantic_atom_id FROM graph_nodes n
 JOIN public.node_semantic_atoms l ON l.node_id=n.node_id AND l.graph_id=n.graph_id
 JOIN semantic_evidence a ON a.semantic_atom_id=l.semantic_atom_id
), title_support AS MATERIALIZED (
 SELECT DISTINCT l.node_id FROM legacy_links l
 JOIN graph_nodes n ON n.node_id=l.node_id JOIN permitted a ON a.atom_id=l.atom_id
 WHERE n.title<>'' AND (
  position(lower(n.title) IN lower(COALESCE(a.content_inline,'')))>0
  OR EXISTS(SELECT 1 FROM unnest(a.participants) p WHERE lower(p)=lower(n.title)))
 UNION
 SELECT DISTINCT l.node_id FROM semantic_links l
 JOIN graph_nodes n ON n.node_id=l.node_id
 JOIN semantic_evidence a ON a.semantic_atom_id=l.semantic_atom_id
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
 WHERE lower(e->>'mention')=lower(n.title)
 UNION
 SELECT c.node_id FROM public.reef_identity_corrections c
 JOIN graph_nodes n ON n.node_id=c.node_id
 JOIN permitted a ON a.atom_id=c.support_atom_id
 WHERE c.graph_id=$1 AND c.state='active' AND c.label=n.title
  AND public.reef_identity_support_valid(c)
), safe_nodes AS MATERIALIZED (
 SELECT n.node_id,n.type='entity' is_entity,
  CASE WHEN t.node_id IS NOT NULL THEN substring(n.title FOR 256) END title
 FROM graph_nodes n LEFT JOIN title_support t ON t.node_id=n.node_id
), bodies AS MATERIALIZED (
 SELECT 'legacy' kind,a.atom_id id,a.content_inline body FROM permitted a
 WHERE a.content_inline IS NOT NULL AND EXISTS(SELECT 1 FROM legacy_links l WHERE l.atom_id=a.atom_id)
 UNION ALL
 SELECT 'semantic',a.semantic_atom_id,a.statement FROM semantic_evidence a
 WHERE EXISTS(SELECT 1 FROM semantic_links l WHERE l.semantic_atom_id=a.semantic_atom_id)
), body_vectors AS MATERIALIZED (
 SELECT b.kind,b.id,setweight(vector,'D') document
 FROM bodies b CROSS JOIN LATERAL public.reef_identity_text_vectors(b.body) vector
), documents AS (
 SELECT n.node_id,setweight(to_tsvector('simple',COALESCE(n.title,'')),
  CASE WHEN n.is_entity THEN 'A'::"char" ELSE 'C'::"char" END) document
 FROM safe_nodes n
 UNION ALL
 SELECT l.node_id,v.document FROM legacy_links l JOIN body_vectors v ON v.kind='legacy' AND v.id=l.atom_id
 UNION ALL
 SELECT l.node_id,v.document FROM semantic_links l JOIN body_vectors v ON v.kind='semantic' AND v.id=l.semantic_atom_id
), matching AS MATERIALIZED (
 SELECT d.node_id,d.document FROM documents d CROSS JOIN q WHERE d.document @@ q.query
), scores AS (
 SELECT m.node_id,sum(ts_rank_cd(m.document,q.query))::real rank
 FROM matching m CROSS JOIN q GROUP BY m.node_id
), coverage AS (
 SELECT m.node_id,count(DISTINCT tok) matched_tokens FROM matching m
 CROSS JOIN unnest($2::text[]) tok
 WHERE m.document @@ (to_tsquery('simple',tok) || to_tsquery('english',tok))
 GROUP BY m.node_id
)
SELECT n.node_id,n.is_entity,s.rank FROM scores s
JOIN safe_nodes n ON n.node_id=s.node_id JOIN coverage c ON c.node_id=s.node_id
WHERE c.matched_tokens >= $5 OR (
 n.is_entity AND array_length(tsvector_to_array(to_tsvector('simple',n.title)),1)>=2
 AND tsvector_to_array(to_tsvector('simple',n.title)) <@ $2::text[])
"#;

pub(super) const PERMITTED_MEMBERS_SQL: &str = r#"
WITH q AS MATERIALIZED (
 SELECT to_tsquery('simple',$3) || to_tsquery('english',$3) query
), permitted AS MATERIALIZED (
 SELECT atom_id FROM public.library_permitted_atoms($2,NULL::uuid[])
)
SELECT l.node_id,l.atom_id,EXISTS(
 SELECT 1 FROM public.reef_identity_text_vectors(a.content_inline) AS v(document)
 CROSS JOIN q WHERE v.document @@ q.query
) AS matches_query
FROM public.node_atoms l
JOIN public.nodes n ON n.node_id=l.node_id AND n.graph_id=$2
JOIN permitted p ON p.atom_id=l.atom_id
JOIN public.atoms a ON a.atom_id=p.atom_id AND a.graph_id=$2
WHERE l.node_id=ANY($1)
-- The existing sibling cap applies after this ordering. Prefer admitted
-- query evidence before fallback siblings, without changing reef scores.
ORDER BY l.node_id,matches_query DESC,l.atom_id
"#;
