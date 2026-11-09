-- PAIRED WITH: tenant 0196 (library_evidence_page_context).
-- Additive definition-2 page context at the existing node/item grain.
-- Default-filled old rows are not v2 evidence: readers require ready_version=2,
-- and the canonical refresher restarts the full graph on a version mismatch.
-- Facts describe actual retained linked versions, never the current pointer.
ALTER TABLE library_node_item_evidence
 ADD COLUMN semantic_independent_version BOOLEAN NOT NULL DEFAULT false,
 ADD COLUMN semantic_synthetic BOOLEAN NOT NULL DEFAULT false,
 ADD COLUMN semantic_latest_captured_at TIMESTAMPTZ,
 ADD COLUMN entity_type_has_match BOOLEAN NOT NULL DEFAULT false,
 ADD COLUMN entity_type_value TEXT,
 ADD COLUMN entity_type_captured_at TIMESTAMPTZ,
 -- Deliberately no FK: invalidation must survive deletion of the winner.
 ADD COLUMN entity_type_semantic_atom_id UUID,
 ADD COLUMN entity_type_ambiguous BOOLEAN NOT NULL DEFAULT false,
 ADD CONSTRAINT library_node_item_evidence_entity_tuple CHECK (
  (NOT entity_type_has_match AND entity_type_value IS NULL
   AND entity_type_captured_at IS NULL AND entity_type_semantic_atom_id IS NULL
   AND NOT entity_type_ambiguous)
  OR (entity_type_has_match AND entity_type_captured_at IS NOT NULL
   AND entity_type_semantic_atom_id IS NOT NULL
   AND (NOT entity_type_ambiguous OR entity_type_value IS NULL))
 ),
 ADD CONSTRAINT library_node_item_evidence_semantic_context CHECK (
  semantic_link OR (NOT semantic_independent_version AND NOT semantic_synthetic
   AND semantic_latest_captured_at IS NULL AND NOT entity_type_has_match)
 );

COMMENT ON COLUMN library_node_item_evidence.semantic_independent_version IS
 'Any actually linked retained semantic version is independent (not baseline or derived_copy).';
COMMENT ON COLUMN library_node_item_evidence.semantic_synthetic IS
 'Any actually linked retained semantic version has synthetic transport or synthetic_demo metadata.';
COMMENT ON COLUMN library_node_item_evidence.semantic_latest_captured_at IS
 'Latest captured_at of actually linked retained semantic versions; NULL when none.';
COMMENT ON COLUMN library_node_item_evidence.entity_type_has_match IS
 'A retained linked outcome entity matches lower(raw node title256); distinguishes no match from a NULL type.';
COMMENT ON COLUMN library_node_item_evidence.entity_type_value IS
 'Exact nullable top-tuple type; NULL also represents an ambiguous tuple, distinguished by entity_type_ambiguous.';
COMMENT ON COLUMN library_node_item_evidence.entity_type_captured_at IS
 'Winning matching tuple capture timestamp, ordered DESC before semantic outcome UUID DESC.';
COMMENT ON COLUMN library_node_item_evidence.entity_type_semantic_atom_id IS
 'Winning retained matching outcome UUID; deletion-safe snapshot without a source FK.';
COMMENT ON COLUMN library_node_item_evidence.entity_type_ambiguous IS
 'Only the latest matching tuple has multiple distinct types, counting SQL NULL as a distinct type.';
