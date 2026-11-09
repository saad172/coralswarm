-- Migration: 0056_meeting_transcripts
-- Description: Preserve the RAW verbatim meeting transcript and capture
--              name-correction provenance from entity extraction.
--
--   `meeting_transcripts` stores the WHOLE transcript verbatim, separate from the
--   per-chunk `atoms` (which remain the retrieval unit). In prod the raw text is
--   AES-256-GCM encrypted to R2 (mirroring the atom blob path) and only the key
--   is stored here (`raw_blob_key`); in dev / no-R2 the text lives inline
--   (`raw_inline`). Idempotent per (graph_id, source_id) so a re-ingest of the
--   same meeting updates the single row rather than piling up duplicates.
--
--   `transcript_corrections` is an OVERLAY, never a rewrite: each row records that
--   entity extraction canonicalized a surface spelling (e.g. "Jordan") to a
--   canonical entity title (e.g. "Morgan"). The raw transcript text is NEVER
--   mutated — corrections are applied only when producing corrected_text for
--   display, and can be reverted/restored via `status`.
--
-- Depends on: 0003_atoms (graphs), 0011_nodes (nodes — node_id reference, soft).
-- Down:
--   DROP TABLE IF EXISTS transcript_corrections;
--   DROP TABLE IF EXISTS meeting_transcripts;

-- ── Raw verbatim transcript (one row per ingested meeting source) ──────────────
CREATE TABLE IF NOT EXISTS meeting_transcripts (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id     UUID        NOT NULL,
    source_id    TEXT        NOT NULL,
    -- Prod: R2 object key ("{graph_id}/{uuid}") of the encrypted raw blob.
    raw_blob_key TEXT,
    -- Dev / no-R2: verbatim raw transcript text stored inline.
    raw_inline   TEXT,
    duration_ms  BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (graph_id, source_id)
);

-- ── Name-correction provenance overlay (extraction surface_form vs canonical) ──
CREATE TABLE IF NOT EXISTS transcript_corrections (
    correction_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id      UUID        NOT NULL,
    -- The chunk atom whose extraction produced this correction.
    atom_id       UUID        NOT NULL,
    -- The base meeting/source id (a chunk atom's "<base>:chunk:<N>" stripped to
    -- "<base>") so corrections group with their meeting transcript.
    source_id     TEXT        NOT NULL,
    -- The spelling EXACTLY as it appeared in the passage (e.g. "Jordan").
    surface_form  TEXT        NOT NULL,
    -- The canonical entity title extraction resolved it to (e.g. "Morgan").
    canonical     TEXT        NOT NULL,
    -- The entity reef this correction points at, when known.
    node_id       UUID,
    -- Extraction's self-reported confidence for the entity (0.0–1.0).
    confidence    REAL,
    -- 'applied' (overlay active) | 'reverted' (user undid it). Reverted rows are
    -- excluded when building corrected_text but kept for the audit trail.
    status        TEXT        NOT NULL DEFAULT 'applied'
                  CHECK (status IN ('applied', 'reverted')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The summary route reads every correction for one meeting (graph_id, source_id).
CREATE INDEX IF NOT EXISTS idx_transcript_corrections_graph_source
    ON transcript_corrections (graph_id, source_id);
