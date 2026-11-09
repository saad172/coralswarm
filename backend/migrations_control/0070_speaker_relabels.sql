-- Migration: 0070_speaker_relabels
-- Description: Let a user RELABEL a diarization speaker post-ingest, tracked as a
--   first-class transcript correction so the relabel is provenance-tracked and
--   revertible EXACTLY like the extraction name-corrections (0056). We extend the
--   existing `transcript_corrections` overlay rather than adding a parallel table
--   (cs-uxp3c-speaker-relabel).
--
--   Three additive columns, all defaulted so existing rows AND the extraction
--   insert path (0056) are unaffected:
--     kind       'name'       — extraction canonicalization (0056, the default)
--                'speaker'     — a user speaker-relabel
--     origin     'extraction' — machine-produced (the default)
--                'user'        — human relabel (actor provenance; 0056 had none)
--     occurrence NULL          — every whole-word occurrence  (all-segments scope)
--                N (1-based)   — only the Nth whole-word occurrence (this-segment scope)
--
--   User relabels carry no source chunk atom, so `atom_id` is written as the nil
--   UUID (all-zeros) sentinel — the column has no foreign key (0056) so this is
--   safe, and nothing reads `atom_id` for corrections. The overlay + revert/restore
--   machinery is otherwise untouched: a relabel is `status='applied'` and can be
--   reverted/restored via the existing /v1/transcript-corrections/{id}/{revert,restore}.
--
-- Depends on: 0056_meeting_transcripts.
-- Down:
--   ALTER TABLE transcript_corrections DROP COLUMN IF EXISTS occurrence;
--   ALTER TABLE transcript_corrections DROP COLUMN IF EXISTS origin;
--   ALTER TABLE transcript_corrections DROP COLUMN IF EXISTS kind;

ALTER TABLE transcript_corrections
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'name'
        CHECK (kind IN ('name', 'speaker'));

ALTER TABLE transcript_corrections
    ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'extraction'
        CHECK (origin IN ('extraction', 'user'));

-- NULL = all-segments scope (global whole-word overlay); N (1-based) = this-segment
-- scope (only the Nth whole-word occurrence is replaced when building corrected_text).
ALTER TABLE transcript_corrections
    ADD COLUMN IF NOT EXISTS occurrence INTEGER;
