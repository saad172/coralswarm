-- CS-122 phase 2: the in-channel backfill prompt.
--
-- Records the user's history-depth choice from the Adaptive Card prompt posted
-- when the bot is added. NULL = no choice made yet (the card is still
-- actionable). Once set ('none' | '7' | '30' | '90' | 'all'), the prompt is
-- locked: the click handler no-ops on any further click (guards stale cards).
ALTER TABLE teams_watched_resources
    ADD COLUMN IF NOT EXISTS history_choice TEXT;
