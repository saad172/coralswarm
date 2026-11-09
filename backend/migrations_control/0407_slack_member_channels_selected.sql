-- Migration: 0402_slack_member_channels_selected
-- Description: connectors redesign layer B3 (scope and pause) -- which of a
--   member's indexed Slack channels they picked in "Only what I pick" mode.
--   `slack_member_channels` is the routing index of every channel the member can
--   read (`users.conversations`); until now member capture took all of them. The
--   member's mode, on/off and pause live in `connector_sources` (0401, RLS
--   covered); this column holds only the picked set, next to the rows it
--   selects, and is read together with that row by
--   `connectors::catalog::scope`. It is ignored in "Everything I can see" mode,
--   which is what a connection made before the catalog keeps.
--
--   Additive, with a constant default, so the ALTER takes no table rewrite.
--
-- Down:
--   ALTER TABLE slack_member_channels DROP COLUMN IF EXISTS selected;

SET LOCAL lock_timeout = '3s';

ALTER TABLE slack_member_channels
    ADD COLUMN selected BOOLEAN NOT NULL DEFAULT FALSE;
