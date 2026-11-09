-- Migration: 0174_connector_installs_microsoft_backfill
-- Description: Microsoft's own explicit backfill into `connector_installs`
--   (migration `0121_connector_installs`) — the routing-index counterpart of
--   `.scratch/m365-enterprise/plan.md`'s Train 2 PR 2.1 ("connector_installs
--   routing for Microsoft"), whose write-side task list explicitly asks for
--   "a new migration file... the way 0121's backfill did for Slack".
--
--   ── Evidence this is very likely a no-op today, and why it still lands ──
--   0121's OWN backfill (its first `INSERT INTO connector_installs ... FROM
--   oauth_tokens ot ...` block) is NOT Slack-specific: it selects `ot.provider`
--   directly and filters only on `ot.provider_workspace_id IS NOT NULL` — no
--   `WHERE ot.provider = 'slack'` anywhere. (The SECOND block, from
--   `slack_integrations`, is the Slack-only one — Microsoft has no equivalent
--   legacy table to mirror.) So at the moment 0121 was applied, it already
--   swept every `oauth_tokens` row that existed then for EVERY provider,
--   Microsoft included — see that migration's own header for the full
--   reasoning this one inherits rather than repeats.
--
--   And going forward, `routes/oauth.rs::callback` writes `connector_installs`
--   (via `record_connector_install`) inside the SAME transaction as its
--   `oauth_tokens` upsert for every provider that reaches the generic OAuth
--   path — already true before this PR, not something this PR's Rust changes
--   introduce (verified directly against this codebase: that call site binds
--   `connector.id()`, not a Slack literal). So any Microsoft OAuth connect
--   made after 0121 shipped already has a matching `connector_installs` row
--   from the moment it was created.
--
--   The remaining gap this migration closes is therefore narrow: any
--   `oauth_tokens` row with `provider = 'microsoft'` that is somehow NOT
--   mirrored into `connector_installs` — e.g. an environment where 0121's own
--   backfill ran before some Microsoft rows existed for a reason not
--   reconstructable from this repo alone, or any future write path that
--   creates an `oauth_tokens` row outside `record_connector_install`'s
--   transaction (the exact class of drift `resolve_connector_install_
--   account_ids`'s self-healing fallback, added by 0121, already tolerates at
--   READ time — this migration is the same insurance applied once, up front,
--   at WRITE time, scoped to this one provider so it is independently
--   greppable and independently re-runnable). `ON CONFLICT ... DO NOTHING`
--   against the SAME partial unique index 0121 targets makes re-running this
--   file's INSERT any number of times a no-op after the first.
--
--   Delimited with the same `BACKFILL BEGIN`/`BACKFILL END` markers 0121
--   uses, for the same reason: so a future `*_pg.rs` test can extract and
--   re-execute this exact statement rather than a hand-copied approximation.
--
-- Depends on: 0121_connector_installs (the table, the partial unique index,
--   and the header this one's reasoning inherits), 0006-numbered `oauth_tokens`.
--
-- Down:
--   -- Not reversible in isolation: rows this migration inserts are
--   -- indistinguishable from rows 0121's own backfill or `record_connector_
--   -- install` could have inserted for the same (provider, workspace,
--   -- account) tuple. No DOWN statement — matches 0121's own precedent of
--   -- treating its backfill INSERTs as non-reversible.

-- BACKFILL BEGIN
INSERT INTO connector_installs
    (provider, external_workspace_id, external_user_id, clerk_org_id, account_id, owner_user_id, created_at)
SELECT
    ot.provider,
    ot.provider_workspace_id,
    NULLIF(ot.provider_user_id, ''),
    a.clerk_org_id,
    ot.account_id,
    NULL,
    ot.created_at
FROM oauth_tokens ot
JOIN accounts a ON a.id = ot.account_id
WHERE ot.provider = 'microsoft'
  AND ot.provider_workspace_id IS NOT NULL
ON CONFLICT (provider, external_workspace_id, account_id) WHERE owner_user_id IS NULL DO NOTHING;
-- BACKFILL END
