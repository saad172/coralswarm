-- Migration: 0006_teams
-- Description: Account DEK column, graphs kind column, team tables,
--              team key envelopes for end-to-end envelope encryption.
--
-- Depends on: 0001_init (accounts, users), 0003_atoms (graphs)
--
-- Down:
--   DROP TABLE IF EXISTS team_member_key_envelopes;
--   DROP TABLE IF EXISTS team_members;
--   DROP TABLE IF EXISTS team_keys;
--   DROP TABLE IF EXISTS teams;
--   ALTER TABLE graphs DROP COLUMN IF EXISTS kind;
--   ALTER TABLE accounts DROP COLUMN IF EXISTS dek_wrapped;

-- ── accounts.dek_wrapped ──────────────────────────────────────────────────────
-- Stores the account data-encryption key (DEK) wrapped under the master KEK.
-- Layout: [nonce 12B][tag 16B][ciphertext 32B] = 60 bytes.
-- Added here (CS-031 was not yet on cs-scaffold).

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS dek_wrapped BYTEA;

-- ── graphs.kind ───────────────────────────────────────────────────────────────
-- Distinguishes personal ('personal') from team ('team') graphs.
-- Defaults to 'personal' for all existing rows.

ALTER TABLE graphs
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'personal'
        CHECK (kind IN ('personal', 'team'));

-- ── Teams ─────────────────────────────────────────────────────────────────────
-- A team is a named group of users within an account that share a team graph.
-- Each team has exactly one associated graph (kind = 'team').

CREATE TABLE IF NOT EXISTS teams (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    graph_id   UUID        NOT NULL REFERENCES graphs(id)   ON DELETE CASCADE,
    name       TEXT        NOT NULL,
    created_by UUID        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Team members ──────────────────────────────────────────────────────────────
-- Many-to-many between teams and users, with an RBAC role and soft-delete.

CREATE TABLE IF NOT EXISTS team_members (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id      UUID        NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    user_id      UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role         TEXT        NOT NULL CHECK (role IN ('admin', 'member')),
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    departing_at TIMESTAMPTZ,
    UNIQUE (team_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_team_members_team_active
    ON team_members (team_id, departing_at)
    WHERE departing_at IS NULL;

-- ── Team keys ─────────────────────────────────────────────────────────────────
-- Versioned symmetric team keys.  The raw key is NEVER stored here;
-- wrapped_key is included for the creator's own envelope convenience
-- (can also be looked up via team_member_key_envelopes).

CREATE TABLE IF NOT EXISTS team_keys (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id   UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    version    INT         NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by UUID        NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    UNIQUE (graph_id, version)
);

-- ── Team member key envelopes ─────────────────────────────────────────────────
-- One row per (team_key_version × member): the team key wrapped under that
-- member's account DEK.
-- Layout of wrapped_key: [nonce 12B][tag 16B][ciphertext 32B] = 60 bytes.

CREATE TABLE IF NOT EXISTS team_member_key_envelopes (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    team_key_id UUID        NOT NULL REFERENCES team_keys(id) ON DELETE CASCADE,
    user_id     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    wrapped_key BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (team_key_id, user_id)
);
