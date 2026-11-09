-- Migration: 0008_accounts_owner_unique
-- Description: Add unique constraint on accounts.owner_user_id to support ON CONFLICT upsert
--
-- Down:
--   ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_owner_user_id_key;

ALTER TABLE accounts ADD CONSTRAINT accounts_owner_user_id_key UNIQUE (owner_user_id);
