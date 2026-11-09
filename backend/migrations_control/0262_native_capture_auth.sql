-- Short-lived host capture grants. No Clerk JWT or plaintext credential persists.
CREATE TABLE native_capture_auth (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 host TEXT NOT NULL CHECK (host IN ('teams','outlook')),
 origin TEXT NOT NULL,
 code_challenge TEXT NOT NULL,
 selector JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 attempt_expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '5 minutes',
 code_hash TEXT UNIQUE,
 code_expires_at TIMESTAMPTZ,
 exchanged_at TIMESTAMPTZ,
 credential_hash TEXT UNIQUE,
 expires_at TIMESTAMPTZ,
 revoked_at TIMESTAMPTZ,
 user_id UUID REFERENCES users(id) ON DELETE CASCADE,
 account_id UUID REFERENCES accounts(id) ON DELETE CASCADE,
 clerk_user_id TEXT,
 session_id TEXT,
 org_id TEXT,
 organization_name TEXT,
 display_name TEXT,
 meeting_id TEXT,
 occurrence_start TEXT,
 occurrence_end TEXT,
 capture_id UUID
);
CREATE INDEX native_capture_auth_expiry ON native_capture_auth(attempt_expires_at, expires_at);
