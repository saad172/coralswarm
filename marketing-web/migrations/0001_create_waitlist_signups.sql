-- Migration: create waitlist_signups table
-- Stores early-access email signups captured via the marketing site's
-- waitlist form (components/EmailForm.tsx -> POST /api/waitlist).

CREATE TABLE IF NOT EXISTS waitlist_signups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
