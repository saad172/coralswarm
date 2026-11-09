# ADR-0015: Retire the desktop app

**Date:** 2026-10-29
**Status:** Accepted
**Deciders:** Alex
**Supersedes:** ADR-0004 (local-first course correction), ADR-0005 (local store, sync, encryption),
ADR-0008 (desktop capture sync). Those ADRs are marked Superseded in a later PR of this series.

---

## Context

The product decision of 2026-10-28 took the desktop app out of the product. CoralSwarm is the web
app, the `cs` CLI, the remote MCP server at `/mcp`, and the connectors. The desktop code is still
in the repo: the Tauri app, the recorder, the `csd` daemon, the staging pipeline, desktop-only
backend routes, the backend's local libSQL mode, and a release pipeline. Keeping it costs CI time,
review attention, and a second storage engine behind every backend change.

## Decision

### 1. Full cut

Remove all of the following:

- The Tauri app (`desktop/`), `recorder/`, the `csd` daemon (`daemon/`), and `staging/`.
- The web↔Tauri bridge in `web/`.
- The desktop-only backend routes `desktop_auth` and `desktop_tokens`.
- The backend's local libSQL mode: `LibsqlStore`, every `tenant.pool == None` branch,
  `sync_client`, and the `libsql-encryption` feature.
- The Ollama local LLM.
- The desktop release pipeline.

### 2. Screen/work capture goes too, server-side included

Remove the `capture_policy` plane, the org capture attestation gate, `SessionKind::Work` and
`work_session` ingest, the Work sessions facet in the library, and the capture settings.

Coding-session capture (agent hooks → MCP) **stays**.

### 3. Meetings stay

`meeting_transcripts`, `routes/meetings.rs`, and the Meet and calendar connectors stay. Meetings
that the desktop app recorded stay as ordinary meetings.

### 4. Hard cut for installed clients

There is no sunset release. Installed copies stop working when the backend routes go.

### 5. Data

1. A one-off binary purges `work_session` sources, sessions, corals, and their citation and
   provenance links, across the control plane and every tenant DB. It is dry-run by default. Order:
   take backups, run on staging, founder approves the production counts, run on production. It
   uses the existing source-retraction path, so a reef whose only evidence was work sessions is
   deleted with it; a reef with any other evidence stays.
2. Forward migrations then `DROP` `desktop_tokens`, `capture_policies`, `org_capture_attestations`,
   and `sync_cursors`. Each carries a `-- contract: safe-after server-…` marker naming the release
   that removed the code using it.
3. Old migrations are never edited (sqlx checksums).

### 6. Tests

CI runs no `cargo test`; PRs are gated by `guards` and Playwright `e2e`. libSQL-backed tests are
deleted with `LibsqlStore`, not ported. Where one covered live behaviour that no e2e scenario
covers, an e2e scenario replaces it.

### 7. Web build

Production web on Fly is a standalone Next.js server (`web/Dockerfile`). The static-export build
existed only for Tauri, so it goes, along with the Clerk static adapter (`clerk-desktop.tsx`); web
uses `@clerk/nextjs` directly.

### 8. External cleanup

- Delete all `desktop-v*` GitHub releases **and** tags.
- Delete the R2 `desktop/` prefix.
- Delete the Tauri and GPG signing secrets.
- Remove the Clerk `coralswarm://` and desktop redirect URLs.
- **Keep** the Apple signing secrets and certificate, for a possible future Apple app.

### 9. Docs

Delete desktop-only docs. Mark ADRs 0004, 0005, and 0008 (desktop capture sync) Superseded by this
ADR. Passing mentions in other ADRs stay as history.

## Delivery

PRs, then the external cleanup:

1. Crates, CI, scripts, this ADR.
2. Web, including the static-export build.
3. Backend desktop features.
4. Backend local mode and its tests, stacked on 3.
5. Data: the purge binary, then the table drops once a later server release exists.
6. Docs.

PRs 1, 2 and 3 run in parallel.

## Consequences

- One storage engine (Postgres) behind the backend, one client surface (web) to ship.
- Users who installed the desktop app lose it without warning. Their meeting data stays; their
  work-session data is purged.
- The desktop and recorder code is recoverable from git history only.
