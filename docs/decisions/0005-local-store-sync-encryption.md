# ADR-0005: Desktop Local Store, Sync, and Encryption-at-Rest

**Date:** 2026-05-25
**Status:** Superseded by [ADR 0015](0015-retire-desktop-app.md)
**Deciders:** Alex
**Implements:** ADR-0004 (local-first desktop, server-trust cloud)
**Amends:** ADR-0002 (local-store implication only; cloud pgvector + model choice stand)

---

## Context

ADR-0004 makes the desktop the primary, offline-capable engine and the cloud a server-trust,
queryable replica synced from the desktop (source of truth = desktop; append-only corals + LWW
reefs; deletes = tombstones + 30-day trash). That requires three things this ADR decides: the
**desktop's local store**, the **sync engine**, and **encryption-at-rest on each end**.

The backend's data layer is ~107 `sqlx` call sites against Postgres; the genuinely
Postgres-specific parts are pgvector `<=>` (×8), `unnest` bulk ops (×12), the FTS keyword path,
and `gen_random_uuid` — bounded but real.

## Decision

### 1. Local store: libsql (SQLite-compatible) on the desktop

The desktop engine uses **libsql** (embedded, SQLite-compatible, native vector index) as its
on-device store. Engine data access goes behind a **`Store` trait** with two implementations:
- **`PostgresStore`** (exists) — the cloud (and dev) backend; pgvector + HNSW (ADR-0002 stands).
- **`LibsqlStore`** (new) — the desktop; native vector index (full-scan/ANN is fine at personal
  scale, 10k–100k corals).

libsql over plain `sqlite-vec`: better vector indexing and the **option** to adopt libsql's
native replication later, without committing the cloud to it now.

**Why not libsql everywhere (drop Postgres cloud):** the cloud is the multi-tenant, always-on,
must-scale tier — the most expensive place to bet on newer tech. We keep it on proven
Postgres+pgvector and **confine the libsql bet to the single-user, recoverable desktop**, at the
cost of two SQL dialects + a hand-built sync engine (next). (Q5 of the design review.)

### 2. Sync engine (custom, bidirectional)

- **Oplog per side:** every mutation is an append-only op
  `(entity, op_type, payload, version, updated_at, device_id)`. Sync = "give me ops since cursor
  X," apply, advance cursor. Resumable and debuggable.
- **Corals (append-only):** immutable; unique by `(graph_id, source_type, source_id)`; flow both
  ways and de-dupe — never conflict.
- **Reefs (editable):** **last-write-wins by `updated_at`** (device_id tiebreak). Safe because
  reefs are regenerable from corals.
- **Deletes:** **tombstones** that propagate everywhere; the cloud keeps a **30-day soft-delete
  trash** (accidental-delete recovery; doubles as a premium "restore").
- **Embeddings:** the 384-d vectors are **synced**, not re-embedded (same model both ends →
  deterministic; avoids desktop CPU recompute; ~1.5 KB/coral).
- **Cloud-originated corals** (premium web/connector ingest) sync **down** to the desktop as
  normal corals. ("Desktop-canonical" governs conflict authority, not who may create.)
- **Transport/auth:** push/pull oplog deltas over TLS, authenticated with the existing
  per-device session token (magic-link sessions).

### 3. Encryption-at-rest — two independent domains (keys are NOT shared)

- **Local desktop:** the libsql file is **encrypted with a device key stored in the OS keychain**
  (Keychain / Credential Manager; optional biometric unlock). The key **never leaves the device
  and is never sent to the cloud.** Connector OAuth tokens also live in the OS keychain.
- **Cloud (Model T):** per-account **DEK wrapped by a master KEK**, with the master key moved from
  an env var into a **KMS / secret manager**. Content is encrypted at rest but the server can
  decrypt (that is Model T). Synced data arrives over TLS and is re-encrypted under the cloud key.

### 4. Embedding / extraction location

Embeddings run **on-device** for desktop ingest (ONNX `bge-small-en-v1.5` in `packages/core`) and
**cloud-side** for premium cloud ingest. Same model both ends → vectors are interchangeable and
sync cleanly. LLM extraction/synthesis runs wherever the query runs (local engine or cloud);
managed LLM is a premium/billed path, BYOK or local model otherwise.

## Consequences

**Positive:** native, file-based, invisible desktop store (no bundled DB server); proven cloud
unchanged; append-only corals make the custom sync the tractable kind; local data protected at
rest even without FileVault; clean separation of the private-local and readable-cloud domains.

**Negative / risks:** two SQL dialects to maintain behind the `Store` trait; a sync engine to
build, test, and operate (oplog, cursors, reconciliation); libsql vector maturity (low blast
radius — desktop only, recoverable); keychain integration per-platform.

## Alternatives considered

- **Embedded Postgres+pgvector on desktop** — zero query rewrite, but ships a Postgres *server* to
  every laptop (per-platform pgvector builds, a daemon, data dir, AV flags, upgrade migrations);
  unbounded ongoing liability vs. a bounded one-time rewrite. Rejected.
- **plain `sqlite-vec`** — fine, but libsql adds better vector indexing + future-sync optionality
  for ~no extra cost. Rejected in favor of libsql.
- **libsql everywhere (drop Postgres cloud)** — gets native sync + one dialect, but bets the
  scaled cloud tier on newer tech and re-platforms a working backend. Rejected (Q5).
- **Re-embed on each side instead of syncing vectors** — saves bandwidth, costs CPU; rejected for
  determinism + simplicity at personal scale.

## References

- ADR-0002 (vector store & embedding); ADR-0004 (local-first desktop, server-trust cloud)
- `backend/src` (`~107 sqlx sites`; `retrieval/keyword.rs`); `packages/core` (`kek.rs`, `embed_ort.rs`)
- libsql / Turso (embedded SQLite-compatible DB with vector + replication)
