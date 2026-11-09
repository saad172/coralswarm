# ADR-0004: Local-First Desktop, Server-Trust Cloud

**Date:** 2026-05-25
**Status:** Superseded by [ADR 0015](0015-retire-desktop-app.md)
**Deciders:** Alex
**Amends:** ADR-0002 (vector store & embedding) — deployment framing only; see "Relationship to ADR-0002"
**Implemented by:** ADR-0005 (local store, sync, encryption)

---

## Revision note

The first draft of this ADR committed to a **zero-knowledge blind-relay cloud** (client-held keys,
server stores ciphertext only). A subsequent design review walked the full decision tree and
**changed the cloud's privacy posture to server-trust (Model T)** while keeping the local-first
desktop thesis. This document reflects the final decision. The zero-knowledge model is recorded
under "Alternatives" as a possible future enterprise mode.

## Context

A review on 2026-05-25 found the implementation had **inverted** the system-design doc's core
thesis. The doc is *local-first* — "the local plane is primary; the cloud is a sync layer, not a
dependency; a user with no internet can ingest, query, and use their personal graph with full
fidelity." What got built was *cloud-first*: one Rust+Postgres backend on Fly does everything
(ingest, embed, extract, query, synthesize, encrypt with a server-held `MASTER_KEK`), and every
client (web, Tauri desktop = webview over web, MCP server, CLI) is a thin HTTP client. Evidence:
`fly.toml`; `packages/core/src/kek.rs::load_kek()` (server-held key); `desktop/tauri.conf.json`
(`frontendDist: ../web/out`, no engine); `mcp-server/src/client.rs` (HTTP → `localhost:8080`).

The data model, embedding **model** (`bge-small-en-v1.5`), auth, MCP tool surface, and scopes
remained faithful. The architecture, keys, and surface priorities had drifted.

## Decision

Re-establish **local-first on the desktop**, with a **server-trust cloud** for premium sync.
The two are separable axes; we keep local-first and consciously choose server-trust for the cloud.

1. **Desktop (Tauri) is the primary surface and runs the engine on-device.** The shared engine
   (`packages/core` + the service/query layer) runs locally against a local store, with
   embeddings (`bge-small-en-v1.5` ONNX) computed on-device. Full ingest / query / reefs / review
   / MCP / CLI work **offline**. (Local store + engine packaging → ADR-0005.)

2. **The cloud is a server-trust replica (Model T), not a blind relay.** Premium users sync their
   ocean to the cloud; the cloud **can read it** in order to serve fast server-side query at
   scale, web/mobile access, and 24/7 cloud-side ingestion/connectors. The existing
   Postgres+pgvector backend already *is* most of this role. **Privacy promise:** *"private by
   default — local-first; your free local ocean never leaves the device; cloud data is encrypted
   at rest and we don't mine it."* This is **not** zero-knowledge, and must be marketed precisely.

3. **Source of truth is the desktop.** The cloud is a synced, queryable replica.
   **Bidirectional sync; append-only corals + last-write-wins reefs; deletes = tombstones with a
   30-day cloud trash.** (Mechanics → ADR-0005.)

4. **Free / Premium / Team split:**

   | Capability | Free (local) | Premium | Team |
   |---|---|---|---|
   | Desktop ingest/embed/query/reefs/review/MCP/CLI, **on-device**, **offline**, one machine | ✅ | ✅ | ✅ |
   | Connectors **running on your desktop** (sync when app is open) | ✅ | ✅ | ✅ |
   | **Cloud sync** of personal ocean → query from anywhere | — | ✅ | ✅ |
   | **Web + mobile** access; **cross-device** desktop sync | — | ✅ | ✅ |
   | Connectors **running in the cloud 24/7**; **add via web** (cloud-side ingest) | — | ✅ | ✅ |
   | **Managed LLM** synthesis (else BYOK / local model) | — | ✅ | ✅ |
   | **Team/shared oceans**, co-presence, offboarding | — | — | ✅ |

5. **Surfaces:** desktop = primary engine. Web/mobile = premium clients of the cloud replica
   (keep the Light-Editorial web UI already built; point it at the cloud). MCP server connects to
   the **local** engine (desktop) for personal use, cloud for shared.

## Relationship to ADR-0002

ADR-0002 stays valid for the **model** (`bge-small-en-v1.5`, 384-d, ONNX, CPU) and for the
**cloud vector store** (Postgres + pgvector — proven at multi-tenant scale, kept deliberately;
see ADR-0005 Q5). What this ADR overturns is ADR-0002's implication that embedding/storage is
*only* server-side: embeddings also run **on-device**, and the **desktop has its own local store**
(decided in ADR-0005).

## Rationale

- **Local-first is the product thesis** (offline, "your data starts and can stay on your machine,"
  the free tier as a genuine — not crippled — product). Reasserting it fixes the drift.
- **Server-trust cloud ships faster and is better for the premium experience:** server-side ANN
  query scales to millions of corals with no whole-ocean client download; mobile-trivial; enables
  24/7 cloud connectors; keeps the **proven Postgres** backend that already exists; recovery is a
  password reset, not a lost-key catastrophe.
- **The axes are separable:** free/local stays fully private regardless; only premium users who
  *opt in* to cloud sync accept the server-trust posture. Honest and defensible.

## Consequences

**Positive:** restores offline + private-by-default; keeps the working Postgres cloud unchanged;
fast, scalable premium query; clean tier ladder; no key-recovery/data-loss landmine.

**Negative / risks:** the operator **can** read premium cloud data → breach blast radius is real,
and we are subpoena-able; the privacy claim must be worded precisely (not "we can't see it").
Two SQL dialects (Postgres cloud / libsql desktop) and a **custom sync engine** to build and
operate (ADR-0005). The cloud and desktop run the same engine logic in different trust roles —
role config must be explicit so the cloud is never mistaken for the private local mode.

## Migration plan (phased)

1. **`Store` trait + engine extraction** — put backend data access behind a trait (Postgres impl
   first); make the ingest/embed/query/extract engine host-agnostic.
2. **Desktop engine + libsql store** — implement the trait on libsql; run the engine in Tauri
   against a local **encrypted** libsql. Free tier (offline, local) works end-to-end.
3. **Sync engine** — oplog + push/pull deltas; append corals / LWW reefs / tombstones+trash;
   per-device auth. Premium.
4. **Cloud hardening + premium surfaces** — master key → KMS; cloud-side 24/7 connectors;
   web/mobile as premium clients.
5. **Team oceans** — shared-key crypto, co-presence, offboarding.

## Alternatives considered

- **Zero-knowledge blind-relay cloud (the first draft).** Rejected for v1: key recovery =
  data-loss risk, cross-device key distribution UX, whole-ocean client download doesn't scale on
  mobile, and it forbids cloud-side connectors. Retained as a **possible future enterprise
  "zero-knowledge mode."**
- **Stay cloud-first / no local plane.** Rejected: abandons the local-first thesis and the
  genuinely-private free tier.

## References

- System design artifacts (Architecture Overview; Team Graph Sync; Free vs Paid Tiers)
- ADR-0002 (vector store & embedding); ADR-0005 (local store, sync, encryption)
- `packages/core` (`embed_ort.rs`, `kek.rs`); `desktop/tauri.conf.json`; `fly.toml`
