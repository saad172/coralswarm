# ADR-0008: Desktop capture stays local; users sync chosen summaries

**Date:** 2026-09-14
**Status:** Superseded by [ADR 0015](0015-retire-desktop-app.md)
**Decider:** Alex
**Builds on:** ADR-0005 (local encrypted store), ADR-0007 (ocean tenancy)

## Context

Desktop captures and describes work on-device. Its raw-OCR uploader has no destination by
decision. Treating every unuploaded observation as unresolved made the seven-day fuse stop every
fresh installation, even when its window documents were complete. The user wants selected
half-hour sessions or whole days available in their member-private cloud ocean.

## Decision

Capture and documentation stay local by default. Only an explicit window/day sync uploads the
model-written title, tasks, decisions, follow-ups and safe app metadata as one `work_session`
coral per window. Raw OCR, paths, domains, file names and focused-window titles are absent from
the wire payload. A revocable desktop token lives only in the OS keychain; its public target
metadata lives in a 0600 sidecar. A durable pending journal prevents a process crash from
pairing a new token with an old API origin. Rust changes the target without returning the token.
Credential clear/change waits for an active HTTP request and prevents every later POST.

Own personal/member-private oceans are exempt from org monitoring attestation. Ambient capture
uses the local primary personal ocean and allows absent cloud policy; live deny lists apply when
available. Manual cloud-targeted capture retains its policy refusal. Observations are resolved
once their enclosing window description reaches a terminal state. The seven-day fuse therefore
signals stalled description, and is checked before OCR. Existing 14-day local retention stays.

This supersedes ADR-0007's no-destination-picker reading **for desktop capture only**: the user
chooses which windows/days and their own private cloud target. Other ocean tenancy rules remain.

## Consequences

Retries retain a stable source id/hash, including after accepted requests lose their response.
Changed summaries create a new revision coral; old-revision retraction remains an explicit future
flow. Selected cloud summaries become available to cloud Ask; this does not add automatic local
ocean ingest. Org promotion of screen-derived reefs requires a follow-up monitoring-notice gate.
See [unit 16](../plans/capture/16-window-sync.md) for schema, state machine and IPC contracts.
