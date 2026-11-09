// CoralSwarm meeting-share API client (CONTRACT B). Mirrors the fetch idiom in
// lib/api.ts (Bearer auth for authed calls); the public read needs no token at
// all — it's the whole point of a share link. Kept as its own module (rather
// than folded into `api`) so the public /shared view can import just this,
// with no Clerk-authed surface pulled in.

// Same per-call base derivation as lib/api.ts (org tenancy, topology T1) — see
// lib/apiBase.ts. Importing it here keeps the public /shared view free of the
// Clerk-authed surface while still resolving to the tenant the page is served
// from: a share opened on `<slug>.coralswarm.example` must be read from that org's
// API host, not the shared consumer plane.
import { apiBase } from "./apiBase";

export class SharesApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function req<T>(path: string, token: string | null, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { ...(init?.headers as Record<string, string>) };
  if (init?.body) headers["Content-Type"] = "application/json";
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(`${apiBase()}${path}`, { ...init, headers });
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      msg = body.error ?? msg;
    } catch {
      /* ignore */
    }
    throw new SharesApiError(msg, res.status);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export type ShareVisibility = "link" | "team";

/** One share row as listed for a meeting (`GET /v1/meetings/shares`). */
export interface Share {
  id: string;
  visibility: ShareVisibility;
  share_url: string;
  created_at: string;
  revoked_at: string | null;
  active: boolean;
}

/** Response from creating a new share — the only place the raw `token` is
 *  returned (it's embedded in `share_url` too; never log either). */
export interface CreateShareResult {
  id: string;
  token: string;
  visibility: ShareVisibility;
  share_url: string;
  created_at: string;
}

/** A decision line in a structured meeting summary. */
export interface ShareSummaryDecision {
  text: string;
  ts_ms?: number | null;
}

/** An action item in a structured meeting summary. */
export interface ShareSummaryActionItem {
  text: string;
  owner?: string | null;
  ts_ms?: number | null;
}

/** Structured meeting summary as produced by the meeting-summary pipeline
 *  (sibling branch cs-uxp3a / PR #208), stored as `meeting_transcripts.summary`
 *  JSONB and surfaced only once `summary_status = 'done'`. Every field is
 *  optional so this stays forward-compatible: the backend returns `null` until
 *  that branch lands, and a partial object still renders whatever it carries. */
export interface ShareSummary {
  overview?: string | null;
  decisions?: ShareSummaryDecision[] | null;
  action_items?: ShareSummaryActionItem[] | null;
}

/** The read-only payload rendered by the public share view. `title` and
 *  `transcript` are best-effort on the backend (`build_shared_payload` returns
 *  `null` when the transcript row/text can't be resolved), so they're modelled as
 *  nullable here — the share view already falls back with `||`. */
export interface PublicShare {
  title: string | null;
  occurred_at: string;
  duration_ms: number | null;
  summary: ShareSummary | null;
  transcript: string | null;
  visibility: ShareVisibility;
}

/** Create a share link for one ingested meeting (`source_id = "meeting {id}"`). */
export function createShare(
  token: string,
  args: { graphId: string; sourceId: string; visibility: ShareVisibility },
): Promise<CreateShareResult> {
  return req<CreateShareResult>("/v1/meetings/shares", token, {
    method: "POST",
    body: JSON.stringify({
      graph_id: args.graphId,
      source_id: args.sourceId,
      visibility: args.visibility,
    }),
  });
}

/** List every share (active + revoked) created for one meeting. */
export async function listShares(token: string, graphId: string, sourceId: string): Promise<Share[]> {
  const res = await req<{ shares: Share[] }>(
    `/v1/meetings/shares?graph_id=${encodeURIComponent(graphId)}&source_id=${encodeURIComponent(sourceId)}`,
    token,
  );
  return res.shares;
}

/** Revoke a share — the link/team stops resolving immediately. */
export async function revokeShare(token: string, shareId: string): Promise<void> {
  await req<{ ok: boolean }>(`/v1/meetings/shares/${encodeURIComponent(shareId)}/revoke`, token, {
    method: "POST",
  });
}

/** Public, unauthenticated read of a "link" share by its token. 404s for a
 *  revoked/unknown token or a "team" share (those need {@link fetchAuthedShare}). */
export function fetchPublicShare(shareToken: string): Promise<PublicShare> {
  return req<PublicShare>(`/v1/shared/${encodeURIComponent(shareToken)}`, null);
}

/** Authed read of a share by its token — resolves "link" shares too, plus
 *  "team" shares when the caller is a member of the owning ocean. */
export function fetchAuthedShare(shareToken: string, authToken: string): Promise<PublicShare> {
  return req<PublicShare>(`/v1/meetings/shares/view/${encodeURIComponent(shareToken)}`, authToken);
}
