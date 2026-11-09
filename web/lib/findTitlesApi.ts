// Typed client for the hidden Cmd-K title-only search:
//
//   GET /v1/find/titles?q=&limit=
//
// Contract: docs/implementation/find-results-api.md. Backend:
// backend/src/services/find_results.rs. The route answers 404 while
// CHAT_FIND_RESULTS_ENABLED is unset on the server, which `ApiError.status ===
// 404` carries here. Results are the same tile shape the chat answer's
// `source_results` carries (`SourceResult`, lib/scopedChatApi.ts), with
// `best` false and `quotes` empty.
//
// The query options follow lib/appQueries.ts: the bearer is read at fetch time
// and never part of a key; the key is `[resource, scope, ...]`.

import { queryOptions } from "@tanstack/react-query";
import { ApiError } from "./api";
import type { QueryCtx } from "./appQueries";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import type { SourceResult } from "./scopedChatApi";
import { guardedFetch } from "./transport";

export const FIND_TITLES_MAX_QUERY_CHARS = 200;
export const FIND_TITLES_DEFAULT_LIMIT = 10;
export const FIND_TITLES_MAX_LIMIT = 25;

export interface FindTitlesResponse {
  /** The trimmed query the server searched. */
  query: string;
  results: SourceResult[];
}

/** The query string of a title search. Exported for tests. */
export function findTitlesQuery(q: string, limit?: number): string {
  const qs = new URLSearchParams({ q: q.trim() });
  if (limit !== undefined) qs.set("limit", String(limit));
  return qs.toString();
}

/** `GET /v1/find/titles`. */
export async function fetchFindTitles(token: string, q: string, limit?: number, signal?: AbortSignal): Promise<FindTitlesResponse> {
  if (!token) throw new Error("Not authenticated");
  const res = await guardedFetch(
    `/v1/find/titles?${findTitlesQuery(q, limit)}`,
    { method: "GET", cache: "no-store", signal, headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() } },
    { signal },
  );
  if (!res.ok) {
    let message = `HTTP ${res.status}`;
    try {
      const body: unknown = await res.json();
      if (body && typeof body === "object" && typeof (body as { error?: unknown }).error === "string") {
        message = (body as { error: string }).error;
      }
    } catch {
      /* keep the status text */
    }
    throw new ApiError(message, res.status);
  }
  return (await res.json()) as FindTitlesResponse;
}

function bearer(ctx: QueryCtx): string {
  const token = ctx.getAuthToken();
  if (!token) throw new Error("Not authenticated");
  return token;
}

/** Titles matching `q`. A key is a different query, so there is no
 *  `keepPreviousData`. Disabled for a blank query. */
export function findTitlesQueryOptions(ctx: QueryCtx, q: string, limit?: number) {
  const trimmed = q.trim();
  return queryOptions<FindTitlesResponse>({
    queryKey: ["find", ctx.scope, "titles", { q: trimmed, limit: limit ?? null }] as const,
    queryFn: async ({ signal }) => fetchFindTitles(bearer(ctx), trimmed, limit, signal),
    enabled: trimmed.length > 0 && trimmed.length <= FIND_TITLES_MAX_QUERY_CHARS,
    staleTime: 30_000,
  });
}
