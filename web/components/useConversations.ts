"use client";

import { useQuery } from "@tanstack/react-query";
import type { ConversationSummary } from "@/lib/api";
import { conversationsQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { useConversationDeletions } from "@/lib/useConversationDeletions";

/* The caller's conversations.
 *
 * No `graph_id`: the listing is deliberately de-filtered across oceans
 * (ADR-0011 §C12), so switching workspace doesn't hide a chat you're looking
 * for.
 *
 * NOTE ON SEARCH: the endpoint does take a `q`, but its query is
 * `WHERE c.graph_id = $1 AND ... (c.title ILIKE $3 OR m.text ILIKE $3)` —
 * graph-scoped. A cross-ocean search (no graph_id) therefore can't work
 * through it, so this hook doesn't offer one. Finding a chat is the command
 * palette's job (⌘K); if chat search lands there, the backend needs an
 * un-graph-scoped search first. */

export function useConversations() {
  // Stable getter, not the bearer string — this hook is mounted in the
  // sidebar on EVERY route, so keying it on `token` re-downloaded the whole
  // conversation list on each ~60s Clerk rotation, in every open tab, forever.
  // The cache entry is shared with the Chats landing list (useChatThread), so
  // a cold load of /chats issues one `GET /v1/conversations`, not two.
  const { getAuthToken, authReady, queryScope } = useApp();
  const deleted = useConversationDeletions(queryScope);
  // Best-effort: the sidebar list is ambient, never an error surface, so a
  // failed read renders as the empty list.
  const query = useQuery({ ...conversationsQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady });
  const conversations = authReady ? (query.data ?? EMPTY).filter((row) => !deleted.has(row.id)) : EMPTY;

  return { conversations, error: query.error, loading: authReady && (query.isPending || (!query.error && query.data === undefined)), reload: query.refetch };
}

const EMPTY: ConversationSummary[] = [];

/** Pinned first, then most recently touched. Shared by every surface that
 *  lists chats so a conversation never sits in two different places. */
export function sortConversations(list: ConversationSummary[]): ConversationSummary[] {
  return [...list].sort((a, b) => {
    if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
    return +new Date(b.updated_at) - +new Date(a.updated_at);
  });
}
