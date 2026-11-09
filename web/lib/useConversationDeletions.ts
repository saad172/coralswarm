"use client";

import { useSyncExternalStore } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { ApiError } from "./api";
import { invalidate, invalidationFor } from "./appQueries";
import { onConversationsChanged } from "./conversationsChanged";
import { pageSectionPath } from "./pageSectionWire";
import { refusePageRead } from "./pageReadAuthority";

interface Deletions { ids: ReadonlySet<string>; listeners: Set<() => void>; stop?: () => void }
const stores = new WeakMap<QueryClient, Map<string, Deletions>>();
function deletionStore(client: QueryClient, scope: string) {
  let scopes = stores.get(client);
  if (!scopes) stores.set(client, scopes = new Map());
  let store = scopes.get(scope);
  if (!store) scopes.set(scope, store = { ids: new Set(), listeners: new Set() });
  return store;
}

/** A successful delete can remove a row immediately without rewriting a
 * server certificate. The mask only removes IDs and is scoped to the caller.
 * Every observed surface still waits on real Stored invalidation/refresh. */
export function useConversationDeletions(scope: string): ReadonlySet<string> {
  const client = useQueryClient();
  const store = deletionStore(client, scope);
  return useSyncExternalStore((listener) => {
    store.listeners.add(listener);
    store.stop ??= onConversationsChanged((id) => {
      if (id) {
        store.ids = new Set([...store.ids, id]);
        void refusePageRead(client, JSON.stringify([scope, pageSectionPath("conversation_detail", { id })]),
          new ApiError("This chat was deleted.", 404));
        store.listeners.forEach((notify) => notify());
        // The sidebar deletes while Home is on screen: its Continue drops the chat.
        void invalidate(client, invalidationFor.chatDeleted());
      }
      void client.invalidateQueries({ queryKey: ["conversations", scope] });
    });
    return () => {
      store.listeners.delete(listener);
      if (!store.listeners.size) { store.stop?.(); store.stop = undefined; }
    };
  }, () => store.ids, () => store.ids);
}
