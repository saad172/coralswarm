"use client";

/* "The chat list changed" — one signal, every surface that lists chats.

   Chats are listed in more than one place: the sidebar history
   (components/useConversations.ts) and the Chats landing list (useChatThread,
   rendered by RecentChats) share one cached query (lib/appQueries.ts), but
   the landing list keeps its own verified snapshot, and anything in Library
   may list them too. Deleting from one of them has to be visible in all of
   them, or the row you just removed is still sitting on screen a few hundred
   pixels away — indistinguishable from a delete that failed. The same goes
   the other way: a chat the first question just created, or the title the
   server later gives it, has to show up in the sidebar without a reload
   (useChatThread emits both).

   A module-level emitter rather than context or a prop chain: the surfaces are
   siblings in three different subtrees, and a delete button in a sidebar row
   should not have to be handed a callback by the app shell to say something the
   whole app cares about. The sidebar's listener turns it into a cache
   invalidation of the shared conversations entry.

   `deletedId` is carried deliberately: a listener that wants to react to ONE
   conversation disappearing — the Chats screen has to abandon the thread it has
   open — must not have to infer it by diffing lists. Inferring it is also
   wrong, because a freshly created conversation is held locally for a couple of
   seconds before the server list catches up, so "not in the new list" does not
   mean "gone". */

type Listener = (deletedId?: string) => void;

const listeners = new Set<Listener>();

/** Subscribe. Returns the unsubscribe function, for an effect's cleanup. */
export function onConversationsChanged(fn: Listener): () => void {
  listeners.add(fn);
  return () => {
    listeners.delete(fn);
  };
}

/** Announce a change — a chat created, renamed or deleted. Pass the id only
 *  when a conversation was deleted.
 *
 *  Iterates a copy: a listener is free to unsubscribe (or subscribe) while
 *  being notified, which mutating the live set mid-iteration would break. */
export function emitConversationsChanged(deletedId?: string): void {
  for (const fn of [...listeners]) fn(deletedId);
}
