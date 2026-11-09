"use client";

import { createContext, useContext } from "react";

/* "New chat" lives in the shell (sidebar button, ⌘N) but the state it has to
   clear — the open conversation and its turns — lives inside the /chats page.
   Navigation alone cannot bridge that gap:

   - The page tracks the open conversation in `?id=`, which it writes with a raw
     `history.replaceState`. The router therefore still believes the URL is a bare
     `/chats`, so pushing `/chats` is a no-op it never re-renders for.
   - Even when the router does know about `?id=`, the page is the same route, so
     it is re-rendered rather than remounted; nothing resets on its own.

   So the shell bumps a counter instead, and the page treats any change as
   "start over". Cross-route clicks need none of this — arriving at /chats from
   elsewhere mounts a fresh thread — which is why the sidebar entry stays a real
   <Link>: it keeps ⌘-click and middle-click working. */
export const NewChatSignalContext = createContext(0);

/** Monotonic counter; a change means the shell asked for a fresh chat. */
export function useNewChatSignal(): number {
  return useContext(NewChatSignalContext);
}

/** Take `?id=` off the Chats URL — both places that track it.
 *
 *  The address bar needs the raw replaceState because the page wrote the id
 *  there behind the router's back; the router needs its own replace because
 *  when it DOES hold the id (you arrived from a chat-history link) that is what
 *  makes `useSearchParams` report it as gone. Neither one covers both cases,
 *  which is why every caller does both.
 *
 *  Pass `router.replace` wrapped in an arrow — an unbound method loses `this`. */
export function clearChatIdFromUrl(replace: (href: string) => void): void {
  if (typeof window !== "undefined" && window.location.search) {
    window.history.replaceState(null, "", "/chats");
  }
  replace("/chats");
}

/** Open a saved chat while already on /chats: push its `?id=` and let the
 *  page's restore effect pick it up (Next mirrors a native pushState into
 *  `useSearchParams`, as the page's own replaceState relies on). The pathname
 *  is kept as is, trailing slash included. Returns false when not on /chats
 *  or `href` is not a chat, and the caller navigates as usual. */
export function openChatInPlace(href: string): boolean {
  if (typeof window === "undefined" || !/^\/chats\/?$/.test(window.location.pathname)) return false;
  const query = /^\/chats\/?(\?id=.*)$/.exec(href)?.[1];
  if (!query) return false;
  window.history.pushState(null, "", `${window.location.pathname}${query}`);
  return true;
}

/** A new chat, optionally seeded with a question. `?q=` prefills the composer
 *  and never auto-sends (see ChatComposer's `initialText`): the user reviews
 *  the question and presses send. Used by Home's ask bar and the Cmd-K
 *  fallback's "Ask instead" row. */
export function newChatHref(query?: string): string {
  const q = query?.trim();
  return q ? `/chats?q=${encodeURIComponent(q)}` : "/chats";
}
