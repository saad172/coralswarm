// Deep-link routes into the real web app, resolved by reading
// `web/app/(app)/**/page.tsx`'s actual `useSearchParams()` reads — see
// widgets/README.md "Deep links" for the full trail. `/reef` and `/ask` are
// dead routes (client-side redirects to `/topics`/`/chats` — see
// `web/components/RouteRedirect.tsx`); link straight to the real
// destination, never through the redirect.

const WEB_BASE = "https://app.coralswarm.example";

/** ask_ocean's "open in coralswarm" — `/chats?q=` prefills the composer with
 *  the original question without auto-sending (same mechanism Today's "Ask
 *  about this day" rail button uses, `lib/journal.ts`'s `askAboutDayHref`).
 *  There is no saved-conversation id an MCP `ask_ocean` call can link to. */
export function chatsHref(question?: string): string {
  if (!question) return `${WEB_BASE}/chats`;
  return `${WEB_BASE}/chats?q=${encodeURIComponent(question)}`;
}

/** get_reef's "open reef" — `/topics?id=` (the reef detail page; ocean is
 *  derived server-side from the reef id, never passed). */
export function topicHref(reefId: string): string {
  return `${WEB_BASE}/topics?id=${encodeURIComponent(reefId)}`;
}

/** A cite chip/source card's "source" link, and search_atoms results —
 *  `/coral?id=` (atom/coral detail page). */
export function coralHref(atomId: string): string {
  return `${WEB_BASE}/coral?id=${encodeURIComponent(atomId)}`;
}

/** search_atoms' "open library" — `/library/sources` does not accept a query
 *  prefill today (see README "Backend fields needed" / deep-link gaps), so
 *  this links to the plain listing. */
export function libraryHref(): string {
  return `${WEB_BASE}/library/sources`;
}

/** recent_activity's "open today" — matches the artboard's own link copy. */
export function todayHref(): string {
  return `${WEB_BASE}/today`;
}
