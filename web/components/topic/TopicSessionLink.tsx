"use client";
import type { LibrarySessionKey } from "@/lib/api";

/** Focus ids carry a UUID, never the provider-local session id. */
export type SessionLinkFocus = `${"outcome" | "source" | "version"}-session:${string}`;

/** A captured session named on a statement. It used to link to Library's
 *  session view; with Library gone there is no session page, so it is plain
 *  text (the provider-qualified id stays visible for support). */
export function SessionLink({ session, focus }: { session: LibrarySessionKey | null | undefined; focus: SessionLinkFocus }) {
  if (!session) return null;
  return (
    <span data-library-focus={focus} className="break-words text-sm text-ink-2 [overflow-wrap:anywhere]">
      {`Captured session · ${session.session_id}`}
    </span>
  );
}
