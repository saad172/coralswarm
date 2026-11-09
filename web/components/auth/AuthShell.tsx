import type { ReactNode } from "react";
import { Wordmark } from "@/components/ui";
import HostChip from "./HostChip";

/* Shared page shell for /login and /signup (4a/4b/4c): the reef-bg canvas,
 * the floated host chip (top-left, matching the mocks), and the centered
 * wordmark above whatever Clerk surface the page renders. Kept here so both
 * pages stay thin — see the task brief's "keep page files thin" instruction.
 *
 * The "Signing in to …" note that used to sit under the wordmark is gone
 * (claim-based tenancy PR 2.1). It read the plane off the hostname, and with
 * every tenant served from one apex there is no plane to read: on
 * `app.<apex>` it said "Signing in to your personal account", which is now
 * where org members sign in too. A sentence that is false for half the people
 * reading it is worse than no sentence. There is nothing to replace it with
 * either — the visitor is signed out, so the app has no membership to name an
 * org from, and a public slug→org lookup would be exactly the existence oracle
 * removing subdomains just got rid of. Which tenant you land in is decided
 * AFTER auth, by the Clerk claim, and is surfaced by the in-app org indicator
 * (PR 2.5). */
export default function AuthShell({ children }: { children: ReactNode }) {
  return (
    <div className="reef-bg relative min-h-screen bg-bg-2 px-6">
      <div className="absolute left-5 top-[18px]">
        <HostChip />
      </div>
      <div className="grid min-h-screen place-items-center py-10">
        <div className="w-full max-w-[420px]">
          <div className="mb-[26px] flex flex-col items-center gap-2 text-ink">
            <Wordmark size={24} />
          </div>
          {children}
        </div>
      </div>
    </div>
  );
}
