import type { ReactNode } from "react";
import { OFFICE_SRC } from "@/lib/addin/office";

/* Hints for the Outlook add-in panes. The pane's first job is a chain of network
 * steps (office.js, then the Microsoft token, then the CoralSwarm session
 * exchange), and until now none of it could start before the app bundle had
 * downloaded, parsed and hydrated. These are in the HTML, so the browser starts
 * them while it is still fetching the bundle:
 *   - office.js is preloaded (fetched now, executed when the pane asks for it);
 *   - the API origin is connected to ahead of the session exchange;
 *   - the Microsoft login host is only DNS-resolved, since the nested-app-auth
 *     path normally goes through the Outlook host and not the network. */
const API_ORIGIN = (() => {
  try { return new URL(process.env.NEXT_PUBLIC_API_BASE ?? "").origin; } catch { return null; }
})();

export default function OutlookLayout({ children }: { children: ReactNode }) {
  return (
    <>
      <link rel="preload" as="script" href={OFFICE_SRC} />
      {API_ORIGIN ? <link rel="preconnect" href={API_ORIGIN} crossOrigin="anonymous" /> : null}
      <link rel="dns-prefetch" href="https://login.microsoftonline.com" />
      {children}
    </>
  );
}
