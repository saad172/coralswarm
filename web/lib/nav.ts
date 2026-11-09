import type { ComponentType, SVGProps } from "react";
import type { Hue } from "@/components/ui";
import {
  IconChats,
  IconCurrents,
  IconAtlas,
  IconToday,
  IconWorkspaces,
  IconConnectors,
  IconSettings,
  IconCoral,
} from "@/components/icons";

/* Single source of truth for app navigation.
   Consumed by the sidebar (components/AppSidebar.tsx), the mobile tab bar
   (components/MobileNav.tsx), the More launcher and the header titles, so
   route/label/hue/icon never drift across surfaces.

   THE MODEL. The old rail listed twelve destinations (Ask, Reefs, Atlas,
   Review, Currents, Team, Add corals, Connectors, Workflows, Teams, Coding
   sessions, Meetings, Settings): a list of every feature, not a model of the
   product. It collapsed to four PLACES plus two utilities:

     Home · Chats · Today · Workspaces          (+ Connectors, Settings)

   - Home is where you land: a launchpad. Ask first, then your own recent
     work to continue and the reefs that formed this week. It uses the
     `currents` glyph because the design system has no home icon.
   - Chats is where you ask. Finding one specific source is a chat answer (it
     returns source result tiles); a hidden Cmd-K title search is the fallback
     (components/CommandPalette.tsx), deliberately not shown anywhere in the UI.
   - Today is what just happened.
   - Workspaces is who can see what. Reefs live inside it, and a reef's detail
     page is the Topic page.

   There is NO Library. It was a browsable index of sources, and it was removed:
   Home's "Recently ingested" feed, chat find results and Today's cards open the
   shared detail panel (components/detail/DetailPanel.tsx), and person, topic
   (reef), source, meeting and session detail pages remain, reached from those.
   Nothing links to an index page; the old `/library*` URLs redirect to /home.

   Graph (`/graph`) is unlisted. The 3.2 canvas still exists for a typed URL
   and for local inspect (`NEXT_PUBLIC_GRAPH_PAGE=true` puts it back on the
   rail). It is not a product surface: FA2 on a full ocean is too laggy to
   ship as a place.

   Hue semantics (docs/design/DESIGN_SYSTEM.md): coral = decisions/brand,
   pink = people, amber = sources/candidates, teal = ingested content,
   plum = systems. */

export type Icon = ComponentType<SVGProps<SVGSVGElement>>;

export type NavItem = {
  href: string;
  label: string;
  hue: Hue;
  Icon: Icon;
  /** Sublabel for the mobile More launcher. */
  meta?: string;
};

/** Local-only rail listing for the unlisted `/graph` canvas. Off unless the
 *  Next process is started with `NEXT_PUBLIC_GRAPH_PAGE=true`. The route
 *  itself stays typed-URL reachable either way. */
export const SHOW_GRAPH_PAGE = process.env.NEXT_PUBLIC_GRAPH_PAGE === "true";

const GRAPH_NAV: NavItem = {
  href: "/graph",
  label: "Graph",
  hue: "plum",
  Icon: IconAtlas,
  meta: "how everything connects",
};

/** The four places. Order is deliberate: Home is where you arrive, Chats is
 *  where you ask, Today is what just happened, Workspaces is who can see what.
 *
 *  "Review" used to sit between Today and Workspaces. It was removed because
 *  it never gated anything: every item on it resolved itself without a human.
 *  An UNSURE merge judgement expires into `GateOutcome::ReAdjudicate`
 *  (`entity_resolution.rs::gate_pair`) so the resolver simply looks again, and
 *  a pending decision is flipped to `review_status: 'confirmed'` by
 *  `deadline_sweep_decisions_in_tx` once past its deadline. The page was a
 *  veto, not a step, and an unexercised veto is just a chore with a badge. */
export const MAIN: NavItem[] = [
  { href: "/home", label: "Home", hue: "coral", Icon: IconCurrents, meta: "ask, continue, what's new" },
  { href: "/chats", label: "Chats", hue: "coral", Icon: IconChats, meta: "ask and follow up" },
  ...(SHOW_GRAPH_PAGE ? [GRAPH_NAV] : []),
  { href: "/today", label: "Today", hue: "plum", Icon: IconToday, meta: "what was worked on" },
  { href: "/workspaces", label: "Workspaces", hue: "pink", Icon: IconWorkspaces, meta: "boundaries and sharing" },
];

/** Pinned to the bottom of the sidebar, above the user row. */
export const UTILITIES: NavItem[] = [
  { href: "/connectors", label: "Connectors", hue: "amber", Icon: IconConnectors, meta: "sources & sync" },
  { href: "/settings", label: "Settings", hue: "plum", Icon: IconSettings, meta: "account, connections, privacy" },
];

/** Add sources — reachable from Home, Chats and Connectors rather than the rail. */
export const ADD: NavItem = {
  href: "/add",
  label: "Add sources",
  hue: "teal",
  Icon: IconCoral,
  meta: "paste, upload, or connect",
};

/* RETIRED ROUTES — deliberately NOT listed here.

   Routes the redesign renamed or archived still have to RESOLVE: links to them
   exist in old chats, in bookmarks, in the Teams
   add-in and in already-sent invitations. Each one is a stub page at
   `app/(app)/<old>/page.tsx` rendering `<RouteRedirect to="/<new>" />`.

   There used to be a `REDIRECTS` map here as well. It had ZERO consumers — the
   stubs each hardcode their own destination — so it was a second copy of the
   same facts with nothing keeping them in step, and it had already drifted: it
   claimed /atlas, /workflows and /microsoft-teams redirected, when those routes
   had no stub and 404'd in production. Removed rather than re-synced, because
   the stub's own location IS the source path and its `to` IS the destination;
   a central list can only ever restate that, or contradict it.

   To find every redirect: `grep -rl RouteRedirect app`.

   They are client-side stubs rather than `redirects()` in next.config.ts:
   the app was once built as a static export, which drops `redirects()`
   silently, and the stubs remain. */

/** Header titles, keyed by the leading path segment. Derived from the nav lists
 *  so a label rename updates the sidebar and the header together, with
 *  overrides for detail routes that aren't destinations. */
export const TITLES: Record<string, string> = {
  ...Object.fromEntries([...MAIN, ...UTILITIES].map((it) => [it.href.slice(1), it.label])),
  topics: "Topic",
  // Unlisted canvas — not in MAIN unless SHOW_GRAPH_PAGE. Keep a title so a
  // typed URL still labels the header.
  graph: "Graph",
  coral: "Item",
  add: ADD.label,
  more: "More",
  onboarding: "Set up",
  // Unlisted: reached from Settings -> Organization -> Members, not the rail.
  people: "People",
};

/** True when `href` is the active route — exact match, or a parent of the
 *  current path. Compares whole segments so `/home` cannot claim
 *  `/homework`, and tolerates trailing slashes. */
export function isActive(pathname: string | null | undefined, href: string): boolean {
  if (!pathname) return false;
  const p = pathname.replace(/\/+$/, "") || "/";
  const h = href.replace(/\/+$/, "") || "/";
  return p === h || p.startsWith(h + "/");
}
