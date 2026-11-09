import type { SVGProps } from "react";

type P = SVGProps<SVGSVGElement>;
const base = (props: P) => ({
  width: 18,
  height: 18,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.8,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
  ...props,
});

export const IconAsk = (p: P) => (
  <svg {...base(p)}>
    <circle cx="11" cy="11" r="7" />
    <path d="m20 20-3.5-3.5" />
  </svg>
);
export const IconReefs = (p: P) => (
  <svg {...base(p)}>
    <circle cx="6" cy="6" r="2.4" />
    <circle cx="18" cy="7" r="2.4" />
    <circle cx="12" cy="17" r="2.4" />
    <path d="M7.7 7.4 10.5 15M16.5 8.6 13.4 15.4M8 6.4h8" />
  </svg>
);
export const IconAtlas = (p: P) => (
  <svg {...base(p)}>
    <circle cx="12" cy="12" r="2.4" />
    <circle cx="5" cy="6" r="1.8" />
    <circle cx="19" cy="7" r="1.8" />
    <circle cx="6" cy="18" r="1.8" />
    <circle cx="18" cy="17" r="1.8" />
    <path d="M10 11 6.4 7.2M14 11l3.4-3.2M10.5 13.5 7 16.7M13.6 13.4l3.1 2.8" />
  </svg>
);
export const IconCurrents = (p: P) => (
  <svg {...base(p)}>
    <path d="M3 14c3-4 6 4 9 0s6-4 9 0" />
    <path d="M3 9c3-4 6 4 9 0s6-4 9 0" />
  </svg>
);
export const IconTeam = (p: P) => (
  <svg {...base(p)}>
    <circle cx="9" cy="8" r="3" />
    <path d="M3.5 19a5.5 5.5 0 0 1 11 0" />
    <path d="M16 6a3 3 0 0 1 0 6M20.5 19a5.5 5.5 0 0 0-3.5-5.1" />
  </svg>
);
export const IconAdd = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 5v14M5 12h14" />
  </svg>
);
export const IconSettings = (p: P) => (
  <svg {...base(p)}>
    <circle cx="12" cy="12" r="3" />
    <path d="M19 12a7 7 0 0 0-.1-1.3l1.8-1.4-2-3.4-2.1.8a7 7 0 0 0-2.2-1.3L12 2h-4l-.4 2.1a7 7 0 0 0-2.2 1.3l-2.1-.8-2 3.4 1.8 1.4A7 7 0 0 0 3 12" />
  </svg>
);
export const IconCoral = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 21V9M12 9c0-3 2-4 2-6M12 9c0-2-2-3-2-5M12 13c2 0 3-1 4-3M12 15c-2 0-3-1-4-3" />
  </svg>
);
export const IconChevron = (p: P) => (
  <svg {...base(p)}>
    <path d="m6 9 6 6 6-6" />
  </svg>
);
export const IconArrowDown = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 5v14M6 13l6 6 6-6" />
  </svg>
);
export const IconThumbUp = (p: P) => (
  <svg {...base(p)}>
    <path d="M7 10v10H4V10z" />
    <path d="M7 10l4-7c1.5 0 2.5 1 2.5 2.5V9h5a2 2 0 0 1 2 2.3l-1.2 7A2 2 0 0 1 17.3 20H7" />
  </svg>
);
export const IconThumbDown = (p: P) => (
  <svg {...base(p)}>
    <g transform="rotate(180 12 12)">
      <path d="M7 10v10H4V10z" />
      <path d="M7 10l4-7c1.5 0 2.5 1 2.5 2.5V9h5a2 2 0 0 1 2 2.3l-1.2 7A2 2 0 0 1 17.3 20H7" />
    </g>
  </svg>
);
export const IconLogout = (p: P) => (
  <svg {...base(p)}>
    <path d="M15 4h3a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1h-3M10 8l-4 4 4 4M6 12h11" />
  </svg>
);
export const IconPlug = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 22v-5M12 7V2M7 7h10l-1 5H8L7 7zM9 17h6" />
  </svg>
);
export const IconConnectors = (p: P) => (
  <svg {...base(p)}>
    <rect x="4" y="4" width="6" height="6" rx="1.4" />
    <rect x="14" y="4" width="6" height="6" rx="1.4" />
    <rect x="4" y="14" width="6" height="6" rx="1.4" />
    <path d="M14 17h6" />
    <path d="M17 14v6" />
  </svg>
);
export const IconWorkflows = (p: P) => (
  <svg {...base(p)}>
    <circle cx="6" cy="6" r="2.2" />
    <circle cx="18" cy="18" r="2.2" />
    <path d="M8 6h6a4 4 0 0 1 4 4v6" />
  </svg>
);
export const IconDots = (p: P) => (
  <svg {...base(p)}>
    <circle cx="5" cy="12" r="1.4" fill="currentColor" stroke="none" />
    <circle cx="12" cy="12" r="1.4" fill="currentColor" stroke="none" />
    <circle cx="19" cy="12" r="1.4" fill="currentColor" stroke="none" />
  </svg>
);
export const IconBack = (p: P) => (
  <svg {...base(p)}>
    <path d="M15 18l-6-6 6-6" />
  </svg>
);
export const IconTeams = (p: P) => (
  <svg {...base(p)}>
    <rect x="4" y="4" width="16" height="16" rx="3" />
    <path d="M9 9h6M9 13h4" />
  </svg>
);
export const IconMeetings = (p: P) => (
  <svg {...base(p)}>
    <rect x="3" y="5" width="13" height="14" rx="2" />
    <path d="m16 10 5-3v10l-5-3z" />
  </svg>
);
/** `</>` — coding sessions (Home's Continue rows). */
export const IconCode = (p: P) => (
  <svg {...base(p)}>
    <path d="m7 8-4 4 4 4M17 8l4 4-4 4M14 4l-4 16" />
  </svg>
);
export const IconSessions = (p: P) => (
  <svg {...base(p)}>
    <rect x="3" y="4" width="18" height="16" rx="2" />
    <path d="m7 10 3 2.5-3 2.5M13 15h4" />
  </svg>
);
export const IconSidebar = (p: P) => (
  <svg {...base(p)}>
    <rect x="3" y="4" width="18" height="16" rx="2.5" />
    <line x1="9" y1="4" x2="9" y2="20" />
  </svg>
);

/* ---------------------------------------------------------------------------
   Redesign icon set.

   Ported from the `ICONS` map in the handoff's AppSidebar.dc.html. Same
   geometry, expressed with <rect rx> where the mock used a square path, so the
   corners match the 1.8 stroke instead of coming to a point. Rendered at
   14–15px by the shell; `base` already carries the 24×24 / 1.8 / round
   contract.

   IconAsk (the search glyph), IconReview, IconAdd, IconConnectors,
   IconSettings, IconMeetings and IconSessions above already match the mock and
   are reused as-is rather than duplicated here.
   --------------------------------------------------------------------------- */

/** Chats — the primary destination. */
export const IconChats = (p: P) => (
  <svg {...base(p)}>
    <path d="M4 6a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H9l-5 4z" />
  </svg>
);

/** Library — shelved spines. */
export const IconLibrary = (p: P) => (
  <svg {...base(p)}>
    <rect x="4" y="5" width="5" height="14" rx="1" />
    <rect x="11" y="5" width="4" height="14" rx="1" />
    <path d="m17.5 5.5 3 13" />
  </svg>
);

/** Today — a calendar day. */
export const IconToday = (p: P) => (
  <svg {...base(p)}>
    <rect x="5" y="5" width="14" height="14" rx="2" />
    <path d="M5 10h14M9 3v4M15 3v4" />
  </svg>
);

/** Workspaces — bounded areas over a shared base. */
export const IconWorkspaces = (p: P) => (
  <svg {...base(p)}>
    <rect x="4" y="7" width="6" height="6" rx="1.2" />
    <rect x="14" y="5" width="6" height="8" rx="1.2" />
    <rect x="4" y="16" width="16" height="4" rx="1.2" />
  </svg>
);

/** People — a Library facet, no longer a destination. */
export const IconPeople = (p: P) => (
  <svg {...base(p)}>
    <circle cx="9" cy="8" r="3" />
    <path d="M3.5 19a5.5 5.5 0 0 1 11 0M16 6a3 3 0 0 1 0 6M20.5 19a5.5 5.5 0 0 0-3.5-5.1" />
  </svg>
);

/** Search — ⌘K. Alias of IconAsk, named for the new rail. */
export const IconSearch = IconAsk;

/** Org / workspace switcher affordance. */
export const IconChevronUpDown = (p: P) => (
  <svg {...base(p)}>
    <path d="m8 9 4-4 4 4M8 15l4 4 4-4" />
  </svg>
);

/** Send / submit — the composer's dark circular button. */
export const IconSend = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 19V5M6 11l6-6 6 6" />
  </svg>
);

export const IconClose = (p: P) => (
  <svg {...base(p)}>
    <path d="M6 6l12 12M18 6 6 18" />
  </svg>
);

export const IconTrash = (p: P) => (
  <svg {...base(p)}>
    <path d="M5 7h14M10 4h4M9 7v11M15 7v11" />
    <path d="M6.5 7l.8 12a2 2 0 0 0 2 1.9h5.4a2 2 0 0 0 2-1.9l.8-12" />
  </svg>
);

export const IconCopy = (p: P) => (
  <svg {...base(p)}>
    <rect x="9" y="9" width="11" height="11" rx="2" />
    <path d="M5 15V6a2 2 0 0 1 2-2h8" />
  </svg>
);

export const IconRefresh = (p: P) => (
  <svg {...base(p)}>
    <path d="M20 11A8 8 0 0 0 6.3 6.3L4 8.5M4 5v3.5h3.5" />
    <path d="M4 13a8 8 0 0 0 13.7 4.7L20 15.5M20 19v-3.5h-3.5" />
  </svg>
);

/* Onboarding glyphs (turns 5–6). */

/** Inbox tray — "you hand it things". */
export const IconInbox = (p: P) => (
  <svg {...base(p)}>
    <path d="M12 16V4M8 8l4-4 4 4M4 16v4h16v-4" />
  </svg>
);

/** Padlock — a private, encrypted space. */
export const IconLock = (p: P) => (
  <svg {...base(p)}>
    <rect x="5" y="11" width="14" height="9" rx="2" />
    <path d="M8 11V8a4 4 0 0 1 8 0v3" />
  </svg>
);

/** Browser-window chip — the "in your browser" platform badge. */
export const IconBrowser = (p: P) => (
  <svg {...base(p)}>
    <rect x="3" y="5" width="18" height="14" rx="2" />
    <path d="M3 9h18" />
  </svg>
);

