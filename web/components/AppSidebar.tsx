"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useUser } from "@clerk/nextjs";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { orgSelfservePendingQueryOptions, prefetchDestination } from "@/lib/appQueries";
import { accountLabel } from "@/lib/profile";
import { MAIN, UTILITIES, isActive, type NavItem } from "@/lib/nav";
import {
  DotMandala,
  CountPill,
  Avatar,
  Eyebrow,
  ErrorNote,
  Popover,
  MenuItem,
  Skeleton,
  activeRow,
  cn,
} from "@/components/ui";
import { IconChevronUpDown, IconAdd, IconSettings } from "@/components/icons";
import ChatHistory from "@/components/ChatHistory";
import { useInstanceSwitcher } from "@/components/useInstanceSwitcher";
import {
  tenantIndicator,
  tenantIndicatorAriaLabel,
  tenantSwitcherAriaLabel,
} from "@/components/tenantIndicator";
import { useJoinRequestCount } from "@/components/org/useJoinRequestCount";
import { useSelfserveEnabled } from "@/components/org/useSelfserveEnabled";
import { CREATE_PATH, RETURN_PATH, pendingTeamTarget, readAttempts, signupStore, type SignupAttempt } from "@/lib/orgSelfserve";

/* The 224px sidebar — the redesign's primary navigation surface.

   Anatomy, top to bottom (AppSidebar.dc.html):
     account switcher · new chat · the four places · chat history ·
     utilities · the user row

   No contextual sub-list any more. Workspaces' oceans live on the Workspaces
   page — a rail whose point is
   STABLE places shouldn't grow and shrink a second level as you move,
   and the sub-list pushed the always-present chat history down whenever it
   appeared.

   Two details that are easy to lose and matter:

   1. INACTIVE ITEMS CARRY A TRANSPARENT 1px BORDER. The active item is white
      with a real border and `shadow-row`; if inactive items had no border,
      every item would jump a pixel as selection moved. `activeRow` in the kit
      encodes this — do not hand-roll the states here.

   2. Navigation is <Link>, never a router push, so ⌘-click and middle-click
      work and Shell's document-level click interceptor (open a chat in place
      on /chats) sees every internal navigation. */

function NavLink({
  item,
  active,
  count,
  countLabel,
  onWarm,
}: {
  item: NavItem;
  active: boolean;
  count?: number;
  /** Accessible name for `count` — required context when the count alone
   *  (a bare number) doesn't say what it's counting, e.g. CS-438's
   *  join-request badge on Settings. */
  countLabel?: string;
  /** Hover or keyboard focus: warm the destination's first read. */
  onWarm?: (href: string) => void;
}) {
  const { href, label, Icon } = item;
  return (
    <Link
      href={href}
      onMouseEnter={onWarm ? () => onWarm(href) : undefined}
      onFocus={onWarm ? () => onWarm(href) : undefined}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex items-center gap-[9px] rounded-row px-[10px] py-[6px] text-[13px]",
        activeRow(active),
      )}
    >
      <span className="grid w-[15px] flex-none place-items-center">
        <Icon width={15} height={15} />
      </span>
      <span className="min-w-0 flex-1 truncate">{label}</span>
      {count ? <CountPill ariaLabel={countLabel}>{count}</CountPill> : null}
    </Link>
  );
}

export default function AppSidebar({ onNewChat }: { onNewChat?: () => void }) {
  const pathname = usePathname();
  const router = useRouter();
  const { userId, authReady, getAuthToken, queryScope, graphs } = useApp();
  const queryClient = useQueryClient();
  // Warm a destination's data while the pointer is on its link, with the same
  // query factories the page reads, so the click lands on a filled cache.
  const warm = (href: string) =>
    prefetchDestination(queryClient, href, { getAuthToken, scope: queryScope, authReady, graphIds: graphs.map((g) => g.id) });
  const instance = useInstanceSwitcher();
  // Which tenant to name, and — the part that used to be wrong — whether that
  // is known yet. See components/tenantIndicator.ts.
  const tenant = tenantIndicator(instance);
  const { user } = useUser();
  // CS-438 (D13): the open join-request count on the Settings utility row —
  // `null` for a non-admin/personal account, so the pill only ever appears
  // for an org admin with something waiting.
  const joinRequestCount = useJoinRequestCount();
  // Self-serve team sign-up: "Create a team" in the switcher, only while the
  // server says sign-up is open (`false` on the flag-off 404, on error and
  // while unknown). It also makes the switcher clickable for a personal-only
  // user, who otherwise has nothing to open it for.
  const selfserve = useSelfserveEnabled();
  // Teams still being set up: the ones THIS person started in this browser
  // (their sign-up attempts are dropped once ready, or on "Start again"),
  // plus, from a backend that sends it, bootstrap's `pending_orgs` (#1540;
  // read from the boot seed only, never fetched). Clerk lists such a team
  // from the moment it is created, but switching into it would hit the
  // binding gate (403 / 503 provisioning) at boot, so picking it opens its
  // setup page instead, but only while sign-up is on: with it off that page
  // can't help, and a plain switch must still reach a finished team. Read
  // after mount: localStorage has no SSR value.
  const [storedTeams, setStoredTeams] = useState<SignupAttempt[]>([]);
  useEffect(() => {
    setStoredTeams(readAttempts(signupStore(userId)).filter((a) => !!a.orgId));
  }, [userId]);
  const serverPending = useQuery({
    ...orgSelfservePendingQueryOptions({ getAuthToken, scope: queryScope }),
    enabled: false,
  }).data;
  const [switcherOpen, setSwitcherOpen] = useState(false);
  // Handed to Popover so clicking the switcher while it is open closes it
  // rather than closing and reopening in the same gesture.
  const switcherTriggerRef = useRef<HTMLButtonElement>(null);
  // Surfaces a rejected `switchTo` (see instanceSwitch.ts — a failed switch
  // never reloads, so this is the only feedback the user gets that nothing
  // actually changed). Cleared at the start of every new attempt so a stale
  // failure doesn't linger once the user tries again.
  const [switchError, setSwitchError] = useState<string | null>(null);
  // True from the moment an account switch is requested until it either
  // rejects or reloads the document. `performInstanceSwitch` awaits Clerk's
  // `setActive` before reloading, and Clerk gives no serialization guarantee
  // between overlapping calls — so without this, closing the popover after
  // picking an account re-enables the trigger, and a second switch started
  // before the first settles can have its `setActive` resolve first and
  // reload the page on the WRONG tenant. There is deliberately no success
  // path that clears this: a successful switch reloads the document, which
  // resets all component state for free. Only the rejection path resets it.
  const [switching, setSwitching] = useState(false);

  // `userId` is a raw Clerk id, so its initial is meaningless ("u" for every
  // account) — derive the label the profile page shows, keeping the id as the
  // pre-load fallback (same call as the old Shell).
  const userName = accountLabel(
    {
      fullName: user?.fullName,
      username: user?.username,
      primaryEmail: user?.primaryEmailAddress?.emailAddress,
    },
    userId ?? "You",
  );


  return (
    <nav
      aria-label="Primary"
      className="flex h-full w-[224px] min-h-0 flex-none flex-col border-r border-line bg-bg-2 text-ink"
    >
      {/* INSTANCE switcher — your personal CoralSwarm vs an organization you
          belong to. Not an ocean picker: an instance is a whole tenant, with
          its own database, members and policy; oceans live inside one and are
          chosen from the Workspaces rail. Switching ocean re-scopes a query,
          switching instance changes whose data you are looking at, so the two
          do not belong in one control.

          A personal-only user gets a plain label with no dropdown chrome —
          nothing to click, nothing to mis-click.

          THREE BRANCHES, AND THE ORDER MATTERS. `loaded` is checked FIRST,
          before `soloInstance`. Both of the two branches that used to be here
          printed `instance.active?.name ?? "Personal"`, and `active` is null
          for as long as Clerk is loading — so a member of three organizations
          was told, in confident semibold, that they were in their personal
          account, moments before the app finished opening an org. A wrong
          tenancy signal is worse than none: someone who sees nothing goes and
          checks, someone who sees "Personal" does not. Checking `loaded` first
          also fixes a second, quieter lie — the hook reports
          `soloInstance: true` while loading, so a multi-org user was shown the
          UNCLICKABLE solo treatment first and the switcher only appeared
          afterwards. */}
      <div className="relative px-[11px] pb-[9px] pt-[12px]">
        {tenant.state === "unknown" ? (
          <div
            role="status"
            aria-busy="true"
            aria-label={tenantIndicatorAriaLabel(tenant)}
            className="flex w-full items-center gap-[8px] px-[1px]"
          >
            {/* The mark stays: it is the same for every account, so it names
                nobody, and keeping it holds the row's height and rhythm so the
                sidebar does not jump when identity lands. */}
            <DotMandala size={22} density={0.8} />
            <Skeleton w={120} h={11} />
          </div>
        ) : instance.soloInstance && !selfserve ? (
          <div className="flex w-full items-center gap-[8px] px-[1px]">
            <DotMandala size={22} density={0.8} />
            <span className="min-w-0 flex-1 truncate text-[13px] font-semibold">{tenant.label}</span>
          </div>
        ) : (
          <button
            ref={switcherTriggerRef}
            type="button"
            onClick={() => setSwitcherOpen((v) => !v)}
            disabled={switching}
            aria-haspopup="menu"
            aria-expanded={switcherOpen}
            aria-label={tenantSwitcherAriaLabel(tenant)}
            className="flex w-full items-center gap-[8px] rounded-row px-[1px] text-left transition-colors hover:bg-bg-hover disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:bg-transparent"
          >
            <DotMandala size={22} density={0.8} />
            <span className="min-w-0 flex-1 truncate text-[13px] font-semibold">{tenant.label}</span>
            <span className="flex-none text-ink-3">
              <IconChevronUpDown width={14} height={14} />
            </span>
          </button>
        )}

        <Popover
          open={switcherOpen}
          onClose={() => setSwitcherOpen(false)}
          triggerRef={switcherTriggerRef}
          className="left-[11px] right-[11px]"
        >
          <div className="px-[12px] pb-[4px] pt-[6px]">
            <Eyebrow>accounts</Eyebrow>
          </div>
          {instance.instances.map((it) => (
            <MenuItem
              key={it.id}
              active={it.id === instance.active?.id}
              disabled={switching}
              disabledReason="Switching accounts…"
              onClick={() => {
                setSwitcherOpen(false);
                setSwitchError(null);
                const setup =
                  it.kind === "org" ? pendingTeamTarget(it.id, { selfserve, stored: storedTeams, serverPending }) : null;
                if (setup) {
                  window.location.assign(`${RETURN_PATH}?${setup}`);
                  return;
                }
                // Set before calling switchTo, not after: the trigger and
                // every MenuItem must already be disabled by the time this
                // handler returns, so a click on a SECOND account can't slip
                // in while this one's `setActive` is still in flight. Clerk
                // doesn't guarantee overlapping switches settle in the order
                // they were started, so without this an earlier-completing
                // second switch could reload the page on the wrong tenant.
                setSwitching(true);
                // Not `void`: switchTo resolves only after the reload has
                // been asked for, so on a SUCCESSFUL switch there is nothing
                // left for this handler to do — the document is on its way
                // out. But a rejected switch never reloads (instanceSwitch.ts
                // propagates it precisely so a caller can surface it), and an
                // uncaught rejection here would both spam the console and
                // leave the user stuck on their previous account with no
                // indication the click did nothing. A rejection is also the
                // only path that needs to re-enable the switcher — a success
                // reloads the document, which resets this state for free.
                instance.switchTo(it).catch((e: unknown) => {
                  setSwitching(false);
                  setSwitchError(
                    e instanceof Error && e.message
                      ? e.message
                      : "Couldn't switch accounts — try again.",
                  );
                });
              }}
            >
              <span className="min-w-0 flex-1 truncate">{it.name}</span>
              {it.kind === "personal" && (
                <span className="flex-none font-mono text-[9.5px] text-ink-4">you</span>
              )}
              {it.kind === "org" && pendingTeamTarget(it.id, { selfserve, stored: storedTeams, serverPending }) && (
                <span className="flex-none font-mono text-[9.5px] text-ink-4">setting up</span>
              )}
            </MenuItem>
          ))}
          {selfserve && (
            <div className="mt-[4px] border-t border-line-2 pt-[4px]">
              <Link
                href={CREATE_PATH}
                onClick={() => setSwitcherOpen(false)}
                className="flex w-full items-center gap-[8px] px-[12px] py-[7px] text-left text-[12.5px] text-ink-2 transition-colors hover:bg-bg-hover hover:text-ink"
              >
                <span className="flex-none text-brand">
                  <IconAdd width={13} height={13} strokeWidth={2} />
                </span>
                <span className="min-w-0 flex-1 truncate">Create a team</span>
              </Link>
            </div>
          )}
        </Popover>

        {/* Outside the Popover on purpose: a failed switch closes the menu
            (it closes eagerly on click, before switchTo even settles), so an
            error rendered inside it would never be seen. */}
        {switchError && <ErrorNote className="mt-[8px]">{switchError}</ErrorNote>}
      </div>

      {/* New chat. There is no Search button: finding a source is a chat answer,
          and the Cmd-K title search is a hidden fallback (CommandPalette). */}
      <div className="flex flex-col gap-[5px] px-[11px] pb-[8px]">
        <Link
          href="/chats"
          onClick={onNewChat}
          className="flex items-center gap-[7px] rounded-field border border-line bg-bg px-[10px] py-[7px] text-[13px] font-medium shadow-card transition-colors hover:border-ink-3"
        >
          <span className="flex-none text-brand">
            <IconAdd width={15} height={15} strokeWidth={2} />
          </span>
          New chat
          <span className="ml-auto font-mono text-[10px] text-ink-4">⌘N</span>
        </Link>
      </div>

      {/* The four places */}
      <div className="flex flex-col gap-px px-[11px] pb-[6px]">
        {MAIN.map((item) => (
          <NavLink key={item.href} item={item} active={isActive(pathname, item.href)} onWarm={warm} />
        ))}
      </div>


      {/* Chat history — always present, not gated on being at /chats. A
          conversation is the thing you most often want to get back to from
          elsewhere in the app, so it takes the sidebar's flexible middle and
          scrolls on its own rather than pushing the utilities off-screen. */}
      <ChatHistory />

      {/* Utilities */}
      <div className="flex flex-col gap-px px-[11px] pb-[8px]">
        {UTILITIES.map((item) => (
          <NavLink
            key={item.href}
            item={item}
            // CS-438 (D13): "Settings" is the one nav entry an org admin
            // sees on every page — the join-request count rides its
            // existing `count` prop rather than a new nav item.
            count={item.href === "/settings" ? (joinRequestCount ?? undefined) : undefined}
            countLabel={
              item.href === "/settings" && joinRequestCount
                ? `${joinRequestCount} ${joinRequestCount === 1 ? "person" : "people"} waiting to join`
                : undefined
            }
            active={isActive(pathname, item.href)}
            onWarm={warm}
          />
        ))}
      </div>

      {/* User row */}
      <div className="flex items-center gap-[8px] border-t border-line px-[11px] py-[9px]">
        <Avatar name={userName} size={24} />
        <span className="min-w-0 flex-1 truncate text-[12.5px] font-medium">{userName}</span>
        <button
          type="button"
          onClick={() => router.push("/settings")}
          aria-label="Account settings"
          className="flex-none text-ink-3 transition-colors hover:text-ink"
        >
          <IconSettings width={15} height={15} />
        </button>
      </div>
    </nav>
  );
}
