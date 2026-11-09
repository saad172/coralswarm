"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";
import AppSidebar from "./AppSidebar";
import MobileNav from "./MobileNav";
import AccountIndicator from "./AccountIndicator";
import CommandPalette from "./CommandPalette";
import PromoteJobsIndicator from "./PromoteJobsIndicator";
import { HEADER_META_ID } from "./HeaderMeta";
import { TrialBanner } from "./billing/TrialBanner";
import { JoinRequestBanner } from "./join/JoinRequestBanner";
import { TITLES } from "@/lib/nav";
import { clearChatIdFromUrl, NewChatSignalContext, openChatInPlace } from "@/lib/newChat";

/* The app shell: 224px sidebar + 52px header + content.

   Replaces the old collapsible icon rail. The redesign fixes the sidebar open
   at 224px — with only four places plus two utilities there is nothing to
   collapse, and a stable rail means the content column never reflows.

   `fullBleed` routes (Chats) manage their own full-height layout and scroll,
   because the composer must pin to the bottom of the viewport rather than sit
   at the end of a scrolling column. */

/** Routes that own their full height and scrolling. `/graph` stays full-bleed
 *  even while unlisted so a typed URL still gets a viewport-owned canvas. */
const FULL_BLEED = ["/chats", "/graph"];

/** Routes that use the wider dashboard column (README: Home and Today max-width
 *  1160, padding 20px 28px 48px) instead of the default 1040 reading column. */
const WIDE = ["/home", "/today"];

/** Routes on the white canvas (README: Today's timeline sits on `--bg`; Home and
 *  Chat find use `--bg-2` so their white cards read as objects). */
const WHITE_CANVAS = ["/today"];

/** A plain left click on an internal, same-tab anchor: a chat link while on
 *  /chats switches chats in place. Anything else is left to the anchor and
 *  the router. */
export function onInternalAnchorClick(e: MouseEvent): void {
  if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
  const anchor = (e.target as HTMLElement | null)?.closest?.("a");
  const href = anchor?.getAttribute("href");
  // Internal, same-tab path navigations only; leave hashes/external/_blank alone.
  if (!href || !href.startsWith("/") || href.startsWith("//")) return;
  if (anchor?.target && anchor.target !== "_self") return;
  if (openChatInPlace(href)) {
    e.preventDefault();
    e.stopPropagation();
  }
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [newChatSignal, setNewChatSignal] = useState(0);
  const onChats = pathname?.startsWith("/chats") ?? false;

  /* "New chat" while already on Chats. The <Link>/router push cannot clear the
     open thread by itself (see lib/newChat.ts), so tell the page directly and
     take `?id=` back out of the address bar — the page put it there with a raw
     replaceState, so the router cannot. `router.replace` covers the other half:
     when the router DOES hold an `?id=` (arriving from a chat-history link), it
     is what makes `useSearchParams` report the id as gone. */
  const startNewChat = useCallback(() => {
    if (!onChats) return; // a cross-route <Link> click mounts a fresh thread
    clearChatIdFromUrl((href) => router.replace(href));
    setNewChatSignal((n) => n + 1);
  }, [onChats, router]);

  const seg = (pathname ?? "/").split("/").filter(Boolean)[0] ?? "";
  const title = TITLES[seg] ?? "";
  const fullBleed = FULL_BLEED.some((p) => pathname?.startsWith(p));
  const whiteCanvas = WHITE_CANVAS.some((p) => pathname === p || pathname?.startsWith(p + "/"));
  const wide = WIDE.some((p) => pathname === p || pathname?.startsWith(p + "/"));

  /* Internal anchor clicks, document-level and in the capture phase so every
     internal anchor in the app is covered, including ones rendered by pages
     that know nothing about this. */
  useEffect(() => {
    document.addEventListener("click", onInternalAnchorClick, true);
    return () => document.removeEventListener("click", onInternalAnchorClick, true);
  }, []);

  /* ⌘K title search (hidden fallback), ⌘N new chat. Bound at the shell so they work from any route. */
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key === "k") {
        e.preventDefault();
        setPaletteOpen(true);
      } else if (e.key === "n") {
        e.preventDefault();
        if (onChats) startNewChat();
        else router.push("/chats");
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [router, onChats, startNewChat]);

  return (
    <NewChatSignalContext.Provider value={newChatSignal}>
      <div className="flex h-screen overflow-hidden bg-bg-2">
        <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />

        {/* Sidebar — desktop only; phones get the bottom tab bar instead. */}
        <div className="hidden lg:flex">
          <AppSidebar onNewChat={startNewChat} />
        </div>

        <div className="flex min-w-0 flex-1 flex-col">
          {/* 52px app header.

              The trailing <AccountIndicator> is the small-screen half of the
              tenancy signal: below `lg` the sidebar above is not rendered, and
              the bottom tab bar carries no account name, so without this there
              is NOTHING on a phone that says which tenant you are in. That used
              to be the hostname's job — claim-based tenancy put every tenant on
              one apex, so it is now the app's job (see AccountIndicator.tsx).

              `lg:hidden` rather than "always": at `lg+` the sidebar's switcher
              already names the account, and showing it in both places would put
              the same word on screen twice.

              <PromoteJobsIndicator> is the background-job pill for a promote.
              It stays after its modal closes and renders nothing when idle.

              `ml-auto` + `min-w-0` on the trailing group is what keeps it from
              fighting the title: the title takes the space it needs and
              truncates, the group pins right, and neither can push the other
              out of the header at any width. */}
          <header className="flex h-[52px] flex-none items-center gap-3 border-b border-line-2 bg-bg px-[20px]">
            <span className="min-w-0 truncate text-[14px] font-medium tracking-[-0.01em] text-ink">{title}</span>
            <div className="ml-auto flex min-w-0 items-center gap-[8px]">
              <span id={HEADER_META_ID} className="flex min-w-0 items-center" />
              <PromoteJobsIndicator />
              <AccountIndicator className="max-w-[52%] lg:hidden" />
            </div>
          </header>

          <JoinRequestBanner />
          <TrialBanner />

          <main data-app-scroll={fullBleed ? undefined : ""} className={
              fullBleed
                ? "min-h-0 flex-1 bg-bg"
                : whiteCanvas
                  ? "min-h-0 flex-1 overflow-y-auto bg-bg"
                  : "min-h-0 flex-1 overflow-y-auto bg-bg-2"
            }>
            {fullBleed ? (
              children
            ) : wide ? (
              <div className="mx-auto max-w-home px-[28px] pb-[48px] pt-[20px]">{children}</div>
            ) : (
              <div className="mx-auto max-w-[1040px] px-[28px] py-[24px]">{children}</div>
            )}
          </main>

          <MobileNav />
        </div>
      </div>
    </NewChatSignalContext.Provider>
  );
}
