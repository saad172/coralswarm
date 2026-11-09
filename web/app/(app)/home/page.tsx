"use client";

/* Home: the launchpad (Claude Design "Home" mock; decisions 2026-11-03).
   Where people land after signing in. Ask comes first, then the one thing
   that needs fixing, then your own recent work, then what formed in your
   oceans this week. Metrics moved off Home (Settings, Plan & usage).

   STATES (lib/homeLaunchpad.ts `homeState`):
     A  populated: greeting, Ask box and 3 prompt starters, the needs-attention
        row (only while a connector is broken), Continue, What's new.
     B  empty ocean (`has_content` false): greeting, Ask box, the
        needs-attention row (as in A) and the 3-step setup checklist.
        Continue and What's new are hidden. Once content has existed the
        checklist never returns (`writeOnboarded`). B is never drawn before
        that mark has been read: until then the page is `loading`.

   DATA: exactly three reads per visit, issued together on every mount (a
   cached answer paints meanwhile), never polled (lib/homeLaunchpadApi.ts has
   the freshness rules). Prompt starters are templates over the Continue and
   What's new rows: no LLM, no request.
   The "new" dot compares each reef's `formed_at` with the last visit this
   browser remembers for the identity; the baseline is read once on mount and
   the stored value moves forward (never back) only after What's new has
   rendered in State A. */

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useUser } from "@clerk/nextjs";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { HeaderMeta } from "@/components/HeaderMeta";
import {
  AskBox,
  AttentionRow,
  ContinueList,
  Greeting,
  HomeSection,
  PromptStarters,
  RefreshControl,
  SectionError,
  SectionNote,
  SetupChecklist,
  SkeletonList,
  WhatsNewList,
} from "@/components/home/Launchpad";
import { homeAttentionQueryOptions, homeContinueQueryOptions, homeWhatsNewQueryOptions } from "@/lib/homeLaunchpadApi";
import {
  firstNameOf,
  freshnessLabel,
  homeState,
  isNewSince,
  newSinceLabel,
  nextLastVisit,
  promptStarters,
  readLastVisit,
  readOnboarded,
  setupChecklist,
  updatedAt,
  writeLastVisit,
  writeOnboarded,
} from "@/lib/homeLaunchpad";
import { greeting as greetingAt } from "@/lib/chatPrompts";
import { EMPTY_SETUP, readSetup, writeSetup, type SetupState } from "@/lib/homeSetup";
import { MCP_URL } from "@/lib/mcpUrl";
import { newChatHref } from "@/lib/newChat";

/** A clock for the "updated N min ago" labels. Re-renders once a minute;
 *  it never issues a request. */
function useMinuteClock(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 60_000);
    return () => window.clearInterval(id);
  }, []);
  return now;
}

interface Memory {
  scope: string;
  lastVisit: string | null;
  onboarded: boolean;
}

export default function HomePage() {
  const router = useRouter();
  const { user } = useUser();
  const { authReady, getAuthToken, queryScope, userId } = useApp();
  const ctx = { getAuthToken, scope: queryScope };

  // The three reads, together, at mount.
  const cont = useQuery({ ...homeContinueQueryOptions(ctx), enabled: authReady });
  const news = useQuery({ ...homeWhatsNewQueryOptions(ctx), enabled: authReady });
  const att = useQuery({ ...homeAttentionQueryOptions(ctx), enabled: authReady });

  const tick = useMinuteClock();
  // Never older than the newest data on screen, so a fresh answer reads
  // "just now" rather than waiting for the next tick.
  const nowMs = Math.max(tick, cont.dataUpdatedAt, news.dataUpdatedAt, att.dataUpdatedAt);
  const now = useMemo(() => new Date(nowMs), [nowMs]);

  // ── Browser memory, per identity (read after mount: no SSR value) ──────
  const [memory, setMemory] = useState<Memory | null>(null);
  useEffect(() => {
    if (!authReady) return;
    setMemory({ scope: queryScope, lastVisit: readLastVisit(queryScope), onboarded: readOnboarded(queryScope) });
  }, [authReady, queryScope]);
  const mine = memory && memory.scope === queryScope ? memory : null;
  const lastVisit = mine?.lastVisit ?? null;

  // Content seen: stored for later visits, and in memory for this one, so a
  // later answer in the same visit (focus, Refresh) that reads empty cannot
  // bring the checklist back.
  const hasContent = att.data?.has_content === true;
  useEffect(() => {
    if (!mine || !hasContent || mine.onboarded) return;
    writeOnboarded(mine.scope, new Date());
    setMemory((m) => (m && m.scope === mine.scope ? { ...m, onboarded: true } : m));
  }, [mine, hasContent]);

  // Until the memory is read, `onboarded` is unknown, so this is `loading`
  // (State A's skeleton), never a flash of the checklist.
  const state = homeState({
    attention: att.data,
    attentionFailed: att.isError && !att.data,
    memoryLoaded: mine !== null,
    onboarded: mine?.onboarded ?? false,
  });

  // What's new has been seen: the next visit compares against this one. Only
  // once it has rendered as State A: in State B, or while loading, the
  // section is not on screen, and advancing the baseline then would hide the
  // dots on reefs the user has never seen. The stored value is re-read here,
  // not taken from `mine`, so a newer visit in another tab is never
  // overwritten by an older answer.
  const seenAt = state === "populated" ? news.data?.generated_at : undefined;
  useEffect(() => {
    if (!mine || !seenAt) return;
    writeLastVisit(mine.scope, nextLastVisit(readLastVisit(mine.scope), seenAt, new Date()));
  }, [mine, seenAt]);

  // ── Setup marks (the clicks the checklist remembers, per user) ─────────
  const uid = userId ?? "anon";
  const [marks, setMarks] = useState<SetupState>(EMPTY_SETUP);
  useEffect(() => {
    setMarks(readSetup(uid));
  }, [uid]);
  const mark = (id: keyof SetupState) =>
    setMarks((prev) => {
      if (prev[id]) return prev;
      const next = { ...prev, [id]: true };
      writeSetup(uid, next);
      return next;
    });
  // `=== true`: a backend that does not send a field yet reads as not done.
  const checklist = setupChecklist(marks, {
    hasSource: att.data?.has_source === true,
    hasMcp: att.data?.has_mcp === true,
    hasAsked: att.data?.has_asked === true,
    needsAttention: !!att.data?.attention,
  });

  const askRef = useRef<HTMLTextAreaElement>(null);
  const ask = (question: string) => {
    mark("ask");
    router.push(newChatHref(question));
  };

  // ── Derived view data ──────────────────────────────────────────────────
  const starters = useMemo(() => promptStarters(cont.data?.items ?? [], news.data?.items ?? []), [cont.data, news.data]);
  // Wait for both sources so the chips do not reshuffle as the second lands.
  const startersReady = (!!cont.data || cont.isError) && (!!news.data || news.isError);

  const fetching = cont.isFetching || news.isFetching || att.isFetching;
  // `refetch()` ignores `enabled`, so before auth it would read with no token.
  const refresh = () => {
    if (!authReady) return;
    void cont.refetch();
    void news.refetch();
    void att.refetch();
  };

  // Freshness is the client's own fetch time against the client's clock.
  const continueMeta = cont.data
    ? freshnessLabel(updatedAt(cont.dataUpdatedAt), nowMs, cont.isFetching)
    : cont.isError
      ? "not loaded"
      : "opening your work…";
  const whatsNewMeta = news.data
    ? [newSinceLabel(news.data.items, lastVisit), freshnessLabel(updatedAt(news.dataUpdatedAt), nowMs, news.isFetching)]
        .filter(Boolean)
        .join(" · ")
    : news.isError
      ? "not loaded"
      : "surfacing new reefs…";

  const name = firstNameOf(user);
  const greeting = greetingAt(now.getHours());

  return (
    // `max-w-thread` with its own 28px gutter at desktop: a 664px column, as
    // in the mock. Below `lg` the shell's gutter is the only one.
    <div className="mx-auto flex max-w-thread flex-col gap-[32px] pb-[16px] pt-[28px] lg:px-[28px]">
      <HeaderMeta>
        <RefreshControl refreshing={fetching} disabled={!authReady} onRefresh={refresh} />
      </HeaderMeta>

      <Greeting
        greeting={greeting}
        name={name}
        lead={state === "empty" ? "Your ocean is empty for now — connect a source and reefs will start forming here." : undefined}
      >
        <AskBox ref={askRef} onAsk={ask} />
        {state !== "empty" && <PromptStarters items={starters} loading={!startersReady} onPick={ask} />}
      </Greeting>

      {/* In both states: in State B a new user whose only source broke sees
          why nothing arrives, above the checklist. */}
      {att.data?.attention && <AttentionRow item={att.data.attention} />}

      {state === "empty" ? (
        <SetupChecklist
          steps={checklist.steps}
          done={checklist.done}
          total={checklist.total}
          mcpUrl={MCP_URL}
          onMcpCopied={() => mark("mcp")}
          onAsk={() => askRef.current?.focus()}
        />
      ) : (
        <>
          <HomeSection label="Continue" eyebrow="continue" meta={continueMeta}>
            {cont.data ? (
              cont.data.items.length > 0 ? (
                <ContinueList items={cont.data.items} now={now} />
              ) : (
                <SectionNote>Nothing to pick up yet. The chats, sessions, meetings and workspaces you open land here.</SectionNote>
              )
            ) : cont.isError ? (
              <SectionError what="your recent work" onRetry={() => void cont.refetch()} />
            ) : (
              <SkeletonList rows={6} label="Loading your recent work" />
            )}
          </HomeSection>

          <HomeSection label="What's new" eyebrow={`what's new · ${news.data?.window_days ?? 7} days`} meta={whatsNewMeta}>
            {news.data ? (
              news.data.items.length > 0 ? (
                <WhatsNewList items={news.data.items} isNew={(item) => isNewSince(item.formed_at, lastVisit)} now={now} />
              ) : (
                <SectionNote>No reefs formed in your oceans in the last {news.data.window_days} days.</SectionNote>
              )
            ) : news.isError ? (
              <SectionError what="what's new" onRetry={() => void news.refetch()} />
            ) : (
              <SkeletonList rows={5} chip label="Loading what's new" />
            )}
          </HomeSection>
        </>
      )}
    </div>
  );
}
