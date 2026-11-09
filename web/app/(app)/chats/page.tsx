"use client";

import { Suspense, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { clearChatIdFromUrl, useNewChatSignal } from "@/lib/newChat";
import { onConversationsChanged } from "@/lib/conversationsChanged";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";
import { useChatThread } from "@/components/chat/useChatThread";
import { ChatHeader } from "@/components/chat/ChatHeader";
import { JumpToLatest } from "@/components/chat/JumpToLatest";
import { ChatComposer } from "@/components/chat/ChatComposer";
import { ThreadTurn } from "@/components/chat/AnswerTurn";
import { DetailPanel } from "@/components/detail/DetailPanel";
import { citationHref } from "@/lib/provenance";
import { fromCitation, fromSourceResult } from "@/components/detail/detailModel";
import { evidenceSources, sourceCount } from "@/components/chat/evidence";

import { FeedbackAvailability } from "@/components/chat/AnswerFeedback";
import { ChatEmpty } from "@/components/chat/ChatEmpty";
import { FirstRunChecklist } from "@/components/chat/FirstRunChecklist";
import { Button, ErrorNote, Loading } from "@/components/ui";
import { latestAssistantTurn, unansweredQuestion } from "@/components/chat/types";
import { failureCopy } from "@/components/chat/failure";
import type { AssistantTurn } from "@/components/chat/types";

import { ChatIdentityBoundary } from "@/components/chat/ChatIdentityBoundary";
import { narrowedScopeLabel } from "@/lib/scopedChatScope";

function ChatScreenInner() {
  const params = useSearchParams();
  const router = useRouter();
  const idParam = params.get("id");
  const chat = useChatThread(idParam, params.toString());
  // Declare the required restore before its asynchronous initialization can
  // defer mounting thread content and its read-only descendants.
  useCriticalPageReadLease("saved conversation initialization", chat.loadingConv);
  const { activeId, scopeAccepted, admissionReady, getNavigationId } = chat;
  const back = params.get("returnLibrary");
  const backHome = back && /^\/home(?:[/?]|$)/.test(back) && back.length <= 12000 ? back : null;
  // `?q=` prefills the composer — Today's "Ask about this day" rail button
  // (lib/journal.ts's `askAboutDayHref`) is the only sender today. This read
  // itself runs on every render; the "prefill once, never re-prefill"
  // semantics actually come from ChatComposer treating `initialText` as a
  // `useState` initializer below, not from reading `qParam` here. We also
  // drop `?q=` from the URL once it's been captured into that initial value
  // (mirrors the raw `history.replaceState` pattern used elsewhere in this
  // file), so a reload or a shared link doesn't resurface an already-sent
  // question — deliberately NEVER auto-sends; the user reviews/edits before
  // hitting send like any other draft.
  const qParam = params.get("q");
  const [prefill, setPrefill] = useState<{ text: string; request: number }>();
  const [initialQ] = useState(qParam ?? undefined);
  useEffect(() => {
    if (!qParam || typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.delete("q");
    window.history.replaceState(null, "", `${url.pathname}${url.search}`);
  }, [qParam]);

  type OpenPanel =
    | { kind: "tile"; turnKey: string; sourceItemId: string }
    | { kind: "source"; turnKey: string; n: number };
  const [open, setOpen] = useState<OpenPanel | null>(null);
  const findPanel = open?.kind === "tile" ? { turnKey: open.turnKey, id: open.sourceItemId } : null;
  const evidenceSelection = open?.kind === "source" ? { answerKey: open.turnKey, n: open.n } : null;
  const setFindPanel = (value: { turnKey: string; id: string } | null) =>
    setOpen(value ? { kind: "tile", turnKey: value.turnKey, sourceItemId: value.id } : null);
  const setEvidenceSelection = (value: { answerKey: string; n: number } | null) =>
    setOpen(value ? { kind: "source", turnKey: value.answerKey, n: value.n } : null);
  const threadScroll = useRef<HTMLDivElement>(null);
  const composerContainer = useRef<HTMLDivElement>(null);
  const [composerHeight, setComposerHeight] = useState(200);
  useLayoutEffect(() => {
    const element = composerContainer.current;
    if (!element) return;
    const measure = () => setComposerHeight(element.getBoundingClientRect().height);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  // Distance from the bottom before older turns are prepended; restoring it
  // keeps the turn the reader was looking at in place.
  const olderAnchor = useRef<number | null>(null);
  useEffect(() => {
    setOpen(null);
  }, [chat.activeId, chat.admissionKey, chat.busy]);

  /* The shell's "New chat" (sidebar button, ⌘N) can only ask for a reset — it
     cannot navigate one into effect, because the `?id=` below is written behind
     the router's back. Ignore the value we mounted with: returning here from
     another route while the counter sits at some past value must not wipe the
     conversation the URL asked for. */
  const newChatSignal = useNewChatSignal();
  const seenNewChatSignal = useRef(newChatSignal);
  const resetThread = chat.newChat;
  useEffect(() => {
    if (newChatSignal === seenNewChatSignal.current) return;
    seenNewChatSignal.current = newChatSignal;
    resetThread();
  }, [newChatSignal, resetThread]);

  /* The chat on screen was deleted — from this screen's own header or landing
     list, or from a sidebar row. Fall back to a blank chat and drop the id from
     the URL: leaving it there means a reload (or a shared link) opens a
     conversation that no longer exists and lands on "Couldn't open that chat".

     Keyed off `activeId` rather than `idParam`, via a ref so the subscription
     isn't torn down and rebuilt every turn: `activeId` is what is actually
     open, while `idParam` is null for the whole first conversation of a visit
     (its id is written with a raw replaceState the router never sees). */
  const activeIdRef = useRef(chat.activeId);
  useEffect(() => {
    activeIdRef.current = chat.activeId;
  }, [chat.activeId, chat.admissionKey, chat.libraryScope]);
  useEffect(
    () =>
      onConversationsChanged((deletedId) => {
        if (!deletedId || deletedId !== activeIdRef.current) return;
        resetThread();
        clearChatIdFromUrl((href) => router.replace(href));
      }),
    [resetThread, router],
  );

  // Give the URL an id once the first turn of a fresh chat creates its
  // conversation (1a → 1b) without a client-router round trip — mirrors
  // ask.page.tsx's own `history.replaceState` use.
  //
  // Only a matching live route may accept this conversation or remove a
  // request scope after the server has persisted its normalization. Raw `replaceState` is NOT
  // invisible to the router (Next 14.1+ mirrors the native history methods
  // into `useSearchParams`), so the old `activeId !== idParam` form
  // un-navigated every sidebar chat switch: `?id=` flips to the next chat
  // while `activeId` still holds the previous one, this effect wrote the OLD
  // id straight back, the router saw it, and useChatThread's restore effect
  // cancelled the incoming fetch and kept you on the chat you tried to leave.
  // An id the thread hasn't caught up with is a navigation in flight — leave
  // it alone and let the restore effect do the catching up.
  useEffect(() => {
    if (activeId && (!idParam || idParam === activeId) && typeof window !== "undefined") {
      const url = new URL(window.location.href);
      if (
        !/^\/chats\/?$/.test(url.pathname) ||
        url.searchParams.toString() !== params.toString() ||
        getNavigationId() !== activeId ||
        (url.searchParams.get("id") === activeId && !url.searchParams.has("library_scope")) ||
        !scopeAccepted ||
        !admissionReady
      )
        return;
      url.searchParams.set("id", activeId);
      url.searchParams.delete("library_scope");
      window.history.replaceState(null, "", `${url.pathname}${url.search}`);
    }
  }, [activeId, idParam, scopeAccepted, admissionReady, getNavigationId, params]);

  const lastAssistant = useMemo<AssistantTurn | null>(
    () => latestAssistantTurn(chat.messages),
    [chat.messages],
  );

  useLayoutEffect(() => {
    const el = threadScroll.current;
    if (olderAnchor.current === null || !el || chat.loadingOlder) return;
    el.scrollTop = el.scrollHeight - olderAnchor.current;
    olderAnchor.current = null;
  }, [chat.messages, chat.loadingOlder]);
  const [threadHeight, setThreadHeight] = useState(0);
  useLayoutEffect(() => {
    const el = threadScroll.current;
    if (!el) return;
    const observer = new ResizeObserver(() => setThreadHeight(el.clientHeight));
    setThreadHeight(el.clientHeight);
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  const lastQuestionKey = chat.messages.findLast((turn) => turn.role === "user")?.key;
  const previousQuestion = useRef(lastQuestionKey);
  useEffect(() => {
    const changed = previousQuestion.current !== lastQuestionKey;
    previousQuestion.current = lastQuestionKey;
    if (!changed || !chat.busy || !lastQuestionKey) return;
    const scroller = threadScroll.current;
    let cancelled = false;
    const cancel = () => {
      cancelled = true;
      scroller?.scrollTo({ top: scroller.scrollTop, behavior: "instant" });
    };
    scroller?.addEventListener("wheel", cancel, { passive: true });
    scroller?.addEventListener("touchstart", cancel, { passive: true });
    scroller?.addEventListener("keydown", cancel);
    let second = 0;
    const first = requestAnimationFrame(() => {
      second = requestAnimationFrame(() => {
        const el = threadScroll.current;
        const question = el?.querySelector<HTMLElement>('[data-latest-question="true"]');
        if (!cancelled && el && question) el.scrollTo({
          top: question.getBoundingClientRect().top - el.getBoundingClientRect().top + el.scrollTop - 28,
          behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
        });
      });
    });
    return () => {
      cancelAnimationFrame(first); cancelAnimationFrame(second);
      scroller?.removeEventListener("wheel", cancel);
      scroller?.removeEventListener("touchstart", cancel);
      scroller?.removeEventListener("keydown", cancel);
    };
  }, [lastQuestionKey, chat.busy]);

  const loadOlder = () => {
    const el = threadScroll.current;
    olderAnchor.current = el ? el.scrollHeight - el.scrollTop : null;
    void chat.loadOlder().then((loaded) => {
      if (!loaded) olderAnchor.current = null;
    });
  };

  const askAbout = (item: { title: string }) => {
    setOpen(null);
    setPrefill({ text: `Tell me about “${item.title}”`, request: Date.now() });
  };
  const latestSources = evidenceSources(lastAssistant);
  const selectedAnswer = evidenceSelection
    ? (chat.messages.find(
        (turn): turn is AssistantTurn =>
          turn.role === "assistant" && turn.key === evidenceSelection.answerKey,
      ) ?? null)
    : null;
  const openEvidence = (answer: AssistantTurn, n: number) => {
    if (!evidenceSources(answer).some((source) => source.n === n)) return false;
    setEvidenceSelection({ answerKey: answer.key, n });
    return true;
  };

  const oceanKindOf = (graphId: string) => chat.graphs.find((g) => g.id === graphId)?.kind;
  const firstTurn = chat.messages[0];
  const unanswered = unansweredQuestion(chat.messages);
  // All-workspace chats show no scope at all; a narrowed one (a workspace
  // subset or any finer selector) shows the "Only: …" chip.
  const narrowedLabel = chat.libraryScope
    ? narrowedScopeLabel(chat.libraryScope, chat.graphs)
    : null;
  const title = chat.activeConv?.title ?? (firstTurn ? firstTurn.text : "New chat");

  return (
    <div className="flex h-full min-h-0 flex-col">
      <ChatHeader
        title={title}
        conv={chat.activeConv}
        sourceCount={sourceCount(latestSources)}
        onOpenEvidence={() => {
          if (lastAssistant && latestSources[0]) openEvidence(lastAssistant, latestSources[0].n);
        }}
      />

      <div className="flex min-h-0 flex-1">
        <div className="relative flex min-h-0 flex-1 flex-col">
          <div ref={threadScroll} className="min-h-0 flex-1 overflow-y-auto">
            <div className="mx-auto flex max-w-[720px] flex-col gap-[20px] px-[24px] pt-[24px]" style={{ paddingBottom: composerHeight + 24 }}>
              {chat.messagesTruncated && (
                <div role="status" className="flex flex-wrap items-center gap-2 text-xs text-ink-3">
                  <span>Earlier saved messages are not shown yet.</span>
                  {chat.canLoadOlder && (
                    <Button size="sm" onClick={loadOlder} disabled={chat.loadingOlder}>
                      {chat.loadingOlder ? "Verifying earlier messages…" : "Load earlier messages"}
                    </Button>
                  )}
                </div>
              )}
              {chat.err && (
                <ErrorNote
                  action={
                    <Button size="sm" onClick={() => router.push("/home")}>
                      Back to Home
                    </Button>
                  }
                >
                  {chat.err}
                </ErrorNote>
              )}

              {chat.loadingConv && chat.messages.length === 0 ? (
                <Loading what="opening this chat" />
              ) : chat.messages.length === 0 ? (
                <>
                  {/* An org's first run: connect, invite, ask. Renders
                      nothing on the personal plane or once it's done. */}
                  <FirstRunChecklist />
                  <ChatEmpty onAsk={chat.send} disabled={chat.readOnly || chat.busy || !!chat.err} narrowed={!!narrowedLabel} />
                </>
              ) : (
                chat.messages.map((m, i) => (
                  <div key={m.key} style={i === chat.messages.length - 1 ? { minHeight: Math.max(0, threadHeight - composerHeight - 60) } : undefined} data-latest-question={m.key === lastQuestionKey ? "true" : undefined}><ThreadTurn
                    turn={m}
                    conversationId={chat.activeId}
                    readOnly={chat.readOnly || chat.loadingConv}
                    activeCitation={selectedAnswer?.key === m.key ? evidenceSelection?.n : null}
                    onCite={(n) => m.role === "assistant" && openEvidence(m, n)}
                    onRegenerate={chat.regenerate}
                    onAsk={chat.send}
                    onRetry={
                      i === chat.messages.length - 1 && !chat.busy && !chat.readOnly
                        ? chat.retry
                        : undefined
                    }
                    find={{
                      openId: findPanel?.turnKey === m.key ? findPanel.id : null,
                      onOpenChange: (id) => {
                        setFindPanel(id ? { turnKey: m.key, id } : null);
                      },
                      oceanKindOf,
                    }}
                  /></div>
                ))
              )}
              {/* A question saved with no reply (older than failure records, or its record could not be
                  written): re-ask it in place, never a second copy. */}
              {chat.activeConv && !chat.busy && !chat.loadingConv && !chat.err && unanswered && (
                <ErrorNote
                  action={
                    !chat.readOnly ? (
                      <Button size="sm" onClick={() => void chat.retry(unanswered.key)}>
                        Retry
                      </Button>
                    ) : undefined
                  }
                >
                  {failureCopy("no_reply")}
                </ErrorNote>
              )}
            </div>
          </div>

          <div ref={composerContainer} className="pointer-events-auto absolute inset-x-0 bottom-0">
          <JumpToLatest scrollRef={threadScroll} />
          <ChatComposer
            key={chat.admissionKey}
            scoped
            model={chat.model}
            setModel={chat.setModel}
            effort={chat.effort}
            setEffort={chat.setEffort}
            style={chat.style}
            setStyle={chat.setStyle}
            busy={chat.busy}
            disabled={chat.readOnly || chat.loadingConv}
            only={
              narrowedLabel
                ? {
                    label: narrowedLabel,
                    onClear: chat.clearNarrowing,
                    extra: backHome && (
                      <Button size="sm" variant="ghost" onClick={() => router.push(backHome)}>
                        Back to Home
                      </Button>
                    ),
                  }
                : undefined
            }
            // "Follow-up" only once there is something to follow: a saved chat
            // (activeId is set while it is still opening) or a sent question.
            placeholder={
              chat.activeId || chat.messages.length > 0 ? "Ask a follow-up…" : "Ask anything…"
            }
            onSend={chat.send}
            onStop={chat.stop}
            initialText={initialQ}
            prefill={prefill}
          />
          </div>
        </div>

        {selectedAnswer &&
          evidenceSelection &&
          (() => {
            const source = evidenceSources(selectedAnswer).find((s) => s.n === evidenceSelection.n);
            const sources = evidenceSources(selectedAnswer).sort((a, b) => a.n - b.n);
            const index = sources.findIndex((s) => s.n === evidenceSelection.n);
            const step = (delta: number) =>
              setEvidenceSelection({
                ...evidenceSelection,
                n: sources[(index + delta + sources.length) % sources.length].n,
              });
            return source ? (
              <DetailPanel
                onAsk={askAbout}
                sourceHref={citationHref(selectedAnswer, source.n)}
                position={{ index, total: sources.length }}
                onPrev={() => step(-1)}
                onNext={() => step(1)}
                item={fromCitation(source, selectedAnswer)}
                onClose={() => setEvidenceSelection(null)}
              />
            ) : null;
          })()}
        {findPanel &&
          (() => {
            const turn = chat.messages.find((m) => m.key === findPanel.turnKey);
            const result =
              turn?.role === "assistant"
                ? turn.sourceResults?.find((r) => r.source_item_id === findPanel.id)
                : null;
            const results = turn?.role === "assistant" ? (turn.sourceResults ?? []) : [];
            const index = results.findIndex((r) => r.source_item_id === findPanel.id);
            const step = (delta: number) =>
              setFindPanel({
                ...findPanel,
                id: results[(index + delta + results.length) % results.length].source_item_id,
              });
            return result ? (
              <DetailPanel
                onAsk={askAbout}
                position={{ index, total: results.length }}
                onPrev={() => step(-1)}
                onNext={() => step(1)}
                item={fromSourceResult(result)}
                onClose={() => setFindPanel(null)}
              />
            ) : null;
          })()}
      </div>
    </div>
  );
}

export default function ChatsPage() {
  return (
    <Suspense fallback={<Loading what="loading chats" />}>
      <ChatIdentityBoundary>
        <FeedbackAvailability><ChatScreenInner /></FeedbackAvailability>
      </ChatIdentityBoundary>
    </Suspense>
  );
}
