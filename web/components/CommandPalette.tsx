"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { ApiError } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { FIND_TITLES_MAX_QUERY_CHARS, fetchFindTitles } from "@/lib/findTitlesApi";
import type { SourceResult } from "@/lib/scopedChatApi";
import { newChatHref } from "@/lib/newChat";
import { OPEN_COMMAND_PALETTE_EVENT } from "@/lib/commandPalette";
import { dayLabel, localDay } from "@/lib/homeView";
import { IconAsk } from "./icons";
import { Dot, cn } from "@/components/ui";
import { DetailPanel } from "./detail/DetailPanel";
import { KIND_META, fromSourceResult, type DetailItem } from "./detail/detailModel";
import { oceanHue } from "@/lib/oceanHue";

/* The hidden Cmd-K / Ctrl-K title search (README "Cmd-K fallback"). It appears
   nowhere in the UI: finding a source is a chat answer, and this stays until
   chat find results prove reliable.

   - A 560px modal over the scrim. Type to search source TITLES only
     (`GET /v1/find/titles`), debounced 200ms; each new keystroke aborts the
     request still in flight (docs/implementation/find-results-api.md).
   - Clicking (or Enter on) a result closes the modal and opens the shared
     DetailPanel.
   - "Ask instead" is always the last row: it opens a new chat seeded with the
     query.
   - A 404 means CHAT_FIND_RESULTS_ENABLED is off on the server. The palette
     then falls back to the old ask-only behaviour: Enter opens a new chat. */

const DEBOUNCE_MS = 200;
const LIMIT = 8;

export default function CommandPalette({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const router = useRouter();
  const { getAuthToken, authReady, graphs } = useApp();
  const kindOf = (graphId: string) => graphs.find((g) => g.id === graphId)?.kind;
  const [value, setValue] = useState("");
  const [results, setResults] = useState<SourceResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [failed, setFailed] = useState(false);
  const [askOnly, setAskOnly] = useState(false);
  const [active, setActive] = useState(0);
  const [panelItem, setPanelItem] = useState<DetailItem | null>(null);
  const inflight = useRef<AbortController | null>(null);

  // Opened from elsewhere in the app (the chat find fallback line).
  useEffect(() => {
    const onOpen = () => onOpenChange(true);
    window.addEventListener(OPEN_COMMAND_PALETTE_EVENT, onOpen);
    return () => window.removeEventListener(OPEN_COMMAND_PALETTE_EVENT, onOpen);
  }, [onOpenChange]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        onOpenChange(true);
      } else if (e.key === "Escape") {
        onOpenChange(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onOpenChange]);

  // Reset the field each time it opens.
  useEffect(() => {
    if (open) {
      setValue("");
      setResults([]);
      setFailed(false);
      setActive(0);
    } else {
      inflight.current?.abort();
    }
  }, [open]);

  // Debounced title search; the previous request is aborted first.
  useEffect(() => {
    inflight.current?.abort();
    const q = value.trim();
    if (!open || askOnly || !authReady || !q || q.length > FIND_TITLES_MAX_QUERY_CHARS) {
      setResults([]);
      setSearching(false);
      return;
    }
    setSearching(true);
    const ctl = new AbortController();
    inflight.current = ctl;
    const timer = setTimeout(async () => {
      try {
        const token = getAuthToken();
        if (!token) throw new Error("Not authenticated");
        // eslint-disable-next-line no-restricted-syntax -- a debounced, abortable keystroke search: every keystroke is a new query that cancels the last, so there is no cache entry worth keeping (find-results-api.md)
        const res = await fetchFindTitles(token, q, LIMIT, ctl.signal);
        if (ctl.signal.aborted) return;
        setResults(res.results);
        setFailed(false);
        setActive(0);
      } catch (err) {
        if (ctl.signal.aborted) return;
        if (err instanceof ApiError && err.status === 404) setAskOnly(true);
        else setFailed(true);
        setResults([]);
      } finally {
        if (!ctl.signal.aborted) setSearching(false);
      }
    }, DEBOUNCE_MS);
    return () => {
      clearTimeout(timer);
      ctl.abort();
    };
  }, [value, open, askOnly, authReady, getAuthToken]);

  const q = value.trim();
  const rows = q ? results.length + 1 : 0; // results, then "Ask instead"

  function askInstead() {
    if (!q) return;
    onOpenChange(false);
    router.push(newChatHref(q));
  }
  function openResult(r: SourceResult) {
    onOpenChange(false);
    setPanelItem(fromSourceResult(r, { oceanKind: kindOf(r.graph.id) }));
  }
  function onInputKey(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown" && rows) {
      e.preventDefault();
      setActive((i) => (i + 1) % rows);
    } else if (e.key === "ArrowUp" && rows) {
      e.preventDefault();
      setActive((i) => (i - 1 + rows) % rows);
    } else if (e.key === "Enter") {
      if (askOnly || active >= results.length) askInstead();
      else openResult(results[active]);
    }
  }

  return (
    <>
      {open && (
        <div
          className="fixed inset-0 z-50 flex items-start justify-center bg-scrim px-4 pt-[18vh]"
          onClick={() => onOpenChange(false)}
        >
          <div
            role="dialog"
            aria-label={askOnly ? "Ask" : "Search titles"}
            className="panel w-full max-w-[560px] gap-0 p-0 shadow-pop"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center gap-3 px-[18px] py-[14px]">
              <IconAsk width={18} height={18} className="shrink-0 text-brand" />
              <input
                autoFocus
                aria-label={askOnly ? "Ask your workspace" : "Search source titles"}
                className="min-w-0 flex-1 bg-transparent font-display text-[20px] italic leading-[1.2] text-ink outline-none placeholder:text-ink-3"
                placeholder={askOnly ? "Ask your workspace…" : "Search titles…"}
                value={value}
                onChange={(e) => setValue(e.target.value)}
                onKeyDown={onInputKey}
              />
              <kbd className="rounded-badge border border-line bg-bg-2 px-[6px] py-[1px] font-mono text-[10px] text-ink-3">↵</kbd>
            </div>

            {!askOnly && q && (
              <div role="listbox" aria-label="Matching titles" className="border-t border-line-2 p-[6px]">
                {results.map((r, i) => {
                  const m = KIND_META[r.kind];
                  const Icon = m.Icon;
                  return (
                    <button
                      key={r.source_item_id}
                      type="button"
                      role="option"
                      aria-selected={active === i}
                      onMouseEnter={() => setActive(i)}
                      onClick={() => openResult(r)}
                      className={cn(
                        "grid w-full grid-cols-[22px_minmax(0,1fr)_auto] items-center gap-[10px] rounded-row px-[10px] py-[7px] text-left transition-colors",
                        active === i ? "bg-bg-hover" : "hover:bg-bg-hover",
                      )}
                    >
                      <span className="grid h-[22px] w-[22px] place-items-center rounded-badge" style={{ background: `color-mix(in srgb, var(--hue-${m.hue}) 18%, transparent)`, color: `var(--hue-${m.hue})` }}>
                        <Icon width={13} height={13} />
                      </span>
                      <span className="min-w-0 truncate text-[13.5px] text-ink">{r.title}</span>
                      <span className="flex items-center gap-[5px] whitespace-nowrap font-mono text-[10px] uppercase tracking-[0.06em] text-ink-3">
                        <Dot hue={oceanHue({ graph_id: r.graph.id, kind: kindOf(r.graph.id) })} size={5} />
                        {r.graph.name}
                        {r.occurred_at && <span className="text-ink-4">{dayLabel(localDay(r.occurred_at))}</span>}
                      </span>
                    </button>
                  );
                })}
                {!searching && !failed && results.length === 0 && (
                  <div className="px-[10px] py-[7px] text-[12.5px] text-ink-3">No source titles match.</div>
                )}
                {failed && <div role="alert" className="px-[10px] py-[7px] text-[12.5px] text-bad">Search failed. Try again.</div>}
                <button
                  type="button"
                  role="option"
                  aria-selected={active === results.length}
                  onMouseEnter={() => setActive(results.length)}
                  onClick={askInstead}
                  className={cn(
                    "flex w-full items-center gap-[10px] rounded-row px-[10px] py-[7px] text-left text-[13px] text-ink-2 transition-colors",
                    active === results.length ? "bg-bg-hover" : "hover:bg-bg-hover",
                  )}
                >
                  <IconAsk width={14} height={14} className="text-brand" />
                  Ask instead: <span className="truncate text-ink">{q}</span>
                </button>
              </div>
            )}

            <div className="border-t border-line px-[18px] py-[10px] font-mono text-[11px] text-ink-3">
              {askOnly ? "Enter to ask in a new chat · Esc to close" : "fallback · titles only — ask in chat to find by what was said"}
            </div>
          </div>
        </div>
      )}
      {panelItem && <DetailPanel item={panelItem} onClose={() => setPanelItem(null)} />}
    </>
  );
}
