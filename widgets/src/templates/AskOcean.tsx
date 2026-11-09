import type { ComponentChildren } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { tokensFor, FONT_MONO, FONT_SERIF } from "../theme";
import {
  useToolOutput,
  useToolInput,
  useTheme,
  useDisplayMode,
  callTool,
  requestFullscreen,
} from "../openai";
import {
  isAskOceanCompleted,
  isAskOceanErrorBody,
  isAskOceanTerminal,
  isAskOceanWorking,
  type AskOceanCompleted,
  type AskOceanErrorBody,
  type AskOceanInput,
  type AskOceanOutput,
  type AskOceanTerminal,
} from "../types";
import { Card } from "../components/Card";
import { Mandala } from "../components/Mandala";
import { Working } from "../components/Working";
import { Empty } from "../components/Empty";
import { Answer } from "../components/Answer";
import { SourceCard } from "../components/SourceCard";
import { SourceExpanded } from "../components/SourceExpanded";
import { AnswerUi, answerUiStyle, citationEvidence } from "../answerUi";
import { chatsHref } from "../links";
import { openExternalLink } from "../openai";
import { VIEWPORT_HEIGHT, gutter, safeAreaPadding, useHostLayout } from "../layout";

const POLL_INTERVAL_MS = 2000;

/** A terminal (`failed`/`cancelled`) task envelope has no answer coming —
 *  ever (see `isAskOceanTerminal`'s doc). Convert it into the same
 *  `AskOceanErrorBody` shape a genuine application-level `AskError` uses so
 *  `AskOceanTemplate` has exactly one error rendering path. `"failed"`
 *  carries the real `TaskError`; `"cancelled"` carries none (reserved by the
 *  schema), so it gets a synthesized one instead of silently falling back to
 *  the working skeleton. */
function terminalToErrorBody(t: AskOceanTerminal): AskOceanErrorBody {
  return {
    error: t.error ?? {
      code: t.status,
      message: t.status === "cancelled" ? "The request was cancelled." : "The task failed with no error detail.",
    },
  };
}

function sourcesMeta(result: AskOceanCompleted): string {
  const n = result.citation_details.length;
  const unreachable = result.unreachable_oceans.length;
  const base = `${n} coral${n === 1 ? "" : "s"}`;
  return unreachable > 0 ? `${base} · ${unreachable} ocean${unreachable === 1 ? "" : "s"} unreachable` : base;
}

/** Card header + the in-card expand footer. Same idea as
 *  recent_activity's chrome budget: bound the answer/sources, not the
 *  expand control, so a host cap cannot scroll or clip "expand". */
export const ASK_OCEAN_INLINE_CHROME_PX = 88;
export const ASK_OCEAN_INLINE_CHROME_TOUCH_PX = 120;

function AskOceanScroll({ children }: { children: ComponentChildren }) {
  const { maxHeight, touch, safeArea } = useHostLayout();
  const chrome = touch ? ASK_OCEAN_INLINE_CHROME_TOUCH_PX : ASK_OCEAN_INLINE_CHROME_PX;
  // Card pads `safeArea.top`/`bottom` on the outer shell. Those insets
  // consume the host's maxHeight, so they have to leave the scroll
  // budget or a filled answer still pushes the expand footer off-screen.
  const scrollBody = maxHeight == null
    ? null
    : Math.max(0, maxHeight - chrome - safeArea.top - safeArea.bottom);
  return (
    <div
      data-scroll-body={scrollBody === null ? "unbounded" : "bounded"}
      style={scrollBody === null ? undefined : { maxHeight: scrollBody, overflowY: "auto" }}
    >
      {children}
    </div>
  );
}

interface AskOceanBodyProps {
  result: AskOceanCompleted;
  question: string;
  fullscreen?: boolean;
  footer?: ComponentChildren;
}

function AskOceanBody({ result, question, fullscreen, footer }: AskOceanBodyProps) {
  const theme = useTheme();
  const t = tokensFor(theme);
  const [expanded, setExpanded] = useState<number | null>(null);
  const { size, touch } = useHostLayout();
  const bodyGutter = gutter(size, 18);
  // The artboards' side-by-side answer/sources split needs real horizontal
  // room for both a readable measure and the 340px rail. Below "wide" the
  // rail stacks under the answer instead of squeezing both.
  const stackSources = size !== "wide";
  // Source cards carry a title plus a snippet; one column at phone widths,
  // two at the ChatGPT-sized container, three only when a source is expanded
  // and there is room left beside it.
  const sourceColumns = size === "compact" ? 1 : 2;
  const remainingColumns = size === "compact" ? 1 : size === "regular" ? 2 : 3;

  const n = result.citation_details.length;
  const status = `answered from ${n} source${n === 1 ? "" : "s"}`;

  if (result.mode === "no_answer" || !result.answer) {
    return (
      <Card t={t} status={`answered from 0 sources`} linkLabel="open in coralswarm" linkHref={chatsHref(question)} footer={footer}>
        <AskOceanScroll>
          <Empty t={t} meta={`0 corals${result.unreachable_oceans.length ? ` · ${result.unreachable_oceans.length} oceans unreachable` : ""}`} />
        </AskOceanScroll>
      </Card>
    );
  }

  const toggle = (index: number) => setExpanded((cur) => (cur === index ? null : index));
  const expandedCitation = expanded === null ? undefined : result.citation_details[expanded - 1];

  const evidence = result.citation_details.map((citation, i) => citationEvidence(t, citation, i + 1));
  const selectEvidence = (id: string) => {
    const source = evidence.find(item => item.id === id);
    if (source?.number != null) toggle(source.number);
  };
  const sourcesSection = (
    <div style={{ padding: `12px ${bodyGutter}px 16px`, display: "flex", flexDirection: "column", gap: 10 }}>
      {expanded !== null && expandedCitation && <SourceExpanded t={t} index={expanded} citation={expandedCitation} onClose={() => setExpanded(null)} />}
      <AnswerUi.EvidenceList key="sources" sources={evidence} activeId={expanded == null ? undefined : evidence[expanded - 1]?.id} columns={expanded === null ? sourceColumns : remainingColumns} label={`Sources · ${sourcesMeta(result)}`} onSelect={selectEvidence} />
    </div>
  );

  if (fullscreen) {
    return (
      <div className="csau-widget" data-sources={stackSources ? "stacked" : "beside"} style={{ ...answerUiStyle(t, touch), display: "flex", flexDirection: stackSources ? "column" : "row", flex: 1, minHeight: 0, overflow: stackSources ? "auto" : undefined }}>
        <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 14, padding: size === "compact" ? "16px 18px 20px" : size === "regular" ? "22px 24px 26px" : "28px 36px 32px", overflow: stackSources ? undefined : "auto" }}>
          <div style={{ fontFamily: FONT_SERIF, fontStyle: "italic", fontWeight: 400, fontSize: 24, lineHeight: 1.25, color: t.ink }}>{question}</div>
          <span style={{ fontFamily: FONT_MONO, fontSize: 10.5, letterSpacing: "0.08em", textTransform: "uppercase", color: t.muted }}>
            {status}
          </span>
          <Answer t={t} text={result.answer} activeChip={expanded} onChipClick={toggle} fontSize={16} />
        </div>
        <div
          style={{
            width: stackSources ? "100%" : 340,
            flex: "none",
            // Stacked, the rail is a section BELOW the answer, so its
            // divider runs along its top edge instead of its left one.
            borderLeft: stackSources ? undefined : `1px solid ${t.lineSoft}`,
            borderTop: stackSources ? `1px solid ${t.lineSoft}` : undefined,
            background: t.headerBg,
            padding: `22px ${gutter(size, 20)}px`,
            display: "flex",
            flexDirection: "column",
            gap: 10,
            boxSizing: "border-box",
            // Stacked, the single outer column scrolls as one document; only
            // the side-by-side arrangement gives each pane its own scroller.
            overflow: stackSources ? undefined : "auto",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontFamily: FONT_MONO, fontSize: 9.5, letterSpacing: "0.13em", textTransform: "uppercase", color: t.muted }}>sources</span>
            <span style={{ flex: 1, height: 1, background: t.lineSoft }} />
            <span style={{ fontFamily: FONT_MONO, fontSize: 10, color: t.mutedSoft }}>{sourcesMeta(result)}</span>
          </div>
          {expanded !== null && expandedCitation && <SourceExpanded t={t} index={expanded} citation={expandedCitation} onClose={() => setExpanded(null)} />}
          {result.citation_details.map((c, i) => (
            <SourceCard key={c.atom_id + i} t={t} index={i + 1} active={expanded === i + 1} citation={c} onClick={() => toggle(i + 1)} />
          ))}
        </div>
      </div>
    );
  }

  return (
    <Card t={t} status={status} linkLabel="open in coralswarm" linkHref={chatsHref(question)} footer={footer}>
      <AskOceanScroll>
        <div style={{ padding: `16px ${bodyGutter}px 6px` }}>
          <Answer t={t} text={result.answer} activeChip={expanded} onChipClick={toggle} />
        </div>
        {sourcesSection}
      </AskOceanScroll>
    </Card>
  );
}

export function AskOceanTemplate() {
  const toolOutput = useToolOutput<AskOceanOutput>();
  const toolInput = useToolInput<AskOceanInput>();
  const theme = useTheme();
  const displayMode = useDisplayMode();
  const { safeArea } = useHostLayout();
  const t = tokensFor(theme);

  const [resolved, setResolved] = useState<AskOceanCompleted | AskOceanErrorBody | null>(null);
  const [taskId, setTaskId] = useState<string | null>(null);
  const startedAtRef = useRef<number>(Date.now());

  useLayoutEffect(() => {
    if (!toolOutput) {
      startedAtRef.current = Date.now();
      setResolved(null);
      setTaskId(null);
      return;
    }
    if (isAskOceanCompleted(toolOutput) || isAskOceanErrorBody(toolOutput)) {
      setResolved(toolOutput);
      setTaskId(null);
    } else if (isAskOceanTerminal(toolOutput)) {
      setResolved(terminalToErrorBody(toolOutput));
      setTaskId(null);
    } else if (isAskOceanWorking(toolOutput)) {
      // A NEW working result must always clear any previously resolved
      // answer — otherwise a second ask_ocean call landing in the same
      // widget instance leaves `resolved` set from the FIRST call and the
      // polling effect below never starts for the second task's id.
      setResolved(null);
      setTaskId(toolOutput.task_id ?? null);
    }
  }, [toolOutput]);

  useEffect(() => {
    if (!taskId || resolved) return;
    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | undefined;

    // Serialized: the next poll is scheduled only after the current one
    // SETTLES (success, terminal result, or rejection) — never a bare
    // `setInterval`, which would let requests overlap if `callTool` ever
    // takes longer than `POLL_INTERVAL_MS`, and which has no rejection
    // handler at all (an unhandled promise rejection on every network
    // hiccup, with the interval still ticking after it).
    const runPoll = async () => {
      if (cancelled) return;
      try {
        const r = await callTool<AskOceanOutput>("get_task_result", { task_id: taskId });
        if (cancelled) return;
        if (!r) {
          // No host reachable (or no result yet) — keep polling; there is
          // nothing more specific to report.
          timeoutId = setTimeout(runPoll, POLL_INTERVAL_MS);
          return;
        }
        if (isAskOceanCompleted(r) || isAskOceanErrorBody(r)) {
          setResolved(r);
          return;
        }
        if (isAskOceanTerminal(r)) {
          setResolved(terminalToErrorBody(r));
          return;
        }
        // Still "working" — schedule the next attempt.
        timeoutId = setTimeout(runPoll, POLL_INTERVAL_MS);
      } catch (e) {
        if (cancelled) return;
        setResolved({
          error: {
            code: "widget_poll_failed",
            message: e instanceof Error ? e.message : "get_task_result polling failed.",
          },
        });
      }
    };

    void runPoll();

    return () => {
      cancelled = true;
      if (timeoutId !== undefined) clearTimeout(timeoutId);
    };
  }, [taskId, resolved]);

  const question = toolInput?.question ?? "";
  const oceanCount = toolInput?.ocean_ids?.length;

  if (resolved && isAskOceanErrorBody(resolved)) {
    return (
      <Card t={t} status="ask_ocean error" linkLabel="open in coralswarm" linkHref={chatsHref(question)} footer={<FullscreenPrompt t={t} onClick={requestFullscreen} />}>
        <AskOceanScroll>
          <Empty t={t} title="Something went wrong." body={resolved.error.message} meta={resolved.error.code} />
        </AskOceanScroll>
      </Card>
    );
  }

  if (!resolved) {
    return <Working t={t} oceanCount={oceanCount} startedAt={startedAtRef.current} />;
  }

  if (displayMode === "fullscreen") {
    return (
      // `height: 100%` resolves against an auto-height body (the widget
      // reports its own intrinsic size), so it collapses and the two inner
      // `overflow: auto` panes never get a scroll boundary. The iframe
      // viewport is the real bound in fullscreen. `boxSizing: border-box`
      // keeps the safe-area padding inside that height.
      <div style={{ width: "100%", height: VIEWPORT_HEIGHT, maxHeight: VIEWPORT_HEIGHT, boxSizing: "border-box", background: t.bg, display: "flex", flexDirection: "column", overflow: "hidden", ...safeAreaPadding(safeArea) }}>
        <FullscreenHeader t={t} question={question} />
        <AskOceanBody result={resolved} question={question} fullscreen />
      </div>
    );
  }

  return (
    <AskOceanBody
      result={resolved}
      question={question}
      footer={<FullscreenPrompt t={t} onClick={requestFullscreen} />}
    />
  );
}

function FullscreenHeader({ t, question }: { t: ReturnType<typeof tokensFor>; question: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, height: 52, padding: "0 16px", borderBottom: `1px solid ${t.lineSoft}`, background: t.bg, boxSizing: "border-box" }}>
      <Mandala size={20} />
      <span style={{ fontSize: 14, fontWeight: 500, letterSpacing: "-0.01em", color: t.ink, flex: 1 }}>CoralSwarm</span>
      <span
        role="button"
        tabIndex={0}
        onClick={() => openExternalLink(chatsHref(question))}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            if (!e.repeat) openExternalLink(chatsHref(question));
          }
        }}
        style={{ display: "inline-flex", alignItems: "center", gap: 5, fontFamily: FONT_MONO, fontSize: 11, color: t.muted, cursor: "pointer" }}
      >
        open in coralswarm
      </span>
    </div>
  );
}

/** A quiet affordance to enter Fullscreen mode when inline — the artboards
 *  treat Fullscreen as a separate display mode the host switches into
 *  (`requestDisplayMode`), not a button the component itself always shows;
 *  this stays minimal so it doesn't compete with the card above it. */
function FullscreenPrompt({ t, onClick }: { t: ReturnType<typeof tokensFor>; onClick: () => void }) {
  // Lives inside `Card` as a footer so hosts that size the iframe to the
  // card box (Claude) cannot clip it the way a sibling-below-the-card
  // used to. Touch hosts still get the 44px minimum.
  const { size, touch } = useHostLayout();
  return (
    <div style={{ display: "flex", justifyContent: "flex-end", padding: `6px ${gutter(size, 14)}px 10px`, borderTop: `1px solid ${t.lineSoft}`, background: t.headerBg }}>
      <span
        role="button"
        tabIndex={0}
        onClick={onClick}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") onClick();
        }}
        style={{ display: "inline-flex", alignItems: "center", fontFamily: FONT_MONO, fontSize: 11, color: t.secondary, cursor: "pointer", padding: "4px 0", minHeight: touch ? 44 : undefined }}
      >
        expand
      </span>
    </div>
  );
}
