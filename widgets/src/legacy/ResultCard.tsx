import { Fragment } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { AnswerUi, answerUiStyle } from "../answerUi";
import { tokensFor } from "../theme";
import { Mandala } from "../components/Mandala";
import { parseInline, splitParagraphs } from "../answerParse";
import { legacyTaskCaller, record, useLegacyBridge } from "./bridge";
import { legacyResult } from "./model";
import "./styles.css";

export function LegacyResultCard() {
  const host = useLegacyBridge();
  const t = tokensFor(host.theme);
  const [polled, setPolled] = useState<{ parent: unknown; value: unknown } | null>(null);
  const output = polled && polled.parent === host.output ? polled.value : host.output;
  const result = legacyResult(output, t);
  const waiting = output == null || result.working === true;
  const [elapsed, setElapsed] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const trigger = useRef<HTMLElement | null>(null);
  const detailHeading = useRef<HTMLElement | null>(null);
  const root = useRef<HTMLElement | null>(null);
  const active = result.sources.find(source => source.id === selected);
  useLayoutEffect(() => { setSelected(null); }, [output]);
  useLayoutEffect(() => { if (active) detailHeading.current?.focus(); }, [selected]);
  useEffect(() => {
    if (selected == null) return;
    const escape = (event: KeyboardEvent) => { if (event.key === "Escape") { event.preventDefault(); setSelected(null); trigger.current?.focus(); } };
    window.addEventListener("keydown", escape);
    return () => window.removeEventListener("keydown", escape);
  }, [selected]);
  useEffect(() => {
    setElapsed(0);
    if (!waiting) return;
    const start = Date.now();
    const interval = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000);
    return () => clearInterval(interval);
  }, [host.output, waiting]);
  useEffect(() => {
    const caller = legacyTaskCaller();
    if (!result.taskId || !caller) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const parent = host.output;
    const poll = async () => {
      try {
        const response = await caller.callTool("get_task_result", { task_id: result.taskId });
        if (cancelled) return;
        if (response.isError) throw new Error("The task could not complete this request.");
        const value = response.structuredContent;
        if (value != null && record(value)?.status !== "working") setPolled({ parent, value });
        else timer = setTimeout(poll, 2000);
      } catch (error) {
        if (!cancelled) setPolled({ parent, value: { error: { message: error instanceof Error ? error.message : "Unable to check the task result." } } });
      }
    };
    void poll();
    return () => { cancelled = true; if (timer !== undefined) clearTimeout(timer); };
  }, [host.output, result.taskId]);
  useEffect(() => {
    if (typeof ResizeObserver === "undefined" || !root.current || typeof window.openai?.notifyIntrinsicHeight !== "function") return;
    const observer = new ResizeObserver(() => window.openai?.notifyIntrinsicHeight?.(root.current?.getBoundingClientRect().height ?? 0));
    observer.observe(root.current);
    return () => observer.disconnect();
  }, []);
  const select = (id: string) => { trigger.current = document.activeElement as HTMLElement; setSelected(current => current === id ? null : id); };
  const close = () => { setSelected(null); trigger.current?.focus(); };
  const viewportHeight = typeof CSS !== "undefined" && CSS.supports("height", "100dvh") ? "100dvh" : "100vh";
  const maxHeight = host.displayMode === "fullscreen" ? viewportHeight : host.maxHeight ?? undefined;
  return <AnswerUi.CardFrame attributes={{
    className: "csau-widget csau-legacy", "data-legacy-result": "", "data-theme": host.theme, ref: element => { root.current = element; },
    style: { ...answerUiStyle(t, host.touch), maxHeight, paddingTop: host.insets.top, paddingRight: host.insets.right, paddingBottom: host.insets.bottom, paddingLeft: host.insets.left },
  }} header={<div className="legacy-header"><Mandala size={18} /><h1>{waiting ? "CoralSwarm result" : result.title}</h1></div>} children={[
    <div className="legacy-body">
      {waiting ? <AnswerUi.Progress label="Waiting for the tool result…" elapsedSeconds={elapsed} mark={<Mandala size={22} />} /> : <>
        {result.notice && <p role="status">{result.notice}</p>}
        {result.answer && <div className="legacy-answer">{splitParagraphs(result.answer).map((paragraph, index) => <p key={index}>{parseInline(paragraph).map((token, i) => token.type === "bold" ? <strong key={i}>{token.value}</strong> : token.type === "text" ? <Fragment key={i}>{token.value}</Fragment> : <button key={i} type="button" className="legacy-citation" aria-label={`Open source ${token.index}`} disabled={!result.sources.some(source => source.number === token.index)} onClick={() => { const source = result.sources.find(source => source.number === token.index); if (source) select(source.id); }}>{token.index}</button>)}</p>)}</div>}
        {active && <AnswerUi.EvidenceDetail source={active} headingId="legacy-source-heading" headingRef={element => { detailHeading.current = element; }} before={<button className="legacy-close" type="button" onClick={close}>Close source</button>} />}
        <AnswerUi.EvidenceList sources={result.sources} activeId={selected ?? undefined} onSelect={select} showExcerpt />
        {result.fallback != null && <pre className="legacy-json">{result.fallback}</pre>}
      </>}
    </div>,
  ]} />;
}
