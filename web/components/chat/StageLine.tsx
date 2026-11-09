"use client";

import { useEffect, useState } from "react";
import { useReducedMotion } from "@/lib/motion";
import { AnswerMeta } from "@/components/ui/Cite";
import { SwarmMandala } from "./SwarmMandala";

export type AnswerStage =
  | { kind: "searching" }
  | { kind: "retrieved"; sources: number }
  | { kind: "writing" }
  | { kind: "checking" };

type Props =
  | { state: "pending"; stage: AnswerStage | null; startedAt: number }
  | { state: "answered"; sources: number; elapsedMs?: number }
  | { state: "stopped"; elapsedMs: number; onAskAgain?: () => void };

export function StageLine(props: Props) {
  const reduced = useReducedMotion();
  const [now, setNow] = useState(Date.now);
  const pending = props.state === "pending";
  useEffect(() => {
    if (!pending) return;
    const timer = setInterval(() => setNow(Date.now()), 100);
    return () => clearInterval(timer);
  }, [pending]);
  const mark = <SwarmMandala settled={!pending} />;
  if (props.state === "answered") {
    return <div className="flex items-center gap-2 text-sm text-ink-3">{mark}<div style={{ animation: reduced ? "none" : "cs-stage-in var(--dur-enter) var(--ease-out)" }}><AnswerMeta>answered from {props.sources} {props.sources === 1 ? "source" : "sources"}{props.elapsedMs == null ? "" : ` · ${(props.elapsedMs / 1000).toFixed(1)}s`}</AnswerMeta></div></div>;
  }
  if (props.state === "stopped") {
    return <div><AnswerMeta mark={mark}>stopped · {(props.elapsedMs / 1000).toFixed(1)}s</AnswerMeta><p className="text-sm text-ink-3">Stopped before it wrote anything. {props.onAskAgain && <button type="button" className="text-brand underline" onClick={props.onAskAgain}>Check saved history</button>}</p></div>;
  }
  const stage = props.stage;
  const label = !stage ? "Preparing your question" : stage.kind === "searching" ? "Searching your workspaces" : stage.kind === "retrieved" ? `Reading ${stage.sources} ${stage.sources === 1 ? "source" : "sources"}` : stage.kind === "writing" ? "Writing answer" : "Checking citations";
  return <div className="flex items-center gap-2 text-sm text-ink-3">
    {mark}
    <span aria-live="polite" aria-atomic="true"><span key={label} style={{ display: "inline-block", animation: reduced ? "none" : "cs-stage-in var(--dur-enter) var(--ease-out)" }}><span style={reduced ? undefined : { backgroundImage: "linear-gradient(90deg,var(--ink-3) 38%,var(--ink) 50%,var(--ink-3) 62%)", backgroundSize: "250% 100%", backgroundClip: "text", color: "transparent", animation: "cs-shimmer 1.8s linear infinite" }}>{label}</span></span></span>
    <span className="font-mono text-xs">{(Math.max(0, now - props.startedAt) / 1000).toFixed(1)}s</span>
  </div>;
}
