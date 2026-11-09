"use client";

/* The recording prompt. CoralSwarm can only save a transcript Teams actually
 * made, so every place the user turns CoralSwarm on says so, in the amber inset
 * the Teams pane introduced (TeamsAddMeeting). When Teams was told to record
 * automatically, a calm hint replaces it.
 *
 *   saved      a meeting that already exists: start it from the meeting
 *   compose    an unsaved one-off: Meeting options is the primary instruction
 *   series     a recurring meeting: a series-level instruction */

import { copy } from "@/lib/addin/copy";
import type { AutoTranscription } from "@/lib/api";
import { Icon } from "./kit";

export type PromptKind = "saved" | "compose" | "series";

export function TranscribePrompt({ kind, auto, reason }: {
  kind: PromptKind;
  /** The server's answer, when it has one. Anything but "enabled" shows the prompt. */
  auto?: AutoTranscription | null;
  reason?: string | null;
}) {
  const t = copy.transcribe;
  if (auto === "enabled") {
    return <p data-testid="transcribe-auto" className="px-3 py-2.5 text-pane-xs text-ink-3">{kind === "series" ? t.autoHintSeries : t.autoHint}</p>;
  }
  const first = kind === "series" ? t.seriesFirst : kind === "compose" ? t.composeFirst : t.steps;
  const second = kind === "series" ? t.seriesFallback : kind === "compose" ? t.composeFallback : null;
  const why = reason && Object.prototype.hasOwnProperty.call(t.reason, reason) ? t.reason[reason] : null;
  return (
    <div data-testid="transcribe-prompt" className="mx-3 my-2.5 flex gap-2 rounded-card bg-hue-amber/15 px-3 py-3 text-left">
      <div className="flex-none self-start"><Icon name="video" hue="coral" size={20} /></div>
      <div className="min-w-0 break-words">
        <p className="font-medium text-ink">{t.title}</p>
        <p className="mt-1 text-pane text-ink-2">{first}</p>
        {second ? <p className="mt-1 text-pane text-ink-2">{second}</p> : null}
        {why ? <p className="mt-1 text-pane-xs text-ink-3">{why}</p> : null}
        <p className="mt-1 text-pane-xs text-ink-3">{t.note}</p>
      </div>
    </div>
  );
}
