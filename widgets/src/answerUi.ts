import { h, type Attributes, type VNode } from "preact";
import { createAnswerUi, type ElementFactory, type EvidenceItem, type NativeProps } from "../../packages/answer-ui/src";
import { FONT_MONO, FONT_SANS, sourceTypeColor, sourceTypeLabel, type Tokens } from "./theme";
import { contextLabel } from "./citation";
import type { CitationDetail } from "./types";
import "../../packages/answer-ui/src/styles.css";
import "./answerUi.css";

const element: ElementFactory<VNode<Attributes & NativeProps>> = (tag, props, ...children) => h(tag, props, ...children);
export const AnswerUi = createAnswerUi(element);
export type { EvidenceItem };

/** The shared renderer reads semantic vars; the host still owns its palette,
 * fonts, safe area and touch targets. No React runtime enters these bundles. */
export function answerUiStyle(t: Tokens, touch = false): Record<string, string | number> {
  return {
    "--csau-font-body": FONT_SANS, "--csau-font-mono": FONT_MONO,
    "--csau-bg": t.bg, "--csau-bg-subtle": t.sourceCardBg, "--csau-bg-hover": t.headerBg,
    "--csau-bg-header": t.headerBg, "--csau-ink": t.ink, "--csau-ink-muted": t.secondary,
    "--csau-ink-faint": t.muted, "--csau-line": t.line, "--csau-line-subtle": t.lineSoft,
    "--csau-accent": t.coral, "--csau-accent-soft": t.coralChipBg, "--csau-accent-ink": t.coralDark,
    "--csau-warning": t.sourceAmber, "--csau-radius-card": "12px", "--csau-radius-control": "10px",
    "--csau-radius-badge": "6px", "--csau-radius-pill": "999px", "--csau-target-size": touch ? "44px" : "0px",
  };
}

export function citationEvidence(t: Tokens, citation: CitationDetail, number: number): EvidenceItem {
  return {
    id: `${citation.atom_id}:${number}`, number,
    title: citation.title?.trim() || citation.content?.trim() || "Untitled coral",
    excerpt: citation.content || undefined,
    provenance: [{label:"Type",value:sourceTypeLabel(citation.source_type)}, {label:"Context",value:contextLabel(citation)}],
    accent: sourceTypeColor(t, citation.source_type),
  };
}
