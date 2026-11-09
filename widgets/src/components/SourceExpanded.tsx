import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { FONT_MONO, sourceTypeColor, type Tokens } from "../theme";
import { CloseIcon, ExternalLinkIcon, iconForSourceType } from "./Icons";
import { whoWhereWhen, contextLabel } from "../citation";
import { AnswerUi, citationEvidence } from "../answerUi";
import { useHostLayout } from "../layout";
import { callTool, openExternalLink } from "../openai";
import type { AtomDetail, CitationDetail } from "../types";
import { coralHref } from "../links";

interface SourceExpandedProps {
  t: Tokens;
  index: number;
  citation: CitationDetail;
  onClose: () => void;
}

/** The expanded source card — SourceExpanded.dc.html. Content comes straight
 *  from `citation_details` (never `callTool(get_atom)`) UNLESS the citation
 *  carries `body_omitted: true` (a date-range-scan coral whose body was
 *  never read), in which case this is the one place that DOES call
 *  `get_atom` — the founder's override in the task brief. */
export function SourceExpanded({ t, index, citation, onClose }: SourceExpandedProps) {
  const [atom, setAtom] = useState<AtomDetail | null>(null);
  const [loadingAtom, setLoadingAtom] = useState(false);
  const { touch } = useHostLayout();
  const heading = useRef<HTMLElement | null>(null);
  const opener = useRef<HTMLElement | null>(null);
  useLayoutEffect(() => {
    if (document.activeElement instanceof HTMLElement && !heading.current?.closest(".csau-evidence-detail")?.contains(document.activeElement)) opener.current = document.activeElement;
    // A host height cap makes the source region scroll; reveal the opened detail.
    heading.current?.focus();
  }, [citation.atom_id, index]);
  useEffect(() => () => { if (opener.current?.isConnected) opener.current.focus(); }, []);
  useEffect(() => {
    const closeWithEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape" && heading.current?.closest(".csau-evidence-detail")?.contains(document.activeElement)) {
        event.preventDefault();
        onClose();
      }
    };
    document.addEventListener("keydown", closeWithEscape);
    return () => document.removeEventListener("keydown", closeWithEscape);
  }, [onClose]);

  useEffect(() => {
    if (!citation.body_omitted) return;
    let cancelled = false;
    // Clear any PREVIOUS citation's atom before this request starts — without
    // this, a second body_omitted citation whose get_atom call fails (or is
    // still in flight) would keep rendering the first citation's content
    // under the new one once `loadingAtom` flips back to false.
    setAtom(null);
    setLoadingAtom(true);
    void callTool<AtomDetail>("get_atom", { atom_id: citation.atom_id })
      .then((result) => {
        if (!cancelled) setAtom(result);
      })
      .catch(() => {
        if (!cancelled) setAtom(null);
      })
      .finally(() => {
        if (!cancelled) setLoadingAtom(false);
      });
    return () => {
      cancelled = true;
    };
    // Only ever re-fetch if the citation itself changes (a new expand).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [citation.atom_id, citation.body_omitted]);

  const Icon = iconForSourceType(citation.source_type);
  const color = sourceTypeColor(t, citation.source_type);
  const { who, where, when } = whoWhereWhen(citation);
  const bodyText = citation.body_omitted ? (atom?.content ?? null) : citation.content;

  const source = {
    ...citationEvidence(t, citation, index),
    excerpt: loadingAtom ? "Loading full coral…" : (bodyText ?? "Content unavailable."),
    provenance: [{ label: "Who", value: who }, { label: "Where", value: where }, { label: "When", value: when }],
  };
  const controlStyle = { display: "inline-flex", alignItems: "center", gap: 5, border: 0, background: "transparent", color: t.muted, cursor: "pointer", minHeight: touch ? 44 : undefined, fontFamily: FONT_MONO, fontSize: 12 };
  return <AnswerUi.EvidenceDetail source={source} headingId={`widget-source-${index}`} headingRef={element => { heading.current = element; }}
    before={<div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
      <Icon size={15} color={color} />
      <span style={{ flex: 1, minWidth: 0, overflowWrap: "anywhere", fontFamily: FONT_MONO, fontSize: 9.5, color: t.muted }}>{contextLabel(citation)}</span>
      <button type="button" style={controlStyle} onClick={() => openExternalLink(coralHref(citation.atom_id))}>source <ExternalLinkIcon size={12} color={t.muted} /></button>
      <button type="button" aria-label="Close" style={controlStyle} onClick={onClose}><CloseIcon size={13} color={t.mutedSoft} /></button>
    </div>}
    after={<>
      {citation.superseded_by && <p style={{ color: t.sourceAmber, fontSize: 12 }}>This source has been superseded.</p>}
      {!!citation.reefs?.length && <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 14 }}>
        <span style={{ fontFamily: FONT_MONO, fontSize: 9.5, letterSpacing: "0.13em", textTransform: "uppercase", color: t.muted }}>settled into</span>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {citation.reefs.map(title => <span key={title} style={{ border: `1px solid ${t.line}`, background: t.bg, borderRadius: 9, padding: "6px 11px", fontSize: 12.5, color: t.ink, maxWidth: "100%", overflowWrap: "anywhere" }}>{title}</span>)}
        </div>
      </div>}
    </>} />;
}
