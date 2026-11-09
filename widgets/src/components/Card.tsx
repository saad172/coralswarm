import type { ComponentChildren } from "preact";
import { Mandala } from "./Mandala";
import { ExternalLinkIcon } from "./Icons";
import { FONT_MONO, type Tokens } from "../theme";
import { openExternalLink } from "../openai";
import { gutter, safeAreaPadding, useHostLayout } from "../layout";
import { AnswerUi, answerUiStyle } from "../answerUi";

interface CardProps {
  t: Tokens;
  /** Mono status line, e.g. "answered from 4 sources · 2.3s". */
  status: string;
  /** "open in coralswarm" / "open reef" / "open library" / "open today". */
  linkLabel?: string;
  linkHref?: string;
  /** In-card chrome that must survive a host height cap (e.g. ask_ocean's
   *  fullscreen "expand" control). Rendered after `children` so a scrolling
   *  body can sit between the header and this footer. */
  footer?: ComponentChildren;
  children: ComponentChildren;
}

/** Host-aware adapter over the shared frame. Keeps widget navigation,
 * safe areas, compact gutters and header/footer height budgets local. */
export function Card({ t, status, linkLabel, linkHref, footer, children }: CardProps) {
  const { size, safeArea, touch } = useHostLayout();
  const headerGutter = gutter(size, 14);
  return (
    <AnswerUi.CardFrame attributes={{
      "data-card": "", className: "csau-widget",
      style: { ...answerUiStyle(t, touch), "--csau-widget-gutter": `${headerGutter}px`, width: "100%", minWidth: 0, boxShadow: t.shadow, ...safeAreaPadding(safeArea) },
    }} header={
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: 9,
        }}
      >
        <Mandala size={18} />
        <span
          style={{
            fontFamily: FONT_MONO,
            fontSize: 11,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            color: t.secondary,
            flex: "1 1 180px",
            minWidth: 0,
            overflowWrap: "anywhere",
          }}
        >
          {status}
        </span>
        {linkLabel && linkHref && (
          <span
            role="link"
            tabIndex={0}
            onClick={() => openExternalLink(linkHref)}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                if (!e.repeat) openExternalLink(linkHref);
              }
            }}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 5,
              fontFamily: FONT_MONO,
              fontSize: 11,
              color: t.secondary,
              cursor: "pointer",
              flex: "none",
              minHeight: touch ? 44 : undefined,
            }}
          >
            {linkLabel}
            <ExternalLinkIcon size={12} color={t.mutedSoft} />
          </span>
        )}
      </div>
    } children={[<>{children}</>]} footer={footer == null ? null : <>{footer}</>} />
  );
}
