/** Shared answer presentation for React (web) and Preact (MCP widgets).
 * Renderers own state, transport, source normalization, and host navigation. */
export type Child<Node> = Node | string | number | null;
export interface NativeProps {
  className?: string;
  id?: string;
  key?: string | number;
  type?: "button";
  role?: string;
  title?: string;
  tabIndex?: number;
  style?: Record<string, string | number | undefined>;
  disabled?: boolean;
  onClick?: () => void;
  ref?: (element: HTMLElement | null) => void;
  [attribute: `aria-${string}`]: string | number | boolean | undefined;
  [attribute: `data-${string}`]: string | number | boolean | undefined;
}
export type ElementFactory<Node> = (tag: string, props: NativeProps | null, ...children: Child<Node>[]) => Node;
export interface EvidenceItem {
  id: string;
  /** Original citation number; omit for unnumbered search/activity evidence. */
  number?: number;
  title: string;
  excerpt?: string;
  provenance: readonly { label: string; value: string }[];
  /** Consumer-provided semantic color or CSS variable, never inferred from data. */
  accent?: string;
  warning?: string;
  /** Consumer-supplied verification mark (e.g. a Jev citation verdict). The
   *  label and glyph carry the meaning; `tone` only colours it. */
  check?: EvidenceCheck;
}
export interface EvidenceCheck {
  label: string;
  glyph: string;
  tone: "ok" | "warn" | "bad" | "muted";
  /** Longer explanation, shown in detail and as the accessible description. */
  description?: string;
}
export interface ProgressProps<Node> { label: string; elapsedSeconds?: number; mark?: Child<Node> }
export interface EvidenceCardProps<Node> {
  source: EvidenceItem;
  onSelect?: () => void;
  active?: boolean;
  showExcerpt?: boolean;
  after?: Child<Node>;
}
export interface EvidenceListProps {
  sources: readonly EvidenceItem[];
  onSelect?: (id: string) => void;
  showExcerpt?: boolean;
  label?: string;
  columns?: 1 | 2 | 3;
  activeId?: string;
}
export interface EvidenceDetailProps<Node> {
  source: EvidenceItem;
  headingId?: string;
  headingRef?: (element: HTMLElement | null) => void;
  before?: Child<Node>;
  after?: Child<Node>;
}
export interface CardFrameProps<Node> {
  header?: Child<Node>;
  children?: readonly Child<Node>[];
  footer?: Child<Node>;
  attributes?: NativeProps;
}

export function createAnswerUi<Node>(h: ElementFactory<Node>) {
  const Check = (check: EvidenceCheck, prefix?: string): Node => h("span", { className: `csau-check csau-check-${check.tone}`, title: check.description },
    h("span", { className: "csau-check-glyph", "aria-hidden": true }, check.glyph),
    h("span", null, prefix ? `${prefix} ${check.label}` : check.label),
  );
  const Progress = ({ label, elapsedSeconds, mark }: ProgressProps<Node>): Node => h("div", { className: "csau csau-progress" },
    mark == null ? null : h("span", { className: "csau-progress-mark", "aria-hidden": true }, mark),
    h("span", { className: "csau-progress-label", role: "status" }, label),
    elapsedSeconds == null ? null : h("span", { className: "csau-elapsed", "aria-label": `${elapsedSeconds} seconds elapsed` }, `${elapsedSeconds}s`),
  );

  const EvidenceCard = ({ source, onSelect, active = false, showExcerpt = false, after }: EvidenceCardProps<Node>): Node => h(onSelect ? "button" : "article", {
    className: `csau csau-evidence-card${active ? " csau-evidence-card-active" : ""}`,
    ...(onSelect ? { type: "button", onClick: onSelect, "aria-label": `Open source${source.number == null ? "" : ` ${source.number}`}: ${source.title}${source.check ? ` (${source.check.label})` : ""}` } as const : {}),
    ...(active && onSelect ? { "aria-pressed": true } : {}),
    style: source.accent ? { "--csau-item-accent": source.accent } : undefined,
  },
    source.number == null ? null : h("span", { className: "csau-source-number" }, source.number),
    h("span", { className: "csau-source-summary" },
      h("span", { className: "csau-source-title" }, source.title),
      showExcerpt && source.excerpt ? h("span", { className: "csau-source-preview" }, source.excerpt) : null,
      h("span", { className: "csau-source-meta" },
        h("span", { className: "csau-source-dot", "aria-hidden": true }),
        h("span", { className: "csau-source-meta-text" }, source.provenance.map(({ value }) => value).filter(Boolean).join(" · ")),
      ),
      source.check ? h("span", { className: "csau-check-row" }, Check(source.check)) : null,
      after ?? null,
    ),
  );

  const EvidenceList = ({ sources, onSelect, showExcerpt, label, columns, activeId }: EvidenceListProps): Node | null => {
    if (!sources.length) return null;
    return h("section", { className: "csau csau-evidence-list", "aria-label": "Answer sources", style: columns ? { "--csau-columns": columns } : undefined },
      h("div", { className: "csau-eyebrow" }, label ?? `${sources.length} source${sources.length === 1 ? "" : "s"}`),
      h("div", { className: "csau-evidence-grid" }, ...sources.map((source) => h("div", { key: source.id, className: "csau-evidence-cell" },
        EvidenceCard({ source, showExcerpt, active: source.id === activeId, onSelect: onSelect ? () => onSelect(source.id) : undefined }),
      ))),
    );
  };

  const EvidenceDetail = ({ source, headingId, headingRef, before, after }: EvidenceDetailProps<Node>): Node => h("section", {
    className: "csau csau-evidence-detail", "aria-labelledby": headingId,
    style: source.accent ? { "--csau-item-accent": source.accent } : undefined,
  },
    before ?? null,
    source.number == null ? null : h("div", { className: "csau-eyebrow csau-detail-number" }, `Source ${source.number}`),
    h("h3", { className: "csau-detail-title", id: headingId, ref: headingRef, tabIndex: headingRef ? -1 : undefined }, source.title),
    source.check ? h("div", { className: "csau-check-detail" }, Check(source.check),
      source.check.description ? h("p", { className: "csau-check-description" }, source.check.description) : null) : null,
    h("dl", { className: "csau-provenance" }, ...source.provenance.flatMap(({ label, value }, i) => [
      h("dt", { key: `label-${i}` }, label), h("dd", { key: `value-${i}` }, value || "Not supplied"),
    ])),
    h("div", { className: "csau-excerpt" }, h("div", { className: "csau-eyebrow" }, "Source excerpt"),
      h("p", { className: "csau-excerpt-text" }, source.excerpt || "No excerpt was supplied for this source.")),
    source.warning ? h("p", { className: "csau-warning" }, `Unconfirmed relationship · ${source.warning}`) : null,
    after ?? null,
  );

  const CardFrame = ({ header, children = [], footer, attributes = {} }: CardFrameProps<Node>): Node => h("section", {
    ...attributes, className: `csau csau-card-frame${attributes.className ? ` ${attributes.className}` : ""}`,
  },
    header == null ? null : h("div", { className: "csau-frame-header" }, header),
    h("div", { className: "csau-frame-body" }, ...children),
    footer == null ? null : h("div", { className: "csau-frame-footer" }, footer),
  );
  return { Progress, EvidenceCard, EvidenceList, EvidenceDetail, CardFrame };
}
