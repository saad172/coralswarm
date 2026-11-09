"use client";

/* The Home launchpad's pieces (Claude Design "Home" mock, HOME_SPEC.md).
   Presentational: app/(app)/home/page.tsx owns the three reads and the
   browser memory, these own the markup. Built from the kit
   (components/ui) per the spec's mapping; every colour, radius, shadow and
   type size is a token (type: `text-ask` and the UI text roles `text-lead`,
   `text-item`, `text-note`, `text-micro`). The 28px kind tile, 22px step
   marker and 6px dot are component sizes. Motion is the house control
   transition plus `cs-pulse` on the new dot and `cs-skeleton` on
   placeholders, both off under reduced motion. */

import Link from "next/link";
import { forwardRef, useState, type ComponentType, type ReactNode, type SVGProps } from "react";
import {
  Button,
  Card,
  Chip,
  CopyPill,
  CountPill,
  Dot,
  ErrorNote,
  Eyebrow,
  IconButton,
  ListRow,
  Meter,
  Mono,
  PageTitle,
  RowMeta,
  RowTitle,
  Skeleton,
  SuggestionPill,
  TitleAccent,
  buttonClasses,
  cn,
} from "@/components/ui";
import {
  IconAdd,
  IconChats,
  IconCode,
  IconMeetings,
  IconRefresh,
  IconSend,
  IconSessions,
  IconWorkspaces,
} from "@/components/icons";
import type { ContinueItem, ContinueKind, HomeAttention, WhatsNewItem } from "@/lib/homeLaunchpadApi";
import {
  attentionMeta,
  continueHref,
  continueMeta,
  reefHref,
  reefHue,
  whatsNewOceanHue,
  type ChecklistStep as ChecklistStepModel,
  type PromptStarter,
} from "@/lib/homeLaunchpad";
import { agoShort, connectorReconnectHref } from "@/lib/homeView";

type Glyph = ComponentType<SVGProps<SVGSVGElement>>;

const KIND_ICON: Record<ContinueKind, Glyph> = {
  chat: IconChats,
  coding: IconCode,
  agent: IconSessions,
  meeting: IconMeetings,
  workspace: IconWorkspaces,
};

// ── Header ──────────────────────────────────────────────────────────────

/** One page-level refresh: re-runs all three reads. Sections keep their
 *  content while it runs. `disabled` until the session can read. */
export function RefreshControl({
  refreshing,
  disabled = false,
  onRefresh,
}: {
  refreshing: boolean;
  disabled?: boolean;
  onRefresh: () => void;
}) {
  return (
    <IconButton label={refreshing ? "Refreshing…" : "Refresh home"} disabled={disabled || refreshing} onClick={onRefresh}>
      <IconRefresh width={14} height={14} />
    </IconButton>
  );
}

// ── 1 · Greeting and Ask ────────────────────────────────────────────────

export function Greeting({
  greeting,
  name,
  lead,
  children,
}: {
  greeting: string;
  name: string | null;
  lead?: string;
  children: ReactNode;
}) {
  return (
    <section aria-label="Ask" className="flex flex-col gap-[16px]">
      <div>
        {/* The name is its own node: `italicWord` would accent its first
            match, and "Go" first matches inside "Good". */}
        <PageTitle>
          {greeting}
          {name && (
            <>
              , <TitleAccent>{name}</TitleAccent>
            </>
          )}
        </PageTitle>
        {lead && <p className="mt-[4px] text-lead text-ink-2">{lead}</p>}
      </div>
      {children}
    </section>
  );
}

/** The composer anatomy at launchpad size: `--r-frame`, `--shadow-card`, and
 *  the field focus (brand border plus the `--focus-ring` ring). Enter opens a
 *  new chat seeded with the question; Shift+Enter is a new line. */
export const AskBox = forwardRef<HTMLTextAreaElement, { onAsk: (question: string) => void }>(function AskBox({ onAsk }, ref) {
  const [value, setValue] = useState("");
  const question = value.trim();
  const submit = () => {
    if (question) onAsk(question);
  };
  return (
    <div className="rounded-frame border border-line bg-bg px-[16px] pb-[12px] pt-[16px] shadow-card [transition:var(--transition-control)] focus-within:border-brand focus-within:ring focus-within:ring-focus-ring">
      <textarea
        ref={ref}
        rows={2}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
            e.preventDefault();
            submit();
          }
        }}
        placeholder="Ask your ocean…"
        aria-label="Ask your ocean"
        className="block w-full resize-none border-0 bg-transparent p-0 font-sans text-ask text-ink outline-none placeholder:text-ink-4"
      />
      <div className="mt-[8px] flex items-center gap-[8px]">
        <Mono className="min-w-0 flex-1 truncate">enter opens a new chat · shift+enter for a new line</Mono>
        <Button
          size="sm"
          variant={question ? "primary" : "secondary"}
          disabled={!question}
          onClick={submit}
          icon={<IconSend width={13} height={13} />}
        >
          Ask
        </Button>
      </div>
    </div>
  );
});

const STARTER_SKELETON_WIDTHS = [148, 196, 170];

/** Each chip asks its whole question; a chip shortened for length shows the
 *  whole question as its tooltip. */
export function PromptStarters({
  items,
  loading,
  onPick,
}: {
  items: readonly PromptStarter[];
  loading: boolean;
  onPick: (question: string) => void;
}) {
  if (loading) {
    return (
      <div className="flex flex-wrap gap-[8px]" aria-hidden>
        {STARTER_SKELETON_WIDTHS.map((w) => (
          <Skeleton key={w} w={w} h={31} className="flex-none rounded-pill" />
        ))}
      </div>
    );
  }
  return (
    <div role="group" aria-label="Prompt starters" className="flex flex-wrap gap-[8px]">
      {items.map((item) => (
        <SuggestionPill
          key={item.question}
          className="max-w-full truncate"
          title={item.label === item.question ? undefined : item.question}
          onClick={() => onPick(item.question)}
        >
          {item.label}
        </SuggestionPill>
      ))}
    </div>
  );
}

// ── 2 · Needs attention ─────────────────────────────────────────────────

/** The first broken connector, with a Fix link to its re-auth page. The
 *  page renders it only while the attention read reports one. */
export function AttentionRow({ item }: { item: HomeAttention }) {
  return (
    <section
      aria-label="Needs attention"
      className="flex items-center gap-[12px] rounded-control border border-warn-line bg-bg py-[10px] pl-[14px] pr-[10px]"
    >
      <span aria-hidden className="h-[7px] w-[7px] flex-none rounded-pill bg-warn ring ring-warn-soft" />
      <span className="min-w-0 flex-1">
        <span className="block truncate text-item font-medium text-ink">{item.label}</span>
        <Mono className="text-ink-3">{attentionMeta(item)}</Mono>
      </span>
      <Link
        href={connectorReconnectHref(item.provider)}
        aria-label={`Fix: ${item.label}`}
        className={buttonClasses({ variant: "primary", size: "sm" })}
      >
        Fix
      </Link>
    </section>
  );
}

// ── Section frame: eyebrow · rule · quiet freshness ─────────────────────

export function HomeSection({
  label,
  eyebrow,
  meta,
  children,
}: {
  /** Accessible name of the section ("Continue"). */
  label: string;
  /** The visible mono eyebrow ("continue"). */
  eyebrow: string;
  meta: string;
  children: ReactNode;
}) {
  return (
    <section aria-label={label}>
      <div className="mb-[8px] flex items-center gap-[10px] px-[4px]">
        <Eyebrow as="h2" className="flex-none">
          {eyebrow}
        </Eyebrow>
        <span aria-hidden className="h-px min-w-[12px] flex-1 bg-line-2" />
        <Mono className="min-w-0 truncate">{meta}</Mono>
      </div>
      {children}
    </section>
  );
}

/** A section with nothing to show, or a read that failed with nothing cached. */
export function SectionNote({ children }: { children: ReactNode }) {
  return (
    <Card quiet className="text-note text-ink-3">
      {children}
    </Card>
  );
}

export function SectionError({ what, onRetry }: { what: string; onRetry: () => void }) {
  return (
    <ErrorNote
      action={
        <Button size="sm" variant="secondary" onClick={onRetry}>
          Retry
        </Button>
      }
    >
      Could not load {what}.
    </ErrorNote>
  );
}

// ── 3 · Continue ────────────────────────────────────────────────────────

function Ago({ iso, now }: { iso: string; now: Date }) {
  return (
    <RowMeta className="w-[52px] text-right">
      <time dateTime={iso}>{agoShort(iso, now)}</time>
    </RowMeta>
  );
}

export function ContinueList({ items, now }: { items: readonly ContinueItem[]; now: Date }) {
  return (
    <Card flush className="overflow-hidden">
      {items.map((item) => {
        const Icon = KIND_ICON[item.kind] ?? IconChats;
        return (
          <ListRow key={`${item.kind}:${item.id}`} href={continueHref(item)} roomy>
            <span aria-hidden className="grid h-[28px] w-[28px] flex-none place-items-center rounded-row bg-bg-2 text-ink-2">
              <Icon width={15} height={15} />
            </span>
            <RowTitle meta={<span className="min-w-0 truncate">{continueMeta(item)}</span>}>{item.title}</RowTitle>
            {/* The count is shown once, here; `continueMeta` never repeats it. */}
            {item.count !== null && item.count >= 2 && <CountPill>{item.count} sessions</CountPill>}
            <Ago iso={item.at} now={now} />
          </ListRow>
        );
      })}
    </Card>
  );
}

// ── 4 · What's new ──────────────────────────────────────────────────────

/** `--brand` + `cs-pulse`: formed after the last visit. An empty slot keeps
 *  the column aligned when the row is not new. */
export function NewDot({ on }: { on: boolean }) {
  if (!on) return <span aria-hidden className="h-[6px] w-[6px] flex-none" />;
  return (
    <span
      role="img"
      aria-label="new since your last visit"
      className="h-[6px] w-[6px] flex-none animate-new-dot rounded-pill bg-brand motion-reduce:animate-none"
    />
  );
}

export function WhatsNewList({
  items,
  isNew,
  now,
}: {
  items: readonly WhatsNewItem[];
  isNew: (item: WhatsNewItem) => boolean;
  now: Date;
}) {
  return (
    <Card flush className="overflow-hidden">
      {items.map((item) => (
        <ListRow key={item.reef_id} href={reefHref(item)} roomy>
          <span className="w-[72px] flex-none">
            <Chip hue={reefHue(item.kind)}>{item.kind}</Chip>
          </span>
          <RowTitle
            meta={
              <>
                <Dot hue={whatsNewOceanHue(item.ocean)} size={6} />
                <span className="min-w-0 truncate">{item.ocean.name}</span>
              </>
            }
          >
            {item.title}
          </RowTitle>
          <NewDot on={isNew(item)} />
          <Ago iso={item.formed_at} now={now} />
        </ListRow>
      ))}
    </Card>
  );
}

// ── First-load skeleton ─────────────────────────────────────────────────

const SKELETON_WIDTHS = ["62%", "48%", "70%", "54%", "66%", "44%"];

/** Drawn only while a section has no data at all. */
export function SkeletonList({ rows, chip = false, label }: { rows: number; chip?: boolean; label: string }) {
  return (
    <Card flush className="overflow-hidden" role="status" aria-label={label} aria-busy="true">
      {SKELETON_WIDTHS.slice(0, rows).map((w) => (
        <div key={w} className="flex items-center gap-[12px] border-b border-line-3 px-[14px] py-[11px] last:border-b-0">
          {chip ? (
            <Skeleton w={56} h={18} className="flex-none rounded-pill" />
          ) : (
            <Skeleton w={28} h={28} className="flex-none rounded-row" />
          )}
          <span className="flex flex-1 flex-col gap-[6px]">
            <Skeleton w={w} h={11} />
            <Skeleton w="26%" h={8} />
          </span>
          <Skeleton w={28} h={8} className="flex-none" />
        </div>
      ))}
    </Card>
  );
}

// ── State B · setup checklist ───────────────────────────────────────────

function ChecklistStep({
  n,
  step,
  title,
  hint,
  action,
}: {
  n: number;
  step: ChecklistStepModel;
  title: string;
  hint: ReactNode;
  action?: ReactNode;
}) {
  return (
    <li
      aria-current={step.current ? "step" : undefined}
      className={cn(
        "flex items-start gap-[12px] border-b border-line-3 px-[16px] py-[14px] last:border-b-0",
        step.current ? "bg-bg" : "bg-bg-subtle",
      )}
    >
      {/* Done but broken (step 1 while a connector needs the user): the warn
          marker of the needs-attention row, not the success check. */}
      <span
        aria-hidden
        className={cn(
          "mt-px grid h-[22px] w-[22px] flex-none place-items-center rounded-pill border font-mono text-micro",
          step.attention
            ? "border-warn-line bg-warn-soft text-warn"
            : step.done
              ? "border-ok bg-ok text-on-brand"
              : step.current
                ? "border-ink text-ink"
                : "border-line text-ink-3",
        )}
      >
        {step.attention ? "!" : step.done ? "✓" : n}
      </span>
      <span className="min-w-0 flex-1">
        <span
          className={cn(
            "block text-item font-medium",
            step.done && !step.attention ? "text-ink-3 line-through decoration-ink-4" : "text-ink",
          )}
        >
          {title}
          {step.done && <span className="sr-only">{step.attention ? " (done, needs attention)" : " (done)"}</span>}
        </span>
        <span className="mt-[2px] block text-note text-ink-2">{hint}</span>
      </span>
      {action && <span className="flex flex-none items-center gap-[8px]">{action}</span>}
    </li>
  );
}

export function SetupChecklist({
  steps,
  done,
  total,
  mcpUrl,
  onMcpCopied,
  onAsk,
}: {
  steps: readonly ChecklistStepModel[];
  done: number;
  total: number;
  mcpUrl: string;
  /** The URL reached the clipboard: step 2 counts as done. */
  onMcpCopied: () => void;
  onAsk: () => void;
}) {
  const byId = Object.fromEntries(steps.map((s) => [s.id, s])) as Record<ChecklistStepModel["id"], ChecklistStepModel>;
  return (
    <Card flush className="overflow-hidden" role="region" aria-label="Set up your ocean">
      <div className="flex items-center gap-[12px] border-b border-line-2 px-[16px] py-[12px]">
        <Eyebrow as="h2" className="flex-1">
          set up your ocean
        </Eyebrow>
        <Mono className="text-ink-2">
          {done} of {total}
        </Mono>
        <Meter value={done} max={total} className="w-[72px]" ariaLabel={`${done} of ${total} setup steps done`} />
      </div>
      <ol>
        <ChecklistStep
          n={1}
          step={byId.connect}
          title="Connect a source"
          hint={
            byId.connect.attention
              ? "Connected, but a source needs attention. Fix it above and reefs start forming here as corals arrive."
              : byId.connect.done
                ? "A source is connected. Reefs start forming here as corals arrive."
                : "Slack, meetings, docs or code: pick a source and what it may read."
          }
          action={
            <Link
              href="/connectors"
              className={buttonClasses({ variant: byId.connect.done ? "ghost" : "primary", size: "sm" })}
            >
              <IconAdd width={13} height={13} aria-hidden />
              {byId.connect.done ? "Add another" : "Connect"}
            </Link>
          }
        />
        <ChecklistStep
          n={2}
          step={byId.mcp}
          title="Add your ocean to Claude, Cursor or Codex"
          hint={
            <>
              Paste this URL into your client&apos;s MCP settings so it can ask your ocean directly.
              <span className="mt-[8px] flex">
                <CopyPill value={mcpUrl} ariaLabel={`Copy the MCP URL ${mcpUrl}`} onCopied={onMcpCopied} />
              </span>
            </>
          }
        />
        <ChecklistStep
          n={3}
          step={byId.ask}
          title="Ask your first question"
          hint="Every answer cites the coral it came from."
          action={
            <Button size="sm" variant="secondary" aria-label="Ask your first question" onClick={onAsk}>
              Ask
            </Button>
          }
        />
      </ol>
    </Card>
  );
}
