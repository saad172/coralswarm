"use client";

import { NO_EDIT_TITLE } from "@/lib/editAccess";
import Link from "next/link";
import { Button, Chip, Dot, Mono, Input, hueVar } from "@/components/ui";
import type { EntityKind, Reef } from "@/lib/api";
import { ENTITY_KIND_META, ENTITY_KINDS, entityKindOf, kindForeground } from "./topicMeta";

/* The in-page header: a Library breadcrumb + Edit/Promote/Ask row (mirrors the
   mock's own frame header, since AppShell's 52px chrome only carries a static
   "Topic" title), then the hero — kind chip, scope, freshness, the display
   title, and (in edit mode) the inline rename/re-type editor archive's
   reef.page.tsx offered and the mock's own copy doesn't model but doesn't
   contradict either. */

export function TopicHeader({
  reef,
  graphName,
  atomCount,
  sourceCount,
  semanticAtomCount,
  unmappedLegacyChunkCount,
  editing,
  draftTitle,
  onDraftTitleChange,
  savingCuration,
  curationErr,
  onStartEdit,
  onCancelEdit,
  onSaveTitle,
  onPickType,
  onShare,
  canPromote = true,
  canEdit = true,
  onAsk,
}: {
  reef: Reef;
  graphName: string;
  atomCount: number;
  sourceCount?: number | null;
  semanticAtomCount?: number | null;
  unmappedLegacyChunkCount?: number | null;
  editing: boolean;
  draftTitle: string;
  onDraftTitleChange: (v: string) => void;
  savingCuration: boolean;
  curationErr: string | null;
  onStartEdit: () => void;
  onCancelEdit: () => void;
  onSaveTitle: () => void;
  onPickType: (t: EntityKind) => void;
  onShare: () => void;
  /** False when there is no shared workspace to promote INTO — the modal
   *  would open with nothing to pick. */
  canPromote?: boolean;
  /** Server-derived (`ReefDetail.can_edit`) — `PATCH /v1/nodes/{id}` would
   *  403 for this caller when false (reachability ≠ ownership: a promoted
   *  team reef can be GET-able but not PATCH-able). Disabled, not hidden —
   *  same idiom as `canPromote` above, for the same reason: the control
   *  stays a stable, discoverable landmark and the tooltip explains why
   *  it's inert, instead of the row silently losing a button. */
  canEdit?: boolean;
  onAsk: () => void;
}) {
  const kind = entityKindOf(reef);
  const m = ENTITY_KIND_META[kind];

  return (
    <div className="mb-[22px]">
      <div className="mb-[18px] flex flex-wrap items-center gap-[8px]">
        <Link href="/home" className="text-[13px] text-ink-3 transition-colors hover:text-ink">
          Home
        </Link>
        <span className="text-ink-4">/</span>
        <span className="text-[13px] text-ink-3">{m.plural}</span>
        <span className="text-ink-4">/</span>
        <span className="truncate text-[14px] font-medium text-ink">{reef.title}</span>
        <div className="ml-auto flex flex-none items-center gap-[6px]">
          {!editing && (
            <Button
              size="sm"
              onClick={onStartEdit}
              disabled={!canEdit}
              title={canEdit ? undefined : NO_EDIT_TITLE}
            >
              Edit
            </Button>
          )}
          {/* "Promote", not "Share". This opens the promote modal — it copies
              the topic into a shared workspace, one-way and widening only.
              "Share" already means something different one screen away (a
              public link, on a chat), and the product's own vocabulary for
              this is promote. */}
          <Button
            size="sm"
            variant="secondary"
            onClick={onShare}
            disabled={!canPromote}
            title={canPromote ? "Copy this topic into a shared workspace" : "No shared workspace to promote into yet"}
          >
            Promote
          </Button>
          <Button size="sm" variant="primary" onClick={onAsk}>
            Ask about this topic
          </Button>
        </div>
      </div>

      {editing ? (
        <div className="flex flex-col gap-[10px]">
          <div className="flex flex-wrap gap-[6px]">
            {ENTITY_KINDS.map((t) => {
              const tm = ENTITY_KIND_META[t];
              const on = t === kind;
              return (
                <button
                  key={t}
                  type="button"
                  onClick={() => onPickType(t)}
                  disabled={savingCuration}
                  className="inline-flex items-center gap-[6px] rounded-pill px-[9px] py-[3px] font-mono text-[10px] uppercase tracking-[0.1em] transition-opacity"
                  style={{
                    color: kindForeground(tm.hue),
                    background: `color-mix(in srgb, ${hueVar(tm.hue)} 18%, transparent)`,
                    opacity: on ? 1 : 0.45,
                  }}
                >
                  {tm.glyph} {tm.label}
                </button>
              );
            })}
          </div>
          <Input
            value={draftTitle}
            autoFocus
            className="font-display text-[26px] italic leading-[1.15]"
            onChange={(e) => onDraftTitleChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && draftTitle.trim()) onSaveTitle();
              if (e.key === "Escape") onCancelEdit();
            }}
          />
          <div className="flex items-center gap-[8px]">
            <Button variant="primary" size="sm" disabled={savingCuration || !draftTitle.trim()} onClick={onSaveTitle}>
              {savingCuration ? "Saving…" : "Save"}
            </Button>
            <Button size="sm" onClick={onCancelEdit}>
              Done
            </Button>
            {curationErr && <span className="text-[12.5px] text-bad">{curationErr}</span>}
          </div>
        </div>
      ) : (
        <>
          <div className="mb-[11px] flex flex-wrap items-center gap-[9px]">
            <Chip
              className="uppercase"
              style={{ color: kindForeground(m.hue), background: `color-mix(in srgb, ${hueVar(m.hue)} 16%, transparent)` }}
            >
              {m.glyph} {m.label}
            </Chip>
            <span className="inline-flex items-center gap-[6px] font-mono text-[9.5px] text-ink-3">
              <Dot hue="teal" size={6} />
              {graphName.toUpperCase()}
            </span>
            <Mono className="text-ink-2">
              {sourceCount != null && <>{sourceCount} preserved source{sourceCount === 1 ? "" : "s"} · </>}
              {semanticAtomCount != null && <>{semanticAtomCount} atom{semanticAtomCount === 1 ? "" : "s"} · </>}
              {atomCount} retrieval item{atomCount === 1 ? "" : "s"}
              {!!unmappedLegacyChunkCount && <span className="block text-ink-2">{unmappedLegacyChunkCount} item{unmappedLegacyChunkCount === 1 ? " has" : "s have"} no preserved source mapping</span>}
            </Mono>
          </div>
          <h1 className="font-display text-[36px] font-normal leading-[1.25] tracking-[-0.01em] text-ink">
            {reef.title}
          </h1>
        </>
      )}
    </div>
  );
}
