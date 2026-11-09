"use client";

import { useState, Suspense } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { useApp } from "@/lib/providers";
import type { ReefType } from "@/lib/api";
import { coralQueryOptions } from "@/lib/appQueries";
import { coralTitle, isRawSlackChannelId, isRawSlackUserId, srcName, fmtDate } from "@/lib/coral";
import { Empty, Loading, Eyebrow, Mono, Dot, Avatar, Panel, Button, type Hue } from "@/components/ui";
import { SourceInventory } from "@/components/sources/SourceInventory";
import { PromoteModal, type PromoteTarget } from "@/components/PromoteModal";
import { DetailBreadcrumb } from "@/components/detail/DetailBreadcrumb";
import { KindChip } from "@/components/detail/KindChip";
import { LinkButton } from "@/components/detail/LinkButton";

/** Deep-link into Ask with the scope pre-seeded to an entity's ocean
 *  (ADR-0011 §E21) — scope only, no auto starter question. Every ocean is
 *  addressable by its own id now, the personal one included. */
function askHref(graphId: string | undefined | null): string {
  if (!graphId) return "/ask";
  return `/ask?scope=${encodeURIComponent(graphId)}`;
}

// Hue semantics (DESIGN_SYSTEM.md): entity→pink, decision→coral,
// thread→amber, reference→plum, atom/coral→teal.
const REEF_HUE: Record<ReefType, Hue> = {
  entity: "pink",
  decision: "coral",
  thread: "amber",
  reference: "plum",
};

// Static-export mode prohibits dynamic [id] segments; this page reads `?id=`.
// Wrapped in <Suspense> because useSearchParams suspends during prerender.
function CoralDetailInner() {
  const params = useSearchParams();
  const atomId = params.get("id") ?? "";
  const { token, authReady, getAuthToken, queryScope, currentGraph, graphs } = useApp();

  // The bearer is read inside the query, never depended on (see `token` in
  // lib/providers.tsx). A coral opened before paints from cache.
  const query = useQuery({
    ...coralQueryOptions({ getAuthToken, scope: queryScope }, atomId),
    enabled: authReady && !!atomId,
  });
  const coral = query.data?.atom ?? null;
  const loading = !query.error && query.data === undefined;
  const err = query.error
    ? (query.error.message.includes("not found") ? "Item not found or access denied." : query.error.message)
    : null;

  // Promote-to-ocean modal (shared component) — "promote this source" below.
  const [promoteTarget, setPromoteTarget] = useState<PromoteTarget | null>(null);
  const [promoteMsg, setPromoteMsg] = useState<string | null>(null);

  if (!atomId) {
    return (
      <>
        <DetailBreadcrumb crumbs={[{ label: "Home", href: "/home" }, { label: "Item" }]} />
        <Empty title="No item selected" hint="Pass an `id` query param to drill into a specific item." />
      </>
    );
  }

  if (loading) return <Loading what="surfacing this item" />;

  if (err && !coral) {
    return (
      <>
        <DetailBreadcrumb crumbs={[{ label: "Home", href: "/home" }, { label: "Item" }]} />
        <Empty title="Item unavailable" hint={err} />
      </>
    );
  }

  if (!coral) return null;

  // Human title for this coral — body first line → "{author} in #{channel}" →
  // typed placeholder. Never the raw source_id.
  const title = coralTitle(coral);

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────────── */}
      <DetailBreadcrumb
        crumbs={[
          { label: "Home", href: "/home" },
          { label: srcName(coral.source_type) },
        ]}
        actions={
          <LinkButton
            href={askHref(coral.graph_id)}
            title="Ask about this — opens Ask with the scope pre-seeded to this workspace"
          >
            Ask about this ↗
          </LinkButton>
        }
      />

      <div className="mb-[12px] flex flex-wrap items-center gap-[9px]">
        <KindChip hue="teal">item · {coral.source_type}</KindChip>
        <Mono className="text-ink-4">one raw unit of evidence</Mono>
      </div>

      <h1 className="mb-[16px] max-w-[24ch] font-display text-[36px] font-normal leading-[1.2] tracking-[-0.01em] text-ink">
        {title}
      </h1>

      <div className="mb-[28px] flex flex-wrap gap-x-[20px] gap-y-[4px] font-mono text-[11.5px] text-ink-3">
        <span>
          <strong className="font-medium text-ink">{coral.linked_reefs.length}</strong> topic
          {coral.linked_reefs.length === 1 ? "" : "s"}
        </span>
        <span>
          <strong className="font-medium text-ink">{coral.participants.length}</strong> participant
          {coral.participants.length === 1 ? "" : "s"}
        </span>
        <span>
          ingested <strong className="font-medium text-ink">{new Date(coral.created_at).toLocaleDateString()}</strong>
        </span>
      </div>

      <SourceInventory key={`${coral.graph_id}:${atomId}`} graphId={coral.graph_id} atomId={atomId} getAuthToken={getAuthToken} supplied={{ page: query.data?.source_inventory ?? null, refresh: () => void query.refetch() }} />

      <div className="grid grid-cols-1 gap-[28px] lg:grid-cols-[minmax(0,1fr)_300px]">
        {/* ── Main column ───────────────────────────────────── */}
        <div className="min-w-0">
          <Eyebrow className="mb-[10px]">body</Eyebrow>
          {coral.content ? (
            <div className="mb-[28px] whitespace-pre-wrap rounded-card border border-line bg-bg px-[20px] py-[16px] font-display text-[16px] leading-[1.6] text-ink">
              {coral.content}
            </div>
          ) : (
            <div className="mb-[28px] rounded-card border border-dashed border-line bg-bg-2 px-[20px] py-[16px] font-mono text-[12px] text-ink-3">
              This item&apos;s full text isn&apos;t available to display right now.
            </div>
          )}

          <Eyebrow className="mb-[10px]">linked topics · {coral.linked_reefs.length}</Eyebrow>

          {coral.linked_reefs.length === 0 ? (
            <Empty
              title="No topics yet"
              hint="The extraction current grows topics after embedding completes. Refresh in a few seconds if this item was just ingested."
            />
          ) : (
            <div className="flex flex-col">
              {coral.linked_reefs.map((r) => {
                const hue = REEF_HUE[r.type] ?? "pink";
                return (
                  <Link
                    key={r.node_id}
                    href={`/topics?id=${r.node_id}`}
                    className="flex items-center gap-[12px] border-b border-line-3 py-[12px] transition-colors last:border-b-0 hover:bg-bg-subtle"
                  >
                    <Dot hue={hue} size={7} />
                    <Avatar name={r.title} hue={hue} size={28} />
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[14px] font-medium text-ink">{r.title}</div>
                      <div className="font-mono text-[11px] tracking-[0.04em] text-ink-3">{r.type} topic</div>
                    </div>
                    <span className="font-mono text-[11px] text-ink-3">→</span>
                  </Link>
                );
              })}
            </div>
          )}

          {(() => {
            const targets = graphs.filter(
              (g) => (g.kind === "team" || g.kind === "org") && g.id !== currentGraph?.id,
            );
            if (targets.length === 0 || !currentGraph) return null;
            // Human label for this source instance — never the raw source_id
            // (an opaque identifier). Mirrors the reef detail page's
            // `sourceLabel`: humanized source type + this coral's own date.
            const label = `${srcName(coral.source_type)}${
              coral.timestamp ? ` · ${fmtDate(coral.timestamp)}` : ""
            }`;
            return (
              <Panel className="mt-[24px]">
                <Eyebrow className="mb-[8px]">promote this source</Eyebrow>
                <p className="mb-[12px] text-[12.5px] leading-[1.6] text-ink-2">
                  Publish every topic grown from this source into a shared workspace (widening only).
                </p>
                <Button
                  size="sm"
                  onClick={() => {
                    setPromoteMsg(null);
                    setPromoteTarget({ kind: "sources", sources: [{ id: coral.source_id, label }], graphId: coral.graph_id });
                  }}
                >
                  Promote →
                </Button>
                {promoteMsg && <p className="mt-[8px] font-mono text-[11px] text-hue-teal">{promoteMsg}</p>}
              </Panel>
            );
          })()}
        </div>

        {/* ── Side column ───────────────────────────────────── */}
        <aside className="self-start">
          <Panel>
            <Eyebrow className="mb-[10px]">at a glance</Eyebrow>
            <dl className="flex flex-col gap-[8px] font-mono text-[12px]">
              {coral.channel_name && (
                <div className="flex justify-between gap-[12px]">
                  <dt className="text-ink-3">channel</dt>
                  <dd className="truncate text-right text-ink">
                    {/* Ingest can persist the raw Slack id when name resolution
                        failed — never render it (no "#" on the placeholder). */}
                    {isRawSlackChannelId(coral.channel_name)
                      ? "Unknown channel"
                      : `#${coral.channel_name}`}
                  </dd>
                </div>
              )}
              {coral.author_name && (
                <div className="flex justify-between gap-[12px]">
                  <dt className="text-ink-3">author</dt>
                  <dd className="truncate text-right text-ink">
                    {isRawSlackUserId(coral.author_name) ? "Unknown author" : coral.author_name}
                  </dd>
                </div>
              )}
              <div className="flex justify-between gap-[12px]">
                <dt className="text-ink-3">scope</dt>
                <dd className="text-right text-ink">{coral.scope}</dd>
              </div>
              <div className="flex justify-between gap-[12px]">
                <dt className="text-ink-3">lang</dt>
                <dd className="text-right text-ink">{coral.fts_language}</dd>
              </div>
              {coral.timestamp && (
                <div className="flex justify-between gap-[12px]">
                  <dt className="text-ink-3">occurred</dt>
                  <dd className="text-right text-ink">{new Date(coral.timestamp).toLocaleString()}</dd>
                </div>
              )}
              <div className="flex justify-between gap-[12px]">
                <dt className="text-ink-3">ingested</dt>
                <dd className="text-right text-ink">{new Date(coral.created_at).toLocaleString()}</dd>
              </div>
            </dl>

            {coral.participants.length > 0 && (
              <>
                <Eyebrow className="mb-[10px] mt-[20px]">participants</Eyebrow>
                <div className="flex flex-col gap-[8px]">
                  {coral.participants.map((p) => {
                    // Unresolved participants come through as raw Slack ids —
                    // mask them (neutral "?" avatar, no id anywhere).
                    const raw = isRawSlackUserId(p);
                    return (
                      <div key={p} className="flex items-center gap-[8px]">
                        <Avatar name={raw ? "?" : p} size={22} />
                        <span className="truncate font-mono text-[12px] text-ink-2">
                          {raw ? "Unknown participant" : p}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </Panel>
        </aside>
      </div>

      {promoteTarget && (
        <PromoteModal
          target={promoteTarget}
          oceans={graphs}
          currentGraphId={currentGraph?.id}
          token={token}
          onClose={() => setPromoteTarget(null)}
          onPromoted={(oceanNames, summary) =>
            setPromoteMsg(
              `Promoted ${summary.reefs ?? 0} topic(s)${summary.corals ? ` + ${summary.corals} coral(s)` : ""} to ${oceanNames.join(", ")}.`,
            )
          }
        />
      )}
    </>
  );
}

export default function CoralDetailPage() {
  return (
    <Suspense fallback={<Loading what="surfacing this item" />}>
      <CoralDetailInner />
    </Suspense>
  );
}
