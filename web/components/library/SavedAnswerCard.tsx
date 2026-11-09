"use client";

import Link from "next/link";
import { DotMandala, Dot, workspaceHue } from "@/components/ui";
import type { ConversationSummary } from "@/lib/api";

/** One "saved from your chats" card — a pinned conversation (see
 *  ChatHeader's "Save to library", which pins via `api.conversations.
 *  setPinned`; this is the other end of that same real action). */
export function SavedAnswerCard({ conv }: { conv: ConversationSummary }) {
  const ws = conv.oceans?.[0];
  const wsName = ws?.ocean_name;
  const age = new Date(conv.updated_at).toLocaleDateString(undefined, { month: "short", day: "numeric" });
  return (
    <Link
      href={`/chats?id=${encodeURIComponent(conv.id)}`}
      className="flex flex-col gap-[6px] rounded-card border border-line bg-bg p-[12px] transition-colors hover:border-ink-3"
    >
      <div className="flex items-center gap-[6px]">
        <DotMandala size={13} density={0.75} />
        <span className="font-mono text-[9px] uppercase tracking-[0.08em] text-ink-4">saved answer</span>
        <span className="ml-auto font-mono text-[9px] text-ink-4">{age}</span>
      </div>
      <div className="truncate text-[12.5px] leading-[1.45] text-ink">{conv.title || "Untitled chat"}</div>
      {wsName && (
        <div className="flex items-center gap-[6px]">
          <Dot hue={workspaceHue(ws?.ocean_id)} size={5} />
          <span className="font-mono text-[9.5px] text-ink-4">{wsName}</span>
        </div>
      )}
    </Link>
  );
}
