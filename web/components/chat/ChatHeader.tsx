"use client";

import { useRef, useState } from "react";
import { MenuItem, Popover } from "@/components/ui";
import { IconDots } from "@/components/icons";
import { useDeleteChat } from "./DeleteChatButton";
import type { ConversationSummary } from "@/lib/api";

/* The chat-specific header row, sitting below AppShell's generic 52px "Chats"
   bar (that bar is shared, off-limits kit territory — see AppShell.tsx — so a
   conversation's own title and actions live in this second row instead of
   replacing it). Title on the left, actions on the right, nothing else: the
   chat's workspaces are shown once, in the composer's scope pill. */
export function ChatHeader({
  title,
  conv,
}: {
  title: string;
  conv: ConversationSummary | null;
  /** Distinct sources behind the latest answer, not its passage count. */
  sourceCount: number;
  onOpenEvidence: () => void;
}) {
  return (
    <div className="flex h-[48px] flex-none items-center gap-[12px] border-b border-line-2 px-[24px]">
      <span className="min-w-0 flex-1 truncate text-[14px] font-medium tracking-[-0.01em] text-ink">
        {title}
      </span>

      <div className="flex flex-none items-center gap-[6px]">
        {conv && <ChatActions conv={conv} />}
      </div>
    </div>
  );
}

/** The "…" overflow. Delete is its only item today; it opens the same
 *  confirmation dialog as the list rows' trash icon. */
function ChatActions({ conv }: { conv: ConversationSummary }) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const del = useDeleteChat(conv);
  if (!del.available) return null;
  return (
    <div className="relative flex-none">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label="Chat actions"
        onClick={() => setOpen((v) => !v)}
        className="grid h-[26px] w-[26px] place-items-center rounded-row text-ink-3 transition-colors hover:bg-bg-hover hover:text-ink focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
      >
        <IconDots width={16} height={16} />
      </button>
      <Popover open={open} onClose={() => setOpen(false)} triggerRef={triggerRef} align="right">
        <MenuItem danger onClick={() => { setOpen(false); del.request(); }}>
          Delete chat
        </MenuItem>
      </Popover>
      {del.dialog}
    </div>
  );
}
