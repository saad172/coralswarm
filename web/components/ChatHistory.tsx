"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Eyebrow, activeRow, cn } from "@/components/ui";
import { DeleteChatButton } from "@/components/chat/DeleteChatButton";
import { useConversations, sortConversations } from "./useConversations";
import { useConversationPrefetch, useIdleConversationPrefetch } from "@/lib/useConversationPrefetch";

/* Recent chats in the sidebar — always present.
 *
 * The redesign gives Library and Workspaces a contextual sub-list that appears
 * only on their own route. Chats deliberately does NOT work that way: a
 * conversation is the thing you most often want to return to from somewhere
 * else in the app, so hiding the list unless you're already on /chats is
 * backwards. It takes the sidebar's flexible middle and scrolls on its own,
 * so the utilities and user row stay pinned.
 *
 * No search input here: the sidebar already has one at the top (⌘K, the
 * command palette). A second field competing with it a few rows below would be
 * two ways to do one thing. */

export default function ChatHistory() {
  const params = useSearchParams();
  const activeId = params.get("id");
  const { conversations, loading, error, reload } = useConversations();
  const list = sortConversations(conversations);
  const warm = useConversationPrefetch(activeId);
  useIdleConversationPrefetch(list.map((c) => c.id), activeId);

  return (
    <div className="flex min-h-0 flex-1 flex-col px-[11px] pt-[12px]">
      <div className="mb-[6px] flex items-center gap-[8px] px-[10px]">
        <Eyebrow>recent chats</Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
      </div>

      {/* The bottom edge fades out so a list cut off mid-row reads as "scroll
          for more"; the matching bottom padding lets the last row scroll clear
          of the fade. A count here said nothing the scrollbar didn't. */}
      <div className="-mr-[4px] min-h-0 flex-1 overflow-y-auto pb-[28px] pr-[4px] [mask-image:linear-gradient(to_bottom,black_calc(100%-28px),transparent)]">
        {loading ? (
          <p className="px-[10px] py-[6px] font-mono text-[10px] text-ink-4">loading…</p>
        ) : error && list.length === 0 ? (
          <p role="alert" className="px-[10px] py-[6px] text-[11.5px] text-ink-3">
            Recent chats could not load. <button type="button" className="underline" onClick={() => void reload()}>Retry</button>
          </p>
        ) : list.length === 0 ? (
          <p className="px-[10px] py-[6px] text-[11.5px] leading-[1.5] text-ink-4">
            No chats yet. Ask something to start one.
          </p>
        ) : (
          <div className="flex flex-col gap-px">
            {list.map((c) => (
              /* The delete control is a SIBLING of the row link, not a child:
                 a button inside an anchor is invalid, and nesting it would put
                 a destructive action inside the hit area you use to open the
                 chat. It reveals on hover and on keyboard focus — pointer-only
                 affordances are unreachable by keyboard, and `focus-within`
                 keeps it visible while it is the focused element. */
              <div key={c.id} className="group/row relative flex items-center">
                <Link
                  href={`/chats?id=${encodeURIComponent(c.id)}`}
                  {...warm(c.id)}
                  aria-current={c.id === activeId ? "page" : undefined}
                  title={c.title}
                  className={cn(
                    "flex min-w-0 flex-1 items-center gap-[7px] rounded-row py-[5px] pl-[10px] pr-[28px] text-[12.5px]",
                    activeRow(c.id === activeId),
                  )}
                >
                  <span className="min-w-0 flex-1 truncate">{c.title}</span>
                  {c.pinned && <span className="flex-none text-[9px] text-brand-2">●</span>}
                </Link>
                <DeleteChatButton
                  conv={c}
                  size={20}
                  className="absolute right-[3px] border-transparent bg-transparent opacity-0 transition-opacity hover:text-bad group-hover/row:opacity-100 group-focus-within/row:opacity-100"
                />
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
