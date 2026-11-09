"use client";
import { useUser } from "@clerk/nextjs";
import { useQuery } from "@tanstack/react-query";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { chatEmptyQueryOptions } from "@/lib/appQueries";
import { buildPrompts, greeting } from "@/lib/chatPrompts";
import { PromptCard } from "./PromptCard";
import { RevealAnswer } from "./RevealAnswer";
export function ChatEmpty({
  onAsk,
  disabled,
  narrowed,
}: {
  onAsk: (text: string) => void;
  disabled?: boolean;
  narrowed?: boolean;
}) {
  const admission = useEvidenceAdmission();
  const { user } = useUser();
  const ctx = { scope: admission.queryScope, getAuthToken: admission.getToken };
  const enabled = admission.ready && !narrowed;
  const query = useQuery({ ...chatEmptyQueryOptions(ctx), enabled });
  const prompts = buildPrompts({
    workspaces: enabled ? query.data?.workspace_activity?.workspaces ?? null : null,
    activity: enabled ? query.data?.activity ?? null : null,
    hour: new Date().getHours(),
  });
  const failed = enabled && query.isError;
  return (
    <section className="flex flex-col gap-7 py-10">
      <RevealAnswer active delay={0} duration={400}><p className="font-mono text-[10.5px] uppercase tracking-widest text-ink-3">
        {greeting(new Date().getHours())}
        {user?.firstName ? `, ${user.firstName}` : ""}
      </p></RevealAnswer>
      <RevealAnswer active duration={400} distance={10} stagger={60} offset={80}>
        <h1 className="flex flex-wrap gap-x-[.24em] font-display text-[44px] leading-[1.08]">
          {["What", "do", "you", "want", "to", "know?"].map((word, i) => (
            <span key={word} className={i === 5 ? "italic text-brand" : undefined}>
              {word}{i < 5 ? " " : ""}
            </span>
          ))}
        </h1>
      </RevealAnswer>
      <RevealAnswer active delay={440}><p className="mt-4 max-w-[520px] text-[14.5px] text-ink-2">
        Every answer cites the Slack thread, meeting or note it came from, and says so when two of
        them disagree.
      </p></RevealAnswer>
      <div>
        <RevealAnswer active delay={500} distance={0}><p className="mb-3 font-mono text-[10px] uppercase tracking-widest text-ink-3">
          from your recent reefs
        </p></RevealAnswer>
        <div className="grid grid-cols-1 gap-2.5 sm:grid-cols-2">
          {prompts.map((prompt, i) => (
            <RevealAnswer key={prompt.text} active delay={520 + i * 50} distance={8}>
              <PromptCard prompt={prompt} onAsk={onAsk} disabled={disabled} />
            </RevealAnswer>
          ))}
        </div>
        <p role="status" className="mt-2 text-xs text-ink-3">
          {enabled && !query.error && query.data === undefined
            ? "Finding recent topics…"
            : failed
              ? "Recent topics could not load. You can still ask a question."
              : ""}
        </p>
        {failed && (
          <button
            type="button"
            onClick={() => {
              void query.refetch();
            }}
            className="text-xs underline"
          >
            Retry recent topics
          </button>
        )}
      </div>
    </section>
  );
}
