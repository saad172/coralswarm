import { Dot } from "@/components/ui";
import type { Prompt } from "@/lib/chatPrompts";
export function PromptCard({
  prompt,
  onAsk,
  disabled,
}: {
  prompt: Prompt;
  onAsk: (text: string) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={() => onAsk(prompt.text)}
      className="w-full rounded-card border border-line bg-bg px-[14px] py-[13px] text-left shadow-card transition-colors hover:border-ink-3 hover:bg-bg-hover disabled:opacity-50"
    >
      <span className="mb-2 flex items-center gap-2 font-mono text-[10px] uppercase tracking-widest text-ink-3">
        <Dot hue={prompt.hue} size={5} />
        {prompt.kind} · {prompt.label}
      </span>
      <span className="text-sm font-medium text-ink">{prompt.text}</span>
    </button>
  );
}
