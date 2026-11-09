"use client";

import { useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import { ScopePill, cn } from "@/components/ui";
import { ComposerSettings } from "./ComposerSettings";
import { IconSend } from "@/components/icons";
import {
  type AnswerStyle,
  type EffortLevel,
  type ModelTier,
} from "@/lib/askScope";

export function ChatComposer({
  model,
  setModel,
  effort,
  setEffort,
  style,
  setStyle,
  busy,
  disabled,
  placeholder = "Ask a follow-up…",
  onSend,
  onStop,
  initialText,
  prefill,
  scoped = false,
  only,
}: {
  model: ModelTier;
  setModel: (m: ModelTier) => void;
  effort: EffortLevel;
  setEffort: (e: EffortLevel) => void;
  style: AnswerStyle;
  setStyle: (s: AnswerStyle) => void;
  busy: boolean;
  disabled?: boolean;
  placeholder?: string;
  onSend: (text: string) => void | Promise<boolean>;
  scoped?: boolean;
  onStop: () => void;
  /** Prefills the box (e.g. Today's "Ask about this day" via `/chats?q=…`) —
   *  never auto-sends. Read once, as the initial value: this is an
   *  uncontrolled field the user can still edit before sending. */
  initialText?: string;
  prefill?: { text: string; request: number };
  /** A chat narrowed below the default (a subset of workspaces, one source,
   *  meetings only, …) shows what it is limited to as one "Only: …" chip.
   *  × widens back to every readable workspace. Absent for default chats. */
  only?: { label: string; onClear: () => void; extra?: ReactNode };
}) {
  const [text, setText] = useState(initialText ?? "");
  useEffect(() => { if (prefill) { setText(prefill.text); textareaRef.current?.focus(); } }, [prefill]);
  const submitting = useRef(false);
  const submit = async () => {
    const t = text.trim();
    if (!t || disabled || busy || submitting.current) return;
    submitting.current = true;
    try {
      const accepted = await onSend(t);
      if (accepted !== false) setText((current) => current.trim() === t ? "" : current);
    } finally { submitting.current = false; }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      submit();
    }
  };

  const textareaRef = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto"; // shrink first, or a deletion never registers
    el.style.height = `${Math.min(el.scrollHeight, parseFloat(getComputedStyle(el).maxHeight) || 168)}px`;
  }, [text]);

  return (
    <div className="flex-none px-[24px] pb-[18px] pt-[14px]">
      <div className="mx-auto max-w-[720px] rounded-frame border border-line bg-bg px-[14px] pb-[10px] pt-[13px] shadow-pop focus-within:border-brand">
        {only && (
          <div className="mb-[8px] flex flex-wrap items-center gap-[8px] px-[4px]">
            <ScopePill hue="coral" className="min-w-0 max-w-full"><span className="min-w-0 truncate">Only: {only.label}</span><button type="button" aria-label="Remove scope" className="shrink-0" disabled={busy || disabled} onClick={only.onClear}>×</button></ScopePill>
            {only.extra}
          </div>
        )}
        <textarea
          aria-label="Question"
          ref={textareaRef}
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={onKeyDown}
          disabled={disabled}
          rows={1}
          placeholder={placeholder}
          className="max-h-[var(--composer-max-h)] w-full resize-none overflow-y-auto border-0 bg-transparent px-[4px] pb-[12px] text-[15px] leading-[1.5] text-ink placeholder:text-ink-4 focus:outline-none disabled:cursor-not-allowed"
        />
        <div className="flex flex-wrap items-center gap-[6px]">
          <ComposerSettings {...{ model, setModel, effort, setEffort, style, setStyle, scoped, disabled }} />

          <button
            type="button"
            onClick={busy ? onStop : submit}
            disabled={!busy && (disabled || !text.trim())}
            style={{ width: busy ? 80 : 32, transition: "width var(--dur-pop) var(--ease-out)" }}
            className={cn(
              "ml-auto flex justify-center h-[32px] flex-none place-items-center gap-2 rounded-pill text-white transition-colors",
              "disabled:cursor-not-allowed disabled:opacity-40",
              busy ? "bg-brand-3 hover:bg-brand-3/90" : "bg-ink hover:bg-ink-2",
            )}
            aria-label={busy ? "Stop" : "Send"}
          >
            {busy ? (
              <><span className="block h-[9px] w-[9px] rounded-badge bg-white" /><span>Stop</span></>
            ) : (
              <IconSend width={16} height={16} />
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
