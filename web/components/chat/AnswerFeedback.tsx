"use client";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useQueryClient } from "@tanstack/react-query";
import { IconThumbUp, IconThumbDown } from "@/components/icons";
import {
  clearMessageFeedback,
  putMessageFeedback,
  MessageFeedbackDisabled,
  FEEDBACK_REASONS,
  type MessageFeedback,
  type FeedbackReason,
} from "@/lib/messageFeedbackApi";
import { conversationKey } from "@/lib/appQueries";
export function FeedbackAvailability({ children }: { children: ReactNode }) {
  return <>{children}</>;
}
const labels = {
  wrong: "Wrong",
  missing_source: "Missing source",
  outdated: "Outdated",
  other: "Other",
};
export function AnswerFeedback({
  conversationId,
  messageId,
  initial,
  readOnly,
}: {
  conversationId: string;
  messageId: string;
  initial?: MessageFeedback | null;
  readOnly?: boolean;
}) {
  const admission = useEvidenceAdmission();
  return (
    <Feedback
      key={`${admission.key}:${conversationId}:${messageId}`}
      {...{ conversationId, messageId, initial, readOnly }}
      getToken={admission.getToken}
      ready={admission.ready}
      scope={admission.queryScope}
    />
  );
}
function Feedback({
  conversationId,
  messageId,
  initial,
  readOnly,
  getToken,
  ready,
  scope,
}: {
  conversationId: string;
  messageId: string;
  initial?: MessageFeedback | null;
  readOnly?: boolean;
  getToken: () => string;
  ready: boolean;
  scope: string;
}) {
  const qc = useQueryClient();
  const [value, setValue] = useState(initial ?? null);
  const [disabled, setDisabled] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const failed = useRef<MessageFeedback | null>(null);
  const [open, setOpen] = useState(false);
  const [reasons, setReasons] = useState<FeedbackReason[]>(initial?.reasons ?? []);
  const [note, setNote] = useState(initial?.note ?? "");
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const active = useRef(true);
  const lock = useRef(false);
  useEffect(() => { if (!lock.current) setValue(initial ?? null); }, [initial]);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  const close = () => {
    setOpen(false);
    trigger.current?.focus();
  };
  useEffect(() => {
    if (!open) return;
    root.current?.querySelector<HTMLButtonElement>('[role="dialog"] button')?.focus();
    const key = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        close();
      }
    };
    const outside = (e: PointerEvent) => {
      if (!root.current?.contains(e.target as Node)) close();
    };
    document.addEventListener("keydown", key);
    document.addEventListener("pointerdown", outside);
    return () => {
      document.removeEventListener("keydown", key);
      document.removeEventListener("pointerdown", outside);
    };
  }, [open]);
  async function save(next: MessageFeedback | null) {
    if (lock.current || readOnly || !ready) return;
    lock.current = true;
    setBusy(true);
    setError(false);
    const previous = value;
    failed.current = next;
    setValue(next);
    try {
      const token = getToken();
      await qc.cancelQueries({ queryKey: conversationKey(scope, conversationId), exact: true });
      if (!active.current) return;
      const stored = next
        ? await putMessageFeedback(token, conversationId, messageId, next)
        : await clearMessageFeedback(token, conversationId, messageId);
      await qc.cancelQueries({ queryKey: conversationKey(scope, conversationId), exact: true });
      if (active.current) {
        setValue(stored ?? null);
        void qc.invalidateQueries({ queryKey: conversationKey(scope, conversationId), exact: true });
      }
    } catch (err) {
      if (active.current) {
        setValue(previous);
        if (err instanceof MessageFeedbackDisabled) { setDisabled(true); }
        else setError(true);
      }
    } finally {
      lock.current = false;
      if (active.current) setBusy(false);
    }
  }
  if (initial === undefined || disabled || readOnly || !ready) return null;
  const button =
    "inline-flex h-[30px] w-[30px] items-center justify-center rounded-field hover:bg-bg-hover aria-pressed:bg-brand-soft aria-pressed:text-brand disabled:opacity-50";
  return (
    <div ref={root} className="relative flex items-center gap-0.5">
      <button
        type="button"
        className={button}
        aria-label="Good answer"
        aria-pressed={value?.vote === "up"}
        disabled={busy}
        onClick={() => {
          setOpen(false);
          void save(value?.vote === "up" ? null : { vote: "up", reasons: [], note: null });
        }}
      >
        <IconThumbUp width={15} height={15} />
      </button>
      <button
        ref={trigger}
        type="button"
        className={button}
        aria-label="Something's off"
        aria-pressed={value?.vote === "down"}
        aria-expanded={open}
        aria-haspopup="dialog"
        aria-disabled={busy}
        onClick={() => {
          if (lock.current) return;
          setReasons(value?.vote === "down" ? value.reasons : []);
          setNote(value?.vote === "down" ? (value.note ?? "") : "");
          setOpen(!open);
          if (value?.vote !== "down") void save({ vote: "down", reasons: [], note: null });
        }}
      >
        <IconThumbDown width={15} height={15} />
      </button>
      <span role="status" className="ml-2 text-xs">
        {error
          ? "Your feedback was not saved. Try again."
          : busy
            ? "Saving…"
            : value
              ? "thanks, noted"
              : ""}
      </span>
      {error && (
        <button
          type="button"
          disabled={busy}
          onClick={() => void save(failed.current)}
          className="text-xs underline"
        >
          Retry feedback
        </button>
      )}
      {open && (
        <div
          role="dialog"
          aria-label="Answer feedback"
          className="absolute left-[-64px] top-full z-20 mt-1.5 flex w-[300px] max-w-[calc(100vw-48px)] flex-col gap-2.5 rounded-card border border-line bg-bg p-3 shadow-pop"
        >
          <p className="font-mono text-[10px] uppercase">what was off? · optional</p>
          <div className="flex flex-wrap gap-2">
            {FEEDBACK_REASONS.map((reason) => (
              <button
                type="button"
                key={reason}
                aria-pressed={reasons.includes(reason)}
                className="rounded-full border border-line px-3 py-1 text-xs aria-pressed:bg-ink aria-pressed:text-bg"
                onClick={() =>
                  setReasons(
                    reasons.includes(reason)
                      ? reasons.filter((r) => r !== reason)
                      : [...reasons, reason],
                  )
                }
              >
                {labels[reason]}
              </button>
            ))}
          </div>
          <textarea
            aria-label="What should it have said?"
            placeholder="What should it have said?"
            rows={2}
            maxLength={2000}
            value={note}
            onChange={(e) => setNote(e.target.value)}
            className="rounded-field border border-line bg-bg p-2 text-sm"
          />
          <div className="flex justify-end gap-3">
            <button
              type="button"
              disabled={busy}
              onClick={() => {
                close();
                if (!reasons.length && !note.trim()) void save(null);
              }}
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => {
                close();
                void save({ vote: "down", reasons, note: note.trim() || null });
              }}
            >
              Send feedback
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
