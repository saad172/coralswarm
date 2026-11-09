"use client";

import { DURATION_MS, EASE } from "@/lib/motion";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { useNarrowChat } from "../chat/useNarrowChat";
import { verdictCopy } from "../chat/answer/labels";
import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import {
  Avatar,
  Button,
  Chip,
  Dot,
  IconButton,
  SourceCard,
  hueForName,
  hueTint,
  hueVar,
} from "@/components/ui";
import { IconChevron, IconClose } from "@/components/icons";
import { KIND_META, askAboutItemHref, reefHref, type DetailItem } from "./detailModel";

export interface DetailPanelProps {
  item: DetailItem;
  onClose: () => void;
  /** `{ index: 1, total: 6 }` reads `2 of 6 · ↑↓` (index is 0-based). */
  position?: { index: number; total: number };
  onPrev?: () => void;
  onNext?: () => void;
  /** DECIDED and CAME FROM are still loading. */
  loading?: boolean;
  onAsk?: (item: DetailItem) => void;
  className?: string;
  sourceHref?: string;
}

function isTypingTarget(t: EventTarget | null): boolean {
  const el = t as HTMLElement | null;
  if (!el || !el.tagName) return false;
  return (
    el.tagName === "INPUT" ||
    el.tagName === "TEXTAREA" ||
    el.tagName === "SELECT" ||
    el.isContentEditable
  );
}

/** The keyboard contract, exported for tests and for pages that need to bind
 *  the same keys without mounting the panel. Returns true when handled. */
export function handlePanelKey(
  e: Pick<KeyboardEvent, "key" | "target" | "defaultPrevented" | "metaKey" | "ctrlKey" | "altKey">,
  h: { onClose: () => void; onPrev?: () => void; onNext?: () => void },
): boolean {
  if (e.defaultPrevented || e.metaKey || e.ctrlKey || e.altKey) return false;
  if (e.key === "Escape") {
    h.onClose();
    return true;
  }
  if (isTypingTarget(e.target)) return false;
  if ((e.key === "ArrowUp" || e.key === "ArrowLeft") && h.onPrev) {
    h.onPrev();
    return true;
  }
  if ((e.key === "ArrowDown" || e.key === "ArrowRight") && h.onNext) {
    h.onNext();
    return true;
  }
  return false;
}

const Eyebrow = ({ children }: { children: string }) => (
  <div className="pb-[8px] font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">
    {children}
  </div>
);

export function DetailPanel(props: DetailPanelProps) {
  const [open, setOpen] = useState(true);
  const reduced = useReducedMotion();
  const close = useRef(props.onClose);
  close.current = props.onClose;
  useEffect(() => {
    if (open) return;
    const timer = setTimeout(() => close.current(), reduced ? 0 : DURATION_MS.exit);
    return () => clearTimeout(timer);
  }, [open, reduced, props.item.id]);
  useEffect(() => setOpen(true), [props.item.id]);
  return (
    <AnimatePresence>
      {open && <PanelContent {...props} onClose={() => setOpen(false)} />}
    </AnimatePresence>
  );
}

function PanelContent({
  item,
  onClose,
  position,
  onPrev,
  onNext,
  loading,
  onAsk,
  className,
  sourceHref,
}: DetailPanelProps) {
  const router = useRouter();
  const narrow = useNarrowChat();
  const reduced = useReducedMotion();
  const ref = useRef<HTMLElement>(null);
  const returnFocus = useRef(true);
  const handlers = useRef({ onClose, onPrev, onNext });
  handlers.current = { onClose, onPrev, onNext };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (handlePanelKey(e, handlers.current)) e.preventDefault();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    const opener = document.activeElement as HTMLElement | null;
    ref.current?.querySelector<HTMLElement>("h2")?.focus({ preventScroll: true });
    const outside = (e: MouseEvent) => {
      if (
        e.target instanceof Element &&
        !ref.current?.contains(e.target) &&
        !e.target.closest("[data-src], .cite")
      )
        handlers.current.onClose();
    };
    document.addEventListener("mousedown", outside);
    return () => {
      document.removeEventListener("mousedown", outside);
      if (returnFocus.current && opener?.isConnected) opener.focus({ preventScroll: true });
    };
  }, []);
  useEffect(() => {
    if (!narrow) return;
    const old = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const trap = (e: KeyboardEvent) => {
      if (e.key !== "Tab") return;
      const els = ref.current?.querySelectorAll<HTMLElement>(
        'button:not(:disabled), a[href], input, textarea, [tabindex="0"]',
      );
      if (!els?.length) {
        e.preventDefault();
        return;
      }
      const first = els[0],
        last = els[els.length - 1];
      if (
        e.shiftKey &&
        (document.activeElement === first || document.activeElement?.tagName === "H2")
      ) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", trap);
    return () => {
      document.body.style.overflow = old;
      document.removeEventListener("keydown", trap);
    };
  }, [narrow]);

  const meta = KIND_META[item.kind];
  const KindIcon = meta.Icon;
  const stepping = !!(onPrev || onNext);
  const moreCount = item.peopleCount - item.people.length;

  return (
    <>
      {narrow && <div className="fixed inset-0 z-40 bg-ink/30" aria-hidden="true" />}
      <motion.aside
        initial={reduced ? false : narrow ? { y: "100%" } : { x: "100%" }}
        animate={{ x: 0, y: 0 }}
        exit={
          reduced
            ? { opacity: 0 }
            : narrow
              ? { y: "100%", transition: { duration: DURATION_MS.exit / 1000, ease: EASE.exit } }
              : { x: "100%", transition: { duration: DURATION_MS.exit / 1000, ease: EASE.exit } }
        }
        transition={{ duration: reduced ? 0 : DURATION_MS.drawer / 1000, ease: EASE.out }}
        aria-modal={narrow || undefined}
        ref={ref}
        tabIndex={-1}
        role="dialog"
        aria-label={`${item.kindLabel}: ${item.title}`}
        className={
          (narrow
            ? "fixed inset-x-0 bottom-0 top-[18%] z-50 rounded-t-frame "
            : "fixed inset-y-0 right-0 z-40 w-panel max-w-full ") +
          "flex flex-col border-l border-line bg-bg shadow-frame focus:outline-none " +
          (className ?? "")
        }
      >
        <div className="flex h-[52px] flex-none items-center gap-[8px] border-b border-line-2 pl-[20px] pr-[14px]">
          <span
            className="grid h-[22px] w-[22px] place-items-center rounded-badge"
            style={{ background: hueTint(meta.hue), color: hueVar(meta.hue) }}
          >
            <KindIcon width={13} height={13} />
          </span>
          <span className="font-mono text-[10px] uppercase tracking-[0.1em] text-ink-3">
            {item.kindLabel}
          </span>
          <span className="flex-1" />
          {position && (
            <span className="font-mono text-[10.5px] text-ink-3">
              {position.index + 1} of {position.total}
              {stepping ? " · ↑↓" : ""}
            </span>
          )}
          {onPrev && (
            <IconButton label="Previous" size={26} onClick={onPrev}>
              <IconChevron width={13} height={13} className="rotate-180" />
            </IconButton>
          )}
          {onNext && (
            <IconButton label="Next" size={26} onClick={onNext}>
              <IconChevron width={13} height={13} />
            </IconButton>
          )}
          <IconButton label="Close" size={26} onClick={onClose}>
            <IconClose width={13} height={13} />
          </IconButton>
        </div>

        <div className="flex min-h-0 flex-1 flex-col gap-[20px] overflow-y-auto p-[20px]">
          <div>
            <div className="mb-[8px] flex items-center gap-[6px] font-mono text-[10.5px] text-ink-3">
              {item.meta && (
                <span>
                  {item.meta} {"·"}
                </span>
              )}
              <Dot hue={item.oceanHue} size={5} />
              <span>{item.oceanName}</span>
            </div>
            <h2
              tabIndex={-1}
              className="mb-[8px] font-display text-[26px] font-normal leading-[1.2] text-ink"
            >
              {item.title}
            </h2>
            {item.outcome && (
              <p className="text-[14.5px] leading-[1.55] text-ink-2">{item.outcome}</p>
            )}
          </div>

          {item.verdict && (
            <p title={verdictCopy(item.verdict)?.description}>
              <span>{verdictCopy(item.verdict)?.label}</span>
              <span>{verdictCopy(item.verdict)?.description}</span>
            </p>
          )}
          {!!item.usedFor?.length && (
            <section aria-label="Used in this answer for">
              <Eyebrow>used in this answer for</Eyebrow>
              {item.usedFor.map((text, i) => (
                <p key={i} className="mb-2 text-sm text-ink-2">
                  {text}
                </p>
              ))}
            </section>
          )}
          {item.decided.length > 0 && (
            <section aria-label="Decided">
              <Eyebrow>decided</Eyebrow>
              <ul>
                {item.decided.map((d, i) => (
                  <li
                    key={i}
                    className="flex items-start gap-[9px] border-t border-line-3 py-[8px] text-[13.5px] leading-[1.45] text-ink first:border-t-0"
                  >
                    <span className="mt-[7px] h-[6px] w-[6px] flex-none rounded-pill bg-brand" />
                    {d}
                  </li>
                ))}
              </ul>
            </section>
          )}

          {item.people.length > 0 && (
            <section aria-label="People">
              <Eyebrow>people</Eyebrow>
              <ul className="flex flex-col gap-[8px]">
                {item.people.map((p) => (
                  <li key={p} className="flex items-center gap-[8px] text-[13px] text-ink">
                    <Avatar name={p} size={20} hue={hueForName(p)} />
                    {p}
                  </li>
                ))}
                {moreCount > 0 && (
                  <li className="font-mono text-[10.5px] text-ink-3">+{moreCount} more</li>
                )}
              </ul>
            </section>
          )}

          {item.sources.length > 0 ? (
            <section aria-label="Came from">
              <Eyebrow>came from</Eyebrow>
              <div className="flex flex-col gap-[8px]">
                {item.sources.map((s, i) => (
                  <SourceCard
                    key={i}
                    n={i + 1}
                    title={s.quote}
                    meta={s.meta}
                    hue={KIND_META[s.kind].hue}
                  />
                ))}
              </div>
            </section>
          ) : (
            loading && (
              <div
                role="status"
                className="font-mono text-[10.5px] uppercase tracking-[0.13em] text-ink-3"
              >
                loading sources{"…"}
              </div>
            )
          )}

          {item.reefs.length > 0 && (
            <section aria-label="Reefs touched">
              <Eyebrow>reefs touched</Eyebrow>
              <div className="flex flex-wrap gap-[6px]">
                {item.reefs.map((r) => (
                  <Link key={r.nodeId} href={reefHref(r, item.graphId)} className="rounded-pill">
                    <Chip>{r.title}</Chip>
                  </Link>
                ))}
              </div>
            </section>
          )}
        </div>

        <div className="flex-none border-t border-line-2 px-[20px] py-[12px]">
          {sourceHref && (
            <Link href={sourceHref} className="mr-4 text-sm text-brand">
              View captured source
            </Link>
          )}
          <Button
            variant="primary"
            onClick={() => {
              returnFocus.current = false;
              if (onAsk) onAsk(item);
              else router.push(askAboutItemHref(item));
            }}
          >
            Ask about this
          </Button>
        </div>
      </motion.aside>
    </>
  );
}
