"use client";

import { useEffect, useState, type RefObject } from "react";

export function JumpToLatest({ scrollRef }: { scrollRef: RefObject<HTMLDivElement | null> }) {
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    let frame = 0;
    const read = () => { frame = 0; setVisible(el.scrollHeight - el.scrollTop - el.clientHeight > 320); };
    const schedule = () => { if (!frame) frame = requestAnimationFrame(read); };
    el.addEventListener("scroll", schedule, { passive: true });
    const observer = new ResizeObserver(schedule);
    observer.observe(el);
    if (el.firstElementChild) observer.observe(el.firstElementChild);
    read();
    return () => { el.removeEventListener("scroll", schedule); observer.disconnect(); cancelAnimationFrame(frame); };
  }, [scrollRef]);
  if (!visible) return null;
  return <button type="button" className="absolute bottom-[calc(100%+40px)] left-1/2 z-10 -translate-x-1/2 whitespace-nowrap rounded-pill border border-line bg-bg px-4 py-2 text-xs text-ink-2 shadow-pop"
    onClick={() => scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth" })}>
    Jump to latest ↓
  </button>;
}
