"use client";
import { useLayoutEffect, useRef, type ReactNode } from "react";
import { animate } from "motion";
import { DURATION_MS, EASE, STAGGER_MS, useReducedMotion } from "@/lib/motion";

export function RevealAnswer({ active, delay, duration = DURATION_MS.reveal, distance = 6, stagger = STAGGER_MS.block, offset = 0, children }: { active: boolean; delay?: number; duration?: number; distance?: number; stagger?: number; offset?: number; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  const reduced = useReducedMotion();
  useLayoutEffect(() => {
    if (!active || reduced !== false || !ref.current) return;
    const root = ref.current;
    const blocks = delay == null ? Array.from(root.firstElementChild?.children ?? []) : [root];
    const animations = blocks.map((element, i) => animate(element, { opacity: [0, 1], y: [distance, 0] }, { duration: duration / 1000, ease: EASE.out, delay: (delay ?? offset + i * stagger) / 1000 }));
    if (delay == null) root.querySelectorAll(".cite").forEach((element, i) => {
      animations.push(animate(element, { opacity: [0, 1], scale: [.4, 1] }, { duration: DURATION_MS.pop / 1000, delay: .46 + i * STAGGER_MS.cite / 1000, ease: EASE.pop }));
    });
    const finish = () => {
      animations.forEach((animation) => animation.complete());
      // Motion completion renders on its next frame. Keyboard focus must reveal
      // the target and every animated ancestor in the same event.
      [...blocks, ...Array.from(root.querySelectorAll(".cite"))].forEach((element) => {
        if (element instanceof HTMLElement) {
          element.style.opacity = "1";
          element.style.transform = "none";
        }
      });
    };
    root.addEventListener("focusin", finish);
    if (root.contains(document.activeElement)) finish();
    return () => { root.removeEventListener("focusin", finish); finish(); };
  }, [active, delay, reduced, duration, distance, stagger, offset]);
  return <div ref={ref}>{children}</div>;
}
