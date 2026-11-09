export { AnimatePresence, LazyMotion, MotionConfig, domAnimation, m, useReducedMotion } from "motion/react";

export type CubicBezier = readonly [number, number, number, number];

export const DURATION_MS = {
  fast: 150,
  pop: 180,
  exit: 200,
  enter: 220,
  collapse: 240,
  drawer: 280,
  reveal: 320,
  settle: 380,
  slow: 500,
} as const;

export const EASE = {
  ease: [0.25, 0.1, 0.25, 1],
  out: [0.22, 1, 0.36, 1],
  exit: [0.4, 0, 1, 1],
  pop: [0.34, 1.32, 0.64, 1],
} as const satisfies Record<string, CubicBezier>;

export const STAGGER_MS = {
  block: 40,
  cite: 30,
  card: 50,
  word: 60,
} as const;
