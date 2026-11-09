"use client";

import { useEffect, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

/* A slot in the shell's 52px header for page-specific right-hand content
   (Home's `last 30 days · 30 sep – 29 oct` line). The shell renders the
   target; a page renders <HeaderMeta> and its children appear there. */

export const HEADER_META_ID = "app-header-meta";

export function HeaderMeta({ children }: { children: ReactNode }) {
  const [target, setTarget] = useState<HTMLElement | null>(null);
  useEffect(() => {
    setTarget(document.getElementById(HEADER_META_ID));
  }, []);
  return target ? createPortal(children, target) : null;
}
