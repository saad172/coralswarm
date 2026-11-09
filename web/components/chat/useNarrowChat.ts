"use client";

import { useEffect, useState } from "react";

/** Matches Tailwind lg: inline context needs room beside the answer column. */
export function useNarrowChat() {
  const [narrow, setNarrow] = useState(false);
  useEffect(() => {
    if (!window.matchMedia) return;
    const media = window.matchMedia("(max-width: 759px)");
    const update = () => setNarrow(media.matches);
    update();
    // Older WebKit (Safari before 14) exposes
    // the original MediaQueryList listener API rather than EventTarget.
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", update);
      return () => media.removeEventListener("change", update);
    }
    media.addListener(update);
    return () => media.removeListener(update);
  }, []);
  return narrow;
}
