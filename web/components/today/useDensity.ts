"use client";

import { useCallback, useEffect, useState } from "react";
import { densityStorageKey, readDensity, writeDensity, type Density } from "@/lib/todayView";

/** The Cards / Compact choice, remembered per user in localStorage. Cards until
 *  the stored value is read (an effect, so the server render and the first
 *  client render agree). */
export function useDensity(userId: string | null | undefined): [Density, (d: Density) => void] {
  const key = densityStorageKey(userId);
  const [density, setDensity] = useState<Density>("cards");
  useEffect(() => {
    setDensity(readDensity(typeof window === "undefined" ? null : window.localStorage, key));
  }, [key]);
  const choose = useCallback(
    (d: Density) => {
      setDensity(d);
      writeDensity(typeof window === "undefined" ? null : window.localStorage, key, d);
    },
    [key],
  );
  return [density, choose];
}
