import type { Hue } from "@/components/ui";

/* Ocean colours on Home (dots, the ingested-per-day stack, the volume bars).

   The backend stores no hue for an ocean (`HomeOcean.hue` is absent today), so
   the client assigns one, deterministically per graph_id:

     - the viewer's private ocean (kind `member`, or `personal` on a personal
       account) is always pink;
     - every other ocean takes teal / amber / plum / coral by a hash of its id.

   Hashing the id (rather than the list position) keeps an ocean's colour
   stable when the list reorders or another ocean appears. Two oceans can
   share a hue; with four hues that is expected, and the legend carries names.
   If a stored hue ever ships (`HomeOcean.hue`), a valid one wins. */

export const PRIVATE_OCEAN_HUE: Hue = "pink";
export const SHARED_OCEAN_HUES: readonly Hue[] = ["teal", "amber", "plum", "coral"];

const ALL: readonly string[] = ["coral", "pink", "amber", "teal", "plum"];

/** Whether an ocean kind is the viewer's own private ocean. */
export function isPrivateOceanKind(kind: string | null | undefined): boolean {
  return kind === "member" || kind === "personal";
}

export function oceanHue(ocean: { graph_id: string; kind?: string | null; hue?: string | null }): Hue {
  if (ocean.hue && ALL.includes(ocean.hue)) return ocean.hue as Hue;
  if (isPrivateOceanKind(ocean.kind)) return PRIVATE_OCEAN_HUE;
  let h = 0;
  for (let i = 0; i < ocean.graph_id.length; i++) h = (h * 31 + ocean.graph_id.charCodeAt(i)) >>> 0;
  return SHARED_OCEAN_HUES[h % SHARED_OCEAN_HUES.length];
}
