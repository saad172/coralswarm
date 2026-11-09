// The clicks Home's setup checklist remembers (State B, lib/homeLaunchpad.ts
// `setupChecklist`): the MCP URL was copied, a first question was asked.
//
// Kept in localStorage per user. A new browser starts these over, which is
// harmless: step 1 (a source is connected) and step 2's server half (an MCP
// client is connected) come from `/v1/home/attention`, and the rest are one
// click each.

export interface SetupState {
  /** The MCP URL was copied from the checklist. */
  mcp: boolean;
  /** A question was asked from Home. */
  ask: boolean;
}

export const EMPTY_SETUP: SetupState = { mcp: false, ask: false };

const setupKey = (userId: string) => `cs.home.setup.${userId}`;

/** The slice of `Storage` Home's browser memory uses; tests pass a Map. */
export type StorageLike = Pick<Storage, "getItem" | "setItem">;

/** `localStorage`, or null during SSR or when the browser blocks it. Shared
 *  with lib/homeLaunchpad.ts (last visit, onboarded). */
export function browserStorage(): StorageLike | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

/** Unknown fields (the previous Home also stored `connect` and `capture`)
 *  are ignored. */
export function readSetup(userId: string, storage: StorageLike | null = browserStorage()): SetupState {
  try {
    const raw = storage?.getItem(setupKey(userId));
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    if (!parsed || typeof parsed !== "object") return { ...EMPTY_SETUP };
    const o = parsed as Record<string, unknown>;
    return { mcp: o.mcp === true, ask: o.ask === true };
  } catch {
    return { ...EMPTY_SETUP };
  }
}

export function writeSetup(userId: string, state: SetupState, storage: StorageLike | null = browserStorage()): void {
  try {
    storage?.setItem(setupKey(userId), JSON.stringify(state));
  } catch {
    /* storage full or blocked: the checklist just does not persist */
  }
}
