/* Office.js deliberately nulls `window.history.pushState` / `replaceState` in some hosts (Outlook desktop,
 * notably on Mac). The Next.js App Router binds and calls both during hydration and on every render, so with
 * them gone the pane dies with "Application error: a client-side exception has occurred".
 *
 * In Outlook desktop the host injects Office.js into the page itself, so it can run BEFORE any script of ours:
 * "capture before office.js, restore after" is not enough on its own. This guard therefore
 *   1. restores a missing function from History.prototype (Office nulls the instance property; the prototype
 *      keeps the original), falling back to a pristine one borrowed from a throwaway same-origin iframe;
 *   2. then turns both into accessors that ignore any later assignment of a non-function, so a lazy
 *      `history.pushState = null` inside Office.initialize / onReady (or the office.js we load ourselves) is a no-op.
 * It must be idempotent and entirely self-contained: `HISTORY_GUARD_SCRIPT` serializes it with toString()
 * and inlines it into the document head, ahead of every Next.js chunk. */
export function guardHistory(win: Window): void {
  const names = ["pushState", "replaceState"] as const;
  try {
    const h = win.history as unknown as Record<string, unknown>;
    const proto = (win as unknown as { History?: { prototype: Record<string, unknown> } }).History;
    const pristine = function (name: string): unknown {
      if (proto && typeof proto.prototype[name] === "function") return proto.prototype[name];
      try {
        const doc = win.document;
        const frame = doc.createElement("iframe");
        frame.style.display = "none";
        (doc.head || doc.documentElement).appendChild(frame);
        const fn = (frame.contentWindow as unknown as { History?: { prototype: Record<string, unknown> } } | null)?.History?.prototype[name];
        if (frame.parentNode) frame.parentNode.removeChild(frame);
        return fn;
      } catch (e) { return undefined; }
    };
    for (let i = 0; i < names.length; i++) {
      (function (name: string) {
        try {
          let current: unknown = h[name];
          if (typeof current !== "function") current = pristine(name);
          if (typeof current !== "function") return;
          Object.defineProperty(h, name, {
            configurable: true,
            enumerable: true,
            get: function () { return current; },
            set: function (value: unknown) { if (typeof value === "function") current = value; },
          });
        } catch (e) { /* frozen history object: nothing more we can do */ }
      })(names[i]);
    }
  } catch (e) { /* never let the guard itself break the page */ }
}

/** Re-run after Office has initialised (idempotent); a no-op where the guard is already in place. */
export function restoreHistory(win: Window = window): void { guardHistory(win); }

/** Inline script for the document head. Runs only on Office-hosted routes. */
export const HISTORY_GUARD_SCRIPT =
  `(function(){const p=location.pathname;if(p.indexOf("/outlook")!==0&&p.indexOf("/addin")!==0)return;(${guardHistory.toString()})(window)})()`;
