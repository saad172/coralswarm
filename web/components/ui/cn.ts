/** Join class names, dropping falsy entries.
 *
 * Deliberately NOT tailwind-merge: nothing here needs conflict resolution
 * (later Tailwind classes already win by source order within a rule set, and
 * the kit's components take a `className` that is appended last), and adding a
 * runtime dependency to the shared import path of every screen is not worth it.
 * If a caller ever needs to override a base utility, the component exposes the
 * relevant prop instead. */
// The parameter type is deliberately wide. Callers write `cond && "cls"` where
// `cond` is often a ReactNode prop (`error && "border-bad"`), and a ReactNode
// that is falsy can be `0`, `0n` or `""` — not just `false | null | undefined`.
// Narrowing this would push a cast onto every such call site.
export type ClassValue = string | number | bigint | boolean | null | undefined;

export function cn(...parts: ClassValue[]): string {
  return parts.filter((p): p is string => typeof p === "string" && p.length > 0).join(" ");
}
