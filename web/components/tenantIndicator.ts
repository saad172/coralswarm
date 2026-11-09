import type { Instance } from "./useInstanceSwitcher";
import { hueForName, type Hue } from "./ui/hue";

/* WHICH TENANT AM I IN — as a pure function of Clerk's identity state.
 *
 * WHY THIS EXISTS AT ALL. The claim-based-tenancy stack deleted a check. Every
 * organization used to live on its own hostname, so the address bar was itself
 * a tenancy indicator: you could read which company's data you were looking at
 * without the app telling you, and the server could refuse a token minted for
 * somebody else's host. One apex now serves every tenant and the tenant is
 * named only by the JWT's `org_id` claim, which is invisible. PR #796's access
 * log makes a wrong-tenant request discoverable AFTERWARDS; the indicator this
 * module drives is the other half — it makes the current tenant visible AT THE
 * TIME. Treat it as a security surface, not decoration.
 *
 * WHY A MODULE AND NOT JUST JSX. `web/` has no component-test harness: vitest
 * runs in `node` and its glob is `.test.ts`, so nothing renders React. An
 * answer that only exists inside a component's render body is an answer with
 * no test. Same shape, and the same reason, as `components/instanceSwitch.ts`
 * (PR 2.2), `lib/activeOcean.ts` (2.3) and `lib/engine/vaultNamespace.ts`
 * (2.4).
 *
 * THE BUG THIS FIXES. `useInstanceSwitcher` returns `active: null` while Clerk
 * is still loading, and both of AppSidebar's render branches coalesced that to
 * `instance.active?.name ?? "Personal"`. So a member of three organizations was
 * told, in confident 13px semibold, that they were in their PERSONAL account —
 * during exactly the window in which the app was about to finish loading an
 * org. A wrong tenancy signal is worse than no tenancy signal, because a user
 * who sees nothing goes and looks, and a user who sees "Personal" does not.
 * Hence the `unknown` state below: it is a first-class answer, not an absence,
 * and the union shape means a caller cannot render this thing without deciding
 * what to do about it.
 *
 * NOTE WHAT IS *NOT* MODELLED HERE: nothing about `soloInstance`, popovers or
 * breakpoints. This module answers "who am I looking at", once, for every
 * surface that shows it; how each surface draws that is the component's job.
 */

/** The subset of `InstanceSwitcher` this needs. Taking the two fields rather
 *  than the whole hook result keeps the module callable from a test with a
 *  literal, and keeps `switchTo`/`instances` out of a question that is purely
 *  about identity. `useInstanceSwitcher()`'s return value satisfies it
 *  structurally, so callers pass it straight through. */
export interface TenantIdentity {
  /** Clerk's `isLoaded`, as re-exported by the hook. False means NOTHING below
   *  is known yet — including, emphatically, whether there is an active org. */
  loaded: boolean;
  /** The active instance once loaded. `null` while loading; `null` post-load
   *  would mean the hook contradicted itself, and is treated as unknown rather
   *  than guessed at. */
  active: Instance | null;
}

/**
 * What a tenancy indicator must show.
 *
 * A DISCRIMINATED UNION, DELIBERATELY, rather than a `string | null` label.
 * The failure mode being designed out is a caller writing `label ?? "Personal"`
 * — which is precisely the line this PR deletes from AppSidebar, twice. There
 * is no label to reach for on the `unknown` variant, so that expression does
 * not typecheck, and every render site is forced to say out loud what it draws
 * while identity is still loading.
 *
 * `personal` carries no hue and `org` does. That is also on purpose: the hue is
 * how two organizations are told apart at a glance, and there is only ever one
 * personal instance to tell apart from anything.
 */
export type TenantIndicator =
  | { state: "unknown" }
  | { state: "personal"; label: string }
  | { state: "org"; label: string; hue: Hue };

/** The name the hook gives the personal instance. Repeated here as a fallback
 *  only — the hook is the source of truth for the live value. */
const PERSONAL_LABEL = "Personal";

/** Last resort when an organization carries no usable identifier at all. Not
 *  reachable through Clerk today (an org always has an id); it exists so this
 *  function's return type can promise a non-empty label, because an EMPTY
 *  label renders as an empty chip, which is the same as having no indicator —
 *  the exact thing this module exists to prevent. */
const UNNAMED_ORG_LABEL = "Organization";

/** First candidate that is more than whitespace, or `null`. */
function firstNonBlank(...candidates: Array<string | null | undefined>): string | null {
  for (const candidate of candidates) {
    const trimmed = candidate?.trim();
    if (trimmed) return trimmed;
  }
  return null;
}

/**
 * The indicator for an identity state.
 *
 * `loaded === false` produces `unknown` REGARDLESS of `active`. That ordering
 * is the load-bearing line of this module: it is not merely a convenience for
 * the current hook, which happens to null `active` while loading, but a
 * statement that an unloaded Clerk's opinion about the active org is not
 * evidence. If a future hook version were to retain the previous session's
 * instance across a reload while `isLoaded` is false, reading it would show
 * the OLD tenant's name over the NEW tenant's data.
 *
 * ORG LABEL FALLBACK: name → slug → id. Not name → "Organization": a generic
 * word is identical for every org, so it would satisfy "the chip is non-empty"
 * while destroying the only property that matters here, which is that two
 * different tenants look different. A whitespace-only org name is rare and a
 * blank tenancy indicator is not a state worth having.
 */
export function tenantIndicator(identity: TenantIdentity): TenantIndicator {
  // Before `active` is even consulted — see the doc comment.
  if (!identity.loaded) return { state: "unknown" };

  const active = identity.active;
  if (!active) return { state: "unknown" };

  if (active.kind === "personal") {
    return { state: "personal", label: firstNonBlank(active.name) ?? PERSONAL_LABEL };
  }

  const label = firstNonBlank(active.name, active.slug, active.id) ?? UNNAMED_ORG_LABEL;
  // Keyed on the LABEL, not the id: `hueForName` is deterministic on the first
  // code point only, and every Clerk org id begins "org_", so keying on the id
  // would paint every organization the same colour and the hue would carry no
  // information at all.
  return { state: "org", label, hue: hueForName(label) };
}

/**
 * Accessible name for a read-only indicator.
 *
 * The organization case says the word "organization" out loud. Sighted users
 * get that from the tinted, hue-coded fill; a screen-reader user hearing only
 * "Acme" has no way to know whether that is a company or a nickname on a
 * personal account, which is the same missing signal in a different medium.
 */
export function tenantIndicatorAriaLabel(indicator: TenantIndicator): string {
  switch (indicator.state) {
    case "unknown":
      return "Loading which account you are in";
    case "personal":
      return `Current account: ${indicator.label}, your personal account`;
    case "org":
      return `Current account: ${indicator.label}, an organization`;
  }
}

/**
 * Accessible name for the sidebar's switcher button — the same statement plus
 * what the control does.
 *
 * Composed from [`tenantIndicatorAriaLabel`] so the read-only indicator and the
 * switcher can never come to describe the same identity differently.
 */
export function tenantSwitcherAriaLabel(indicator: TenantIndicator): string {
  return `${tenantIndicatorAriaLabel(indicator)}. Switch account`;
}
