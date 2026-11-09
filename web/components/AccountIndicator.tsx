"use client";

import { Dot, Skeleton, cn, hueTint, hueVar } from "@/components/ui";
import { useInstanceSwitcher } from "./useInstanceSwitcher";
import { tenantIndicator, tenantIndicatorAriaLabel } from "./tenantIndicator";

/* THE SMALL-SCREEN TENANCY INDICATOR.
 *
 * Below `lg` the 224px sidebar is not rendered at all (AppShell wraps it in
 * `hidden lg:flex`) and the bottom tab bar shows only the five places, so the
 * name of the tenant you are inside appeared NOWHERE on a phone. That was
 * tolerable while every organization had its own hostname and the address bar
 * said which one you were in; claim-based tenancy puts every tenant on one
 * apex, so the app has to say it. This is the thing that says it.
 *
 * WHY IN THE 52px HEADER AND NOT THE TAB BAR. The founder chose "keep the
 * sidebar treatment, add a matching indicator for small screens", and the
 * mockup drew that indicator as a top strip. `MobileNav` is a BOTTOM tab bar —
 * 64px + safe-area inset, six equal `flex-1` tabs — with no room for a name
 * that can be twenty characters long without wrecking the tab layout. The
 * header is where the mockup actually put it, so the header is where it goes.
 *
 * `lg:hidden` is applied by the CALLER (AppShell), not baked in here, because
 * the reason it hides at `lg` is that a DIFFERENT component takes over there —
 * that is a fact about the shell's composition, not about this chip.
 *
 * WHY `useInstanceSwitcher` AND NOT A CHEAPER `useOrganization()`. This is a
 * second mount of the same hook the sidebar already runs, and it does not need
 * the membership list. It uses it anyway because the sidebar and this chip must
 * be INCAPABLE of disagreeing: `useOrganization().isLoaded` is a different flag
 * from the one the sidebar branches on, so deriving the two indicators from two
 * hooks would open a window in which the header says "Personal" while the
 * sidebar says "Acme" — the same class of wrong-tenant signal this PR exists to
 * remove, just spread across two components. The cost is near zero: Clerk's
 * pagination hook runs through a shared query client keyed deterministically on
 * its params, with a 60s stale time (`usePagesOrInfinite` in @clerk/shared), so
 * two mounts with identical params share one query rather than issuing two.
 *
 * WHAT IS TESTED AND WHAT IS NOT. Every decision — the label, the loading
 * state, the hue, the accessible name — is in `./tenantIndicator.ts` and has
 * unit tests. What is left in this file is only how those answers are drawn,
 * and `web/` has no component-test harness, so typecheck plus the production
 * build is the whole of its coverage. That split is deliberate: the part that
 * can be wrong in a way that matters is the part that is testable. */

/** Shared geometry, so the three states are the same size and the header does
 *  not reflow when identity resolves. */
const PILL =
  "inline-flex min-w-0 items-center gap-[6px] rounded-pill border py-[3px] pl-[7px] pr-[9px] text-[11.5px]";

export default function AccountIndicator({ className }: { className?: string }) {
  const instance = useInstanceSwitcher();
  const indicator = tenantIndicator(instance);
  const label = tenantIndicatorAriaLabel(indicator);

  if (indicator.state === "unknown") {
    /* A skeleton, not a name and not nothing. Rendering nothing would collapse
       the header and then push the title sideways a moment later; rendering a
       name would be the very lie this component was built to stop. A pulsing
       placeholder of the right size says "not known yet" in the one vocabulary
       that cannot be mistaken for an answer. */
    return (
      <span
        role="status"
        aria-busy="true"
        aria-label={label}
        className={cn(PILL, "border-line", className)}
      >
        <Skeleton w={5} h={5} className="rounded-pill" />
        <Skeleton w={54} h={9} />
      </span>
    );
  }

  if (indicator.state === "personal") {
    /* Quiet on purpose. Personal is the no-organization state: it is where the
       user's own data lives and the state that carries no risk of writing into
       a company's ocean by mistake. It still has to be legible — "Personal" is
       a real, correct answer once loaded — but it does not need to shout. */
    return (
      <span
        aria-label={label}
        className={cn(PILL, "border-line bg-bg font-medium text-ink-2", className)}
      >
        <span aria-hidden className="h-[5px] w-[5px] flex-none rounded-pill bg-ink-3" />
        <span className="truncate">{indicator.label}</span>
      </span>
    );
  }

  /* Organizations are hue-filled and bordered in their own colour. Two things
     are being bought with that: the chip is hard to skim past, and two
     different orgs are distinguishable pre-verbally, before the name is read.
     The TEXT stays `ink` rather than taking the hue — the kit's `Chip` tints
     both, which is fine for a metadata mark but not for a signal whose whole
     job is to be read at a glance on a phone (hue-on-tint is thin contrast,
     `amber` especially). Colour and weight carry the alarm; ink carries the
     word. */
  return (
    <span
      aria-label={label}
      className={cn(PILL, "font-semibold text-ink", className)}
      style={{ background: hueTint(indicator.hue), borderColor: hueVar(indicator.hue) }}
    >
      <Dot hue={indicator.hue} size={6} />
      <span className="truncate">{indicator.label}</span>
    </span>
  );
}
