import type { Instance } from "./useInstanceSwitcher";

/* The switch itself, as a pure function of its dependencies.
 *
 * Switching account instance has to do TWO things in a fixed order: hand the
 * new tenant to Clerk, then reload the document. The order is the whole point
 * and it is not decorative —
 *
 *   `void setActive(...); reload();`
 *
 * fires the reload before Clerk has persisted the new active organization, so
 * the tab comes back on the PREVIOUS tenant. That is the exact bug the reload
 * exists to prevent, and it is invisible in any test that only asks "was
 * reload called?" — it was, just too early. Hence this module: the hook has no
 * test harness (vitest here runs in `node` and its glob is `.test.ts`, so
 * nothing renders React), and an ordering guarantee that cannot be tested is
 * an ordering guarantee that will regress.
 *
 * Precedent for a pure model module living beside its component:
 * components/connectors/slackOnboardingModel.ts.
 *
 * WHY A RELOAD AT ALL (claim-based tenancy decision 7). A switch mints a
 * token for the new tenant, but component-local `useState` all over the tree
 * still holds the previous tenant's rows. `QueryCacheIdentityBoundary`
 * (lib/providers.tsx) clears the query cache on an identity change and cannot
 * reach that local state. Before PR 2.1 every org lived on its own hostname,
 * so a switch was a real cross-origin document navigation and the reload came
 * for free as a side effect of unloading the tab. One apex now serves every
 * tenant, nothing navigates, and the reload has to be explicit. */

export interface InstanceSwitchDeps {
  /** Clerk's `setActive` from `useOrganizationList()`. */
  setActive: (params: { organization: string | null }) => Promise<unknown>;
  /** `() => window.location.reload()` in the browser. */
  reload: () => void;
}

/** Clerk's active-organization value for a target instance: `null` is how
 *  Clerk spells "no org", i.e. the user's personal instance. */
export function activeOrganizationFor(target: Instance): string | null {
  return target.kind === "personal" ? null : target.id;
}

/** Switch the active account instance, then reload.
 *
 *  Rejects — WITHOUT reloading — if `setActive` rejects. A failed switch
 *  leaves the session on the tenant it was already on, so reloading would
 *  cost the user their page state to show them the same tenant back, and
 *  worse, would look exactly like a successful switch that silently landed on
 *  the wrong instance. Staying put is the honest outcome; the rejection is
 *  propagated rather than swallowed so a caller can surface it. */
export async function performInstanceSwitch(
  target: Instance,
  deps: InstanceSwitchDeps,
): Promise<void> {
  // `await`, not `void`. See the module comment — this line is the PR.
  await deps.setActive({ organization: activeOrganizationFor(target) });
  deps.reload();
}
