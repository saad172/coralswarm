// Which ocean the app opens into, and whether to tell the server about it.
//
// Pure decision logic for `lib/providers.tsx`'s `bootstrap()` — the selection
// half, next door to `lib/oceanBootstrap.ts`'s "what to do with an empty list"
// half. No I/O, so this is where the actual "what should happen" logic gets
// full unit coverage and `providers.tsx` just executes the answer.
//
// WHAT THIS IS. Claim-based tenancy decision 8: the active ocean is now
// persisted server-side per (user, account) — `GET /v1/me`'s
// `active_graph_id`, written by `PUT /v1/me/active-graph`, migration
// `0201_user_active_graph`. It survives a second browser, a second device, and
// an org switch.
//
// THIS IS A NEW FEATURE, NOT A MIGRATION OF AN OLD ONE. It is worth being
// precise about how little was there before, because it changes what the code
// below has to be careful about:
//
//   * `providers.tsx`'s `setCurrentGraphId` — the only function that ever
//     recorded a deliberate ocean SWITCH — had ZERO callers. It was declared
//     on the context interface, defined, returned, and exposed on the
//     provider value, and never invoked anywhere in `web/`. So the only
//     `cs_graph_id` write that actually ran was the one at the end of
//     `bootstrap`, which merely re-saves whatever `bootstrap` itself just
//     picked.
//   * `resetOcean` never cleared `localStorage` — it clears React state only.
//
// Put together: `cs_graph_id` is ONE GLOBAL KEY that outlives an org switch.
// Org A's ocean id is genuinely present in `localStorage` during org B's boot.
// The only thing that saves it is `list.find((g) => g.id === saved)` failing
// to match, because org B's reachable list does not contain org A's ocean.
//
// THAT LOOKUP MISS IS LOAD-BEARING. It reads like defensive coding for an
// impossible case and it is not — it is the entire cross-org guard on the
// localStorage path. Do not "simplify" it to `saved ?? list[0]`.
//
// The server pointer does not have that problem by construction: it is keyed
// on (user_id, account_id), so a read while standing in org B cannot return
// org A's row. That is why it wins.

/** The minimum shape [`pickActiveOcean`] needs — `lib/api.ts`'s `Graph`
 *  structurally satisfies it, so callers pass `Graph[]` directly without a
 *  cast and this module stays free of the API surface. */
export interface OceanRef {
  id: string;
}

/** Where the opened ocean came from. Reported for tests and for the "should I
 *  tell the server?" decision — not shown to the user. */
export type ActiveOceanSource =
  /** `GET /v1/me`'s `active_graph_id`, and it is reachable here. */
  | "server"
  /** The legacy per-browser `cs_graph_id` value, and it is reachable here. */
  | "local"
  /** Neither was usable, so the first reachable ocean. */
  | "first";

export interface ActiveOceanChoice {
  id: string;
  source: ActiveOceanSource;
}

/**
 * Choose which of `oceans` to open, given the server's stored pointer and the
 * legacy per-browser one.
 *
 * PRECEDENCE: server, then local, then the first reachable ocean.
 *
 * Server first is the point of the feature — it is the only one of the two
 * that is scoped to the account being booted, so it is the only one that can
 * be trusted without a cross-org caveat. The local value is demoted to a
 * FALLBACK for exactly one case: the server has no answer yet. That case is
 * real and common — every existing user's first boot after this ships, plus
 * any deploy where `/v1/me` predates the field (`undefined`), plus any build
 * with no control pool. Without the local fallback all of those
 * users would be bounced to `oceans[0]` once, losing a selection they had
 * made; with it, they keep it and it gets promoted to the server on that same
 * boot.
 *
 * There is deliberately NO "first load before /v1/me resolves" case to
 * arbitrate. `providers.tsx` awaits `api.me` BEFORE calling `bootstrap`, so
 * the server's answer — including the answer "none" — is always in hand by the
 * time this runs. If `/v1/me` fails, the whole boot fails and is retried;
 * there is no half-state in which the UI picks an ocean without having asked.
 *
 * BOTH PREFERENCES ARE INTERSECTED WITH `oceans`, WHICH IS THE AUTHORITY. A
 * preference is a hint, never a grant: `oceans` is the reachable list the
 * server independently returned from `GET /v1/graphs`, and an id that is not
 * in it selects nothing. That is what makes an unreachable pointer — an ocean
 * deleted, archived, or whose access was revoked since it was stored, or one
 * belonging to a different account entirely — a silent, harmless fallback
 * rather than an error or a leak. It is re-evaluated on every boot, so unlike
 * a write-time ownership check it cannot go stale.
 *
 * Callers must not invoke this with an empty `oceans`: `lib/oceanBootstrap.ts`
 * has already decided what an empty list means (create, or throw), and there
 * is no sensible ocean to return here. Throws rather than returning a
 * sentinel, so the mistake is loud.
 */
export function pickActiveOcean(
  oceans: readonly OceanRef[],
  serverPreferred: string | null | undefined,
  localPreferred: string | null | undefined,
): ActiveOceanChoice {
  if (oceans.length === 0) {
    throw new Error("pickActiveOcean: called with no reachable oceans");
  }
  const reachable = (id: string | null | undefined): id is string =>
    !!id && oceans.some((o) => o.id === id);

  if (reachable(serverPreferred)) return { id: serverPreferred, source: "server" };
  if (reachable(localPreferred)) return { id: localPreferred, source: "local" };
  return { id: oceans[0].id, source: "first" };
}

/**
 * Whether to `PUT /v1/me/active-graph` after opening `pickedId`.
 *
 * False only when the server already stores exactly this id — there is nothing
 * to tell it. Everything else reports, including the two cases that make the
 * feature work at all:
 *
 *   * the server has NO pointer yet (`serverKnownId` null/undefined) — this is
 *     how the table gets populated for every existing user, from whatever
 *     their browser or the first-reachable fallback produced;
 *   * the server's pointer was UNREACHABLE here and the fallback picked
 *     something else — reporting overwrites a pointer to an ocean this user
 *     can no longer open, so the stale value does not linger for every future
 *     boot on every device.
 *
 * The server also no-ops an unchanged write (`ON CONFLICT ... WHERE ... IS
 * DISTINCT FROM`), so this check is not what protects the row — it is what
 * keeps a request off the boot path when there is provably nothing to say.
 */
export function shouldReportActiveOcean(
  pickedId: string,
  serverKnownId: string | null | undefined,
): boolean {
  return pickedId !== serverKnownId;
}
