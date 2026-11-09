// Which on-device store the private ocean opens, as a pure function of who is
// signed in. No I/O and no React, so the naming decision — the whole security
// property of PR 2.4 — gets real unit coverage while `provider.tsx` just
// executes the answer. (Same shape as `lib/activeOcean.ts` for `providers.tsx`,
// and `components/instanceSwitch.ts` for the switcher hook: `web/` has no
// component-test harness — vitest runs in `node` and its glob is `.test.ts` —
// so anything that must not regress lives in a plain module like this one.)
//
// WHAT WAS WRONG. `idbKV()`'s database name was the constant `"coralswarm"`,
// for every signed-in user and every organization, in every browser profile.
// One name means one IndexedDB database, so two tenants reached from the same
// browser shared a store. Before PR 2.1 each org had its own hostname and the
// same-origin policy made that unreachable in practice; one apex now serves
// every tenant, so the origin no longer separates them and the name has to.
//
// WHAT LEAKS THROUGH A SHARED NAME — IT IS THE KEY *AND* THE CORPUS. The store
// holds exactly two keys:
//
//   * `cs_device_ocean_key` (keyvault.ts) — the unwrapped private-ocean master
//     key, cached so an enrolled device skips the passphrase. Shared, org B
//     decrypts org A's private corals.
//   * `cs_private_corals` (coralstore.ts) — the corals themselves. Content is
//     AES-GCM ciphertext, but `emb: Array.from(coral.embedding)` is written
//     UNENCRYPTED beside it, because local vector search needs it in the clear.
//
// The second one matters on its own. An embedding is not a hash: it is a dense
// semantic projection of the text, so possession of the vectors alone supports
// nearest-neighbour probing ("is there anything in here about <query>?") and
// published embedding-inversion work recovers substantial input text from them.
// So a shared database name leaks meaningful cross-tenant content EVEN WITHOUT
// the master key. Namespacing is not only master-key hygiene.
//
// WHY (user, org) AND NOT SOMETHING ELSE. Same axis `providers.tsx`'s
// `QueryCacheIdentityBoundary` already clears its cache on
// (`user?.id`, `orgId`), and for the same reason it gives there: switching org
// keeps the same Clerk user, so a user-only key never fires, and the org also
// decides which tenant DATABASE the requests go to. The identity comes from
// Clerk (`useAuth()`), never from `useApp()`'s `accountId`/`internalUserId` —
// those are null until `api.me()` resolves, so keying on them would open a
// store named for a null identity during boot and a different one afterwards.
//
// NO MIGRATION PATH IS PROVIDED, DELIBERATELY. `NEXT_PUBLIC_ENGINE` is set in
// no `.env.example`, no `fly.toml`, no `Dockerfile`, no `next.config.ts` env
// block and no `vercel.json`, so the engine has never run in production and
// there is no `"coralswarm"` database in any real user's browser to carry
// forward. A migration would be code with no input. Note that no identity can
// ever produce the bare legacy name (every name carries a `:u:` segment), so
// any such database is inert rather than silently adopted.

/** The signed-in identity, in Clerk's own `useAuth()` shape so the caller
 *  passes it straight through with no reinterpretation in between. */
export interface VaultIdentity {
  /** Clerk's `isLoaded`. False during the pre-session render, when `userId`
   *  and `orgId` are both `undefined` and mean nothing. */
  isLoaded: boolean;
  /** Clerk's `userId`: the signed-in user, or `null` when signed out. */
  userId: string | null | undefined;
  /** Clerk's `orgId`: the active organization, or `null` for the user's own
   *  personal instance. `null` here is an ANSWER ("no org"), not "unknown". */
  orgId: string | null | undefined;
}

/** Prefix on every name, so a store is identifiable in devtools and can never
 *  collide with an unrelated origin-shared database. */
const PREFIX = "coralswarm";

/**
 * The identity segment shared by everything this module names, or `null` when
 * there is no identity to name.
 *
 * `null` MEANS "DO NOT OPEN ANYTHING", NOT "USE A DEFAULT". A default would be
 * a real database that some future signed-in identity could also be routed
 * into; refusing to name one cannot collide with anything, because it is not a
 * name. That is the safe direction of the tradeoff: the worst case of refusing
 * is that the private engine stays unmounted for that render and the app
 * behaves exactly as it does with the engine switched off, which is the
 * shipping configuration anyway. The worst case of defaulting is the bug this
 * PR exists to remove.
 *
 * Two identity states produce `null`:
 *   * Clerk has not loaded (`isLoaded` false) — nothing is known yet;
 *   * nobody is signed in (`userId` null) — there is no one to namespace by.
 *
 * ENCODING. Each component is `encodeURIComponent`-escaped, which escapes `:`
 * to `%3A`, so no user id can contain the `:o:` separator and impersonate a
 * different (user, org) pair. Without that, a user id of `x:o:org_acme` on the
 * personal instance would produce the same string as user `x` inside
 * `org_acme`. Clerk ids (`user_…`, `org_…`) do not contain `:` today, which is
 * exactly why this would be an easy thing to get away with and a silent thing
 * to break later.
 *
 * `orgId` `null` and `undefined` deliberately map to the SAME empty segment.
 * Post-load both spell "no active org", i.e. the personal instance, and giving
 * them separate names would split one user's personal ocean across two stores
 * depending on which Clerk happened to report. The empty segment is an
 * unambiguous sentinel: every real org id is non-empty, so nothing else can
 * encode to it.
 */
export function vaultIdentityKey(identity: VaultIdentity): string | null {
  if (!identity.isLoaded) return null;
  if (!identity.userId) return null;
  const user = encodeURIComponent(identity.userId);
  const org = encodeURIComponent(identity.orgId ?? "");
  return `u:${user}:o:${org}`;
}

/**
 * The IndexedDB database name for this identity's private-ocean store, or
 * `null` when there is no identity (see [`vaultIdentityKey`] — the caller must
 * then open no store at all, not a fallback one).
 *
 * Callers must not hardcode a name past this function. `idbKV` takes its
 * database name as a REQUIRED argument for that reason: there is no default
 * left to fall back into.
 */
export function vaultDatabaseName(identity: VaultIdentity): string | null {
  const key = vaultIdentityKey(identity);
  if (key === null) return null;
  return `${PREFIX}:${key}`;
}

/**
 * The `sessionStorage` key latching "we already re-checked org escrow for this
 * identity this session", or `null` when there is no identity.
 *
 * Folded in here rather than deferred because it is the same defect on a
 * different store. The latch used to be the bare constant `cs_escrow_checked`,
 * keyed by nothing, so after an org switch within one session the check was
 * skipped: the incoming org's members would never be enrolled into that org's
 * recovery, and an admin recovery that everyone believed was configured would
 * come up empty. It is engine-gated and therefore inert today, which is the
 * argument for fixing it now rather than discovering it once the engine ships.
 *
 * Same identity as the database name, on purpose: the thing being enrolled is
 * the key held in that database, so the two must not be able to disagree about
 * whose key it is.
 */
export function escrowCheckKey(identity: VaultIdentity): string | null {
  const key = vaultIdentityKey(identity);
  if (key === null) return null;
  return `cs_escrow_checked:${key}`;
}
