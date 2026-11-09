"use client";

// React wiring for the on-device private engine (ADR-0007 D5/D9/D10), device-trust
// model. Gated by NEXT_PUBLIC_ENGINE — when off, the provider isn't mounted and
// useEngine() returns null, so the app is the server-backed cloud app.
//
// Flow on a device:
//   • device already enrolled (key cached locally) → auto-unlock, no passphrase.
//   • new device with a server blob → prompt passphrase once to enroll.
//   • no blob at all → first-time setup (choose a passphrase).
// The passphrase is only needed to enroll a device or after a reset.
//
// SPLIT IN TWO, BY IDENTITY (claim-based tenancy PR 2.4). `EngineProvider`
// resolves who is signed in and does nothing else; `EngineSession` holds every
// piece of state and is mounted UNDER A `key` OF THE DATABASE NAME. See the
// comment on `EngineProvider` for why that boundary is where it is.
//
// AND BY BEARER, NOT ONLY BY STORE. Naming the store per identity is only half
// the property: a session also has to act with a token that authorizes the same
// identity, and Clerk's `orgId` moves synchronously on a switch while the app's
// token does not. `lib/engine/engineSession.ts` holds that decision (and the
// full account of what a stale bearer cost); nothing below re-derives it.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useAuth } from "@clerk/nextjs";
import { api } from "../api";
import { idbKV } from "./idbkv";
import { createVault, unwrapBlob, cacheDeviceKey, loadDeviceKey, clearDevice } from "./keyvault";
import type { VaultIdentity } from "./vaultNamespace";
import { engineSessionFor } from "./engineSession";
import { wrapOceanKeyForRecovery } from "./escrow";
import { fromBase64, toBase64 } from "./crypto";
import { PrivateEngine } from "./privateEngine";
import { serverEmbedder, CLOUD_EMBED_MODEL } from "./serverEmbedder";

export const ENGINE_ENABLED = process.env.NEXT_PUBLIC_ENGINE === "true";

/** Best-effort org-escrow enrollment (ADR-0007 D6): if the org has set up a
 *  recovery key, wrap this member's ocean key to it and store the opaque blob so
 *  admins can recover the member M-of-N. No-op (silently) when the org has no
 *  recovery configured, or off the cloud. Idempotent (PUT upserts).
 *
 *  `token` IS NON-NULLABLE, AND THAT IS THE POINT. Both calls below resolve the
 *  ORGANIZATION from the token's `org_id` claim — `getRecoveryPubkey` reads the
 *  recovery public key for that account, `putWrappedKey` files the blob under
 *  it. Handed a token for a different organization than `key` belongs to, this
 *  function wraps one tenant's private-ocean key to another tenant's recovery
 *  public key and stores it in that tenant's escrow, where its admins can
 *  reconstruct it M-of-N. Only `engineSessionFor` may decide that a token and a
 *  key belong together; this signature refuses the `string | null` that used to
 *  let any caller pass whatever bearer was lying around. */
async function escrowEnroll(token: string, key: CryptoKey): Promise<void> {
  try {
    const rec = await api.escrow.getRecoveryPubkey(token); // 404 → no org recovery
    const wrapped = await wrapOceanKeyForRecovery(key, fromBase64(rec.pubkey));
    await api.escrow.putWrappedKey(token, toBase64(wrapped));
  } catch {
    /* no org recovery configured, or unreachable — escrow is best-effort */
  }
}

export type EngineStatus = "checking" | "needs-setup" | "locked" | "ready";

export interface EngineCtx {
  status: EngineStatus;
  engine: PrivateEngine | null;
  /** First-time setup: choose a passphrase (generates the key + stores the blob). */
  setup: (passphrase: string) => Promise<void>;
  /** New-device enrollment: unlock the server blob with the passphrase. */
  unlock: (passphrase: string) => Promise<boolean>;
  /** Forgot the passphrase: drop this device + the server blob and start fresh. */
  reset: () => Promise<void>;
}

const Ctx = createContext<EngineCtx | null>(null);

/**
 * Resolve the signed-in identity, then hand the whole engine to a session that
 * is mounted under it.
 *
 * WHY THIS IS A SEPARATE COMPONENT — THE FIRST-RENDER CAPTURE. The store used
 * to be created as `const [kv] = useState(() => idbKV())`. A `useState`
 * initializer runs ONCE, at first render, and the value is kept for the life of
 * the component: computing a namespaced name in there would pin the store to
 * whichever identity happened to be current at first render and never follow an
 * org switch. That is a worse bug than the one being fixed, because it looks
 * fixed. So identity is resolved OUTSIDE the stateful component and passed in
 * as `key`: React cannot preserve state across a key change, so a switch tears
 * the session down and builds a fresh one — including `engine` and `serverBlob`,
 * which a `useMemo` on the KV alone would have left holding the previous org's
 * unlocked key and blob.
 *
 * THIS DELIBERATELY DOES NOT LEAN ON PR 2.2's `window.location.reload()`. A
 * switch does currently reload the document (`components/instanceSwitch.ts`),
 * which would remount everything and hide any staleness here. Depending on that
 * would make this PR's security property a side effect of a sibling PR: if the
 * reload is ever reverted, made conditional, or skipped on some path, the store
 * would silently go back to being shared with no test failing. The `key` makes
 * the guarantee local and unconditional.
 *
 * IDENTITY COMES FROM CLERK, NOT FROM `useApp()`. `useApp()`'s `accountId` and
 * `internalUserId` are null until `api.me()` resolves, so keying on them would
 * open a database named for a null identity during boot and a different one
 * once the call landed — two stores per session, the first of them shared.
 * Clerk's `useAuth()` is the session itself and is authoritative from the
 * moment `isLoaded` flips.
 *
 * NO IDENTITY → NO ENGINE, AND NO STORE. When Clerk has not loaded or nobody is
 * signed in, `vaultDatabaseName` returns null and this renders a null context:
 * `useEngine()` returns null, `EngineGate` passes its children straight
 * through, and the app behaves exactly as it does with the engine switched off.
 * Nothing is opened under an unknown identity. In practice this branch is
 * unreachable — `app/(app)/layout.tsx` only mounts this once `ready && token`,
 * which already implies a loaded, signed-in Clerk session — but "there is no
 * store to open" is the honest answer to "whose store is this?" and it cannot
 * collide with a real one, because it is not a name.
 *
 * AND: NO BEARER FOR *THIS* IDENTITY → NO ENGINE EITHER. The `key` above makes
 * the STORE follow an org switch instantly, because it is derived from Clerk's
 * own `orgId`. The token cannot: `useApp()`'s `token` is React state written
 * only after an `await getToken()`, and for a newly-active org that is a real
 * round trip to Clerk (the cached JWT carries the previous `org_id`). So there
 * is a committed render — with the mount effect of a brand-new `EngineSession`
 * flushing inside it — where the store is org B's and the token is org A's, and
 * every request that session makes in that window is executed against org A.
 * `engineSessionFor` is what closes it: it hands back a session only when the
 * bearer's own claims name the same (user, org) the store is named for, so the
 * switch window renders the SAME null context as "not signed in yet" and the
 * engine appears the instant the right token lands. See
 * `lib/engine/engineSession.ts` for what a stale bearer actually did to org
 * escrow, and for why comparing through `vaultIdentityKey` rather than
 * comparing `orgId` strings is the part that cannot drift.
 */
export function EngineProvider({ token, children }: { token: string | null; children: ReactNode }) {
  const { isLoaded, userId, orgId } = useAuth();
  const identity: VaultIdentity = { isLoaded, userId, orgId };
  const session = engineSessionFor(identity, token);
  if (session === null) {
    return <Ctx.Provider value={null}>{children}</Ctx.Provider>;
  }
  return (
    <EngineSession
      key={session.dbName}
      dbName={session.dbName}
      latchKey={session.latchKey}
      token={session.token}
    >
      {children}
    </EngineSession>
  );
}

/** The engine for exactly one identity. Never mounted without a namespaced
 *  `dbName`, and never reused across identities — `EngineProvider` keys it.
 *
 *  `token` is `string`, NOT `string | null`, and that is load-bearing rather
 *  than tidy. It is the compile-time half of this component's invariant: the
 *  bearer it holds authorizes the very identity `dbName` is named for, because
 *  the only way to obtain one is `engineSessionFor`. A nullable prop would
 *  re-admit `<EngineSession token={token}>` with the provider's raw, possibly
 *  stale bearer — the exact line this fix removes — and it would type-check. */
function EngineSession({
  token,
  dbName,
  latchKey,
  children,
}: {
  token: string;
  dbName: string;
  latchKey: string;
  children: ReactNode;
}) {
  const [status, setStatus] = useState<EngineStatus>("checking");
  const [engine, setEngine] = useState<PrivateEngine | null>(null);
  const [serverBlob, setServerBlob] = useState<string | null>(null);
  // `useMemo`, not `useState(() => …)`. Under the `key` above `dbName` cannot
  // change within one mount, so the two behave identically today — but a
  // `useState` initializer would quietly outlive a `dbName` prop change if
  // anyone ever dropped the key, and this one cannot.
  const kv = useMemo(() => idbKV(dbName), [dbName]);

  const prepare = useCallback(async (key: CryptoKey): Promise<PrivateEngine | null> => {
    const next = new PrivateEngine(kv, key, serverEmbedder(token));
    try {
      await next.reindexIfStale(CLOUD_EMBED_MODEL);
    } catch {
      // Leave unpublished so Ask never mixes MiniLM leftovers with cloud.
      return null;
    }
    return next;
  }, [kv, token]);

  useEffect(() => {
    let active = true;
    (async () => {
      // 1. Already enrolled on this device → auto-unlock (no passphrase).
      const cached = await loadDeviceKey(kv);
      // THE GUARD GOES HERE, ONCE, BEFORE THE BRANCH — not on each individual
      // state write further down. It used to sit only on the writes (`if
      // (active) open(cached)`), which stops a torn-down session from
      // rendering but not from ACTING: everything below this line either
      // issues a request or writes `sessionStorage`, and neither is undone by
      // a later render. `active` is false here whenever the identity changed
      // while this open was in flight — an ordinary occurrence, since
      // `EngineProvider` withdraws the session the moment Clerk's `orgId`
      // moves — so continuing would mean a dead session latching an
      // enrollment it will never perform, and firing requests nobody is left
      // to read.
      if (!active) return;
      if (cached) {
        const next = await prepare(cached);
        if (!active || !next) return;
        setEngine(next);
        setStatus("ready");
        // Cover the case where the org enabled recovery AFTER this device
        // enrolled: enroll once per session (cheap PUT, guarded).
        //
        // The latch is keyed by IDENTITY (`latchKey`), not by the bare constant
        // it used to be: one global key meant that after an org switch inside a
        // single session this check was skipped entirely, so the incoming org's
        // recovery would never see this member's key.
        //
        // NOTE THE ORDER: the latch is set BEFORE the enrollment and is never
        // cleared, and `sessionStorage` survives `window.location.reload()`.
        // So an enrollment performed here under the wrong bearer is not merely
        // a wrong write — it also consumes this identity's one chance at a
        // right one for the rest of the browser session. That is why the
        // binding is enforced before the session exists at all rather than
        // being re-checked at this line.
        if (typeof sessionStorage !== "undefined" && !sessionStorage.getItem(latchKey)) {
          sessionStorage.setItem(latchKey, "1");
          void escrowEnroll(token, cached);
        }
        return;
      }
      // 2. New device — enroll against the server blob if one exists.
      try {
        const { blob } = await api.privateKey.get(token);
        if (!active) return;
        setServerBlob(blob);
        setStatus("locked");
      } catch {
        if (active) setStatus("needs-setup");
      }
    })();
    return () => {
      active = false;
    };
  }, [kv, token, prepare, latchKey]);

  const setup = useCallback(
    async (passphrase: string) => {
      const { key, blob } = await createVault(passphrase);
      // Unconditional now that `token` is non-nullable. The `if (token)` this
      // replaces was a silent hazard, not a nicety: on a null token it skipped
      // the server PUT and carried on, leaving a device holding a private-ocean
      // key with NO blob to enroll any other device from — a key the user can
      // never reach again from a second browser, reported as success.
      await api.privateKey.put(token, blob); // for other devices
      await cacheDeviceKey(kv, key);
      void escrowEnroll(token, key); // org recovery, if configured
      const next = await prepare(key);
      if (!next) return;
      setEngine(next);
      setStatus("ready");
    },
    [kv, token, prepare],
  );

  const unlock = useCallback(
    async (passphrase: string) => {
      if (!serverBlob) return false;
      try {
        const key = await unwrapBlob(passphrase, serverBlob);
        await cacheDeviceKey(kv, key);
        void escrowEnroll(token, key); // org recovery, if configured
        const next = await prepare(key);
        if (!next) return false;
        setEngine(next);
        setStatus("ready");
        return true;
      } catch {
        return false;
      }
    },
    [kv, serverBlob, prepare, token],
  );

  const reset = useCallback(async () => {
    await clearDevice(kv);
    // Also unconditional, and for a sharper reason than `setup`'s: the copy in
    // `EngineGate` promises reset "discards the private-ocean key EVERYWHERE".
    // Skipping the DELETE on a null token cleared this device only and still
    // reported the everywhere outcome.
    await api.privateKey.del(token).catch(() => {});
    setEngine(null);
    setServerBlob(null);
    setStatus("needs-setup");
  }, [kv, token]);

  return (
    <Ctx.Provider value={{ status, engine, setup, unlock, reset }}>{children}</Ctx.Provider>
  );
}

/** Returns the engine context, or null when the engine is disabled / unmounted. */
export function useEngine(): EngineCtx | null {
  return useContext(Ctx);
}
