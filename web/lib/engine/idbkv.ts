// IndexedDB-backed KV for the private ocean's on-device store (browser only).
// Implements the keyvault KV interface; in tests an in-memory KV is used instead.
//
// `dbName` IS REQUIRED AND HAS NO DEFAULT. It used to default to the constant
// `"coralswarm"`, which made every tenant reachable from one browser profile
// share a single database — the master key in `cs_device_ocean_key` and the
// (partly unencrypted) corpus in `cs_private_corals` alike. `lib/engine/
// vaultNamespace.ts` now derives the name from the signed-in (user, org), and
// removing the default is what makes that non-optional: a caller that forgets
// fails to compile rather than silently reopening the shared store.

import type { KV } from "./keyvault";

export function idbKV(dbName: string, storeName = "kv"): KV {
  // The type says `string`, which does not exclude `""` — and `""` is a
  // perfectly valid IndexedDB name, so an empty one would open a real
  // database shared by every caller that got there. That can only happen by
  // mistake (`vaultNamespace` returns `null`, never `""`, when it has no
  // identity to name), so make the mistake loud instead of quietly
  // reconstructing the bug this argument exists to prevent.
  if (!dbName) throw new Error("idbKV: a namespaced database name is required");
  function open(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(dbName, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(storeName);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }
  function run<T>(mode: IDBTransactionMode, fn: (s: IDBObjectStore) => IDBRequest<T>): Promise<T> {
    return open().then(
      (db) =>
        new Promise<T>((resolve, reject) => {
          const r = fn(db.transaction(storeName, mode).objectStore(storeName));
          r.onsuccess = () => resolve(r.result);
          r.onerror = () => reject(r.error);
        }),
    );
  }
  return {
    async get(key) {
      const v = await run<unknown>("readonly", (s) => s.get(key));
      return (v as string | undefined) ?? null;
    },
    async set(key, value) {
      await run("readwrite", (s) => s.put(value, key));
    },
    async del(key) {
      await run("readwrite", (s) => s.delete(key));
    },
  };
}
