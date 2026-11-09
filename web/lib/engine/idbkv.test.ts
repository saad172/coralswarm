import { describe, expect, it } from "vitest";
import { idbKV } from "./idbkv";

// `idbKV` touches `indexedDB` lazily — only inside get/set/del — so it can be
// CONSTRUCTED in the node test env. That is all these tests do: they pin the
// argument contract, not the storage behaviour (there is no fake IndexedDB
// here, and the KV interface is already covered against an in-memory KV in
// keyvault.test.ts / privateEngine.test.ts).

describe("idbKV — the database name is not optional", () => {
  it("refuses an empty name instead of opening a shared database", () => {
    // `""` is a valid IndexedDB name, so this would silently be a real
    // database shared by every caller that reached it — the exact defect the
    // required argument exists to remove, reintroduced by a falsy value that
    // the type system cannot reject.
    expect(() => idbKV("")).toThrow(/namespaced database name/);
  });

  it("accepts a namespaced name", () => {
    // The complement: the guard must not be so eager that a real name from
    // `vaultNamespace.vaultDatabaseName` trips it.
    const kv = idbKV("coralswarm:u:user_2a:o:org_2x");
    expect(typeof kv.get).toBe("function");
    expect(typeof kv.set).toBe("function");
    expect(typeof kv.del).toBe("function");
  });
});
