import { describe, it, expect } from "vitest";
import {
  createVault,
  unwrapBlob,
  cacheDeviceKey,
  loadDeviceKey,
  clearDevice,
  type KV,
} from "./keyvault";
import { encryptString, decryptString } from "./crypto";

function memKV(): KV {
  const m = new Map<string, string>();
  return {
    async get(k) {
      return m.get(k) ?? null;
    },
    async set(k, v) {
      m.set(k, v);
    },
    async del(k) {
      m.delete(k);
    },
  };
}

describe("engine/keyvault (device-trust)", () => {
  it("createVault → unwrapBlob with the passphrase yields the same key (new device)", async () => {
    const { key, blob } = await createVault("pp-1");
    const packed = await encryptString(key, "secret");
    const key2 = await unwrapBlob("pp-1", blob);
    expect(await decryptString(key2, packed)).toBe("secret");
  });

  it("a wrong passphrase can't unwrap the server blob", async () => {
    const { blob } = await createVault("right");
    await expect(unwrapBlob("wrong", blob)).rejects.toBeDefined();
  });

  it("device cache round-trips the key (auto-unlock, no passphrase)", async () => {
    const kv = memKV();
    const { key } = await createVault("pp");
    const packed = await encryptString(key, "x");
    await cacheDeviceKey(kv, key);
    const cached = await loadDeviceKey(kv);
    expect(cached).not.toBeNull();
    expect(await decryptString(cached as CryptoKey, packed)).toBe("x");
  });

  it("clearDevice removes the cache", async () => {
    const kv = memKV();
    await cacheDeviceKey(kv, (await createVault("pp")).key);
    await clearDevice(kv);
    expect(await loadDeviceKey(kv)).toBeNull();
  });
});
