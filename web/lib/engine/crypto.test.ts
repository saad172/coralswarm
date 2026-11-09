import { describe, it, expect } from "vitest";
import {
  generateOceanKey,
  encryptString,
  decryptString,
  deriveKek,
  wrapKey,
  unwrapKey,
  randomBytes,
  exportRawKey,
  importRawKey,
  toBase64,
  fromBase64,
} from "./crypto";

describe("engine/crypto", () => {
  it("encrypts and decrypts a string round-trip", async () => {
    const key = await generateOceanKey();
    const packed = await encryptString(key, "hello private ocean");
    expect(await decryptString(key, packed)).toBe("hello private ocean");
  });

  it("fails to decrypt with the wrong key", async () => {
    const k1 = await generateOceanKey();
    const k2 = await generateOceanKey();
    const packed = await encryptString(k1, "secret");
    await expect(decryptString(k2, packed)).rejects.toBeDefined();
  });

  it("wraps + unwraps the ocean key under a passphrase KEK", async () => {
    const salt = randomBytes(16);
    const kek = await deriveKek("correct horse battery staple", salt);
    const oceanKey = await generateOceanKey();
    const wrapped = await wrapKey(kek, oceanKey);
    // Re-derive the KEK from the same passphrase + salt (as on another device).
    const kek2 = await deriveKek("correct horse battery staple", salt);
    const unwrapped = await unwrapKey(kek2, wrapped);
    const packed = await encryptString(oceanKey, "x");
    expect(await decryptString(unwrapped, packed)).toBe("x");
  });

  it("a wrong passphrase cannot unwrap", async () => {
    const salt = randomBytes(16);
    const oceanKey = await generateOceanKey();
    const wrapped = await wrapKey(await deriveKek("right", salt), oceanKey);
    await expect(unwrapKey(await deriveKek("wrong", salt), wrapped)).rejects.toBeDefined();
  });

  it("exports a 32-byte raw key and base64 round-trips", async () => {
    const raw = await exportRawKey(await generateOceanKey());
    expect(raw.length).toBe(32);
    expect([...fromBase64(toBase64(raw))]).toEqual([...raw]);
  });

  it("imports a raw key usable as a KEK", async () => {
    const kek = await importRawKey(randomBytes(32));
    const ocean = await generateOceanKey();
    const wrapped = await wrapKey(kek, ocean);
    expect(await unwrapKey(kek, wrapped)).toBeDefined();
  });
});
