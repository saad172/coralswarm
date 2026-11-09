import { describe, it, expect } from "vitest";
import { generateOceanKey, encryptString, decryptString } from "./crypto";
import {
  setupOrgRecovery,
  wrapOceanKeyForRecovery,
  recoverOceanKey,
  encodeShare,
  decodeShare,
} from "./escrow";
import { wrapKeyToBlob, unwrapBlob } from "./keyvault";

describe("engine/escrow (asymmetric M-of-N org recovery, per-user)", () => {
  it("recovers a member's ocean key from threshold admin shares", async () => {
    // Org sets up recovery ONCE; a member wraps their own key to the public half.
    const { recoveryPublicKey, shares } = await setupOrgRecovery(5, 3);
    const oceanKey = await generateOceanKey();
    const packed = await encryptString(oceanKey, "private decision");
    const wrapped = await wrapOceanKeyForRecovery(oceanKey, recoveryPublicKey);

    const recovered = await recoverOceanKey(wrapped, [shares[0], shares[2], shares[4]]);
    expect(await decryptString(recovered, packed)).toBe("private decision");
  });

  it("one recovery setup serves many members (per-user wrapping)", async () => {
    const { recoveryPublicKey, shares } = await setupOrgRecovery(4, 2);
    const a = await generateOceanKey();
    const b = await generateOceanKey();
    const wa = await wrapOceanKeyForRecovery(a, recoveryPublicKey);
    const wb = await wrapOceanKeyForRecovery(b, recoveryPublicKey);

    const ra = await recoverOceanKey(wa, [shares[1], shares[3]]);
    const rb = await recoverOceanKey(wb, [shares[0], shares[2]]);
    const pa = await encryptString(a, "alice");
    const pb = await encryptString(b, "bob");
    expect(await decryptString(ra, pa)).toBe("alice");
    expect(await decryptString(rb, pb)).toBe("bob");
  });

  it("fewer than threshold admins cannot recover", async () => {
    const { recoveryPublicKey, shares } = await setupOrgRecovery(5, 3);
    const oceanKey = await generateOceanKey();
    const wrapped = await wrapOceanKeyForRecovery(oceanKey, recoveryPublicKey);
    await expect(recoverOceanKey(wrapped, [shares[0], shares[1]])).rejects.toBeDefined();
  });

  it("the recovered key is extractable (can be re-cached for the user)", async () => {
    const { recoveryPublicKey, shares } = await setupOrgRecovery(3, 2);
    const oceanKey = await generateOceanKey();
    const wrapped = await wrapOceanKeyForRecovery(oceanKey, recoveryPublicKey);
    const recovered = await recoverOceanKey(wrapped, [shares[0], shares[2]]);
    expect(recovered.extractable).toBe(true);
  });

  it("shares survive encode → distribute (string) → decode → combine", async () => {
    const { recoveryPublicKey, shares } = await setupOrgRecovery(5, 3);
    const oceanKey = await generateOceanKey();
    const wrapped = await wrapOceanKeyForRecovery(oceanKey, recoveryPublicKey);
    // Round-trip the share strings as an admin would paste them.
    const pasted = [shares[0], shares[1], shares[4]].map(encodeShare).map(decodeShare);
    const recovered = await recoverOceanKey(wrapped, pasted);
    const packed = await encryptString(oceanKey, "x");
    expect(await decryptString(recovered, packed)).toBe("x");
  });

  it("end-to-end recovery re-delivery: recover → re-wrap → member unlocks", async () => {
    // Member enrolls + escrows their key.
    const { recoveryPublicKey, shares } = await setupOrgRecovery(3, 2);
    const oceanKey = await generateOceanKey();
    const secret = await encryptString(oceanKey, "the member's private note");
    const wrapped = await wrapOceanKeyForRecovery(oceanKey, recoveryPublicKey);

    // Admin reconstructs and re-wraps under a fresh recovery passphrase.
    const recovered = await recoverOceanKey(wrapped, [shares[0], shares[1]]);
    const blob = await wrapKeyToBlob("temp-recovery-pass", recovered);

    // Member unlocks the re-delivered blob on their device and reads their data.
    const unlocked = await unwrapBlob("temp-recovery-pass", blob);
    expect(await decryptString(unlocked, secret)).toBe("the member's private note");
  });
});
