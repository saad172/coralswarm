// B2B M-of-N org-escrow recovery (ADR-0007 D6), asymmetric per-user model.
//
// Why asymmetric: escrow is PER USER — every member's private-ocean key must be
// wrapped to the org recovery key. With a symmetric recovery key that would mean
// handing the recovery secret to every member (so any one of them could recover
// everyone) — defeating M-of-N. Instead the org recovery key is an RSA keypair:
//   • the PUBLIC key is published to all members (safe) — each member wraps their
//     own ocean key to it at enrollment; the server stores that opaque blob.
//   • the PRIVATE key is Shamir-split across N admins, M required. It is only
//     ever reconstructed client-side, transiently, during a recovery.
// The server stays blind: it holds only wrapped-ocean-key blobs and, per admin,
// their opaque share. No single admin (and not the server) can decrypt.
//
// Composes engine/crypto (RSA-OAEP) + engine/shamir; fully unit-testable.

import {
  generateRecoveryKeypair,
  exportRecoveryPublicKey,
  exportRecoveryPrivateKey,
  importRecoveryPublicKey,
  importRecoveryPrivateKey,
  rsaWrapOceanKey,
  rsaUnwrapOceanKey,
  toBase64,
  fromBase64,
} from "./crypto";
import { split, combine, type Share } from "./shamir";

export interface OrgRecoverySetup {
  /** Recovery PUBLIC key (SPKI DER) — publish to every member, server-stored. */
  recoveryPublicKey: Uint8Array;
  /** Recovery PRIVATE key (PKCS8) split M-of-N — one opaque share per admin. */
  shares: Share[];
}

/**
 * Set up the org recovery key ONCE for the org (not per user): generate the
 * recovery keypair, publish the public half, and Shamir-split the private half
 * `threshold`-of-`adminCount` across admins.
 */
export async function setupOrgRecovery(
  adminCount: number,
  threshold: number,
): Promise<OrgRecoverySetup> {
  const pair = await generateRecoveryKeypair();
  const recoveryPublicKey = await exportRecoveryPublicKey(pair.publicKey);
  const pkcs8 = await exportRecoveryPrivateKey(pair.privateKey);
  const shares = split(pkcs8, adminCount, threshold);
  return { recoveryPublicKey, shares };
}

/**
 * A member wraps their own (extractable) ocean key to the org recovery public
 * key at enrollment. The returned blob is stored server-side, per user.
 */
export async function wrapOceanKeyForRecovery(
  oceanKey: CryptoKey,
  recoveryPublicKey: Uint8Array,
): Promise<Uint8Array> {
  const pub = await importRecoveryPublicKey(recoveryPublicKey);
  return rsaWrapOceanKey(pub, oceanKey);
}

/**
 * Reconstruct the recovery private key from `threshold`+ admin shares and unwrap
 * a member's ocean key. Fewer than `threshold` shares yield wrong key bytes, so
 * the import/unwrap fails — partial collusion cannot recover.
 */
export async function recoverOceanKey(
  wrappedOceanKey: Uint8Array,
  shares: Share[],
): Promise<CryptoKey> {
  const pkcs8 = combine(shares);
  const priv = await importRecoveryPrivateKey(pkcs8);
  return rsaUnwrapOceanKey(priv, wrappedOceanKey);
}

/** Serialize a share to a single distributable string: `"<x>.<base64(y)>"`. */
export function encodeShare(s: Share): string {
  return `${s.x}.${toBase64(s.y)}`;
}

/** Parse a share string produced by `encodeShare`. Throws on a malformed input. */
export function decodeShare(str: string): Share {
  const dot = str.indexOf(".");
  if (dot < 1) throw new Error("malformed share");
  const x = Number(str.slice(0, dot));
  if (!Number.isInteger(x) || x < 1 || x > 255) throw new Error("share index out of range");
  return { x, y: fromBase64(str.slice(dot + 1)) };
}
