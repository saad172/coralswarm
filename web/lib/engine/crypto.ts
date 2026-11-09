// Client-side crypto for the server-blind private ocean (ADR-0007 D5).
//
// AES-256-GCM for coral content + key wrapping; PBKDF2 to derive a key-encryption
// key (KEK) from the user's passphrase. Uses the Web Crypto API, available in
// both the browser and Node (so this module is unit-testable). The private-ocean
// key is generated on-device and only ever leaves wrapped — the server stores
// opaque blobs and never sees plaintext or the raw key.

const subtle = (): SubtleCrypto => globalThis.crypto.subtle;
const enc = new TextEncoder();
const dec = new TextDecoder();

// TS 5.7 types Uint8Array as Uint8Array<ArrayBufferLike>, which doesn't satisfy
// the Web Crypto `BufferSource` (wants ArrayBuffer-backed). Our buffers are
// always ArrayBuffer-backed at runtime, so narrow at the call boundary.
const bs = (u: Uint8Array): BufferSource => u as unknown as BufferSource;

const IV_LEN = 12; // AES-GCM nonce
const PBKDF2_ITERATIONS = 210_000; // OWASP 2023 guidance for PBKDF2-HMAC-SHA256

export function randomBytes(n: number): Uint8Array {
  const b = new Uint8Array(n);
  globalThis.crypto.getRandomValues(b);
  return b;
}

/** Derive an AES-GCM key-encryption key from a passphrase + salt. */
export async function deriveKek(passphrase: string, salt: Uint8Array): Promise<CryptoKey> {
  const base = await subtle().importKey("raw", bs(enc.encode(passphrase)), "PBKDF2", false, [
    "deriveKey",
  ]);
  return subtle().deriveKey(
    { name: "PBKDF2", salt: bs(salt), iterations: PBKDF2_ITERATIONS, hash: "SHA-256" },
    base,
    { name: "AES-GCM", length: 256 },
    false,
    ["wrapKey", "unwrapKey", "encrypt", "decrypt"],
  );
}

/** Generate a fresh, extractable AES-256 data key (the private-ocean key). */
export async function generateOceanKey(): Promise<CryptoKey> {
  return subtle().generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
}

/** Import 32 raw bytes as an AES-256-GCM key (e.g. a reconstructed Shamir secret). */
export async function importRawKey(raw: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("raw", bs(raw), { name: "AES-GCM", length: 256 }, true, [
    "wrapKey",
    "unwrapKey",
  ]);
}

/** `[iv | ciphertext+tag]` packed buffer for a UTF-8 string. */
export async function encryptString(key: CryptoKey, plaintext: string): Promise<Uint8Array> {
  const iv = randomBytes(IV_LEN);
  const ct = new Uint8Array(
    await subtle().encrypt({ name: "AES-GCM", iv: bs(iv) }, key, bs(enc.encode(plaintext))),
  );
  return concat(iv, ct);
}

export async function decryptString(key: CryptoKey, packed: Uint8Array): Promise<string> {
  const iv = packed.slice(0, IV_LEN);
  const ct = packed.slice(IV_LEN);
  const pt = await subtle().decrypt({ name: "AES-GCM", iv: bs(iv) }, key, bs(ct));
  return dec.decode(pt);
}

/** Wrap an (extractable) data key under a KEK → `[iv | wrapped]`. */
export async function wrapKey(kek: CryptoKey, key: CryptoKey): Promise<Uint8Array> {
  const iv = randomBytes(IV_LEN);
  const wrapped = new Uint8Array(
    await subtle().wrapKey("raw", key, kek, { name: "AES-GCM", iv: bs(iv) }),
  );
  return concat(iv, wrapped);
}

export async function unwrapKey(kek: CryptoKey, packed: Uint8Array): Promise<CryptoKey> {
  const iv = packed.slice(0, IV_LEN);
  const wrapped = packed.slice(IV_LEN);
  return subtle().unwrapKey(
    "raw",
    bs(wrapped),
    kek,
    { name: "AES-GCM", iv: bs(iv) },
    { name: "AES-GCM", length: 256 },
    true,
    ["encrypt", "decrypt"],
  );
}

/** Export a key as raw bytes (for Shamir-splitting the org recovery key, or
 *  caching the unlocked ocean key on a trusted device). */
export async function exportRawKey(key: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array(await subtle().exportKey("raw", key));
}

/** Import raw bytes as the AES-256-GCM ocean key (encrypt/decrypt + extractable
 *  so it can be re-cached/wrapped). Used to restore a device-cached key. */
export async function importOceanKey(raw: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("raw", bs(raw), { name: "AES-GCM", length: 256 }, true, [
    "encrypt",
    "decrypt",
  ]);
}

// ── Asymmetric org-recovery escrow (ADR-0007 D6) ───────────────────────────
// The org recovery key is an RSA-OAEP keypair. The PUBLIC key is published to
// every member (safe to share) so each member wraps their own ocean key to it
// at enrollment — per-user escrow with no shared secret handed around. The
// PRIVATE key is Shamir-split across admins (M-of-N) and only ever reconstructed
// client-side during a recovery. RSA-OAEP-2048/SHA-256 wraps a 256-bit AES key
// comfortably (max OAEP plaintext ≈ 190 bytes).

const RSA_PARAMS = {
  name: "RSA-OAEP",
  modulusLength: 2048,
  publicExponent: new Uint8Array([1, 0, 1]),
  hash: "SHA-256",
} as const;

/** Generate the org recovery keypair (extractable, for export + Shamir-split). */
export async function generateRecoveryKeypair(): Promise<CryptoKeyPair> {
  return subtle().generateKey(RSA_PARAMS, true, ["wrapKey", "unwrapKey"]);
}

/** Export the recovery public key as SPKI DER bytes (published to members). */
export async function exportRecoveryPublicKey(key: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array(await subtle().exportKey("spki", key));
}

/** Export the recovery private key as PKCS8 DER bytes (to Shamir-split). */
export async function exportRecoveryPrivateKey(key: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array(await subtle().exportKey("pkcs8", key));
}

/** Import published SPKI bytes as the recovery public key (wrap-only). */
export async function importRecoveryPublicKey(spki: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("spki", bs(spki), RSA_PARAMS, false, ["wrapKey"]);
}

/** Import reconstructed PKCS8 bytes as the recovery private key (unwrap-only). */
export async function importRecoveryPrivateKey(pkcs8: Uint8Array): Promise<CryptoKey> {
  return subtle().importKey("pkcs8", bs(pkcs8), RSA_PARAMS, false, ["unwrapKey"]);
}

/** Wrap an (extractable) ocean key to the recovery public key → opaque blob. */
export async function rsaWrapOceanKey(pub: CryptoKey, oceanKey: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array(await subtle().wrapKey("raw", oceanKey, pub, { name: "RSA-OAEP" }));
}

/** Unwrap an ocean key from its recovery-wrapped blob (recovery path). The
 *  result is extractable so it can be re-cached / re-wrapped for the user. */
export async function rsaUnwrapOceanKey(priv: CryptoKey, wrapped: Uint8Array): Promise<CryptoKey> {
  return subtle().unwrapKey(
    "raw",
    bs(wrapped),
    priv,
    { name: "RSA-OAEP" },
    { name: "AES-GCM", length: 256 },
    true,
    ["encrypt", "decrypt"],
  );
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

// base64 helpers for storing/transmitting opaque blobs.
export function toBase64(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

export function fromBase64(b64: string): Uint8Array {
  const s = atob(b64);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}
