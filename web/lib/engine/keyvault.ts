// Private-ocean key custody (ADR-0007 D5), device-trust model:
//   • The key is generated on-device and wrapped under a passphrase-derived KEK
//     into an opaque BLOB. The blob is stored on the server (server-blind) so a
//     NEW device can fetch it and enroll by entering the passphrase once.
//   • After setup/unlock, the unwrapped key is cached on THIS device (IndexedDB),
//     so subsequent sessions auto-unlock with no passphrase. The passphrase is
//     only needed to enroll a new device (or after a reset).
//
// Tradeoff: the device cache stores the usable key locally — anyone with access
// to this device (or via XSS) could read the private ocean. That's the explicit
// "don't re-prompt per session" choice; the passphrase is a device-enrollment
// secret, not a per-session lock.

import {
  generateOceanKey,
  deriveKek,
  wrapKey,
  unwrapKey,
  exportRawKey,
  importOceanKey,
  randomBytes,
  toBase64,
  fromBase64,
} from "./crypto";

export interface KV {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  del(key: string): Promise<void>;
}

const DEVICE_KEY = "cs_device_ocean_key";

interface Blob {
  salt: string; // base64 PBKDF2 salt
  wrapped: string; // base64 [iv | wrapped-ocean-key]
}

/** Generate a fresh ocean key and wrap it under the passphrase. Returns the key
 *  (cache it on this device) and the opaque blob (store on the server). */
export async function createVault(passphrase: string): Promise<{ key: CryptoKey; blob: string }> {
  const salt = randomBytes(16);
  const kek = await deriveKek(passphrase, salt);
  const key = await generateOceanKey();
  const wrapped = await wrapKey(kek, key);
  const blob: Blob = { salt: toBase64(salt), wrapped: toBase64(wrapped) };
  return { key, blob: JSON.stringify(blob) };
}

/** Wrap an EXISTING (extractable) ocean key under a passphrase into the same
 *  opaque blob shape `createVault` produces. Used by org recovery: an admin
 *  re-wraps a member's recovered key under a fresh recovery passphrase, and the
 *  member unlocks it on their device with `unwrapBlob` (new-device enrollment). */
export async function wrapKeyToBlob(passphrase: string, key: CryptoKey): Promise<string> {
  const salt = randomBytes(16);
  const kek = await deriveKek(passphrase, salt);
  const wrapped = await wrapKey(kek, key);
  const blob: Blob = { salt: toBase64(salt), wrapped: toBase64(wrapped) };
  return JSON.stringify(blob);
}

/** Unwrap a server-stored blob with the passphrase (new-device enrollment).
 *  Throws on a wrong passphrase (AES-GCM auth failure). */
export async function unwrapBlob(passphrase: string, blob: string): Promise<CryptoKey> {
  const { salt, wrapped } = JSON.parse(blob) as Blob;
  const kek = await deriveKek(passphrase, fromBase64(salt));
  return unwrapKey(kek, fromBase64(wrapped));
}

/** Cache the unlocked key on this device so future sessions skip the passphrase. */
export async function cacheDeviceKey(kv: KV, key: CryptoKey): Promise<void> {
  await kv.set(DEVICE_KEY, toBase64(await exportRawKey(key)));
}

/** Load the device-cached key, or null if this device isn't enrolled yet. */
export async function loadDeviceKey(kv: KV): Promise<CryptoKey | null> {
  const b = await kv.get(DEVICE_KEY);
  if (!b) return null;
  return importOceanKey(fromBase64(b));
}

/** Forget this device's cached key (sign-out / reset). */
export async function clearDevice(kv: KV): Promise<void> {
  await kv.del(DEVICE_KEY);
}
