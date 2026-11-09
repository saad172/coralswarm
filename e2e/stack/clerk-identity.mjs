// Test identities only. The separate nightly prefix also fences OLD shared
// sweepers, which know only cs-e2e- and cannot understand lease metadata.
export const TEST_NAMESPACES = Object.freeze({ e2e: 'cs-e2e-', nightly: 'cs-nightly-' });
export const E2E_EMAIL_DOMAIN = 'example.com';
/** @typedef {'e2e' | 'nightly'} TestNamespace */
/** @typedef {{ id: string; expiresAt: number }} OwnerLease */
/** @typedef {{ namespace: TestNamespace; owner: string; lease?: OwnerLease }} TestIdentity */

export function testNamespace(env = process.env) {
  const value = env.E2E_IDENTITY_NAMESPACE ?? 'e2e';
  if (value !== 'e2e' && value !== 'nightly') throw new Error('E2E_IDENTITY_NAMESPACE must be e2e or nightly');
  return value;
}

/** @param {string} owner */
export function validateOwner(owner) {
  if (!/^[a-z0-9]+$/.test(owner)) throw new Error('test owner must be a nonempty lowercase alphanumeric run ID');
  return owner;
}

/** @param {unknown} value @returns {value is OwnerLease} */
export function validLease(value) {
  if (!value || typeof value !== 'object') return false;
  const lease = /** @type {OwnerLease} */ (value);
  return typeof lease.id === 'string' && /^[a-zA-Z0-9-]{1,80}$/.test(lease.id)
    && Number.isSafeInteger(lease.expiresAt) && lease.expiresAt > 0;
}

/** A lease is a use deadline, not user age. Never resume an expired lease:
 * renew ALL owned users before expiry or use a new owner/fixture. Retained
 * fixtures must retain/renew their lease too. No API calls occur here.
 * @param {string} owner @returns {TestIdentity} */
export function loadTestIdentity(owner, env = process.env, now = Date.now()) {
  const namespace = testNamespace(env);
  validateOwner(owner);
  if (namespace === 'nightly' && !env.E2E_RUN_ID) throw new Error('nightly identities require explicit E2E_RUN_ID');
  const hasLease = env.E2E_OWNER_LEASE_ID !== undefined || env.E2E_OWNER_LEASE_EXPIRES_AT !== undefined;
  const lease = hasLease ? { id: env.E2E_OWNER_LEASE_ID ?? '', expiresAt: Number(env.E2E_OWNER_LEASE_EXPIRES_AT) } : undefined;
  if ((namespace === 'nightly' || hasLease) && (!validLease(lease) || lease.expiresAt <= now)) {
    throw new Error('test identity requires a valid unexpired owner lease');
  }
  return { namespace, owner, ...(lease ? { lease } : {}) };
}

/** Same Clerk test-address suffix and 64-character local-part limit.
 * @param {TestIdentity} identity @param {string} label @param {string} id */
export function testIdentityEmail(identity, label, id) {
  const { namespace, owner } = identity;
  validateOwner(owner);
  if (!Object.hasOwn(TEST_NAMESPACES, namespace) || !/^[a-f0-9]{8}$/.test(id) || !/^[a-zA-Z0-9._-]*$/.test(label)) {
    throw new Error('invalid test identity namespace, label or random suffix');
  }
  const base = `${TEST_NAMESPACES[namespace]}${owner}--${id}+clerk_test`;
  const room = 64 - base.length;
  if (room < 0) throw new Error('test run ID exceeds the Clerk email local-part limit');
  return `${TEST_NAMESPACES[namespace]}${owner}-${label.slice(0, room)}-${id}+clerk_test@${E2E_EMAIL_DOMAIN}`;
}

/** Attach to every fixture user, including users created through an invite.
 * Namespace/owner must agree with the actual test email.
 * @param {TestIdentity} identity */
export function testUserMetadata(identity) {
  return { e2e: true, e2eOwnership: { version: 1, ...identity } };
}

/** @param {string} email @returns {TestIdentity | null} */
export function identityFromTestEmail(email) {
  if (typeof email !== 'string' || email.split('@')[0].length > 64) return null;
  const match = /^(cs-e2e-|cs-nightly-)([a-z0-9]+)-[a-zA-Z0-9._-]*-[a-f0-9]{8}\+clerk_test@example\.com$/.exec(email);
  return match ? { namespace: match[1] === TEST_NAMESPACES.nightly ? 'nightly' : 'e2e', owner: match[2] } : null;
}

/** Mixed emails or metadata/namespace/owner disagreement fail closed.
 * Legacy users may be finalized by their exact owner, but missing metadata
 * cannot supply an expired lease for maintenance reclamation.
 * @param {any} user @returns {TestIdentity | null} */
export function identityFromTestUser(user) {
  const identities = (user.emailAddresses ?? []).map((e) => identityFromTestEmail(e.emailAddress));
  const identity = identities[0];
  if (!identity || identities.some((other) => !other || other.owner !== identity.owner || other.namespace !== identity.namespace)) return null;
  const metadata = user.privateMetadata?.e2eOwnership;
  if (metadata !== undefined && (metadata?.version !== 1 || metadata.owner !== identity.owner || metadata.namespace !== identity.namespace)) return null;
  if (metadata?.lease !== undefined && !validLease(metadata.lease)) return null;
  return { ...identity, ...(metadata?.lease ? { lease: metadata.lease } : {}) };
}

/** Exact owner or the existing CI run-attempt shard family, never a free-form
 * prefix. A CI family contains only numeric shards/q/n1.
 * @param {string | null} prefix */
export function ownerSelector(prefix) {
  if (prefix === null) return () => false;
  const owner = validateOwner(prefix.endsWith('-') ? prefix.slice(0, -1) : prefix);
  if (/^gh\d+x\d+s$/.test(owner)) {
    const pattern = new RegExp(`^${owner}(?:[1-9]\\d*|q|n1)$`);
    return (candidate) => candidate === owner || pattern.test(candidate);
  }
  return (candidate) => candidate === owner;
}
