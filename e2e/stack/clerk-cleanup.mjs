#!/usr/bin/env node
// Shared Clerk DEV instance: ordinary setup/teardown finalizes only its owner.
// Creation age NEVER proves abandonment. Maintenance must explicitly opt in
// and establish a dead owner or an expired, recorded lease; unknown is kept.
import { createClerkClient } from '@clerk/backend';
import os from 'node:os';
import { requireClerk } from './config.mjs';
import { withRateLimitRetry } from './clerk-client.mjs';
import { TEST_NAMESPACES, testNamespace, identityFromTestUser, ownerSelector } from './clerk-identity.mjs';

export { E2E_EMAIL_DOMAIN } from './clerk-identity.mjs';
export const E2E_EMAIL_PREFIX = TEST_NAMESPACES[testNamespace()];

/** Fixed before workers spawn. Ordinary identities retain generated IDs;
 * a long/retained nightly requires an explicit owner. */
export function runId() {
  if (!process.env.E2E_RUN_ID) {
    if (testNamespace() === 'nightly') throw new Error('nightly identities require explicit E2E_RUN_ID');
    process.env.E2E_RUN_ID = process.env.GITHUB_RUN_ID
      ? `gh${process.env.GITHUB_RUN_ID}x${process.env.GITHUB_RUN_ATTEMPT ?? 1}`
      : `l${os.hostname().slice(0, 8)}${process.pid.toString(36)}${Date.now().toString(36)}`;
  }
  const id = process.env.E2E_RUN_ID.replace(/[^a-z0-9]/gi, '').toLowerCase();
  if (!id) throw new Error('empty test run ID');
  return id;
}

const client = () => withRateLimitRetry(createClerkClient({ secretKey: requireClerk().secretKey }));
/** On a read, a 404 is a resource somebody already finalized. Anything else,
 * such as a 429 that outlasted its retries, surfaces, never as nothing to delete. */
const absent = (e) => { if (e?.status === 404) return null; throw e; };
const auditDeletion = (record) => console.log(JSON.stringify({ event: 'e2e-cleanup-deleted', ...record }));

/** @typedef {import('./clerk-identity.mjs').TestIdentity} TestIdentity */
/** @typedef {{ prefix?: string | null; namespace?: import('./clerk-identity.mjs').TestNamespace;
 * mode?: 'owner' | 'abandoned'; staleAfterMs?: number;
 * ownerStatus?: (identity: TestIdentity) => Promise<'dead' | 'active' | 'unknown'>;
 * now?: () => number; clerk?: any; audit?: (record: any) => void }} CleanupOptions */

/** prefix:null grants no ordinary authority. Explicit maintenance may scan
 * foreign owners. ownerStatus must use positive liveness evidence, e.g. a
 * verified terminal CI run, NEVER creation age. Live status overrides expiry.
 * staleAfterMs is accepted for old callers but grants no deletion authority.
 * @param {CleanupOptions} options */
function cleanupPolicy(options) {
  const namespace = options.namespace ?? testNamespace();
  if (namespace !== 'e2e' && namespace !== 'nightly') throw new Error('unsupported cleanup namespace');
  const mode = options.mode ?? 'owner';
  if (mode !== 'owner' && mode !== 'abandoned') throw new Error('unsupported cleanup mode');
  const prefix = options.prefix === undefined ? `${runId()}-` : options.prefix;
  const select = mode === 'abandoned' && prefix === null ? () => true : ownerSelector(prefix);
  return { namespace, async reason(user) {
    const identity = identityFromTestUser(user);
    if (!identity || identity.namespace !== namespace || !select(identity.owner)) return null;
    if (mode === 'owner') return { identity, reason: 'owner-finalization' };
    const status = await options.ownerStatus?.(identity) ?? 'unknown';
    if (status === 'active') return null;
    if (status === 'dead') return { identity, reason: 'verified-dead-owner' };
    if (status !== 'unknown') return null;
    if (identity.lease && identity.lease.expiresAt <= (options.now ?? Date.now)()) return { identity, reason: 'expired-owner-lease' };
    return null;
  } };
}

/** Re-read immediately before deleting. Membership in an E2E-named org does
 * not prove ownership: its creator must satisfy the same owner/cleanup policy.
 * @param {string} userId @param {any} clerk @param {ReturnType<typeof cleanupPolicy>} policy
 * @param {(record: any) => void} audit */
async function deleteEligibleUser(userId, clerk, policy, audit) {
  const user = await clerk.users.getUser(userId).catch(absent);
  const decision = user && await policy.reason(user);
  if (!decision) return { deleted: false, orgs: 0 };
  const { identity } = decision;
  let orgs = 0;
  const memberships = await clerk.users.getOrganizationMembershipList({ userId }).catch((e) => absent(e) ?? { data: [] });
  for (const membership of memberships.data) {
    let org = membership.organization;
    if (!org?.name?.startsWith('E2E ')) continue;
    if (!org.createdBy) org = await clerk.organizations.getOrganization({ organizationId: org.id }).catch(absent);
    if (!org?.createdBy) continue;
    const currentUser = await clerk.users.getUser(userId).catch(absent);
    const currentDecision = currentUser && await policy.reason(currentUser);
    if (!currentDecision || currentDecision.identity.owner !== identity.owner) break;
    const creator = org.createdBy === userId ? currentUser : await clerk.users.getUser(org.createdBy).catch(absent);
    const owner = creator && await policy.reason(creator);
    if (!owner || owner.identity.owner !== identity.owner || owner.identity.namespace !== identity.namespace) continue;
    const deleted = await clerk.organizations.deleteOrganization(org.id).then(() => true, () => false);
    if (deleted) { orgs++; audit({ resource: 'organization', id: org.id, owner: identity.owner, namespace: identity.namespace, reason: currentDecision.reason }); }
  }
  // A long membership traversal must not turn a renewed lease into deletion.
  const current = await clerk.users.getUser(userId).catch(absent);
  const stillEligible = current && await policy.reason(current);
  if (!stillEligible || stillEligible.identity.owner !== identity.owner) return { deleted: false, orgs };
  const deleted = await clerk.users.deleteUser(userId).then(() => true, () => false);
  if (deleted) audit({ resource: 'user', id: userId, owner: identity.owner, namespace: identity.namespace, reason: stillEligible.reason });
  return { deleted, orgs };
}

/** Finalize a known user only if owned by this run. A caller-supplied ID,
 * E2E org name or creation age never overrides that check.
 * @param {string} userId @param {any} clerk */
export async function deleteE2eUser(userId, clerk = client()) {
  return deleteEligibleUser(userId, clerk, cleanupPolicy({}), auditDeletion);
}

/** Owner finalization by default; explicit abandoned-owner maintenance uses
 * this same entry point. No destructive tests need a real Clerk client.
 * @param {CleanupOptions} [options] */
export async function cleanupE2eArtifacts(options = {}) {
  const policy = cleanupPolicy(options);
  const clerk = options.clerk ?? client();
  let users = 0; let orgs = 0; let offset = 0;
  for (;;) {
    const { data } = await clerk.users.getUserList({ query: TEST_NAMESPACES[policy.namespace], limit: 100, offset });
    if (!data.length) break;
    let kept = 0;
    for (const user of data) {
      if (!await policy.reason(user)) { kept++; continue; }
      const result = await deleteEligibleUser(user.id, clerk, policy, options.audit ?? auditDeletion);
      orgs += result.orgs;
      if (result.deleted) users++; else kept++;
    }
    if (data.length < 100) break;
    offset += kept;
  }
  return { users, orgs };
}

if (process.argv[1]?.endsWith('clerk-cleanup.mjs')) {
  const prefix = process.env.E2E_CLEANUP_PREFIX;
  cleanupE2eArtifacts(prefix === undefined ? {} : { prefix }).then(
    ({ users, orgs }) => console.log(`[e2e] removed ${users} Clerk test user(s) and ${orgs} org(s) (owner-scoped)`),
    () => { console.error('Clerk test cleanup failed; inspect owned deletion receipts'); process.exit(1); },
  );
}
