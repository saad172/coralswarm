import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import * as identity from './clerk-identity.mjs';
import * as clerkClient from './clerk-client.mjs';

const NOW = 1_800_000_000_000;
const email = (owner, prefix = 'cs-e2e-') => `${prefix}${owner}-fixture-1234abcd+clerk_test@example.com`;
const user = (id, owner, extra = {}) => ({ id, createdAt: NOW - 4 * 60 * 60_000,
  emailAddresses: [{ emailAddress: email(owner) }], privateMetadata: {}, ...extra });

function fakeClerk(users) {
  const remaining = new Map(users.map(u => [u.id, u]));
  const deleted = []; const deletedOrgs = [];
  return { deleted, deletedOrgs, remaining,
    users: {
      async getUserList({ query, limit, offset }) { return { data: [...remaining.values()]
        .filter(u => u.emailAddresses.some(e => e.emailAddress.includes(query))).slice(offset, offset + limit) }; },
      async getUser(id) { if (!remaining.has(id)) throw Object.assign(new Error('missing'), { status: 404 }); return remaining.get(id); },
      async getOrganizationMembershipList({ userId }) { return { data: remaining.get(userId)?.memberships ?? [] }; },
      async deleteUser(id) { deleted.push(id); remaining.delete(id); },
    },
    organizations: {
      async getOrganization({ organizationId }) {
        return users.flatMap(u => u.memberships ?? []).find(m => m.organization.id === organizationId)?.organization;
      },
      async deleteOrganization(id) { deletedOrgs.push(id); },
    },
  };
}

function loadCleanup(clerk, { env = {}, now = NOW, imports = {} } = {}) {
  const source = readFileSync(new URL('./clerk-cleanup.mjs', import.meta.url), 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const exports = {}; const audit = []; const waits = []; let clock = now;
  const environment = { E2E_RUN_ID: 'ownrun', ...env };
  runInNewContext(outputText, { exports,
    process: { env: environment, argv: ['node', 'test'] },
    Date: { now: () => now }, console: { log: value => audit.push(JSON.parse(value)) },
    require(name) {
      if (name === '@clerk/backend') return { createClerkClient: () => clerk };
      if (name === 'node:os') return { default: { hostname: () => 'fakehost' } };
      if (name === './config.mjs') return { requireClerk: () => ({ secretKey: 'fake-never-sent' }) };
      if (name === './clerk-client.mjs') return { ...clerkClient, withRateLimitRetry: (client, options) =>
        clerkClient.withRateLimitRetry(client, { ...options, now: () => clock, sleep: async (ms) => { waits.push(ms); clock += ms; } }) };
      if (name === './clerk-identity.mjs') return { ...identity, testNamespace: () => identity.testNamespace(environment) };
      if (name in imports) return imports[name];
      throw new Error(`Unexpected import: ${name}`);
    },
  });
  return { ...exports, audit, waits };
}

const limited = { status: 429, retryAfter: 1, errors: [{ code: 'too_many_requests' }], message: 'Too Many Requests' };

test('finalization retries a rate-limited read instead of reading it as nothing to delete', async () => {
  const clerk = fakeClerk([user('user_own', 'ownrun')]);
  const read = clerk.users.getUser; let answered = false;
  clerk.users.getUser = async (id) => { if (!answered) { answered = true; throw limited; } return read.call(clerk.users, id); };
  const cleanup = loadCleanup(clerk);
  const result = await cleanup.deleteE2eUser('user_own');
  assert.equal(result.deleted, true); assert.equal(result.orgs, 0);
  assert.deepEqual(cleanup.waits, [1000]);
  assert.deepEqual(clerk.deleted, ['user_own']);
});

test('a rate limit that outlasts its retries fails cleanup rather than leaking the user', async () => {
  const clerk = fakeClerk([user('user_own', 'ownrun')]);
  clerk.users.getUser = async () => { throw { ...limited, retryAfter: 60 }; };
  const cleanup = loadCleanup(clerk);
  await assert.rejects(cleanup.cleanupE2eArtifacts(), (e) => e.status === 429);
  assert.deepEqual(cleanup.waits, [60_000, 60_000, 30_000]);
  assert.deepEqual(clerk.deleted, []);
});

test('a user somebody already finalized still reads as nothing to delete', async () => {
  const clerk = fakeClerk([]);
  const result = await loadCleanup(clerk).deleteE2eUser('user_gone');
  assert.equal(result.deleted, false); assert.equal(result.orgs, 0);
});

test('ordinary cleanup preserves another run’s four-hour-old active fixture', async () => {
  const clerk = fakeClerk([user('user_other', 'otherrun', {
    privateMetadata: identity.testUserMetadata({ namespace: 'e2e', owner: 'otherrun', lease: { id: 'live', expiresAt: NOW + 60_000 } }),
  })]);
  const cleanup = loadCleanup(clerk);
  await cleanup.cleanupE2eArtifacts();
  assert.deepEqual(clerk.deleted, []);
});

test('quota-style null-prefix cleanup must not delete foreign users for age', async () => {
  const clerk = fakeClerk([user('user_other', 'otherrun')]);
  const cleanup = loadCleanup(clerk);
  await cleanup.cleanupE2eArtifacts({ prefix: null, staleAfterMs: 30 * 60_000 });
  assert.deepEqual(clerk.deleted, []);
});

test('owner finalization deletes only owned users/orgs and audits IDs without emails or tokens', async () => {
  const own = user('user_own', 'ownrun', { memberships: [
    { organization: { id: 'org_own', name: 'E2E own', createdBy: 'user_own' } },
    { organization: { id: 'org_foreign', name: 'E2E foreign', createdBy: 'user_other' } },
    { organization: { id: 'org_prod', name: 'Customer', createdBy: 'user_own' } },
    { organization: { id: 'org_unknown', name: 'E2E unknown' } },
  ] });
  const clerk = fakeClerk([own, user('user_other', 'otherrun'), user('user_prefix_collision', 'ownrun2')]);
  const cleanup = loadCleanup(clerk);
  const result = await cleanup.cleanupE2eArtifacts();
  assert.equal(result.users, 1); assert.equal(result.orgs, 1);
  assert.deepEqual(clerk.deleted, ['user_own']);
  assert.deepEqual(clerk.deletedOrgs, ['org_own']);
  assert.deepEqual(cleanup.audit, [
    { event: 'e2e-cleanup-deleted', resource: 'organization', id: 'org_own', owner: 'ownrun', namespace: 'e2e', reason: 'owner-finalization' },
    { event: 'e2e-cleanup-deleted', resource: 'user', id: 'user_own', owner: 'ownrun', namespace: 'e2e', reason: 'owner-finalization' },
  ]);
  assert.doesNotMatch(JSON.stringify(cleanup.audit), /@|token|secret/i);
});

test('direct fixture finalization cannot delete a supplied foreign user ID', async () => {
  const clerk = fakeClerk([user('user_other', 'otherrun')]);
  await loadCleanup(clerk).deleteE2eUser('user_other', clerk);
  assert.deepEqual(clerk.deleted, []);
});

const leased = (id, owner, expiresAt, extra = {}) => user(id, owner, {
  privateMetadata: identity.testUserMetadata({ namespace: 'e2e', owner, lease: { id: 'lease-1', expiresAt } }), ...extra,
});

test('explicit maintenance reclaims positively dead owners and expired leases, failing closed otherwise', async () => {
  const clerk = fakeClerk([
    user('user_dead', 'deadrun'), leased('user_expired', 'expiredrun', NOW - 1),
    leased('user_live', 'liverun', NOW + 60_000), user('user_unknown', 'unknownrun'),
    user('user_badlease', 'badrun', { privateMetadata: { e2eOwnership: {
      version: 1, namespace: 'e2e', owner: 'badrun', lease: { id: 'bad', expiresAt: 'yesterday' },
    } } }),
    user('user_badowner', 'emailowner', { privateMetadata: identity.testUserMetadata({ namespace: 'e2e', owner: 'otherowner' }) }),
    user('user_mixed', 'deadrun', { emailAddresses: [{ emailAddress: email('deadrun') }, { emailAddress: 'human@customer.com' }] }),
  ]);
  const cleanup = loadCleanup(clerk);
  await cleanup.cleanupE2eArtifacts({ mode: 'abandoned', prefix: null,
    ownerStatus: async ({ owner }) => owner === 'deadrun' ? 'dead' : 'unknown' });
  assert.deepEqual(clerk.deleted, ['user_dead', 'user_expired']);
  assert.deepEqual(cleanup.audit.map(r => r.reason), ['verified-dead-owner', 'expired-owner-lease']);
});

test('verified live status and failed liveness lookup never authorize reclamation', async () => {
  const clerk = fakeClerk([leased('user_live', 'liverun', NOW - 1)]);
  const cleanup = loadCleanup(clerk);
  await cleanup.cleanupE2eArtifacts({ mode: 'abandoned', prefix: null, ownerStatus: async () => 'active' });
  await assert.rejects(cleanup.cleanupE2eArtifacts({ mode: 'abandoned', prefix: null,
    ownerStatus: async () => { throw new Error('liveness unavailable'); } }), /liveness unavailable/);
  assert.deepEqual(clerk.deleted, []);
});

test('quota reclamation is owner-scoped and never finalizes active users', async () => {
  const clerk = fakeClerk([leased('user_foreign', 'otherrun', NOW - 1),
    leased('user_active', 'ownrun', NOW + 60_000), leased('user_expired', 'ownrun', NOW - 1)]);
  await loadCleanup(clerk).cleanupE2eArtifacts({ mode: 'abandoned', prefix: 'ownrun-' });
  assert.deepEqual(clerk.deleted, ['user_expired']);
});

test('maintenance re-reads lease after discovery before any delete', async () => {
  const expired = leased('user_renewed', 'otherrun', NOW - 1);
  const clerk = fakeClerk([expired]);
  clerk.users.getUser = async () => leased('user_renewed', 'otherrun', NOW + 60_000);
  await loadCleanup(clerk).cleanupE2eArtifacts({ mode: 'abandoned', prefix: null });
  assert.deepEqual(clerk.deleted, []);
});

test('lease renewal during membership traversal preserves both user and organization', async () => {
  const expired = leased('user_renewed', 'otherrun', NOW - 1, { memberships: [
    { organization: { id: 'org_renewed', name: 'E2E renewal', createdBy: 'user_renewed' } },
  ] });
  const clerk = fakeClerk([expired]);
  clerk.users.getOrganizationMembershipList = async () => {
    clerk.remaining.set(expired.id, leased(expired.id, 'otherrun', NOW + 60_000));
    return { data: expired.memberships };
  };
  await loadCleanup(clerk).cleanupE2eArtifacts({ mode: 'abandoned', prefix: null });
  assert.deepEqual(clerk.deleted, []);
  assert.deepEqual(clerk.deletedOrgs, []);
});

test('failed deletes produce neither success counts nor success audit receipts', async () => {
  const clerk = fakeClerk([user('user_own', 'ownrun')]);
  clerk.users.deleteUser = async () => { throw new Error('fake API failure'); };
  const cleanup = loadCleanup(clerk);
  const result = await cleanup.cleanupE2eArtifacts();
  assert.equal(result.users, 0);
  assert.deepEqual(cleanup.audit, []);
});

test('explicit CI run-attempt cleanup covers its shards, q and n1, but no other run or broad prefix', async () => {
  const owners = ['gh123x1s1', 'gh123x1s2', 'gh123x1sq', 'gh123x1sn1', 'gh123x10s1', 'gh123x1sneighbour'];
  const clerk = fakeClerk(owners.map((owner, i) => user(`user_${i}`, owner)));
  const cleanup = loadCleanup(clerk);
  await cleanup.cleanupE2eArtifacts({ prefix: 'gh123x1s' });
  assert.deepEqual(clerk.deleted, ['user_0', 'user_1', 'user_2', 'user_3']);
  for (const prefix of ['', '*', 'cs-e2e-', 'ownrun--', 'ownrun/']) {
    await assert.rejects(cleanup.cleanupE2eArtifacts({ prefix }), /owner/);
  }
  await assert.rejects(cleanup.cleanupE2eArtifacts({ namespace: 'production' }), /namespace/);
});

test('pagination does not lose retained foreign users or skip owned users after deletions', async () => {
  const users = Array.from({ length: 215 }, (_, i) => user(`user_${i}`, i % 2 ? 'ownrun' : 'otherrun'));
  const clerk = fakeClerk(users);
  await loadCleanup(clerk).cleanupE2eArtifacts();
  assert.equal(clerk.deleted.length, 107);
  assert.equal(clerk.remaining.size, 108);
});

test('nightly namespace is invisible to the legacy age-only sweeper, yet has exact-owner finalization', async () => {
  const env = { E2E_IDENTITY_NAMESPACE: 'nightly', E2E_RUN_ID: 'nightlyrun',
    E2E_OWNER_LEASE_ID: 'lease-1', E2E_OWNER_LEASE_EXPIRES_AT: String(NOW + 60_000) };
  const configured = identity.loadTestIdentity('nightlyrun', env, NOW);
  const address = identity.testIdentityEmail(configured, 'admin', '1234abcd');
  // This is the deployed legacy sweeper's actual recognition contract.
  const legacyMatch = address.startsWith('cs-e2e-') && address.endsWith('+clerk_test@example.com');
  assert.equal(legacyMatch, false);
  assert.equal(address.startsWith('cs-nightly-nightlyrun-'), true);
  const own = user('user_nightly', 'nightlyrun', { emailAddresses: [{ emailAddress: address }],
    privateMetadata: identity.testUserMetadata(configured) });
  const foreign = user('user_nightly_other', 'other', { emailAddresses: [{ emailAddress: email('other', 'cs-nightly-') }] });
  const clerk = fakeClerk([own, foreign, user('user_ordinary', 'nightlyrun')]);
  const cleanup = loadCleanup(clerk, { env });
  assert.equal(cleanup.E2E_EMAIL_PREFIX, 'cs-nightly-');
  await cleanup.cleanupE2eArtifacts();
  assert.deepEqual(clerk.deleted, ['user_nightly']);
});

test('identity loader validates allowlist, explicit nightly owner, lease and Clerk test-address limits', () => {
  const ordinary = identity.loadTestIdentity('run1', {}, NOW);
  assert.equal(identity.testIdentityEmail(ordinary, 'admin', '1234abcd'), email('run1').replace('fixture', 'admin'));
  const long = identity.testIdentityEmail(ordinary, 'a'.repeat(100), '1234abcd');
  assert.equal(long.split('@')[0].length, 64);
  assert.deepEqual(identity.identityFromTestEmail(long), { namespace: 'e2e', owner: 'run1' });
  assert.throws(() => identity.testIdentityEmail({ ...ordinary, owner: 'x'.repeat(64) }, '', '1234abcd'), /local-part/);
  assert.throws(() => identity.loadTestIdentity('run1', { E2E_IDENTITY_NAMESPACE: 'cs-prod-' }, NOW), /namespace|NAMESPACE/);
  assert.throws(() => identity.loadTestIdentity('run1', { E2E_IDENTITY_NAMESPACE: 'nightly' }, NOW), /explicit/);
  assert.throws(() => identity.loadTestIdentity('run1', { E2E_IDENTITY_NAMESPACE: 'nightly', E2E_RUN_ID: 'run1' }, NOW), /lease/);
  assert.throws(() => identity.loadTestIdentity('run1', { E2E_OWNER_LEASE_ID: 'lease-1', E2E_OWNER_LEASE_EXPIRES_AT: String(NOW) }, NOW), /lease/);
  assert.equal(identity.identityFromTestEmail('cs-e2e-ownrun-fixture-1234abcd+clerk_test@customer.com'), null);
  assert.equal(identity.identityFromTestEmail('person@example.com'), null);
});
