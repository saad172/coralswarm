import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadTestIdentity, testIdentityEmail } from '../stack/clerk-identity.mjs';
import { adoptInvitedParticipant } from './fixture-identity.mjs';
const owner = 'ownedrun';
const env = { E2E_IDENTITY_NAMESPACE: 'nightly', E2E_RUN_ID: owner, E2E_OWNER_LEASE_ID: 'lease-owned', E2E_OWNER_LEASE_EXPIRES_AT: String(Date.now() + 3_600_000) };
const identity = loadTestIdentity(owner, env);
const email = testIdentityEmail(identity, 'member', '1234abcd');
const user = { id: 'user_fixture', emailAddresses: [{ emailAddress: email }], privateMetadata: {} };
test('actual invite adoption validates owner/namespace/address before its sole fake metadata update', async () => {
  const calls = [];
  assert.equal((await adoptInvitedParticipant({ identity, email, user, updateMetadata: async (...args) => calls.push(args) })).lease_attached, true);
  assert.deepEqual(calls, [[user.id, { privateMetadata: { e2e: true, e2eOwnership: { version: 1, ...identity } } }]]);
});
test('foreign/legacy/mixed/metadata-mismatched invite never invokes metadata update', async () => {
  let calls = 0;
  for (const invalid of [ { ...user, emailAddresses: [{ emailAddress: email.replace('nightly', 'e2e') }] },
    { ...user, emailAddresses: [...user.emailAddresses, { emailAddress: 'human@example.com' }] },
    { ...user, privateMetadata: { e2eOwnership: { version: 1, namespace: 'nightly', owner: 'foreign' } } } ]) {
    await assert.rejects(adoptInvitedParticipant({ identity, email, user: invalid, updateMetadata: async () => calls++ }), /mismatch/);
  }
  assert.equal(calls, 0);
});
test('lease expiration across cadence/setup refuses continuation before metadata use', async () => {
  const expired = Number(env.E2E_OWNER_LEASE_EXPIRES_AT);
  assert.throws(() => loadTestIdentity(owner, env, expired), /lease/);
  let calls = 0;
  await assert.rejects(adoptInvitedParticipant({ identity: { ...identity, lease: { ...identity.lease, expiresAt: Date.now() - 1 } }, email, user, updateMetadata: async () => calls++ }), /lease/);
  assert.equal(calls, 0);
});
