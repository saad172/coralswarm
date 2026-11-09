import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
import * as identity from '../stack/clerk-identity.mjs';
import * as clerkClient from '../stack/clerk-client.mjs';

// Execute the real createUser/quota call site. Every SDK, timer and cleanup
// effect is fake; importing this test cannot mint/delete a real Clerk user.
function loadClerk(env = {}, { answers = [{ errors: [{ code: 'user_quota_exceeded' }] }] } = {}) {
  const now = 1_800_000_000_000;
  const environment = { E2E_RUN_ID: 'ownrun', ...env };
  const cleanupCalls = []; const creates = []; const waits = []; const extended = []; let clock = now;
  // The first create answers `answers` in order, then succeeds.
  const fake = { users: { async createUser(options) {
    creates.push(options);
    const answer = answers[creates.length - 1];
    if (answer) throw answer;
    return { id: 'user_owned', ...options };
  } } };
  const { outputText } = ts.transpileModule(readFileSync(new URL('./clerk.ts', import.meta.url), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const exports = {};
  runInNewContext(outputText, { exports, process: { env: environment }, Date: { now: () => now }, Buffer,
    require(name) {
      if (name === '@clerk/backend') return { createClerkClient: () => fake };
      if (name === '@playwright/test') return { test: { info: () => ({ timeout: 1000, setTimeout(ms) { extended.push(ms); } }) } };
      if (name === 'node:crypto') return { randomUUID: () => '1234abcd-0000-0000-0000-000000000000' };
      if (name === 'node:timers/promises') return { setTimeout: async () => {} };
      if (name === '../stack/config.mjs') return { requireClerk: () => ({ secretKey: 'fake-never-sent' }) };
      if (name === '../stack/clerk-client.mjs') return { ...clerkClient, withRateLimitRetry: (client, options) =>
        clerkClient.withRateLimitRetry(client, { ...options, now: () => clock, sleep: async (ms) => { waits.push(ms); clock += ms; } }) };
      if (name === '../stack/clerk-cleanup.mjs') return { runId: () => 'ownrun', deleteE2eUser: async () => {},
        cleanupE2eArtifacts: async options => { cleanupCalls.push(options); return { users: 0, orgs: 0 }; } };
      if (name === '../stack/clerk-identity.mjs') return { ...identity,
        loadTestIdentity: owner => identity.loadTestIdentity(owner, environment, now) };
      throw new Error(`Unexpected import: ${name}`);
    },
  });
  return { ...exports, cleanupCalls, creates, waits, extended };
}

test('a rate-limited create waits out Clerk\'s Retry-After, stretches the scenario, and never reclaims quota', async () => {
  const clerk = loadClerk({}, { answers: [{ status: 429, retryAfter: 2, errors: [{ code: 'too_many_requests' }] }] });
  const created = await clerk.createUser('limited');
  assert.equal(created.id, 'user_owned');
  assert.equal(clerk.creates.length, 2);
  assert.deepEqual(clerk.waits, [2000]);
  assert.deepEqual(clerk.extended, [3000]);
  assert.deepEqual(clerk.cleanupCalls, []);
});

test('actual quota retry requests only abandoned resources of this owner, never a global age sweep', async () => {
  const clerk = loadClerk();
  await clerk.createUser('quota');
  assert.deepEqual(JSON.parse(JSON.stringify(clerk.cleanupCalls)), [{ prefix: 'ownrun-', mode: 'abandoned' }]);
  assert.equal(clerk.creates.length, 2);
  assert.equal(clerk.creates[1].privateMetadata.e2eOwnership.owner, 'ownrun');
});

test('actual user creation preserves Clerk test addressing and attaches nightly owner/lease metadata', async () => {
  const clerk = loadClerk({ E2E_IDENTITY_NAMESPACE: 'nightly', E2E_OWNER_LEASE_ID: 'lease-owned',
    E2E_OWNER_LEASE_EXPIRES_AT: '1800003600000' });
  const created = await clerk.createUser('admin');
  assert.equal(created.emailAddress[0], 'cs-nightly-ownrun-admin-1234abcd+clerk_test@example.com');
  assert.equal(created.privateMetadata.e2e, true);
  assert.deepEqual(JSON.parse(JSON.stringify(created.privateMetadata.e2eOwnership)), {
    version: 1, namespace: 'nightly', owner: 'ownrun', lease: { id: 'lease-owned', expiresAt: 1_800_003_600_000 },
  });
});

test('invalid/expired nightly lease fails before any SDK create or cleanup call', async () => {
  const clerk = loadClerk({ E2E_IDENTITY_NAMESPACE: 'nightly', E2E_OWNER_LEASE_ID: 'lease-owned',
    E2E_OWNER_LEASE_EXPIRES_AT: '1800000000000' });
  await assert.rejects(clerk.createUser('admin'), /lease/);
  assert.deepEqual(clerk.creates, []);
  assert.deepEqual(clerk.cleanupCalls, []);
});
