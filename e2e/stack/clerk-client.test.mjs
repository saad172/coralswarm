import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isRateLimited, rateLimitBackoffMs, withRateLimitRetry } from './clerk-client.mjs';

const limited = (retryAfter) => ({ status: 429, retryAfter, errors: [{ code: 'too_many_requests' }], message: 'Too Many Requests' });

/** A client whose `users.getUser` answers `answers` in order, then echoes the id; every wait is recorded instead of slept. */
function harness(answers, options = {}) {
  const waits = []; let clock = 1_800_000_000_000; const calls = [];
  class Users {
    constructor() { this.request = 'bound'; }
    async getUser(id) {
      calls.push({ self: this, id });
      const answer = answers.shift();
      if (answer) throw answer;
      return { id, via: this.request };
    }
  }
  const client = { users: new Users(), telemetry: { record() {} } };
  const retrying = withRateLimitRetry(client, { ...options, now: () => clock, sleep: async (ms) => { waits.push(ms); clock += ms; } });
  return { client, retrying, waits, calls };
}

test('a 429 is retried after the Retry-After Clerk sent, on the real API instance', async () => {
  const h = harness([limited(2)]);
  assert.deepEqual(await h.retrying.users.getUser('user_1'), { id: 'user_1', via: 'bound' });
  assert.deepEqual(h.waits, [2000]);
  assert.equal(h.calls.length, 2);
  assert.ok(h.calls.every((c) => c.self === h.client.users), 'methods keep their API instance as this');
});

test('without Retry-After the wait doubles from 1 s, capped at 10 s, with jitter', () => {
  assert.equal(rateLimitBackoffMs(limited(3), 0), 3000);
  assert.equal(rateLimitBackoffMs(limited(undefined), 0, () => 0), 500);
  assert.equal(rateLimitBackoffMs(limited(undefined), 0, () => 1), 1000);
  assert.equal(rateLimitBackoffMs(limited(undefined), 1, () => 0), 1000);
  assert.equal(rateLimitBackoffMs(limited(undefined), 10, () => 1), 10_000);
});

test('the total wait is bounded, then the 429 itself surfaces', async () => {
  const h = harness([limited(60), limited(60), limited(60), limited(60)], { waitMs: 150_000 });
  await assert.rejects(h.retrying.users.getUser('user_1'), (e) => e.status === 429);
  assert.deepEqual(h.waits, [60_000, 60_000, 30_000]);
  assert.equal(h.calls.length, 4);
});

test('every other error surfaces at once, so createUser still sees a full instance', async () => {
  const quota = { status: 403, errors: [{ code: 'user_quota_exceeded' }] };
  const h = harness([quota]);
  await assert.rejects(h.retrying.users.getUser('user_1'), (e) => e === quota);
  assert.deepEqual(h.waits, []);
  assert.equal(isRateLimited(quota), false);
  assert.equal(isRateLimited({ errors: [{ code: 'too_many_requests' }] }), true);
});

test('each wait is reported before it happens, so a scenario can stretch its timeout', async () => {
  const reported = [];
  const h = harness([limited(1), limited(4)], { onWait: (ms) => reported.push(ms) });
  await h.retrying.users.getUser('user_1');
  assert.deepEqual(reported, [1000, 4000]);
  assert.deepEqual(h.waits, reported);
});
