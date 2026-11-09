import { randomUUID } from 'node:crypto';
import { test, expect } from '../lib/fixtures';
import { deleteE2eUser } from '../lib/clerk';
import { fixtureSql } from '../lib/pageBudgetFixture';
import { CONTROL_DB, REMOTE } from '../stack/config.mjs';

// Like the release smoke, this exercises the unaccelerated 60 s scheduler
// and real managed worker. Only provider HTTP is fake; no extraction seed,
// manual admission, shortened interval, or direct worker invocation.
test('standing admits a fresh API capture despite more than four batches of idle accounts', async ({ makeUser }) => {
  test.skip(REMOTE, 'idle inventory belongs only to an isolated local stack');
  test.setTimeout(180_000);
  const who = await makeUser('standing-scan');
  const fixtureTag = randomUUID();
  const accounts = Array.from({ length: 106 }, () => `00000001-${randomUUID().slice(9)}`);
  const ids = accounts.map((id) => `'${id}'`).join(',');
  try {
    // Explicit synthetic inventory, owned by this scenario. Low UUIDs put
    // the real user's account after >4 batches. No additional Clerk users.
    fixtureSql(CONTROL_DB, `BEGIN; INSERT INTO users(id,email,clerk_user_id) SELECT id,id::text||'@fixture.local','standing-${fixtureTag}-'||id::text FROM unnest(ARRAY[${ids}]::uuid[]) id; INSERT INTO accounts(id,owner_user_id) SELECT id,id FROM unnest(ARRAY[${ids}]::uuid[]) id; COMMIT;`);
    const graph = await who.api.personalGraph();
    const source = await who.api.ingest(graph.id, 'The standing scan pilot starts on April 9 in Lisbon.');
    await who.api.waitForExtraction(graph.id, source, 140_000);
    const items = await who.api.json('GET', `/v1/sources/items?graph_id=${graph.id}&source_type=note`);
    const item = items.items.find((row: { source_id: string }) => row.source_id === source);
    expect(item.extraction_status).toBe('done');
  } finally {
    try {
      fixtureSql(CONTROL_DB, `DO $$ DECLARE owned uuid; BEGIN FOR owned IN SELECT a.id FROM accounts a JOIN users u ON u.id=a.owner_user_id WHERE a.id=ANY(ARRAY[${ids}]::uuid[]) AND a.owner_user_id=a.id AND u.clerk_user_id LIKE 'standing-${fixtureTag}-%' LOOP PERFORM set_config('app.account_id',owned::text,true); PERFORM billing_enter_account(owned); DELETE FROM accounts WHERE id=owned; END LOOP; DELETE FROM users WHERE id=ANY(ARRAY[${ids}]::uuid[]) AND clerk_user_id LIKE 'standing-${fixtureTag}-%'; END $$;`);
    } finally {
      await deleteE2eUser(who.user.id);
    }
  }
});
