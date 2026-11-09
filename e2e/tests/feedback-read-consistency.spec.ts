import { spawn } from 'node:child_process';
import { expect, test } from '../lib/fixtures';
import { fixtureSql } from '../lib/pageBudgetFixture';
import { CONTROL_DB, CONTAINERS } from '../stack/config.mjs';

test('saved feedback remains current while the conversation worker is held', async ({ makeUser }) => {
  const owner = await makeUser('feedback-consistency');
  const graph = await owner.api.personalGraph();
  await owner.api.seedSource(graph.id, 'The Halcyon launch is March 14.');
  const answer = await owner.api.json('POST', '/v1/queries', {
    text: 'When is the Halcyon launch?', library_scope: { graph_ids: [graph.id] },
  });
  const id: string = answer.conversation_id;
  const message: string = answer.message_id;
  const path = `/v1/load/conversations/${id}`;
  const read = () => owner.api.json('GET', path);
  let initial: any;
  await expect.poll(async () => {
    initial = await read();
    return initial.computed_at !== null && initial.stale === false;
  }).toBe(true);
  expect(initial.payload.messages.find((m: any) => m.id === message).feedback).toBeNull();
  const stored = { vote: 'down', reasons: ['outdated'], note: 'The date moved again.' };
  expect(await owner.api.json('PUT', `/v1/conversations/${id}/messages/${message}/feedback`, stored)).toEqual(stored);
  expect(fixtureSql(CONTROL_DB, `SELECT vote FROM message_feedback WHERE message_id='${message}'`)).toBe('-1');
  // Acquire after the real PUT commits: dirty triggers also read receipt
  // metadata. This barrier prevents a worker from replacing the old page.
  const child = spawn('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', CONTROL_DB], { stdio: ['pipe', 'pipe', 'pipe'] });
  await new Promise<void>((resolve, reject) => {
    child.stdout.on('data', (chunk) => { if (String(chunk).includes('feedback-worker-barrier')) resolve(); });
    child.once('error', reject);
    child.once('exit', () => reject(new Error('feedback barrier exited before acquisition')));
    child.stdin.write("BEGIN; SET LOCAL lock_timeout='3s'; LOCK TABLE answer_support_receipts IN ACCESS EXCLUSIVE MODE; SELECT 'feedback-worker-barrier';\n");
  });
  const release = async () => {
    const ended = new Promise<void>((resolve) => child.once('exit', () => resolve()));
    child.stdin.end('ROLLBACK;\n'); await ended;
  };
  try {
    const reload = await read();
    expect(reload.computed_at).toBe(initial.computed_at);
    expect(reload.stale).toBe(true);
    expect(reload.payload.messages.find((m: any) => m.id === message).feedback).toEqual(stored);
  } finally { await release(); }
});
