import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import { chromium, expect } from '@playwright/test';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const required = ['/v1/load/oceans', '/v1/conversations'];

// Harness-only route fixture: the production helper and Playwright routing /
// assertions are real. The app and budget recorder are replaced here; actual
// auth, admission, certificates and performance need the real-stack scenarios.
function loadHelper(assertPageLoadBudget, observeAbsence) {
  const source = readFileSync(new URL('./initialAuthBrowser.ts', import.meta.url), 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const exports = {};
  runInNewContext(outputText, {
    exports, URL, setTimeout, clearTimeout,
    require(name) {
      if (name === './pageLoadBudget') return { assertPageLoadBudget };
      if (name === '@playwright/test') return {
        expect(actual, message) {
          const assertion = expect(actual, message);
          return new Proxy(assertion, {
            get(target, key) {
              if (key === 'toHaveCount') return async (count) => {
                if (count === 0) observeAbsence();
                return target.toHaveCount(count);
              };
              return target[key];
            },
          });
        },
      };
      return require(name);
    },
  });
  return exports.initialReadsOverlapBoot;
}

async function fixture(t, bootstrapAddress, { delayedBoot = false, failAbsence = false, omitRead = false } = {}) {
  const browser = await chromium.launch();
  t.after(() => browser.close());
  let entered = 0;
  let delivered = 0;
  let absenceChecks = 0;
  const server = createServer((request, response) => {
    const address = new URL(request.url, 'http://initial-auth.fixture');
    if (address.pathname === '/workspaces') {
      response.setHeader('Content-Type', 'text/html');
      return response.end(`
      <script>
        const bootstrap = new Promise(resolve => { window.startBootstrap = () => resolve(
          ${bootstrapAddress === null ? 'Promise.resolve()' : `fetch(${JSON.stringify(bootstrapAddress)})`}
        ); });
        ${delayedBoot ? '' : 'window.startBootstrap();'}
        const reads = Promise.all(${JSON.stringify(omitRead ? required.slice(1) : required)}.map(address => fetch(address)))
          .then(() => { window.readsCompleted = true; });
        Promise.all([
          bootstrap, reads
        ]).then(() => {
          document.body.innerHTML = '<main data-page-loaded="true" data-page-failed="false">Workspaces</main>';
        });
      </script>`);
    }
    if (address.pathname.startsWith('/v1/bootstrap')) delivered++;
    response.setHeader('Content-Type', 'application/json');
    response.end('{}');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
  const context = await browser.newContext({ baseURL: `http://127.0.0.1:${server.address().port}` });
  const page = await context.newPage();
  const matchers = [];
  const route = page.route.bind(page);
  page.route = async (matcher, handler) => {
    matchers.push(matcher);
    return route(matcher, async (requestRoute) => {
      entered++;
      return handler(requestRoute);
    });
  };
  const unroute = page.unroute.bind(page);
  page.unroute = async (matcher) => {
    assert.equal(matcher, matchers[0], 'route and unroute use the same matcher');
    return unroute(matcher);
  };
  let recorderSettled = false;
  const helper = loadHelper(async (measuredPage, path, budget) => {
    assert.equal(path, '/workspaces');
    assert.deepEqual({ ...budget }, { maxRequests: 6, maxDepth: 1, mode: 'owed' });
    try {
      await measuredPage.goto(path, { waitUntil: 'domcontentloaded' });
      await expect(page.getByRole('main')).toBeVisible();
      return required.map(path => ({ path, method: 'GET' }));
    } finally { recorderSettled = true; }
  }, () => {
    absenceChecks++;
    assert.equal(entered, 1, 'bootstrap must actually enter the hold before absence assertions');
    assert.equal(delivered, 0, 'bootstrap response must still be held');
    if (failAbsence) throw new Error('injected absence assertion failure');
  });
  return { page, helper, state: () => ({ entered, delivered, absenceChecks, recorderSettled }) };
}

test('early reads alone cannot release absence assertions before the bootstrap handler enters', { timeout: 15_000 }, async (t) => {
  const { page, helper, state } = await fixture(t, '/v1/bootstrap?billing_banner=trial_v1', { delayedBoot: true });
  const running = helper(page, '/workspaces', required);
  void running.catch(() => {});
  await page.waitForFunction(() => window.readsCompleted);
  assert.equal(state().absenceChecks, 0);
  await page.evaluate(() => window.startBootstrap());
  await running;
  assert.equal(state().entered, 1);
  assert.equal(state().absenceChecks, 2);
});

for (const address of [
  '/v1/bootstrap?billing_banner=unknown',
  '/v1/bootstrap?billing_banner=trial_v1&extra=1',
  '/v1/bootstrap?billing_banner=trial_v1&billing_banner=trial_v1',
  '/v1/bootstrap/subpath',
  null,
]) {
  test(`no hold proof for ${address ?? 'missing bootstrap'} fails at the 4s request deadline`, { timeout: 15_000 }, async (t) => {
    const { page, helper, state } = await fixture(t, address);
    await assert.rejects(helper(page, '/workspaces', required), /bootstrap.*hold.*4.*000/i);
    assert.equal(state().entered, 0);
    assert.equal(state().absenceChecks, 0);
    assert.equal(state().recorderSettled, true);
  });
}

for (const failure of ['absence', 'read']) {
  test(`${failure} failure releases held bootstrap and settles the recorder and request waits`, { timeout: 15_000 }, async (t) => {
    const { page, helper, state } = await fixture(t, '/v1/bootstrap?billing_banner=trial_v1', {
      failAbsence: failure === 'absence', omitRead: failure === 'read',
    });
    const listeners = page.listenerCount('request');
    await assert.rejects(helper(page, '/workspaces', required), failure === 'absence' ? /injected absence assertion failure/ : /Timeout.*4000ms/);
    assert.equal(state().entered, 1);
    assert.equal(state().delivered, 1);
    assert.equal(state().recorderSettled, true);
    assert.equal(page.listenerCount('request'), listeners, 'no recorder/request-wait listeners survive failure');
    await page.evaluate(() => fetch('/v1/bootstrap?billing_banner=trial_v1'));
    assert.equal(state().entered, 1, 'failed helper removes its route');
  });
}

for (const address of ['/v1/bootstrap', '/v1/bootstrap?billing_banner=trial_v1']) {
  test(`actual bootstrap hold intercepts ${address} before asserting absence`, { timeout: 15_000 }, async (t) => {
    const { page, helper, state } = await fixture(t, address);
    await helper(page, '/workspaces', required);
    assert.deepEqual(state(), { entered: 1, delivered: 1, absenceChecks: 2, recorderSettled: true });
    // A second request proves the handler was removed, rather than just released.
    await page.evaluate(address => fetch(address), address);
    assert.equal(state().entered, 1);
    assert.equal(state().delivered, 2);
  });
}
