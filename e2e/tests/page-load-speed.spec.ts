import { test, expect } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync, existsSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { pageRoutes } from '../../web/lib/pageRoutes';
import { seedPageBudgetFixture, representativePath, fixtureSql, type PageBudgetFixture } from '../lib/pageBudgetFixture';
import { signIn, activateOrganization } from '../lib/fixtures';
import { addMember, orgApi } from '../lib/isolation';
import { Api } from '../lib/api';
import { clerkApi } from '../lib/clerk';
import { API_BASE, WEB_BASE, PORTS, FAKE, CONTAINERS, BINS, REMOTE, STACK_ID, CONTROL_DB } from '../stack/config.mjs';
import { assembleContinuation, continuationBindings, bindingHash, strictReceipt, finalContinuationUnion } from '../perf/continuation-adoption.mjs';
import { caseIdentity } from '../perf/continuation.mjs';
import { classifyDocument, safeDocumentUrl } from '../perf/auth-evidence.mjs';
import { pageRedirectDestination } from '../../web/lib/pageRedirectDestination';
import { loadTestIdentity } from '../stack/clerk-identity.mjs';
import { adoptInvitedParticipant } from '../perf/fixture-identity.mjs';
import type { PageLoadDeclaration } from '../perf/nightly';
import { E2E_EMAIL_PREFIX, runId } from '../stack/clerk-cleanup.mjs';
import { prepareHealthyLoad, cachedScopedToken, tokenScope } from '../perf/healthy-setup.mjs';
import { storedReadEvidence } from '../../web/lib/pageLoadEvidence';
import { loadCanonicalSeed } from '../perf/seed.mjs';
import { RateCadence, pairInfoRateContract } from '../perf/rate-cadence.mjs';
import { DEFAULT_TRANSPORT_CONFIG } from '../../web/lib/transport';
import { observeLoad, discoveredReads, refreshDiscovered, routeStatistics, type LoadObservation } from '../perf/nightly';

const evidenceDir = process.env.PAGE_LOAD_EVIDENCE_DIR ?? path.resolve('page-load-evidence');
function save(name: string, value: unknown) {
  mkdirSync(evidenceDir, { recursive: true });
  writeFileSync(path.join(evidenceDir, name), JSON.stringify(value, null, 2) + '\n');
}

// One test deliberately owns ONE org and the complete run. Playwright cannot
// respawn a worker after a route failure and quietly seed another 600k org.
// All failures retain evidence; no retries, quarantining, or route skips.
test('@nightly actual pageRoutes speed at canonical 600k/60k scale', async ({ browser, request }, info) => {
  // 1,920 fresh documents plus real worker setup exceed 90 minutes on hosted CI.
  // This bounds orchestration only; every request/model/latency gate is unchanged.
  test.setTimeout((process.env.PAGE_LOAD_CONTINUATION_FILE ? 90 : 120) * 60_000);
  expect(REMOTE).toBe(false);
  const continuationFile = process.env.PAGE_LOAD_CONTINUATION_FILE;
  const continuationHash = process.env.PAGE_LOAD_CONTINUATION_SHA256;
  if (!!continuationFile !== !!continuationHash) throw new Error('continuation requires explicit file AND exact SHA256');
  if (continuationFile && !process.env.PAGE_LOAD_REUSE_FIXTURE) throw new Error('continuation must reuse the SAME retained protected fixture');
  if (continuationFile) {
    const rawInput = readFileSync(continuationFile, 'utf8');
    if (createHash('sha256').update(rawInput).digest('hex') !== continuationHash) throw new Error('continuation input hash mismatch');
    if (path.dirname(path.resolve(continuationFile)) === path.resolve(evidenceDir)
      || JSON.parse(rawInput).artifacts?.some((artifact: { path: string }) => path.dirname(path.resolve(artifact.path)) === path.resolve(evidenceDir))
      || existsSync(evidenceDir) && readdirSync(evidenceDir).some(name => /^route-\d+\.json$/.test(name))) throw new Error('continuation output must be fresh and separate from original proof/input');
  }
  const actualPinHashes: Record<string, string> = {};
  const identity = loadTestIdentity(runId());
  expect(identity.namespace, 'nightly must use its protected namespace').toBe('nightly');
  save('owner-lease.json', identity);
  const exclusions = pageRoutes.filter((r) => r.budget !== 'page').map((entry) => {
    expect(entry.budget).toBe(entry.route.startsWith('/dev/') ? 'dev' : 'addin');
    return { route: entry.route, reason: entry.budget === 'dev' ? 'development-only page' : 'requires Office/Teams/Meet host', samples: entry.samples };
  });
  save('route-inventory.json', { source: 'web/lib/pageRoutes.ts', included: pageRoutes.filter((r) => r.budget === 'page'), explicit_host_exclusions: exclusions });
  for (const [name, binary] of Object.entries(BINS)) {
    const actualName = path.basename(binary);
    const receipt = JSON.parse(readFileSync(path.join(evidenceDir, `pin-${actualName}.json`), 'utf8'));
    actualPinHashes[actualName] = createHash('sha256').update(readFileSync(binary)).digest('hex');
    expect(actualPinHashes[actualName], `${name}: immutable pin`).toBe(receipt.sha256);
  }
  // Exercise the real internal endpoint with positive and negative auth on
  // the owned backend. This proves no deployed endpoint availability claim.
  const endpoint = `http://127.0.0.1:${PORTS.internal}/internal/admin/page-read-health`;
  const endpointEvidence = [];
  for (const [label, headers] of [ ['missing', {}], ['invalid', { 'X-Internal-Token': 'wrong-l6-token' }],
    ['valid', { 'X-Internal-Token': FAKE.internalToken }] ] as const) {
    const response = await request.get(endpoint, { headers });
    endpointEvidence.push({ label, status: response.status(), body: await response.json() });
    expect(response.status(), label).toBe(label === 'valid' ? 200 : 401);
    if (label === 'valid') {
      save('owned-health-response.json', endpointEvidence.at(-1)!.body);
      execFileSync('python3', ['../scripts/check_page_read_health.py', path.join(evidenceDir, 'owned-health-response.json')]);
    }
  }
  expect((await request.get(`${API_BASE}/internal/admin/page-read-health`)).status()).toBe(404);
  save('owned-health-auth.json', endpointEvidence);

  let fixture: PageBudgetFixture;
  let fixtureReceipt: ReturnType<typeof loadCanonicalSeed>;
  if (process.env.PAGE_LOAD_REUSE_FIXTURE) {
    const retained = JSON.parse(readFileSync(process.env.PAGE_LOAD_REUSE_FIXTURE, 'utf8'));
    expect(retained.owner_identity, 'retained fixture must have this exact live lease; no expired-owner resume').toEqual(identity);
    expect(retained.run_id).toBe(runId()); expect(retained.stack_id).toBe(STACK_ID);
    const data = retained.fixture;
    expect(data.email.startsWith(`${E2E_EMAIL_PREFIX}${runId()}-`)).toBe(true);
    expect(data.orgId).toMatch(/^org_[A-Za-z0-9]+$/);
    for (const value of [data.account, data.actor, data.graph]) expect(value).toMatch(/^[a-f0-9-]{36}$/);
    expect(data.db).toBe(fixtureSql(CONTROL_DB, `SELECT 'coralswarm_tenant_'||replace(slug,'-','_') FROM tenant_provisioning_jobs WHERE clerk_org_id='${data.orgId}'`));
    const counts = JSON.parse(fixtureSql(data.db, "SELECT json_build_object('atoms',(SELECT count(*) FROM atoms),'nodes',(SELECT count(*) FROM nodes),'links',(SELECT count(*) FROM node_atoms),'disabled_user_triggers',(SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='D'))"));
    expect(counts).toEqual(retained.canonical.counts);
    expect(createHash('sha256').update(readFileSync('../backend/tests/fixtures/page_models_600k.sql')).digest('hex')).toBe(retained.canonical.sql_sha256);
    fixture = { ...data, api: await orgApi(request, data.userId, data.orgId) };
    const me = await fixture.api.json('GET', '/v1/me'); expect(me.account_id).toBe(data.account); expect(me.user_id).toBe(data.actor);
    fixtureReceipt = retained.canonical;
    save('fixture-reuse.json', { source: process.env.PAGE_LOAD_REUSE_FIXTURE, reseeded: false, data_writes: false, counts, account: data.account, actor: data.actor, org_id: data.orgId });
  } else {
    fixture = await seedPageBudgetFixture(request, 'org');
    const graphs = await fixture.api.graphs();
    const ocean = graphs.find((g) => g.kind === 'org')!;
    expect(ocean).toBeTruthy();
    const owner = await clerkApi.users.getUser(fixture.userId);
    const other = await addMember(request, { orgId: fixture.orgId!, name: 'L6 speed', oceanId: ocean.id,
      adminInOrg: fixture.api, admin: { user: owner, email: fixture.email, api: new Api(request, fixture.userId) } }, 'l6-scale-member');
    const participant = await adoptInvitedParticipant({ identity: loadTestIdentity(runId()), email: other.email, user: other.user,
      updateMetadata: (id: string, metadata: Parameters<typeof clerkApi.users.updateUserMetadata>[1]) => clerkApi.users.updateUserMetadata(id, metadata) });
    save('participant-ownership.json', participant);
    const otherMe = await other.inOrg.json('GET', '/v1/me');
    fixtureReceipt = loadCanonicalSeed(fixture.db, { acct: fixture.account, u1: fixture.actor, u2: otherMe.user_id,
      g_org: ocean.id, g_team: randomUUID(), g_mem: fixture.graph });
  }
  const { api: _fixtureApi, ...fixtureData } = fixture;
  save('retained-fixture.json', { fixture: fixtureData, canonical: fixtureReceipt, owner_identity: identity, run_id: runId(), stack_id: STACK_ID });
  // This connection is the same actual role selected by the tenant registry,
  // not an owner pretending that RLS exists. Timed observations are HTTP.
  const roleProof = execFileSync('docker', ['exec', '-i', CONTAINERS.pg, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'coralswarm_tenant_app', '-d', fixture.db, '-c',
    "SELECT json_build_object('role',current_user,'superuser',rolsuper,'bypassrls',rolbypassrls,'row_security',current_setting('row_security'),'owned_relations',(SELECT count(*) FROM pg_class WHERE relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))) FROM pg_roles WHERE rolname=current_user"], { encoding: 'utf8' });
  const role = JSON.parse(roleProof);
  expect(role.superuser).toBe(false); expect(role.bypassrls).toBe(false); expect(role.row_security).toBe('on'); expect(role.owned_relations).toBe(0);
  save('fixture.json', { ...fixtureReceipt, serving_role: role, org_id: fixture.orgId,
    browser_audience: 'org admin, also a real org member; public entries use an anonymous context',
    baseline_scope: 'canonical synthetic distribution across org/team/member graphs, plus disclosed L5 real representative capture/topic' });
  const authenticated = await browser.newContext();
  const publicContext = await browser.newContext();
  const login = await authenticated.newPage();
  await signIn(login, fixture.email);
  if (fixture.orgId) await activateOrganization(login, fixture.orgId);
  const browserCredential = await login.evaluate(async () => ({ session: window.Clerk.session?.id, jwt: await window.Clerk.session?.getToken({ skipCache: true }) }));
  if (!browserCredential.session || !browserCredential.jwt) throw new Error('Missing verified real browser session');
  const browserScope = tokenScope(browserCredential.jwt).scope;
  const initialScope = JSON.parse(browserScope);
  expect(initialScope[0], 'real login issuer').toBeTruthy();
  expect(initialScope[1], 'real login user').toBe(fixture.userId);
  expect(initialScope[2], 'real login organization').toBe(fixture.orgId ?? null);
  const browserToken = cachedScopedToken(async () => (await clerkApi.sessions.getToken(browserCredential.session!, undefined as unknown as string)).jwt, browserScope);
  save('setup-credential-scope.json', { issuer_present: !!initialScope[0], user_matches: initialScope[1] === fixture.userId, organization_matches: initialScope[2] === (fixture.orgId ?? null), session_present: true, source: 'same actual signed-in browser session; cached minted JWT never recorded' });
  await login.close();
  const results: { route: string; path: string; status: string; error?: string; measured_loads: number; attempted_loads: number; discovery_returned: boolean; fatal_outcome?: string }[] = [];
  const cadence = new RateCadence(pairInfoRateContract(new URL('../../', import.meta.url)), DEFAULT_TRANSPORT_CONFIG.maxAttempts);
  const eligible = pageRoutes.filter((entry) => entry.budget === 'page');
  const pair = eligible.find((entry) => entry.route === '/addin/approve');
  const google = eligible.find((entry) => entry.samples.some((sample) => sample === '/connectors/google'));
  if (!pair || !google || google.samples[0] !== '/connectors/google') throw new Error('Missing exact discriminants in actual pageRoutes');
  const ordered = [pair, google, ...eligible.filter((entry) => entry !== pair && entry !== google)];
  const execution = ordered.flatMap((entry) => entry.samples.map((template) => ({ route: entry.route, path: representativePath(template, fixture) })));
  expect(execution).toHaveLength(96);
  expect(new Set(execution.map((item) => `${item.route}|${item.path}`)).size).toBe(96);
  const declarationFor = (item: { route: string }) => {
    const route = eligible.find(entry => entry.route === item.route);
    if (!route) throw new Error('unknown actual route declaration');
    return { access: route.access, audience: route.access === 'public' ? 'anonymous' as const : 'authenticated' as const,
      webBase: WEB_BASE, action: 'action' in route ? route.action : undefined };
  };
  const harnessSource = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  save('execution-inventory.json', { harness_source: harnessSource, source: 'actual pageRoutes, only reordered', expected: 96, measured_loads_each: 20, execution });
  const relevantSources = ['e2e/tests/page-load-speed.spec.ts', 'e2e/perf/nightly.ts', 'e2e/perf/auth-evidence.mjs', 'e2e/perf/healthy-setup.mjs', 'e2e/perf/rate-cadence.mjs', 'e2e/perf/continuation.mjs', 'e2e/perf/continuation-adoption.mjs', 'e2e/perf/semantic-timing.mjs', 'e2e/lib/pageLoadBudget.ts', 'e2e/lib/pageLoadClock.mjs', 'web/lib/pageLoadEvidence.ts', 'web/lib/pageLoadPollCandidates.mjs'];
  const currentSource = { revision: harnessSource, sha256: bindingHash(Object.fromEntries(relevantSources.map(file => [file, createHash('sha256').update(readFileSync(path.join('..', file))).digest('hex')]))) };
  let adoption: ReturnType<typeof assembleContinuation> | null = null;
  if (continuationFile) {
    const webProof = JSON.parse(readFileSync(path.join(evidenceDir, 'web-reuse-verification.json'), 'utf8'));
    const actualBundle = execFileSync('python3', ['-c', "import pathlib,hashlib; r=pathlib.Path('../web/.next/standalone'); h=hashlib.sha256(); [(h.update(str(p.relative_to(r)).encode()+bytes([0])+hashlib.sha256(p.read_bytes()).digest())) for p in sorted(r.rglob('*')) if p.is_file()]; print(h.hexdigest())"], { encoding: 'utf8' }).trim();
    expect(actualBundle).toBe(webProof.bundle_sha256);
    expect(readFileSync('../web/.next/BUILD_ID', 'utf8').trim()).toBe(webProof.build_id);
    const bindings = continuationBindings({ fixture, canonicalSeed: fixtureReceipt, identity, stackId: STACK_ID,
      backendSha256: actualPinHashes['coralswarm-backend'], migrators: { control: actualPinHashes['migrate_control'], tenant: actualPinHashes['migrate_tenants'] },
      web: { bundleSha256: actualBundle, buildId: webProof.build_id, compiledApi: webProof.compiled_api, productionTree: webProof.web_tree, buildSource: webProof.build_reused_from } });
    adoption = assembleContinuation({ inputText: readFileSync(continuationFile, 'utf8'), inputSha256: continuationHash!, inventory: execution, bindings, currentSource, now: Date.now(),
      declarationFor, classifyDocument, safeDocumentUrl, resolveAlias: pageRedirectDestination, routeStatistics, validateSetupBody: (address: string, body: unknown) => storedReadEvidence(address, body) !== null,
      readPriorBlob: (ref: { path: string }) => readFileSync(ref.path, 'utf8'),
      verifyArtifact: (artifact: { path: string; sha256: string }) => createHash('sha256').update(readFileSync(artifact.path)).digest('hex') === artifact.sha256 });
    save('continuation-plan.json', adoption);
    save('selected-execution.json', { currentSource, inputSha256: adoption.inputSha256, acceptedPrior: adoption.plan.acceptedCompleted, selected: adoption.plan.remaining, leaseLineage: adoption.plan.leaseLineage, fullInventoryCount: execution.length });
  }
  const selected = new Set((adoption?.plan.remaining ?? execution).map(caseIdentity));
  const priorAccepted = adoption?.plan.acceptedCompleted ?? [];
  const currentReceipts: { id: string; path: string; sha256: string }[] = [];
  let parkError: Error | null = null;
  const liveIdentity = () => {
    try { const current = loadTestIdentity(runId()); if (JSON.stringify(current) !== JSON.stringify(identity)) throw new Error('owner lease changed during this scenario'); return current; }
    catch (error) { parkError = new Error(`Owner lease refused: ${(error as Error).message}; remaining documents explicitly not run`); throw parkError; }
  };
  let index = 0;
  try {
    for (const entry of ordered) {
      for (const template of entry.samples) {
        const routePath = representativePath(template, fixture);
        if (!selected.has(caseIdentity({ route: entry.route, path: routePath }))) continue; // Explicitly approved prior twenty; never navigated again.
        liveIdentity(); // No discovery or retained-fixture use after the owner lease expires.
        const declaration: PageLoadDeclaration = { action: 'action' in entry ? entry.action : undefined,
          expectedRefusals: 'refusals' in entry ? entry.refusals : [],
          document: { access: entry.access, audience: entry.access === 'public' ? 'anonymous' : 'authenticated', webBase: WEB_BASE, ...(entry.access === 'public' ? {} : {
            sessionId: browserCredential.session, userId: fixture.userId, orgId: fixture.orgId ?? null, scope: browserScope }),
            action: 'action' in entry ? entry.action : undefined } };
        const context = entry.access === 'public' ? publicContext : authenticated;
        const discoveryPage = await context.newPage();
        const loads: LoadObservation[] = [];
        let attemptedLoads = 0; let discoveryReturned = false;
        const setups: unknown[] = [];
        const receipt: Record<string, unknown> = { route: entry.route, path: routePath, access: entry.access,
          audience: entry.access === 'public' ? 'anonymous' : 'org-admin (member and admin eligible)',
          mapping: 'validated actual initial browser GET/Stored responses, exact scope + query; no parallel route/model registry' };
        const filename = `route-${String(execution.findIndex(item => item.route === entry.route && item.path === routePath)).padStart(3, '0')}.json`; index++;
        const validationErrors: string[] = [];
        await context.tracing.start({ screenshots: false, snapshots: false, sources: true });
        try {
          const discovery = await observeLoad(discoveryPage, routePath, true, declaration); receipt.discovery = discovery; discoveryReturned = true; cadence.observe(discovery.records, `${routePath}:discovery`); receipt.cadence = cadence.receipt(); save(filename, receipt);
          if (discovery.document.fatal) {
            receipt.fatal_outcome = discovery.document.kind;
            parkError = new Error(`Discovery document ${discovery.document.kind}: ${routePath}; complete observation preserved; remaining documents explicitly not run`); throw parkError;
          }
          validationErrors.push(...discovery.errors.map((error) => `discovery: ${error}`));
          if (entry.access !== 'public') {
            if (!discovery.records.some((row) => row.method === 'GET' && row.priority === 'critical' && row.total_ms !== null))
              validationErrors.push(`authenticated representative has no actual critical page_read timing evidence: ${routePath}`);
          }
          const reads = discoveredReads(discovery); receipt.discovered_reads = reads;
          declaration.document!.expectedReads = reads;
          await discoveryPage.close();
          const setupToken = entry.access === 'public' ? async () => '' : browserToken;
          liveIdentity(); // Discovery may have crossed the owner lease deadline.
          const refresh = await refreshDiscovered(request, reads, setupToken);
          receipt.refresh = refresh; save(filename, receipt);
          validationErrors.push(...refresh.errors.map((error) => `worker refresh: ${error}`));
          if (routePath === '/connectors/google' && refresh.errors.length) {
            parkError = new Error(`Google registered worker refresh failed after ${refresh.elapsed_ms}ms: ${refresh.errors.join('; ')}; explicitly park before repeated deadlines`);
            throw parkError;
          }
          for (let iteration = 0; iteration < 20; iteration++) {
            // A new document has no previous React-query answer to suppress
            // a measured request. Clerk cookies/real org scope remain real.
            await cadence.before(discovery.records);
            receipt.cadence = cadence.receipt(); save(filename, receipt);
            try {
              liveIdentity(); // Check AFTER cadence; never resume an expired owner.
              const setup = await prepareHealthyLoad({ request, reads, token: setupToken, apiBase: API_BASE,
                refresh: refreshDiscovered, validateBody: (address: string, body: unknown) => storedReadEvidence(address, body) !== null });
              setups.push({ load: iteration + 1, ...setup });
            } catch (error) {
              const failure = error as Error & { receipt?: unknown };
              setups.push({ load: iteration + 1, receipt: failure.receipt, error: failure.message });
              receipt.per_load_setup = setups; save(filename, receipt);
              parkError = new Error(`Real healthy setup failed before load ${iteration + 1}: ${failure.message}`); throw parkError;
            }
            receipt.per_load_setup = setups; save(filename, receipt);
            liveIdentity(); // Setup may itself consume the remaining lease.
            const page = await context.newPage();
            try {
              attemptedLoads++;
              const load = await observeLoad(page, routePath, false, declaration); loads.push(load); cadence.observe(load.records, `${routePath}:load-${iteration + 1}`); receipt.cadence = cadence.receipt();
              receipt.loads = loads; save(filename, receipt);
              if (load.document.fatal) {
                receipt.fatal_outcome = load.document.kind;
                parkError = new Error(`Measured document ${load.document.kind}: ${routePath}, load ${iteration + 1}; complete observation preserved; remaining documents explicitly not run`); throw parkError;
              }
              validationErrors.push(...load.errors.map((error) => `load ${iteration + 1}: ${error}`));
            } finally { await page.close(); }
          }
          receipt.validation_errors = validationErrors;
          expect(validationErrors, `all discovery/refresh/measured attempts for ${routePath}`).toEqual([]);
          receipt.statistics = routeStatistics(loads, discovery);
          receipt.status = 'passed';
          results.push({ route: entry.route, path: routePath, status: 'passed', measured_loads: loads.length, attempted_loads: attemptedLoads, discovery_returned: discoveryReturned });
        } catch (error) {
          receipt.status = 'failed'; receipt.error = (error as Error).message;
          results.push({ route: entry.route, path: routePath, status: 'failed', error: (error as Error).message, measured_loads: loads.length, attempted_loads: attemptedLoads, discovery_returned: discoveryReturned, fatal_outcome: receipt.fatal_outcome as string | undefined });
        } finally {
          if (!discoveryPage.isClosed()) await discoveryPage.close();
          const trace = path.join(evidenceDir, filename.replace(/\.json$/, ".trace.zip"));
          await context.tracing.stop({ path: trace });
          receipt.trace = trace; receipt.validation_errors = validationErrors;
          receipt.document_inventory = { discovery_attempted: true, discovery_returned: discoveryReturned, measured_attempted: attemptedLoads, measured_returned: loads.length, measured_not_run: 20 - attemptedLoads, harness_source: harnessSource };
          save(filename, receipt);
          currentReceipts.push({ id: filename, path: path.resolve(evidenceDir, filename), sha256: createHash('sha256').update(readFileSync(path.join(evidenceDir, filename))).digest('hex') });
          await info.attach(filename, { path: path.join(evidenceDir, filename), contentType: 'application/json' });
          save('summary.json', { profile: 'ordinary dev opt0 e2e; browser gate p95 <500ms', expected_representatives: pageRoutes.filter((r) => r.budget === 'page').reduce((n, r) => n + r.samples.length, 0),
            harness_source: harnessSource, continuation_input_sha256: adoption?.inputSha256, prior_accepted: priorAccepted, observed: results,
            unvisited: execution.filter(item => selected.has(caseIdentity(item)) && !results.some(result => result.route === item.route && result.path === item.path)),
            measured_attempted: results.reduce((n, result) => n + result.attempted_loads, 0),
            measured_returned: results.reduce((n, result) => n + result.measured_loads, 0),
            measured_not_run: selected.size * 20 - results.reduce((n, result) => n + result.attempted_loads, 0),
            explicit_host_exclusions: exclusions, retry_count: 0, synthetic_counts: fixtureReceipt.synthetic_counts });
        }
        if (parkError) throw parkError;
        if (!adoption && index === 2 && results.some((result) => result.status !== 'passed')) throw new Error('Discriminants failed; remaining representatives are not run, never credited');
      }
    }
  } finally { await authenticated.close(); await publicContext.close(); }
  expect(results).toHaveLength(selected.size);
  expect(results.filter((result) => result.status !== 'passed'), 'every selected route must complete all twenty loads').toEqual([]);
  if (adoption) {
    const principal = { kind: 'original-clerk-principal', sessionId: browserCredential.session, userId: fixture.userId, orgId: fixture.orgId, issuer: initialScope[0], scope: browserScope };
    const combined = finalContinuationUnion(adoption.plan, currentReceipts, (receipt: { route: string }) => strictReceipt(receipt, { principal, declaration: declarationFor(receipt), bindings: adoption!.plan.bindings,
      classifyDocument, safeDocumentUrl, resolveAlias: pageRedirectDestination, routeStatistics, validateSetupBody: (address: string, body: unknown) => storedReadEvidence(address, body) !== null,
      guardWitness: { checkedBeforeRedaction: true, sourceSha256: createHash('sha256').update(readFileSync('../e2e/perf/auth-evidence.mjs')).digest('hex') } }), (ref: { path: string }) => readFileSync(ref.path, 'utf8'));
    save('combined-coverage.json', combined);
  }
});
