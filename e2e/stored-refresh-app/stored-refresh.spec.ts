import { test as base, expect } from '@playwright/test';
import { clerkApi, createUser, deleteE2eUser, primaryEmail } from '../lib/clerk';
import { signIn } from '../lib/fixtures';
import { emptyAnswerPollsAndResolves, storedAnswerSurvivesRecovery, withheldAndRefusedAnswersClearData } from '../lib/storedRefreshBrowser';
import { bootstrapWithoutOptionalClerkUI, initialReadsOverlapBoot } from '../lib/initialAuthBrowser';

// Shapes replay the successful landing reads from run 41921323287, with
// synthetic identities. Authentication, Next routing and AppProvider are real.
const graph = { id: '11111111-1111-4111-8111-111111111111', name: 'Personal', kind: 'personal', is_primary: true,
  archived_at: null, created_at: '2026-10-30T00:00:00Z', my_team_role: null, owner_user_id: null, team_id: null };
const test = base.extend<{ authenticated: void }>({
  authenticated: [async ({ page }, use) => {
    const user = await createUser('storedclock');
    try {
      await page.route(/^http:\/\/(localhost|127\.0\.0\.1):\d+\/v1\//, async route => {
        const path = new URL(route.request().url()).pathname;
        const now = new Date().toISOString();
        const replies: Record<string, unknown> = {
          '/v1/bootstrap': { activities_enabled: true, gist_enabled: true, billing_banner: null,
            graphs: [graph], join_requests_count: null, my_join_requests: [], pending_orgs: [],
            selfserve_org: false, selfserve_signup_enabled: true,
            me: { account_id: '22222222-2222-4222-8222-222222222222', active_graph_id: graph.id,
              device_id: 'fixture-session', is_personal_org: false, org_teams_enabled: null, role: 'owner', user_id: user.id } },
          '/v1/graphs': [graph],
          '/v1/home/continue': { items: [], generated_at: now },
          '/v1/home/whats-new': { items: [], window_days: 7, generated_at: now },
          '/v1/home/attention': { has_content: false, has_source: false, has_mcp: false, has_asked: false, attention: null, generated_at: now },
          '/v1/conversations': { model: 'conversation_list', version: 1, payload: { conversations: [], shared: [] }, computed_at: now, stale: false, refresh_eta: null },
          '/v1/connectors': { model: 'connectors_list', version: 1, payload: { connectors: [] }, computed_at: now, stale: false, refresh_eta: null },
          '/v1/load/oceans': { components: [{ account_id: '22222222-2222-4222-8222-222222222222', anchor_graph_id: null,
            refresh_path: '/v1/load/oceans/component/refresh', stored: { model: 'oceans_index', version: 1, computed_at: now, stale: false, refresh_eta: null,
              payload: { oceans: [{ graph, counts: { reef_total: 0, atom_count: 0, source_count: 0, reef_counts: {} } }], members: [], team_members: {}, archived: null } } }] },
          '/v1/me/timezone': {},
          '/v1/me/active-graph': {},
          '/v1/telemetry/client': {},
        };
        expect(path in replies, `Unexpected backend call: ${path}`).toBe(true);
        await route.fulfill({ json: replies[path] });
      });
      await signIn(page, primaryEmail(user));
      await use();
    } finally {
      await deleteE2eUser(user.id, clerkApi);
    }
  }, { auto: true }],
});

test('stored answers stay useful with finite polling through real app auth', async ({ page }) => storedAnswerSurvivesRecovery(page));
test('empty answers poll until computed through real app auth', async ({ page }) => emptyAnswerPollsAndResolves(page));
test('authority changes clear retained data through real app auth', async ({ page }) => withheldAndRefusedAnswersClearData(page));
test('fresh document auth bootstrap does not wait for optional Clerk UI', async ({ page }) => bootstrapWithoutOptionalClerkUI(page));
for (const [path, required] of [
  ['/home', ['/v1/home/continue', '/v1/home/whats-new', '/v1/home/attention', '/v1/conversations']],
  ['/connectors', ['/v1/connectors', '/v1/conversations']],
  ['/workspaces', ['/v1/load/oceans', '/v1/conversations']],
] as const) test(`fresh ${path} reads overlap bootstrap without publishing before admission`, async ({ page }) => initialReadsOverlapBoot(page, path, [...required]));

base('bundled Clerk still renders the signed-out login UI', async ({ page }) => {
  await page.goto('/login');
  await expect(page.getByRole('textbox').first()).toBeVisible();
});
