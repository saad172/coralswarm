import { expect, test } from "../lib/fixtures";
import { Api } from "../lib/api";
import { createUser } from "../lib/clerk";
import { API_BASE, RELEASE_SHA, REMOTE } from "../stack/config.mjs";

// `@smoke`: the scenarios that also run against a deployed target
// (E2E_TARGET=staging, README "Remote mode") at release time, with real
// providers. So they use only production routes: no seeding route, no fakes.
// Locally they run like any other scenario, against the stack's fakes.

test("backend is live and ready, serving the release @smoke", async ({ request }) => {
  for (const path of ["/healthz", "/readyz"]) {
    const res = await request.get(`${API_BASE}${path}`);
    expect(res.status(), path).toBe(200);
  }
  if (RELEASE_SHA) {
    // Enforced as soon as the backend reports its build commit on /healthz
    // (it stamps CORALSWARM_GIT_SHA at build time but serves it only in MCP
    // serverInfo today).
    const health = await (await request.get(`${API_BASE}/healthz`)).json();
    const served: string | undefined = health.sha ?? health.git_sha ?? health.commit ?? health.version;
    if (served) {
      expect(RELEASE_SHA.startsWith(served) || served.startsWith(RELEASE_SHA), `serving ${served}, release is ${RELEASE_SHA}`).toBeTruthy();
    } else {
      test.info().annotations.push({ type: "warning", description: `backend exposes no build sha; cannot confirm it serves ${RELEASE_SHA}` });
    }
  }
});

test("an unauthenticated API call is refused @smoke", async ({ request }) => {
  const res = await request.get(`${API_BASE}/v1/graphs`);
  expect(res.status()).toBe(401);
});

test("a new user's first authenticated call provisions their personal ocean @smoke", async ({ request }) => {
  const user = await createUser("smoke");
  const api = new Api(request, user.id);
  const graph = await api.personalGraph();
  expect(graph.kind).toBe("personal");
  const bootstrap = await api.json("GET", "/v1/bootstrap");
  expect(JSON.stringify(bootstrap)).toContain(graph.id);
});

test("a user signs in through Clerk and lands in their own account @smoke", async ({ page, signedIn }) => {
  await page.goto("/chats");
  await expect(page.getByRole("button", { name: /^Current account: .*your personal account/ })).toBeVisible();
  await expect(page.getByRole("textbox", { name: "Question" })).toBeEnabled();
  expect(signedIn.email).toContain("+clerk_test@");
});

// The one real-provider answer: content captured through the real ingest
// route, extracted by the real managed pipeline (the standing admission
// sweep, then the extraction model) and answered by the real synthesis model.
//
// Slow by design, and the one scenario exempt from the 30 s rule: the sweep
// ticks every 60 s, so extraction lands up to a tick after capture plus the
// model call (measured locally: 11-58 s; budgeted 140 s so one slow tick on
// staging does not fail a release). Runs on every remote target; locally only with
// E2E_REAL_EXTRACTION=1, since the PR gate already covers ask through
// `ask.spec.ts` and should not pay two minutes per run for it.
test("content captured through the API is extracted and cited in an answer @smoke", async ({ page, signedIn }) => {
  test.skip(!REMOTE && !process.env.E2E_REAL_EXTRACTION, "real extraction waits on the 60 s sweep; set E2E_REAL_EXTRACTION=1");
  test.setTimeout(180_000);
  const graph = await signedIn.api.personalGraph();
  const sourceId = await signedIn.api.ingest(
    graph.id,
    "The Marlowe pilot starts on April 9 in the Lisbon office. Dana Okafor approved the pilot budget.",
  );
  await signedIn.api.waitForExtraction(graph.id, sourceId, 140_000);

  await page.goto(`/chats?graph_id=${graph.id}`);
  const question = page.getByRole("textbox", { name: "Question" });
  await expect(question).toBeEnabled();
  await question.fill("When does the Marlowe pilot start?");
  await question.press("Enter");
  const sources = page.getByRole("region", { name: "Answer sources" });
  await expect(sources).toBeVisible({ timeout: 30_000 });
  await expect(sources.getByRole("button", { name: /^Open source 1:/ })).toBeVisible();
  await expect(page.locator(".cite").first()).toBeVisible();
});
