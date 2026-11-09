import { randomUUID } from "node:crypto";
import type { Request } from "@playwright/test";
import { fixtureSql } from "../lib/pageBudgetFixture";
import { CONTROL_DB } from "../stack/config.mjs";
import { expect, test } from "../lib/fixtures";

// Home, the launchpad: the landing page after sign-in. Three reads per visit
// (GET /v1/home/continue, /v1/home/whats-new, /v1/home/attention), never
// polled. State B (an empty ocean) shows the setup checklist; State A shows
// Continue and What's new over the viewer's own seeded work.

const HOME_READS = ["/v1/home/attention", "/v1/home/continue", "/v1/home/whats-new"];

function homeReads(requests: Request[]): string[] {
  return requests
    .filter((r) => r.method() === "GET" && new URL(r.url()).pathname.startsWith("/v1/home/"))
    .map((r) => new URL(r.url()).pathname)
    .sort();
}

test("a new user lands on Home and sees the setup checklist (State B)", async ({ page, signedIn }) => {
  const requests: Request[] = [];
  page.on("request", (r) => requests.push(r));
  await page.goto("/");
  await expect(page).toHaveURL(/\/home/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(/^Good (morning|afternoon|evening), E2E$/);
  const ask = page.getByRole("textbox", { name: "Ask your ocean" });
  await expect(ask).toBeVisible();

  const setup = page.getByRole("region", { name: "Set up your ocean" });
  await expect(setup).toBeVisible();
  await expect(setup.getByText("0 of 3")).toBeVisible();
  await expect(setup.getByRole("link", { name: "Connect" })).toHaveAttribute("href", "/connectors");
  await expect(setup.getByText("https://api.coralswarm.example/mcp")).toBeVisible();
  await expect(page.getByText(/Your ocean is empty for now/)).toBeVisible();
  // Continue and What's new stay hidden until content exists.
  await expect(page.getByRole("region", { name: "Continue" })).toHaveCount(0);
  await expect(page.getByRole("region", { name: "What's new" })).toHaveCount(0);

  // Step 3 focuses the Ask box.
  await setup.getByRole("button", { name: "Ask your first question" }).click();
  await expect(ask).toBeFocused();

  await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
  expect(homeReads(requests)).toEqual(HOME_READS);
  expect(signedIn.email).toContain("+clerk_test@");

  // Every visit is fresh and costs the same three reads: leave through the
  // sidebar and come back inside the 5 minutes. Hovering Home warms nothing;
  // the mount reads each once.
  const nav = page.getByRole("navigation", { name: "Primary" });
  await nav.getByRole("link", { name: "Chats", exact: true }).click();
  await expect(page).toHaveURL(/\/chats/);
  const home = nav.getByRole("link", { name: "Home", exact: true });
  await home.hover();
  await home.click();
  await expect(setup).toBeVisible();
  await expect.poll(() => homeReads(requests).length).toBe(6);
  await page.waitForTimeout(1_000);
  expect(homeReads(requests)).toEqual([...HOME_READS, ...HOME_READS].sort());
});

test("a new user whose only source broke sees why nothing arrives, above the checklist (State B)", async ({ page, signedIn }) => {
  // A connected source whose credential died before anything was captured:
  // the ocean is still empty (State B), and step 1 is connected but broken.
  const me = await signedIn.api.json("GET", "/v1/me");
  fixtureSql(CONTROL_DB, `INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token,credential_invalid_at,credential_invalid_reason) VALUES('${me.account_id}','google_gmail','gmail.readonly','${me.user_id}','\\x00'::bytea,now(),'google_invalid_grant')`);

  const requests: Request[] = [];
  page.on("request", (r) => requests.push(r));
  await page.goto("/home");

  const setup = page.getByRole("region", { name: "Set up your ocean" });
  const row = page.getByRole("region", { name: "Needs attention" });
  await expect(setup).toBeVisible();
  await expect(row).toContainText("Gmail needs you to sign in again");
  await expect(row.getByRole("link", { name: "Fix: Gmail needs you to sign in again" })).toHaveAttribute("href", "/connectors");
  // The row sits above the checklist.
  expect(await row.evaluate((el, other) => !!(el.compareDocumentPosition(other!) & Node.DOCUMENT_POSITION_FOLLOWING), await setup.elementHandle())).toBe(true);
  // Step 1 is done (it is connected) but explains itself rather than reading as a success.
  await expect(setup.getByText("1 of 3")).toBeVisible();
  const step1 = setup.getByRole("listitem").filter({ hasText: "Connect a source" });
  await expect(step1).toContainText("Connected, but a source needs attention");
  await expect(step1).not.toContainText("A source is connected");
  await expect(page.getByRole("region", { name: "What's new" })).toHaveCount(0);

  // What's new was read but never shown, so the visit is not remembered.
  await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
  expect(homeReads(requests)).toEqual(HOME_READS);
  expect(await page.evaluate(() => Object.keys(localStorage).filter((k) => k.startsWith("cs.home.lastVisit.")).length)).toBe(0);
});

test("Home shows your recent work and what formed this week (State A), and Ask opens a chat", async ({ page, signedIn }) => {
  const graph = await signedIn.api.personalGraph();
  // Content: one captured source, and a decision reef formed from it. The reef
  // former runs on the 60 s sweep plus an LLM call, so its result is written
  // here the way managed extraction writes it (workers::source_atoms): the
  // reef is named by a mention its semantic atom carries, linked through
  // node_semantic_atoms, and mirrored onto the version's chunk corals
  // (node_atoms_bridge). What's new admits a reef only by that name
  // (library_node_title): an ingested coral's body lives in the object store,
  // so a reef linked to it alone has no readable name and is never listed.
  const reefTitle = `Orchid launch moves to March 14 ${randomUUID().slice(0, 6)}`;
  const seeded = await signedIn.api.seedSource(graph.id, `Home e2e: the Orchid rollout checklist is owned by Priya. Decision: ${reefTitle}.`, `e2e-home-${randomUUID()}`);
  const version: string = seeded.source_version_id;
  const reef = fixtureSql(CONTROL_DB, `INSERT INTO nodes(graph_id,type,title) VALUES('${graph.id}','decision','${reefTitle}') RETURNING node_id`);
  const fact = fixtureSql(CONTROL_DB, `UPDATE semantic_atoms SET entities=jsonb_build_array(jsonb_build_object('mention','${reefTitle}','type','topic','status','linked')) WHERE graph_id='${graph.id}' AND source_version_id='${version}' AND statement LIKE 'Decision:%' RETURNING semantic_atom_id`);
  expect(fact).toMatch(/^[a-f0-9-]{36}$/);
  fixtureSql(CONTROL_DB, `INSERT INTO node_semantic_atoms(node_id,semantic_atom_id,graph_id) VALUES('${reef}','${fact}','${graph.id}')`);
  const corals = fixtureSql(CONTROL_DB, `WITH linked AS (INSERT INTO node_atoms(node_id,atom_id) SELECT '${reef}',c.atom_id FROM source_version_chunks c WHERE c.graph_id='${graph.id}' AND c.source_version_id='${version}' RETURNING 1) SELECT count(*) FROM linked`);
  expect(Number(corals)).toBeGreaterThan(0);
  // A saved chat of the viewer's own, for Continue.
  const answer = await signedIn.api.json("POST", "/v1/queries", { text: "When is the Orchid launch?", library_scope: { graph_ids: [graph.id] } });
  const conversation: string = answer.conversation_id;
  expect(conversation, JSON.stringify(answer).slice(0, 300)).toBeTruthy();

  const requests: Request[] = [];
  page.on("request", (r) => requests.push(r));
  await page.goto("/home");

  const cont = page.getByRole("region", { name: "Continue" });
  await expect.poll(() => cont.getByRole("link").evaluateAll((links) => links.map((a) => a.getAttribute("href")))).toContain(`/chats?id=${conversation}`);

  const news = page.getByRole("region", { name: "What's new" });
  const reefRow = news.getByRole("link").filter({ hasText: reefTitle });
  await expect(reefRow).toHaveAttribute("href", `/topics?id=${reef}&graph_id=${graph.id}`);
  await expect(reefRow).toContainText("decision");
  // A first visit has no baseline, so nothing is marked new yet.
  await expect(news.getByRole("img", { name: "new since your last visit" })).toHaveCount(0);

  // Prompt starters are templates over the data on the page. A decision title
  // is a statement, so it is quoted; the chip shortens it and its tooltip (and
  // the question it asks) keeps it whole.
  const starter = page.getByRole("group", { name: "Prompt starters" }).getByTitle(`What led to “${reefTitle}”?`);
  await expect(starter).toBeVisible();
  await expect(starter).toHaveText("What led to “Orchid launch moves to March 14…”?");
  await expect(page.getByRole("region", { name: "Set up your ocean" })).toHaveCount(0);
  await expect(page.getByRole("region", { name: "Needs attention" })).toHaveCount(0);

  // Three reads, and no polling after the page settles.
  await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
  await page.waitForTimeout(2_500);
  expect(homeReads(requests)).toEqual(HOME_READS);

  // The visit is remembered once What's new has rendered. Move that memory a
  // week back (a later visit after a quiet week; a server-side cache of
  // What's new cannot then hide a reef inserted for the test) and the reef
  // formed since gets the dot.
  const remembered = () => page.evaluate(() => Object.keys(localStorage).filter((k) => k.startsWith("cs.home.lastVisit.")).length);
  await expect.poll(remembered).toBe(1);
  await page.evaluate(() => {
    const weekAgo = new Date(Date.now() - 6 * 86_400_000).toISOString();
    for (const k of Object.keys(localStorage)) if (k.startsWith("cs.home.lastVisit.")) localStorage.setItem(k, weekAgo);
  });
  await page.reload();
  await expect(reefRow.getByRole("img", { name: "new since your last visit" })).toBeVisible();
  await expect(news.getByText(/^1 new since your last visit/)).toBeVisible();

  // Enter in the Ask box opens a new chat seeded with the question.
  const ask = page.getByRole("textbox", { name: "Ask your ocean" });
  await ask.fill("When does the Orchid launch?");
  await ask.press("Enter");
  await expect(page).toHaveURL(/\/chats/);
  await expect(page.getByRole("textbox", { name: "Question" })).toHaveValue("When does the Orchid launch?");
});

test("Home draws its skeleton only before the first answer", async ({ page, signedIn }) => {
  expect(signedIn.email).toContain("+clerk_test@");
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => { release = resolve; });
  await page.route("**/v1/home/**", async (route) => {
    await held;
    await route.continue();
  });
  await page.goto("/home");
  await expect(page.getByRole("status", { name: "Loading your recent work" })).toBeVisible();
  await expect(page.getByRole("status", { name: "Loading what's new" })).toBeVisible();
  await expect(page.locator('[data-page-loaded="false"]')).toBeAttached();
  release();
  // A brand-new user's answer is State B; the skeleton does not come back.
  await expect(page.getByRole("region", { name: "Set up your ocean" })).toBeVisible();
  await expect(page.getByRole("status", { name: "Loading your recent work" })).toHaveCount(0);
  await expect(page.locator('[data-page-loaded="true"]')).toBeAttached();
});

// Retired and unlisted routes (the Library is gone; People and Topic remain as
// pages you land on, never as index pages).
test("old Library links land on Home, /library/people on People, and /topics needs an id", async ({ page, signedIn }) => {
  expect(signedIn.email).toContain("+clerk_test@");
  await page.goto("/library/sources");
  await expect(page).toHaveURL(/\/home/);
  await page.goto("/library/people");
  await expect(page).toHaveURL(/\/people/);
  await expect(page.getByRole("heading", { name: "People" })).toBeVisible();
  await page.goto("/topics");
  await expect(page).toHaveURL(/\/home/);
});
