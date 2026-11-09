import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { test } from "node:test";
import { runInNewContext } from "node:vm";
import { chromium, expect } from "@playwright/test";
import ts from "typescript";

const require = createRequire(import.meta.url);

// Exercise the actual fixture with real Playwright navigation/assertions, but
// replace Clerk and account creation: no credentials, backend or shared stack.
function loadFixtures(clerk = {}) {
  const source = readFileSync(new URL("./fixtures.ts", import.meta.url), "utf8");
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const exports = {};
  runInNewContext(outputText, {
    exports,
    Buffer,
    URL,
    require(name) {
      if (name === "@clerk/testing/playwright") return { clerk, setupClerkTestingToken: async () => {} };
      if (name === "./clerk" || name === "./api") return {};
      return require(name);
    },
  });
  return exports;
}

function deferred() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
}

test("signIn owns the authenticated Home navigation before callers measure or clear cold caches", { timeout: 15_000 }, async (t) => {
  const browser = await chromium.launch();
  t.after(() => browser.close());
  const context = await browser.newContext({ baseURL: "http://signin.fixture" });
  const page = await context.newPage();
  const activated = deferred();
  const homeRead = deferred();
  const releaseHome = deferred();
  const requests = [];
  page.on("request", (request) => requests.push(new URL(request.url()).pathname));
  await context.route("http://signin.fixture/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/") return route.fulfill({ contentType: "text/html", body: `<script>location.replace("/login")</script>` });
    if (path === "/login") return route.fulfill({ contentType: "text/html", body: `
      <script>
        window.Clerk = { loaded: true, user: null, session: null };
        window.addEventListener("finish-signin-navigation", () => location.replace("/home"));
      </script><p>Sign in</p>` });
    if (path === "/home") {
      const authenticated = (await context.cookies()).some((cookie) => cookie.name === "signed-in");
      return route.fulfill({ contentType: "text/html", body: authenticated ? `
        <div data-page-settled="false" data-page-loaded="false"><main>Home</main></div>
        <script>
          window.Clerk = { loaded: true, user: { id: "user-fixture" }, session: { id: "session-fixture" } };
          fetch("/v1/home/metrics").then(response => response.json()).then(() => {
            document.querySelector("[data-page-settled]").dataset.pageSettled = "true";
            document.querySelector("[data-page-loaded]").dataset.pageLoaded = "true";
          });
        </script>` : `<script>location.replace("/login")</script>` });
    }
    if (path === "/v1/home/metrics") {
      homeRead.resolve();
      await releaseHome.promise;
      return route.fulfill({ json: { computed_at: "2026-10-31T17:05:49Z", data: {} } });
    }
    throw new Error(`Unexpected fixture request: ${path}`);
  });
  const { signIn } = loadFixtures({
    loaded: async ({ page }) => {
      await page.waitForURL("**/login");
      await page.waitForFunction(() => window.Clerk?.loaded);
    },
    signIn: async ({ page }) => {
      await context.addCookies([{ name: "signed-in", value: "yes", url: "http://signin.fixture" }]);
      await page.evaluate(() => {
        window.Clerk.user = { id: "user-fixture" };
        window.Clerk.session = { id: "session-fixture" };
      });
      activated.resolve();
    },
  });
  let returned = false;
  const signingIn = signIn(page, "fixture@example.test").then(() => { returned = true; });
  // Observe failures even if an assertion stops this test before the final await.
  void signingIn.catch(() => {});
  await Promise.race([activated.promise, signingIn]);
  await page.evaluate(() => window.Clerk.session.id);
  assert.equal(returned, false, "Clerk session activation must not release the caller while /login still owns a redirect");
  await page.evaluate(() => window.dispatchEvent(new Event("finish-signin-navigation")));
  await homeRead.promise;
  await expect(page).toHaveURL("http://signin.fixture/home");
  await expect(page.locator('[data-page-settled="false"]')).toBeAttached();
  assert.equal(returned, false, "the Home URL alone must not release the caller while its critical reads are pending");
  releaseHome.resolve();
  await signingIn;
  await expect(page.locator('[data-page-settled="true"]')).toBeAttached();
  assert.deepEqual(requests.filter((path) => path.startsWith("/v1/")), ["/v1/home/metrics"]);
  // Cold-cache deletion and org activation remain caller-owned and can begin
  // here, after Home's real read completed, before the target document mounts.
  assert.equal(await page.evaluate(() => window.Clerk.session.id), "session-fixture");
});

function token(organization) {
  return `header.${Buffer.from(JSON.stringify({ sub: "user-fixture", o: organization ? { id: organization } : undefined })).toString("base64url")}.signature`;
}

async function orgPage(t, initialOrganization, tokenOrganization = initialOrganization) {
  const browser = await chromium.launch();
  t.after(() => browser.close());
  const context = await browser.newContext({ baseURL: "http://org.fixture" });
  const page = await context.newPage();
  await context.route("http://org.fixture/home", (route) => route.fulfill({ contentType: "text/html", body: '<div data-page-settled="true"><main>Home</main></div>' }));
  await page.goto("/home");
  await page.evaluate(({ organization, jwt }) => {
    window.Clerk = {
      organization: organization ? { id: organization } : null,
      session: { lastActiveOrganizationId: organization, getToken: async () => jwt },
      setActive: () => { throw new Error("redundant activation must not run"); },
    };
  }, { organization: initialOrganization, jwt: token(tokenOrganization) });
  return { context, page };
}

test("an already-active org is verified by a fresh token without redundant SDK activation", { timeout: 15_000 }, async (t) => {
  const { page } = await orgPage(t, "org-fixture");
  const { activateOrganization } = loadFixtures();
  await activateOrganization(page, "org-fixture");
});

test("matching Clerk resources cannot stand in for a JWT naming the requested org", { timeout: 15_000 }, async (t) => {
  const { page } = await orgPage(t, "org-fixture", "org-other");
  const { activateOrganization } = loadFixtures();
  await assert.rejects(activateOrganization(page, "org-fixture"), /fresh session JWT must name the requested organization/);
});

test("a real org switch waits for its scoped backend bootstrap and Home settlement", { timeout: 15_000 }, async (t) => {
  const { page, context } = await orgPage(t, null);
  const bootstrap = deferred();
  const release = deferred();
  await context.route("http://org.fixture/v1/bootstrap", async (route) => {
    assert.equal(route.request().headers().authorization, `Bearer ${token("org-fixture")}`);
    bootstrap.resolve();
    await release.promise;
    await route.fulfill({ json: { me: { account_id: "account-fixture" } } });
  });
  await page.evaluate((jwt) => {
    window.Clerk.setActive = async ({ organization }) => {
      window.Clerk.organization = { id: organization };
      window.Clerk.session.lastActiveOrganizationId = organization;
      window.Clerk.session.getToken = async () => jwt;
      document.querySelector("[data-page-settled]").dataset.pageSettled = "false";
      void fetch("/v1/bootstrap", { headers: { Authorization: `Bearer ${jwt}` } }).then((response) => response.json()).then(() => {
        document.querySelector("[data-page-settled]").dataset.pageSettled = "true";
      });
    };
  }, token("org-fixture"));
  const { activateOrganization } = loadFixtures();
  let returned = false;
  const activating = activateOrganization(page, "org-fixture").then(() => { returned = true; });
  void activating.catch(() => {});
  await Promise.race([bootstrap.promise, activating]);
  await expect(page.locator('[data-page-settled="false"]')).toBeAttached();
  assert.equal(returned, false, "SDK completion must not release a still-loading app scope");
  release.resolve();
  await activating;
  await expect(page.locator('[data-page-settled="true"]')).toBeAttached();
});

test("an SDK activation that never settles fails at its own finite deadline", { timeout: 15_000 }, async (t) => {
  const { page } = await orgPage(t, null);
  await page.clock.install();
  await page.evaluate(() => {
    window.Clerk.setActive = () => {
      window.activationStarted = true;
      return new Promise(() => {});
    };
  });
  const { activateOrganization } = loadFixtures();
  const activating = activateOrganization(page, "org-fixture");
  const rejected = assert.rejects(activating, /Clerk organization activation\/token did not complete/);
  // Wait for the actual operation's deadline to be registered, then advance
  // virtual browser time. This is a timeout test, never a readiness delay.
  await page.waitForFunction(() => window.activationStarted === true);
  await page.clock.fastForward(10_001);
  await rejected;
});
