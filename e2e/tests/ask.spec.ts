import { expect, test } from "../lib/fixtures";
import { controlAskProgress } from "../lib/askProgressDelivery";

// Journey (a): a signed-in user asks a question in an ocean that holds
// content and gets an answer that cites it.
for (const delivery of ['gated', 'coalesced'] as const) test(`ask in an ocean with content renders a cited answer (${delivery} progress)`, async ({ page, signedIn }) => {
  const graph = await signedIn.api.personalGraph();
  await signedIn.api.seedSource(
    graph.id,
    "The Halcyon launch moved to March 14 after the security review. Priya Raman owns the rollout checklist.",
  );

  await page.goto(`/chats?graph_id=${graph.id}`);
  await controlAskProgress(page, delivery);
  const question = page.getByRole("textbox", { name: "Question" });
  await expect(question).toBeEnabled();
  await page.getByRole("button", { name: "Answer settings", exact: true }).click();
  await page.getByRole("tab", { name: "Detailed", exact: true }).click();
  await page.keyboard.press("Escape");
  const request = page.waitForRequest(r => r.method() === "POST" && r.url().includes("/v1/queries"));
  await question.fill("When is the Halcyon launch?");
  await page.evaluate(() => {
    const events: string[] = [];
    (window as unknown as { answerEvents: string[] }).answerEvents = events;
    const observer = new MutationObserver(() => {
      for (const label of ["Searching your workspaces", "Reading", "Writing answer", "Checking citations"]) {
        if (Array.from(document.querySelectorAll('[aria-live="polite"]')).some((node) => node.textContent?.includes(label)) && !events.includes(label)) events.push(label);
      }
      if (document.querySelector(".cite") && !events.includes("answer")) events.push("answer");
    });
    observer.observe(document.body, { subtree: true, childList: true, characterData: true });
  });
  await question.press("Enter");
  expect((await request).postDataJSON().style).toBe("detailed");

  const sources = page.getByRole("region", { name: "Answer sources" });
  await expect(sources).toBeVisible({ timeout: 20_000 });
  await expect(sources.getByRole("button", { name: /^Open source 1:/ })).toBeVisible();
  expect(await page.evaluate(() => (window as unknown as { askWireStages: string[] }).askWireStages)).toEqual([
    'searching', 'retrieved', 'writing', 'checking',
  ]);
  if (delivery === 'gated') {
    await expect.poll(() => page.evaluate(() => (window as unknown as { answerEvents: string[] }).answerEvents)).toEqual([
      "Searching your workspaces", "Reading", "Writing answer", "Checking citations", "answer",
    ]);
  }
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await sources.getByRole("button", { name: /^Open source 1:/ }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  // The answer prose carries an inline citation chip for the passage it used.
  await expect(page.locator(".cite").first()).toBeVisible();
  await expect(page.getByText(/Halcyon launch moved to March 14/).first()).toBeVisible();
});
