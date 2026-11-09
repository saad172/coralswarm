import { expect, test } from "../lib/fixtures";
test("chat suggestions and negative feedback survive reload", async ({ page, signedIn }) => {
  const graph = await signedIn.api.personalGraph();
  await signedIn.api.seedSource(
    graph.id,
    "The Halcyon launch moved to March 14. Priya Raman owns the rollout checklist.",
  );
  await page.goto("/chats");
  await expect(page.getByRole("heading", { name: "What do you want to know?" })).toBeVisible();
  await expect(page.getByRole("button", { name: /What changed.*this week/ }).first()).toBeVisible();
  const question = page.getByRole("textbox", { name: "Question" });
  await question.fill("When is the Halcyon launch?");
  await question.press("Enter");
  const down = page.getByRole("button", { name: "Something's off" });
  await down.click();
  await page.getByRole("button", { name: "Outdated", exact: true }).click();
  await page
    .getByRole("textbox", { name: "What should it have said?" })
    .fill("The date moved again.");
  await page.getByRole("button", { name: "Send feedback" }).click();
  await expect(page.getByText("thanks, noted")).toBeVisible();
  await page.reload();
  await expect(page.getByRole("button", { name: "Something's off" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
});
