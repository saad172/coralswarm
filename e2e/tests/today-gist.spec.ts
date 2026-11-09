import { waitForComputed } from "../lib/readModels";
import { expect, test } from "../lib/fixtures";
import { personalUser } from "../lib/isolation";

// Today gist (GET /v1/journal/gist), API level. Contract:
// docs/implementation/journal-gist-api.md. The stack boots the backend with
// TODAY_GIST_ENABLED=1 (stack/config.mjs). The compile runs in the journal
// sweep, which spends nothing here (managed enrichment is off in the stack),
// so a fresh period answers `gist: null`; the scenario proves the contract,
// the period math, the activity span and that one user can never name another
// user's ocean.

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

test.describe("Today gist", () => {
  test.describe.configure({ timeout: 180_000 });
  test("the day's gist answers with its period and span, and stays private to its owner", async ({ request }) => {
    const [owner, other] = await Promise.all([personalUser(request, "gist-owner"), personalUser(request, "gist-other")]);
    const ocean = await owner.api.personalGraph();
    await other.api.graphs();
    await owner.api.ingest(ocean.id, "The usage tier is still unpriced; Acme gets a soft cap.");

    const day = todayUtc();
    const mine = (await waitForComputed(owner.api, `/v1/journal/gist?granularity=day&anchor=${day}&tz=UTC`, true, p => !!p.span?.first_activity)).payload;
    expect(mine).toMatchObject({
      period_start: `${day}T00:00:00Z`,
      open: true,
      who: "mine",
      granularity: "day",
      tz: "UTC",
      gist: null,
      compiled_at: null,
    });
    expect(new Date(mine.period_end).getTime() - new Date(mine.period_start).getTime()).toBe(24 * 3600_000);
    // The span is the owner's own captured activity today.
    expect(mine.span?.first_activity, JSON.stringify(mine)).toBeTruthy();
    expect(new Date(mine.span.last_activity).getTime()).toBeGreaterThanOrEqual(new Date(mine.span.first_activity).getTime());

    // Week and month resolve their own periods (weeks start Monday).
    const week = (await waitForComputed(owner.api, `/v1/journal/gist?granularity=week&anchor=${day}&tz=UTC`)).payload;
    expect(new Date(week.period_start).getUTCDay()).toBe(1);
    const month = (await waitForComputed(owner.api, `/v1/journal/gist?granularity=month&anchor=${day}&tz=UTC`)).payload;
    expect(month.period_start).toBe(`${day.slice(0, 8)}01T00:00:00Z`);

    // Team on the owner's own ocean: answered (no second member, so no gist).
    const team = (await waitForComputed(owner.api, `/v1/journal/gist?who=team&graph_id=${ocean.id}&tz=UTC`)).payload;
    expect(team).toMatchObject({ who: "team", gist: null });

    // The other user: no activity of the owner's, and naming the owner's
    // ocean is a 404 for both views.
    const theirs = (await waitForComputed(other.api, `/v1/journal/gist?anchor=${day}&tz=UTC`)).payload;
    expect(theirs.span).toBeNull();
    for (const who of ["mine", "team"]) {
      const res = await other.api.call("GET", `/v1/journal/gist?who=${who}&graph_id=${ocean.id}`);
      expect(res.status()).toBe(404);
    }
  });

  test("the parameters are validated", async ({ request }) => {
    const user = await personalUser(request, "gist-valid");
    await user.api.graphs();
    const status = async (query: string) => (await user.api.call("GET", `/v1/journal/gist?${query}`)).status();
    expect(await status("granularity=hour")).toBe(400);
    expect(await status("anchor=2026-02-30")).toBe(400);
    expect(await status("who=everyone")).toBe(400);
    expect(await status("tz=Mars/Base")).toBe(400);
    expect(await status("who=team")).toBe(400); // team needs graph_id
    expect(await status("anchor=2026-10-01&tz=UTC")).toBe(200);
  });
});
