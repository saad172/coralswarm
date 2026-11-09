// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { membersAdminFixture } from "@/lib/testing/membersAdminFixture";
import type { MembersAdminPayload } from "@/lib/pageSectionTypes/membersAdmin";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, type AutoAddQueuedPreview, type AutoAddSettings, type OrgDomain } from "@/lib/api";
import { AutoAddSetting } from "./AutoAddSetting";

const identity = vi.hoisted(() => ({ app: { authReady: true, queryScope: "org-scope", getAuthToken: () => "TOKEN" as string | null } }));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));

// BLOCKER 1 fix (code review on PR #1048): `domains` is no longer fetched
// by this component — it's a prop from `members/page.tsx` now (the SAME
// list `DomainVerification.tsx` reads), so there's no `api.orgs.domains`
// left to mock here at all.
const wire = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string) => { if (path !== "/v1/load/org/members") throw new Error(`Unexpected page read ${path}`); return wire(); },
  api: {
    orgs: {
      autoAdd: { get: vi.fn(), put: vi.fn(), queuedPreview: vi.fn(), admitQueued: vi.fn() },
    },
    billing: { seatTiers: vi.fn() },
  },
}));

const putSettings = vi.mocked(api.orgs.autoAdd.put);
const queuedPreview = vi.mocked(api.orgs.autoAdd.queuedPreview);
const admitQueued = vi.mocked(api.orgs.autoAdd.admitQueued);

let payload: MembersAdminPayload;
function serveMembers(changes: Partial<MembersAdminPayload> = {}) {
  payload = { ...payload, ...changes };
  wire.mockResolvedValue(membersAdminFixture(payload));
}

function offSettings(overrides: Partial<AutoAddSettings> = {}): AutoAddSettings {
  return { enabled: false, tier: "standard", ...overrides };
}

function armedDomain(overrides: Partial<OrgDomain> = {}): OrgDomain {
  return {
    id: "dom_1",
    domain: "acme.example",
    verified: true,
    method: "dns_txt",
    verified_at: "2026-09-20T10:00:00Z",
    claimed_at: "2026-09-18T10:00:00Z",
    dns_record_name: "_coralswarm.acme.example",
    dns_record_value: "cs-verify-abc123",
    confirmation_email: null,
    confirmation_sent_at: null,
    ...overrides,
  };
}

function passer(id: string, overrides: Partial<AutoAddQueuedPreview["passers"][number]> = {}) {
  return { request_id: id, email: `${id}@acme.example`, name: null, requested_at: "2026-09-20T10:00:00Z", ...overrides };
}

/** Every test wants a DNS-armed domain by default (most of this file is
 *  about the toggle itself, not the gate) — override `domains`/`domainsErr`
 *  for the tests in the "domain gate" describe block below that are
 *  actually about the prop. `onRetryDomains` defaults to a fresh spy so any
 *  test can assert on it without wiring one up itself. */
function renderSetting(
  props: Partial<{
    domains: OrgDomain[] | null;
    domainsErr: string | null;
    onRetryDomains: () => void;
    onAdmitted: () => void;
  }> = {},
) {
  const domains = "domains" in props ? (props.domains ?? null) : [armedDomain()];
  return render(
    <AutoAddSetting
      domains={domains}
      domainsErr={props.domainsErr ?? null}
      onRetryDomains={props.onRetryDomains ?? vi.fn()}
      onAdmitted={props.onAdmitted}
    />,
  );
}

beforeEach(() => {
  identity.app = { authReady: true, queryScope: "org-scope", getAuthToken: () => "TOKEN" };
  wire.mockReset(); payload = membersAdminFixture().payload; serveMembers();
  putSettings.mockReset();
  queuedPreview.mockReset();
  admitQueued.mockReset();
  serveMembers({ seat_tiers: [{ code: "standard", credits: 1, price_usd_cents: 0 }] });
});
afterEach(() => {
  cleanup();
});

describe("AutoAddSetting — the domain gate (D11: meaningless without a verified domain)", () => {
  it("offers no toggle at all when nothing is verified by DNS", async () => {
    renderSetting({ domains: [] });
    await waitFor(() => expect(screen.getByText(/none is verified yet/i)).toBeInTheDocument());
    expect(screen.queryByRole("switch")).not.toBeInTheDocument();
  });

  it("names a claimed domain that isn't DNS-verified rather than pretending none was claimed", async () => {
    renderSetting({ domains: [armedDomain({ domain: "acme.com", method: "email" })] });
    await waitFor(() => expect(screen.getByText(/acme.com/)).toBeInTheDocument());
    expect(screen.getByText(/isn't verified that way yet/i)).toBeInTheDocument();
    expect(screen.queryByRole("switch")).not.toBeInTheDocument();
  });

  /** BLOCKER 1 fix (code review on PR #1048): `domains`/`domainsErr` are
   *  props now, not an internal fetch — this drives the failed-read state
   *  directly through them, and asserts the retry actually calls the
   *  page's own `onRetryDomains` (the shared read `DomainVerification`'s
   *  own "Try again" calls too), rather than re-fetching internally. */
  it("fails closed — no toggle — when the domain read itself fails, and 'Try again' calls the page's own retry", () => {
    const onRetryDomains = vi.fn();
    renderSetting({ domains: null, domainsErr: "Couldn't check this organization's verified domains.", onRetryDomains });
    expect(screen.getByRole("button", { name: /try again/i })).toBeInTheDocument();
    expect(screen.queryByRole("switch")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(onRetryDomains).toHaveBeenCalledTimes(1);
  });

  it("offers the working toggle, with D11's own sentence, once a domain is DNS-verified", async () => {
    renderSetting();
    await waitFor(() => expect(screen.getByRole("switch")).toBeInTheDocument());
    expect(screen.getByText("Automatically add colleagues with an @acme.example address.")).toBeInTheDocument();
  });

  // Kept from the superseded refresh-key fix (ab214634, Cursor Agent + founder),
  // whose comment made the point that matters: the gate must update WITHOUT
  // remounting, because a remount would discard an open ask-once dialog. That
  // fix passed a bumping key and had AutoAddSetting re-fetch; this one passes
  // the domains themselves, so there is no second copy of the list to go stale.
  // The guarantee is the same and is still worth a test.
  it("updates the gate when new domains arrive, without remounting", async () => {
    const { rerender } = renderSetting({ domains: [] });
    await waitFor(() => expect(screen.getByText(/none is verified yet/i)).toBeInTheDocument());
    expect(screen.queryByRole("switch")).not.toBeInTheDocument();

    rerender(
      <AutoAddSetting domains={[armedDomain()]} domainsErr={null} onRetryDomains={() => {}} />,
    );
    await waitFor(() => expect(screen.getByRole("switch")).toBeInTheDocument());
  });
});

describe("AutoAddSetting — the '#domains' escape hatch (item 5, code review on PR #1048)", () => {
  it("'Manage domains' links to '#domains' — DomainVerification.test.tsx asserts that component's root actually carries that id, so a rename on either side breaks a test", async () => {
    renderSetting({ domains: [] });
    const link = await screen.findByRole("link", { name: /manage domains/i });
    expect(link).toHaveAttribute("href", "#domains");
  });
});

describe("AutoAddSetting — flipping on with nothing queued", () => {
  it("just turns on — no dialog", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockResolvedValue({ tier: "standard", free_slots: 5, would_admit: 0, passers: [] });
    renderSetting();
    const toggle = await screen.findByRole("switch");
    fireEvent.click(toggle);

    await waitFor(() => expect(putSettings).toHaveBeenCalledWith("TOKEN", { enabled: true, tier: "standard" }));
    await waitFor(() => expect(queuedPreview).toHaveBeenCalled());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("does not check the queue when flipping OFF", async () => {
    serveMembers({ auto_add: offSettings({ enabled: true }) });
    putSettings.mockResolvedValue(offSettings({ enabled: false }));
    renderSetting();
    const toggle = await screen.findByRole("switch");
    fireEvent.click(toggle); // on -> off
    await waitFor(() => expect(putSettings).toHaveBeenCalledWith("TOKEN", { enabled: false, tier: "standard" }));
    expect(queuedPreview).not.toHaveBeenCalled();
  });

  /** THE fix (code review on PR #1041): the test this replaces was named
   *  for BOTH halves of this guarantee but only ever drove the flip-off
   *  half — it never rendered the tier `<Select>` (which only appears while
   *  `settings.enabled` is true) or fired a change on it, so `isFlipOn`
   *  being hardcoded `false` in `handleTierChange` had no test actually
   *  exercising that line. A future edit passing `settings.enabled` there
   *  instead (re-opening the ask-once dialog on every tier change) would
   *  have passed every test in this file. This one renders the real
   *  control, with a second fetched tier to change TO, and fires a real
   *  change event. */
  it("does not check the queue on a tier-only change — driven through the real <select>, not just asserted", async () => {
    serveMembers({ auto_add: offSettings({ enabled: true, tier: "standard" }) });
    serveMembers({ seat_tiers: [
      { code: "standard", credits: 1, price_usd_cents: 0 },
      { code: "pro", credits: 5, price_usd_cents: 500 },
    ] });
    putSettings.mockResolvedValue(offSettings({ enabled: true, tier: "pro" }));
    renderSetting();

    const select = await screen.findByLabelText("Auto-add slot tier");
    fireEvent.change(select, { target: { value: "pro" } });

    await waitFor(() => expect(putSettings).toHaveBeenCalledWith("TOKEN", { enabled: true, tier: "pro" }));
    expect(queuedPreview).not.toHaveBeenCalled();
    // `enabled` stayed true in the request this click sent — a tier change
    // is not a flip, on either side of the boolean.
    expect(putSettings).toHaveBeenCalledWith("TOKEN", expect.objectContaining({ enabled: true }));
  });
});

describe("AutoAddSetting — D11 'ask once' dialog", () => {
  it("all-fit: lists the people, offers one click, no shortfall language", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockResolvedValue({
      tier: "standard",
      free_slots: 5,
      would_admit: 2,
      passers: [passer("a", { name: "Ada" }), passer("b", { name: "Bea" })],
    });
    admitQueued.mockResolvedValue({ admitted: ["a", "b"], not_admitted: [] });
    const onAdmitted = vi.fn();
    renderSetting({ onAdmitted });

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    expect(screen.getByText("Ada")).toBeInTheDocument();
    expect(screen.getByText("Bea")).toBeInTheDocument();
    expect(screen.getByText("2 queued people would be added.")).toBeInTheDocument();
    expect(screen.queryByText(/slots free/)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Admit all 2" }));

    // FIX (code review on PR #1041): `admitQueued` now always takes a
    // `maxAdmit` third argument. "all-fit" caps at every passer — nothing
    // held back.
    await waitFor(() => expect(admitQueued).toHaveBeenCalledWith("TOKEN", ["a", "b"], 2));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.getByText("Added all 2.")).toBeInTheDocument();
    expect(onAdmitted).toHaveBeenCalledTimes(1);
  });

  /** THE shape from this story's own brief: 20 queued, 5 free, "Admit the 5
   *  oldest", drain promise about the other 15 — with the dialog LISTING
   *  all 20 (D11: "show the admin the queued requests"). Corrected (code
   *  review on PR #1041): the click now sends all 20 ids (not just the
   *  oldest 5) with `maxAdmit: 5` — sending fewer than were shown was the
   *  bug: `drain_org` only ever collects a candidate whose `source` has
   *  been patched to `pool_full`, which only happens for an id
   *  `admit-queued` actually saw. The button's own words stay true through
   *  the cap, not through a narrower send. */
  it("partial: matches the brief's mock, lists all 20, sends all 20 with a cap of 5", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    const passers = Array.from({ length: 20 }, (_, i) => passer(`req_${i}`));
    queuedPreview.mockResolvedValue({ tier: "standard", free_slots: 5, would_admit: 5, passers });
    // The pool shrank a little further between the preview and the click —
    // even the capped, promise-sized request can still honestly fall short.
    admitQueued.mockResolvedValue({
      admitted: passers.slice(0, 3).map((p) => p.request_id),
      not_admitted: passers.slice(3, 5).map((p) => ({ request_id: p.request_id, reason: "pool_full" })),
    });
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    // The dialog still LISTS every one of the 20 — D11's own requirement.
    expect(screen.getByText("20 queued people would be added.")).toBeInTheDocument();
    expect(screen.getByText("5 Standard slots free")).toBeInTheDocument();
    expect(screen.getByText("The other 15 stay in the queue and are added as slots free up.")).toBeInTheDocument();
    expect(screen.getByText("person@example.com")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Admit the 5 oldest" }));

    // ALL 20 ids are sent — the cap of 5 is what keeps the promise, not a
    // narrower `request_ids`.
    await waitFor(() =>
      expect(admitQueued).toHaveBeenCalledWith(
        "TOKEN",
        passers.map((p) => p.request_id),
        5,
      ),
    );
    await waitFor(() => expect(screen.getByText(/3 of the 5 were added/)).toBeInTheDocument());
    expect(screen.getByText(/the pool ran out/)).toBeInTheDocument();
  });

  /** Corrected (code review on PR #1041): this branch used to be a dead
   *  end — no button at all, while the copy still promised the backlog
   *  would be added "once a slot frees up". It now offers, and this test
   *  drives, the real action: submit every passer at `max_admit: 0`. */
  it("none-fit: offers a real 'queue them' action instead of a dead end", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockResolvedValue({
      tier: "standard",
      free_slots: 0,
      would_admit: 0,
      passers: [passer("a"), passer("b")],
    });
    admitQueued.mockResolvedValue({
      admitted: [],
      not_admitted: [
        { request_id: "a", reason: "pool_full" },
        { request_id: "b", reason: "pool_full" },
      ],
    });
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    expect(screen.getByText(/once a Standard slot frees up/)).toBeInTheDocument();
    expect(screen.getByText(/No Standard slot is free right now/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Queue all 2 for the next slot" }));

    // Every passer sent, capped at zero — nobody admitted, no slot spent,
    // but the backend still patches both to `pool_full` for the drain.
    await waitFor(() => expect(admitQueued).toHaveBeenCalledWith("TOKEN", ["a", "b"], 0));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(
      screen.getByText("Nobody was added — they're queued to be added automatically as slots free up."),
    ).toBeInTheDocument();
  });

  it("'Not now' dismisses without admitting anyone", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockResolvedValue({ tier: "standard", free_slots: 5, would_admit: 1, passers: [passer("a")] });
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "Not now" }));

    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(admitQueued).not.toHaveBeenCalled();
  });

  /** FIX 2 (code review on PR #1041): "Not now" used to strand the backlog
   *  — there was no way back into the dialog short of toggling auto-add off
   *  and back on. The standing "Review who's queued" control (visible
   *  whenever auto-add is on) re-opens it on demand. */
  it("'Not now' does not strand the backlog — 'Review who's queued' reopens the same dialog", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockResolvedValue({ tier: "standard", free_slots: 5, would_admit: 1, passers: [passer("a")] });
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "Not now" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());

    fireEvent.click(await screen.findByRole("button", { name: "Review who's queued" }));

    await screen.findByRole("dialog");
    expect(screen.getByText("1 queued person would be added.")).toBeInTheDocument();
  });

  /** FIX 2's second half: a failed preview read used to vanish with no
   *  trace — the exact same "no way back" strand as "Not now", just caused
   *  by a network failure instead of a deliberate dismissal. It now leaves
   *  a visible, retryable error rather than silently doing nothing, while
   *  still not rolling back the toggle's own already-successful save. */
  it("a failed preview read is not silent — shows a retryable error, and does not roll back the toggle", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    queuedPreview.mockRejectedValueOnce(new Error("boom"));
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await waitFor(() => expect(queuedPreview).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    // The toggle itself still reflects the successful save — not rolled back.
    expect(screen.getByRole("switch")).toHaveAttribute("aria-checked", "true");
    // The failure is visible, with a way back — not silent.
    expect(screen.getByText("Couldn't check the queued requests.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Try again" })).toBeInTheDocument();

    queuedPreview.mockResolvedValueOnce({ tier: "standard", free_slots: 5, would_admit: 1, passers: [passer("a")] });
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));

    await screen.findByRole("dialog");
    expect(queuedPreview).toHaveBeenCalledTimes(2);
  });
});

describe("AutoAddSetting — saving errors", () => {
  it("maps an unknown/inactive tier (400) to calm copy", async () => {
    const { ApiError } = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
    putSettings.mockRejectedValue(new ApiError("unknown or inactive seat tier", 400, null, null));
    renderSetting();
    fireEvent.click(await screen.findByRole("switch"));
    await waitFor(() => expect(screen.getByText("That slot tier isn't available.")).toBeInTheDocument());
  });
});

describe("AutoAddSetting — FIX 4: the admit-outcome notice never dangles '— .'", () => {
  /** 5 sent, 2 approved by hand by another admin in the meantime —
   *  `admit_queued` drops those 2 from BOTH `admitted` and `not_admitted`,
   *  so `not_admitted` comes back empty while `shortfall` is still 2. The
   *  old render was `{headline}{shortfall > 0 && " — " + clause + "."}`
   *  with `clause === ""`, showing "3 of the 5 were added. — .". */
  it("a shortfall with no reported reason is named, never rendered as a dangling em dash", async () => {
    putSettings.mockResolvedValue(offSettings({ enabled: true }));
    const passers = Array.from({ length: 5 }, (_, i) => passer(`req_${i}`));
    queuedPreview.mockResolvedValue({ tier: "standard", free_slots: 5, would_admit: 5, passers });
    admitQueued.mockResolvedValue({ admitted: passers.slice(0, 3).map((p) => p.request_id), not_admitted: [] });
    renderSetting();

    fireEvent.click(await screen.findByRole("switch"));
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "Admit all 5" }));

    await waitFor(() => expect(screen.getByText(/3 of the 5 were added/)).toBeInTheDocument());
    expect(screen.getByText(/they were no longer open requests by the time this ran/)).toBeInTheDocument();
    // The bug this replaces: `shortfall > 0` alone used to gate the em
    // dash, so an EMPTY reason clause still rendered "— .". Assert that
    // exact dangling shape is gone from the page, not just that some other
    // text is present.
    expect(document.body.textContent).not.toMatch(/—\s*\.(?!\S)/);
  });
});
