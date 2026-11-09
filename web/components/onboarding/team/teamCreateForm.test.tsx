// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, type OrgCreateResult } from "@/lib/api";
import { readAttempt, type AttemptStore, type KV } from "@/lib/orgSelfserve";
import { TeamCreateForm, setSleepForTests } from "./TeamCreateForm";

function memStore(): AttemptStore {
  const data = new Map<string, string>();
  const kv: KV = {
    getItem: (k) => data.get(k) ?? null,
    setItem: (k, v) => void data.set(k, v),
    removeItem: (k) => void data.delete(k),
  };
  return { kv, user: "user_1" };
}

function result(p: Partial<OrgCreateResult> = {}): OrgCreateResult {
  return {
    org_id: "org_new",
    status: "awaiting_payment",
    checkout_url: "https://checkout.stripe.com/c/pay/cs_1",
    session_id: "cs_1",
    trial: true,
    seats: 3,
    trial_credits: 3000,
    trial_days: 90,
    resume_payment: false,
    replayed: false,
    ...p,
  };
}

let keys = 0;
type Props = Parameters<typeof TeamCreateForm>[0];
type AnyMock = ReturnType<typeof vi.fn>;

function setup(createMock: AnyMock, extra: { trialCheckout?: AnyMock; goToCheckout?: AnyMock } = {}) {
  const create = createMock as unknown as Props["create"];
  const trialCheckout = (extra.trialCheckout ?? vi.fn()) as unknown as Props["trialCheckout"];
  const storage = memStore();
  const goToCheckout = (extra.goToCheckout ?? vi.fn()) as AnyMock & Props["goToCheckout"];
  const goTo = vi.fn();
  render(
    <TeamCreateForm
      create={create}
      trialCheckout={trialCheckout}
      goToCheckout={goToCheckout}
      goTo={goTo}
      returnUrl={(path, q) => `https://app.test${path}?${q}`}
      priceCents={2000}
      store={storage}
      mintKey={() => `test-key-${++keys}`}
    />,
  );
  return { storage, goToCheckout, goTo };
}

function fill(name: string, seats: string) {
  fireEvent.change(screen.getByLabelText(/team name/i), { target: { value: name } });
  fireEvent.change(screen.getByLabelText(/seats/i), { target: { value: seats } });
}

const submit = () => fireEvent.click(screen.getByRole("button", { name: /continue to payment/i }));

beforeEach(() => {
  setSleepForTests(() => Promise.resolve());
});
afterEach(cleanup);

describe("TeamCreateForm", () => {
  it("shows the trial copy with the seat count and price, defaulting to 2 seats", () => {
    setup(vi.fn());
    expect(screen.getByLabelText(/seats/i)).toHaveValue(2);
    expect(screen.getByText(/you won't be charged today/)).toBeInTheDocument();
    expect(screen.getByText(/for 2 seats at \$20 per seat per month/)).toBeInTheDocument();
    fill("Acme", "5");
    expect(screen.getByText(/for 5 seats at \$20 per seat per month/)).toBeInTheDocument();
  });

  it("validates before calling the server", () => {
    const create = vi.fn();
    setup(create);
    fill("", "1");
    submit();
    expect(create).not.toHaveBeenCalled();
    expect(screen.getByText(/Use 2 to 120 characters/)).toBeInTheDocument();
    expect(screen.getByText(/Pick between 2 and 1,000 seats/)).toBeInTheDocument();
  });

  it("creates with a key and return URLs carrying the attempt, then goes to Stripe", async () => {
    const create = vi.fn().mockResolvedValue(result());
    const { storage, goToCheckout } = setup(create);
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goToCheckout).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_1"));
    const [body, key] = create.mock.calls[0];
    expect(body).toMatchObject({ name: "Acme", seats: 3 });
    expect(body.success_url).toMatch(/^https:\/\/app\.test\/onboarding\/team\/return\?k=/);
    expect(body.cancel_url).toMatch(/^https:\/\/app\.test\/onboarding\/team\/canceled\?k=/);
    expect(body.success_url).toContain(encodeURIComponent(key));
    expect(readAttempt(storage)).toMatchObject({ key, orgId: "org_new" });
  });

  it("trial:false shows the repeat-trial copy and waits for a click", async () => {
    const create = vi.fn().mockResolvedValue(result({ trial: false, seats: 4 }));
    const { goToCheckout } = setup(create);
    fill("Acme", "4");
    submit();
    expect(await screen.findByText(/already used a free trial.*4 seats × \$20 now/)).toBeInTheDocument();
    expect(goToCheckout).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /continue to payment/i }));
    expect(goToCheckout).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_1");
  });

  it("resends the SAME key after create_in_progress, then proceeds", async () => {
    const create = vi
      .fn()
      .mockRejectedValueOnce(new ApiError("busy", 409, "create_in_progress"))
      .mockResolvedValueOnce(result({ replayed: true }));
    const { goToCheckout } = setup(create);
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goToCheckout).toHaveBeenCalled());
    expect(create).toHaveBeenCalledTimes(2);
    expect(create.mock.calls[1][1]).toBe(create.mock.calls[0][1]);
  });

  it("a retry after a dropped connection reuses the key", async () => {
    const create = vi.fn().mockRejectedValueOnce(new TypeError("fetch failed")).mockResolvedValueOnce(result());
    setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByText(/won't create a second team/)).toBeInTheDocument();
    submit();
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create.mock.calls[1][1]).toBe(create.mock.calls[0][1]);
  });

  it("a Clerk configuration refusal shows a generic error, not the name-field error", async () => {
    const create = vi.fn().mockRejectedValueOnce(new ApiError("unavailable", 503, "org_create_unavailable"));
    setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByText(/couldn't create your team right now/)).toBeInTheDocument();
    expect(screen.queryByText(/Try a different name/)).not.toBeInTheDocument();
  });

  it("a server field error lands under the field, and the next try uses a new key", async () => {
    const create = vi.fn().mockRejectedValueOnce(new ApiError("bad", 422, "org_create_refused")).mockResolvedValueOnce(result());
    setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByText(/couldn't create a team with that name/)).toBeInTheDocument();
    submit();
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create.mock.calls[1][1]).not.toBe(create.mock.calls[0][1]);
  });

  it("honours retry_after_s: the button stays off and says when", async () => {
    const create = vi
      .fn()
      .mockRejectedValue(new ApiError("slow down", 429, "org_create_rate_limited", null, null, { retry_after_s: 120 }));
    setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByText(/create another in about 2 minutes/)).toBeInTheDocument();
    expect(screen.getByText(/ready again in/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /continue to payment/i })).toBeDisabled();
  });

  it("checkout_url null with resume_payment calls trial-checkout", async () => {
    const create = vi.fn().mockResolvedValue(result({ checkout_url: null, resume_payment: true }));
    const trialCheckout = vi.fn().mockResolvedValue({
      checkout_url: "https://checkout.stripe.com/c/pay/cs_2",
      session_id: "cs_2",
      trial: true,
      seats: 3,
      trial_credits: 3000,
      trial_days: 90,
    });
    const { goToCheckout } = setup(create, { trialCheckout });
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goToCheckout).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_2"));
    expect(trialCheckout.mock.calls[0][0]).toBe("org_new");
    expect(trialCheckout.mock.calls[0][1]).toMatchObject({ seats: 3 });
  });

  it("checkout already complete goes to the return page", async () => {
    const create = vi.fn().mockResolvedValue(result({ checkout_url: null, status: "provisioning", trial: null }));
    const { goTo } = setup(create);
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goTo).toHaveBeenCalledWith(expect.stringMatching(/\/onboarding\/team\/return\?k=/)));
  });

  it("409 signup_complete goes to that team's status page, no error", async () => {
    const create = vi
      .fn()
      .mockRejectedValue(new ApiError("paid", 409, "signup_complete", null, null, { code: "signup_complete", org_id: "org_2paidTEAM1" }));
    const { goTo, storage } = setup(create);
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goTo).toHaveBeenCalledWith(expect.stringMatching(/\/onboarding\/team\/return\?.*org=org_2paidTEAM1/)));
    expect(readAttempt(storage)?.orgId).toBe("org_2paidTEAM1");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("create_in_progress waits the server's retry_after_s before resending", async () => {
    const waits: number[] = [];
    setSleepForTests(async (ms) => void waits.push(ms));
    const create = vi
      .fn()
      .mockRejectedValueOnce(new ApiError("busy", 409, "create_in_progress", null, null, { retry_after_s: 4 }))
      .mockResolvedValueOnce(result({ replayed: true }));
    const { goToCheckout } = setup(create);
    fill("Acme", "3");
    submit();
    await waitFor(() => expect(goToCheckout).toHaveBeenCalled());
    expect(waits).toEqual([4000]);
  });

  it("S3: a code-less 400 keeps the key, so trying again can't make a second team", async () => {
    const create = vi.fn().mockRejectedValueOnce(new ApiError("bad", 400, null)).mockResolvedValueOnce(result());
    const { storage } = setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByText(/Couldn't start checkout/)).toBeInTheDocument();
    expect(readAttempt(storage)).not.toBeNull();
    submit();
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create.mock.calls[1][1]).toBe(create.mock.calls[0][1]);
  });

  it("checkout_complete links to the team's setup page", async () => {
    const create = vi.fn().mockRejectedValue(new ApiError("done", 409, "checkout_complete"));
    setup(create);
    fill("Acme", "3");
    submit();
    const link = await screen.findByRole("link", { name: /see your team.s setup/i });
    expect(link.getAttribute("href")).toMatch(/^\/onboarding\/team\/return\?k=/);
  });

  it("a 410 offers Start again", async () => {
    const create = vi.fn().mockRejectedValue(new ApiError("gone", 410, "checkout_expired"));
    const { storage } = setup(create);
    fill("Acme", "3");
    submit();
    expect(await screen.findByRole("button", { name: /start again/i })).toBeInTheDocument();
    expect(readAttempt(storage)).toBeNull();
  });
});
