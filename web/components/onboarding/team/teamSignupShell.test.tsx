// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, screen } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import { TeamSignupShell } from "./TeamSignupShell";

const mocks = vi.hoisted(() => ({ bootstrap: vi.fn(), seatTiers: vi.fn() }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string) => {
    if (path === "/v1/bootstrap") return mocks.bootstrap();
    if (path === "/v1/billing/seat-tiers") return mocks.seatTiers();
    throw new Error(`Unexpected page read ${path}`);
  },
  api: { bootstrap: mocks.bootstrap, billing: { seatTiers: mocks.seatTiers } },
}));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({
    ready: true,
    authReady: true,
    bootError: null,
    retryBoot: vi.fn(),
    getAuthToken: () => "personal-token",
    queryScope: "user_1::personal",
    userId: "user_1",
  }),
}));
vi.mock("@clerk/nextjs", () => ({ useClerk: () => ({ setActive: vi.fn() }) }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ replace: vi.fn() }) }));

afterEach(() => {
  cleanup();
  mocks.bootstrap.mockReset();
  mocks.seatTiers.mockReset();
});

function page(continueWhenFlagUnknown: boolean) {
  mocks.seatTiers.mockResolvedValue([]);
  render(
    <TeamSignupShell continueWhenFlagUnknown={continueWhenFlagUnknown}>
      {() => <p>the page itself</p>}
    </TeamSignupShell>,
  );
}

describe("TeamSignupShell flag gate", () => {
  it("S4: a failed flag read is unknown, not off: the return pages carry on", async () => {
    mocks.bootstrap.mockRejectedValue(new ApiError("down", 503, null));
    page(true);
    expect(await screen.findByText("the page itself", {}, { timeout: 5_000 })).toBeInTheDocument();
    expect(screen.queryByText(/isn't open yet/)).toBeNull();
  });

  it("S4: a failed flag read on the create form offers a retry, not 'not open'", async () => {
    mocks.bootstrap.mockRejectedValue(new ApiError("down", 503, null));
    page(false);
    expect(await screen.findByText(/couldn't check whether team sign-up is open/, {}, { timeout: 5_000 })).toBeInTheDocument();
    expect(screen.queryByText(/isn't open yet/)).toBeNull();
    // Query retries are off; the transport owns actual network retries.
    expect(mocks.bootstrap).toHaveBeenCalledTimes(1);
  });

  it("an explicit off still shows the 'not open yet' card, on every page", async () => {
    mocks.bootstrap.mockResolvedValue({ me: {}, graphs: [], join_requests_count: null, selfserve_signup_enabled: false });
    page(true);
    expect(await screen.findByText(/isn't open yet/)).toBeInTheDocument();
    expect(screen.queryByText("the page itself")).toBeNull();
  });

  it("on: renders the page", async () => {
    mocks.bootstrap.mockResolvedValue({ me: {}, graphs: [], join_requests_count: null, selfserve_signup_enabled: true });
    page(false);
    expect(await screen.findByText("the page itself")).toBeInTheDocument();
  });
});
