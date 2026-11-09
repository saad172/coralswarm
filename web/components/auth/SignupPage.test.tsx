// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { render } from "@/lib/testing/queryRender";
import SignupPage from "@/app/signup/page";

const state = vi.hoisted(() => ({ loaded: false, signedIn: false, replace: vi.fn(), read: vi.fn() }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ replace: state.replace }), useSearchParams: () => new URLSearchParams({ pt: "opaque-token" }) }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => ({ isLoaded: state.loaded, isSignedIn: state.signedIn }),
  SignUp: ({ initialValues }: { initialValues?: { emailAddress?: string } }) => <div data-testid="signup">{initialValues?.emailAddress ?? "empty form"}</div> }));
vi.mock("@/components/auth/AuthShell", () => ({ default: ({ children }: { children: React.ReactNode }) => <>{children}</> }));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: (...args: unknown[]) => state.read(...args) }));
afterEach(() => { cleanup(); state.loaded = false; state.signedIn = false; state.read.mockReset(); state.replace.mockClear(); });

it.each(["loading", "signed-in"])("does not fetch public prefill or mount signup while %s", async (kind) => {
  state.loaded = kind === "signed-in"; state.signedIn = kind === "signed-in";
  state.read.mockResolvedValue({ email: null });
  render(<SignupPage />);
  await act(async () => { await Promise.resolve(); });
  expect(state.read).not.toHaveBeenCalled();
  expect(screen.queryByTestId("signup")).not.toBeInTheDocument();
  if (state.signedIn) await waitFor(() => expect(state.replace).toHaveBeenCalledWith("/home"));
});

it("keeps the opaque-token email prefill for a loaded signed-out visitor", async () => {
  state.loaded = true;
  state.read.mockResolvedValue({ email: "visitor@example.test" });
  render(<SignupPage />);
  expect(await screen.findByText("visitor@example.test")).toBeInTheDocument();
  expect(state.read).toHaveBeenCalledTimes(1);
  expect(state.read.mock.calls[0][0]).toBe("/v1/email/prefill?t=opaque-token");
  expect(state.replace).not.toHaveBeenCalled();
});
