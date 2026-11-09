// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, ApiError } from "./api";
import { __resetTransportForTests } from "./transport";

function mockFetch(status: number, body: unknown = {}) {
  const f = vi.fn().mockResolvedValue({ ok: status < 300, status, json: async () => body });
  vi.stubGlobal("fetch", f);
  return f;
}
const assign = vi.fn();
function at(pathname: string) {
  Object.defineProperty(window, "location", { configurable: true, value: { pathname, assign } });
}

beforeEach(() => at("/settings"));
afterEach(() => {
  vi.unstubAllGlobals();
  assign.mockClear();
  __resetTransportForTests();
});

describe("account deletion lock routing", () => {
  it("sends a pending-deletion 403 to the scheduled page", async () => {
    mockFetch(403, { code: "account_deletion_pending" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).toHaveBeenCalledWith("/account/delete/pending");
  });

  it("sends an in-progress 403 to the same page", async () => {
    mockFetch(403, { code: "account_deletion_in_progress" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).toHaveBeenCalledWith("/account/delete/pending");
  });

  it("sends 401 account_deleted to /account-deleted", async () => {
    mockFetch(401, { code: "account_deleted" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).toHaveBeenCalledWith("/account-deleted");
  });

  it("does not redirect from the deletion pages themselves", async () => {
    at("/account/delete/pending");
    mockFetch(403, { code: "account_deletion_pending" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).not.toHaveBeenCalled();
  });

  it("leaves ordinary errors alone", async () => {
    mockFetch(403, { code: "forbidden" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).not.toHaveBeenCalled();
  });
});

describe("delete-account wire shapes", () => {
  it("POST sends confirm_email; cancel is a bodyless POST; token cancel is unauthenticated", async () => {
    let f = mockFetch(202, { id: "d", status: "grace", deletion: {} });
    await api.meDeleteAccount.post("tok", "a@b.co");
    let [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/v1/me/delete-account");
    expect(init.body).toBe(JSON.stringify({ confirm_email: "a@b.co" }));

    f = mockFetch(200, { status: "cancelled" });
    await api.meDeleteAccount.cancel("tok");
    [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/v1/me/delete-account/cancel");
    expect(init.method).toBe("POST");
    expect(init.body).toBeUndefined();

    f = mockFetch(200, { status: "cancelled" });
    await api.accountDeletion.cancelWithToken("t0k");
    [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/v1/account-deletion/cancel");
    expect((init.headers as Record<string, string>).Authorization).toBeUndefined();
    expect(init.body).toBe(JSON.stringify({ token: "t0k" }));
  });

  it("exposes the 409 body on ApiError", async () => {
    mockFetch(409, { code: "shared_personal_account", members: [{ user_id: "u" }] });
    const e = await api.meDeleteAccount.post("tok", "a@b.co").catch((x) => x);
    expect(e.code).toBe("shared_personal_account");
    expect(e.body.members).toHaveLength(1);
  });
});
