// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, ApiError } from "./api";
import { __resetTransportForTests } from "./transport";

function mockFetch(status: number, body: unknown = {}) {
  const f = vi.fn().mockResolvedValue({ ok: status < 300, status, json: async () => body, blob: async () => new Blob(["x"]) });
  vi.stubGlobal("fetch", f);
  return f;
}
const assign = vi.fn();
function at(pathname: string) {
  Object.defineProperty(window, "location", { configurable: true, value: { pathname, assign } });
}

beforeEach(() => at("/chats"));
afterEach(() => {
  vi.unstubAllGlobals();
  assign.mockClear();
  __resetTransportForTests();
});

describe("org deletion lock routing", () => {
  it("sends a member of a deleting org to the org deletion page", async () => {
    mockFetch(403, { code: "org_deletion_pending" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).toHaveBeenCalledWith("/org/deletion");
  });

  it("sends the after-grace refusal there too", async () => {
    mockFetch(403, { code: "org_deletion_in_progress" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).toHaveBeenCalledWith("/org/deletion");
  });

  it("does not redirect from the org deletion page itself", async () => {
    at("/org/deletion");
    mockFetch(403, { code: "org_deletion_pending" });
    await expect(api.me("tok")).rejects.toBeInstanceOf(ApiError);
    expect(assign).not.toHaveBeenCalled();
  });
});

describe("org deletion wire shapes", () => {
  it("names the org in the path; the request sends confirm_name; undo is a bodyless POST", async () => {
    let f = mockFetch(202, { deletion: { id: "d", state: "requested" } });
    await api.orgDeletion.request("tok", "org_1", "Acme");
    let [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/v1\/orgs\/org_1\/deletion$/);
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({ confirm_name: "Acme" });

    f = mockFetch(200, { status: "canceled" });
    await api.orgDeletion.cancel("tok", "org_1");
    [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/v1\/orgs\/org_1\/deletion\/cancel$/);
    expect(init.method).toBe("POST");
    expect(init.body).toBeUndefined();
  });

  it("downloads the export with the bearer and surfaces its error code", async () => {
    let f = mockFetch(200);
    const blob = await api.orgDeletion.exportFile("tok", "org_1");
    expect(blob).toBeInstanceOf(Blob);
    const [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/v1\/orgs\/org_1\/deletion\/export$/);
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer tok");

    f = mockFetch(409, { code: "export_closed", error: "closed" });
    await expect(api.orgDeletion.exportFile("tok", "org_1")).rejects.toMatchObject({ code: "export_closed", status: 409 });
    expect(f).toHaveBeenCalled();
  });
});
