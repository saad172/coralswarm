import { afterEach, expect, it, vi } from "vitest";
import { credentialScope, sameCredentialScope } from "./credentialScope";
import { api, ApiError, setTokenRefresher } from "./api";
import { guardedFetch } from "./transport";
vi.mock("./transport", async () => ({ ...(await vi.importActual<typeof import("./transport")>("./transport")), guardedFetch: vi.fn() }));
const token = (claims: object) => `h.${btoa(JSON.stringify(claims))}.s`;
const original = token({ sub: "user", org_id: "org", iss: "issuer", iat: 1 });
afterEach(() => { setTokenRefresher(null); vi.resetAllMocks(); });
it("recognizes both Clerk org encodings and rejects opaque/malformed claims", () => {
  expect(credentialScope(token({ sub: "user", o: { id: "org" }, iss: "issuer" }))).toEqual({ subject: "user", org: "org", issuer: "issuer" });
  expect(sameCredentialScope(original, token({ sub: "user", o: { id: "org" }, iss: "issuer", iat: 2 }))).toBe(true);
  for (const bad of ["opaque", "h.invalid.s", token({ sub: "user" }), token({ sub: "user", iss: "issuer", org_id: {} })]) expect(credentialScope(bad)).toBeNull();
});
it.each(["subject", "org", "issuer", "opaque"])("never replays either generic client shape after %s changes", async (kind) => {
  const claims = { sub: "user", org_id: "org", iss: "issuer", iat: 2 };
  if (kind === "subject") claims.sub = "other";
  if (kind === "org") claims.org_id = "other";
  if (kind === "issuer") claims.iss = "other";
  setTokenRefresher(async () => kind === "opaque" ? "opaque" : token(claims));
  vi.mocked(guardedFetch).mockImplementation(async () => new Response(JSON.stringify({ error: "Expired" }), { status: 401 }));
  await expect(api.graphs.atoms(original, "graph")).rejects.toBeInstanceOf(ApiError);
  await expect(api.graphs.atomsPage(original, "graph")).rejects.toBeInstanceOf(ApiError);
  expect(guardedFetch).toHaveBeenCalledTimes(2);
});
it("permits one refresh for the same authenticated principal", async () => {
  const fresh = token({ sub: "user", org_id: "org", iss: "issuer", iat: 2 });
  setTokenRefresher(async () => fresh);
  vi.mocked(guardedFetch).mockResolvedValueOnce(new Response("{}", { status: 401 })).mockResolvedValueOnce(new Response("[]"));
  await expect(api.graphs.atoms(original, "graph")).resolves.toEqual([]);
  expect(vi.mocked(guardedFetch).mock.calls[1][1]?.headers).toMatchObject({ Authorization: `Bearer ${fresh}` });
});
