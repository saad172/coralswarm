import { afterEach, describe, expect, it, vi } from "vitest";
import { api, ApiError, type AddinToken } from "./api";
import { __resetTransportForTests } from "./transport";
import {
  addinListErrorCopy,
  toTokenRows,
  withoutToken,
} from "@/components/connectors/addinTokensModel";

/* The Gmail Add-on token section, driven through a MOCKED `fetch` — the same
 * seam `api.test.ts` uses, and the closest this project gets to a component
 * test (there is no DOM harness here; see `vitest.config.ts`). What each case
 * asserts is the thing a render would show: the rows, the row that a revoke
 * removes, and the empty state's input.
 *
 * Asserting on the URL/method `fetch` ACTUALLY receives, not on the arguments
 * handed to `api.addin.*`, is the point: the sibling backend PR's contract is
 * `GET /v1/addin/tokens` and `DELETE /v1/addin/tokens/{id}`, and a client that
 * drifted off either would fail only in production. */

function mockFetch(...responses: Array<{ status: number; body?: unknown }>) {
  const fetchMock = vi.fn();
  for (const r of responses) {
    fetchMock.mockResolvedValueOnce({
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      json: async () => r.body ?? {},
    });
  }
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const tokens: AddinToken[] = [
  {
    id: "11111111-1111-1111-1111-111111111111",
    name: "Gmail Add-on",
    created_at: "2026-09-03T15:00:00Z",
    last_used_at: "2026-09-10T18:30:00Z",
  },
  {
    id: "22222222-2222-2222-2222-222222222222",
    name: "Gmail Add-on",
    created_at: "2026-09-05T15:00:00Z",
    last_used_at: null,
  },
];

afterEach(() => {
  vi.unstubAllGlobals();
  // The transport keeps a per-origin circuit breaker and in-flight state
  // across calls; a failure case in one test must not bleed into the next.
  __resetTransportForTests();
});

describe("the list renders", () => {
  it("GETs /v1/addin/tokens with the bearer and turns the payload into rows", async () => {
    const fetchMock = mockFetch({ status: 200, body: { tokens } });

    const { tokens: list } = await api.addin.listTokens("tok");
    const rows = toTokenRows(list);

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/v1/addin/tokens");
    expect(init.method ?? "GET").toBe("GET");
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer tok");

    expect(rows).toEqual([
      {
        id: tokens[1].id,
        name: "Gmail Add-on",
        createdLabel: "9/5/2026",
        lastUsedLabel: "never",
      },
      {
        id: tokens[0].id,
        name: "Gmail Add-on",
        createdLabel: "9/3/2026",
        lastUsedLabel: "9/10/2026",
      },
    ]);
  });
});

describe("revoke removes the row", () => {
  it("DELETEs /v1/addin/tokens/{id} and the list loses exactly that row", async () => {
    const fetchMock = mockFetch({ status: 200, body: { tokens } }, { status: 204 });

    const { tokens: list } = await api.addin.listTokens("tok");
    expect(toTokenRows(list)).toHaveLength(2);

    await api.addin.revokeToken("tok", tokens[0].id);
    const after = withoutToken(list, tokens[0].id);

    const [url, init] = fetchMock.mock.calls[1] as [string, RequestInit];
    expect(url).toContain(`/v1/addin/tokens/${tokens[0].id}`);
    expect(init.method).toBe("DELETE");

    const rows = toTokenRows(after);
    expect(rows).toHaveLength(1);
    expect(rows.map((r) => r.id)).toEqual([tokens[1].id]);
  });

  it("'Revoke all' issues one DELETE per token and empties the list", async () => {
    const fetchMock = mockFetch({ status: 204 }, { status: 204 });

    let remaining = tokens;
    for (const t of tokens) {
      await api.addin.revokeToken("tok", t.id);
      remaining = withoutToken(remaining, t.id);
    }

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect((fetchMock.mock.calls[0] as [string, RequestInit])[0]).toContain(tokens[0].id);
    expect((fetchMock.mock.calls[1] as [string, RequestInit])[0]).toContain(tokens[1].id);
    expect(toTokenRows(remaining)).toEqual([]);
  });
});

describe("the empty state", () => {
  it("an empty `tokens` array produces no rows (and no error)", async () => {
    mockFetch({ status: 200, body: { tokens: [] } });
    const { tokens: list } = await api.addin.listTokens("tok");
    expect(toTokenRows(list)).toEqual([]);
  });
});

describe("degradation, not a crash, while the sibling backend PR is unmerged", () => {
  /* Production answers 404 for this route until that PR deploys. The client
     must surface a REJECTION the section can catch and render as its own
     copy — not throw something unhandled, and not resolve to an empty list
     that would render as "you have no tokens". */
  it("a 404 rejects with an ApiError the section turns into 'not available yet' copy", async () => {
    mockFetch({ status: 404, body: { error: "not found" } });

    const err = await api.addin.listTokens("tok").then(
      () => null,
      (e: unknown) => e,
    );

    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(404);
    expect(addinListErrorCopy((err as ApiError).status)).toContain(
      "isn't available on this backend yet",
    );
  });

  it("an org-context 400 rejects rather than rendering as an empty list", async () => {
    mockFetch({
      status: 400,
      body: { error: "add-on pairing is available in your personal workspace only" },
    });

    const err = await api.addin.listTokens("tok").then(
      () => null,
      (e: unknown) => e,
    );

    expect((err as ApiError).status).toBe(400);
  });
});
