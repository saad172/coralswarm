import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";
import { __resetTransportForTests } from "./transport";

afterEach(() => { vi.unstubAllGlobals(); __resetTransportForTests(); });
describe("mailbox watched wire contract", () => {
  for (const provider of ["google", "microsoft"] as const) {
    it(`${provider} lists with an opaque encoded cursor and uses PUT to stop`, async () => {
      const fetch = vi.fn()
        .mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ threads: [], next_cursor: null }) })
        .mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ following: false }) });
      vi.stubGlobal("fetch", fetch);
      await api.connectors.watchedThreads("bearer", provider, "opaque/cursor?+");
      const [listUrl, listInit] = fetch.mock.calls[0] as [string, RequestInit];
      expect(listUrl).toContain(`/v1/connectors/${provider}/watched?limit=50&cursor=opaque%2Fcursor%3F%2B`);
      expect(new Headers(listInit.headers).get("Authorization")).toBe("Bearer bearer");
      const response = await api.connectors.setThreadFollow("bearer", provider, "thread/id?+", false);
      expect(response.following).toBe(false);
      const [stopUrl, stopInit] = fetch.mock.calls[1] as [string, RequestInit];
      expect(stopUrl).toContain(`/v1/connectors/${provider}/watched/thread%2Fid%3F%2B`);
      expect(stopInit.method).toBe("PUT");
      expect(JSON.parse(stopInit.body as string)).toEqual({ follow: false });
    });
  }
});
