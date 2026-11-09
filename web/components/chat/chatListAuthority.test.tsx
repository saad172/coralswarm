// @vitest-environment jsdom
import { act, cleanup, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { renderHook } from "@/lib/testing/queryRender";
import { useQueryClient } from "@tanstack/react-query";
import { ApiError, apiRequest } from "@/lib/api";
import { useChatThread } from "./useChatThread";
const identity = vi.hoisted(() => ({ scope: "u:personal", token: () => "token", graphs: [] }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: identity.scope,
  queryScope: identity.scope, ready: true, getToken: identity.token, graphs: identity.graphs }) }));
vi.mock("@/lib/api", async (actual) => ({ ...await actual<typeof import("@/lib/api")>(), apiRequest: vi.fn() }));
const wire = vi.mocked(apiRequest);
const envelope = { model: "conversation_list", version: 1, payload: { conversations: [{ id: "11111111-1111-4111-8111-111111111111", title: "Private title" }], shared: [] }, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null };
afterEach(() => { cleanup(); wire.mockReset(); identity.scope = "u:personal"; });
it.each(["withheld", "refused", "invalid payload"])("immediately removes titles after %s and never revives them on 503 or scope switch", async (kind) => {
  wire.mockResolvedValue(envelope);
  const hook = renderHook(() => ({ thread: useChatThread(null), qc: useQueryClient() }));
  await waitFor(() => expect(hook.result.current.thread.conversations[0]?.title).toBe("Private title"));
  if (kind === "withheld") wire.mockResolvedValue({ ...envelope, computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:02Z", payload: { conversations: [], shared: [] } });
  else if (kind === "invalid payload") wire.mockResolvedValue({ ...envelope, payload: { conversations: [{ id: "invalid", title: "must not retain old title" }], shared: [] } });
  else wire.mockRejectedValue(new ApiError("denied", 403));
  await act(async () => { await hook.result.current.qc.invalidateQueries(); });
  await waitFor(() => expect(hook.result.current.thread.conversations).toEqual([]));
  wire.mockRejectedValue(new ApiError("offline", 503));
  await act(async () => { await hook.result.current.qc.invalidateQueries(); });
  await waitFor(() => expect(hook.result.current.thread.conversations).toEqual([]));
  identity.scope = "u:org"; hook.rerender();
  expect(hook.result.current.thread.conversations).toEqual([]);
});
