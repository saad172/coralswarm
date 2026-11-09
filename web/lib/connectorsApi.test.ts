import { expect, it, vi } from "vitest";
vi.mock("./api", () => ({ apiRequest: vi.fn() }));
import { apiRequest } from "./api";
import { connectorsApi } from "./connectorsApi";
it("omits absent cursors and encodes path and query values", () => {
 connectorsApi.scopeItems("t", "google", "chat", { cursor: undefined });
 expect(apiRequest).toHaveBeenLastCalledWith("/v1/connectors/google/sources/chat/scope-items?", "t", undefined, false, { priority: "normal", signal: undefined });
 connectorsApi.scopeItems("t", "a/b", "c/d", { q: "a&b", cursor: "next" });
 expect(apiRequest).toHaveBeenLastCalledWith("/v1/connectors/a%2Fb/sources/c%2Fd/scope-items?q=a%26b&cursor=next", "t", undefined, false, { priority: "normal", signal: undefined });
});
it("sends the explicit None history choice and hosted pending ID", () => {
 connectorsApi.backfill("t", "slack", "channels", 0);
 expect(apiRequest).toHaveBeenLastCalledWith("/v1/connectors/slack/sources/channels/backfill", "t", { method: "POST", body: '{"days":0}' });
 connectorsApi.complete("t", "jira", "pending");
 expect(apiRequest).toHaveBeenLastCalledWith("/v1/connectors/jira/connect/complete", "t", { method: "POST", body: '{"pending_id":"pending"}' });
});

it("keeps live scope picker reads normal and forwards cancellation", () => {
 const controller = new AbortController();
 connectorsApi.scopeItems("t", "slack", "channels", {}, controller.signal);
 expect(apiRequest).toHaveBeenLastCalledWith("/v1/connectors/slack/sources/channels/scope-items?", "t", undefined, false, { priority: "normal", signal: controller.signal });
});
