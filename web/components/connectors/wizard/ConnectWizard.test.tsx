// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, cleanup } from "@testing-library/react";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { connectorsApi, type ConnectorSummary } from "@/lib/connectorsApi";
import * as pageReads from "@/lib/pageSectionRefresh";
import { ConnectWizard } from "./ConnectWizard";
vi.mock("@/lib/providers", () => ({ useApp: () => ({ getAuthToken: () => "token" }) }));
afterEach(() => { cleanup(); vi.restoreAllMocks(); sessionStorage.clear(); });
const connector: ConnectorSummary = { key: "slack", name: "Slack", status: "setting_up", beta: false, sources_on: 1, sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: "scope", sources: [{ key: "channels", label: "Channels", enabled: true, paused: false, status: "off", scope_shape: "items", scope: null, history: { supported: true, max_days: 90 }, last_captured_at: null, week: [], history_days: null, history_requested_at: null, setup_completed_at: null, backfill: null }] };
beforeEach(() => { vi.spyOn(pageReads, "refreshPageSection").mockResolvedValue({ ...connector, account_label: null, connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } }); });
function show(c = connector) { const close = vi.fn(); render(<QueryClientProvider client={new QueryClient()}><ConnectWizard connector={c} onClose={close} /></QueryClientProvider>); return close; }
it("starts with an empty pick and records None only when finished", async () => {
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValue({ items: [{ id: "a", label: "General", selected: false, meta: null }], next_cursor: null, total: 1 });
 const scope = vi.spyOn(connectorsApi, "setScope").mockResolvedValue(connector.sources[0]);
 const history = vi.spyOn(connectorsApi, "backfill").mockResolvedValue(connector.sources[0]);
 const close = show();
 await screen.findByText("General");
 expect(screen.getByRole("checkbox")).not.toBeChecked();
 fireEvent.click(screen.getByRole("button", { name: "Continue" }));
 await screen.findByText("None");
 expect(scope).toHaveBeenCalledWith("token", "slack", "channels", { mode: "pick", item_ids: [] });
 expect(history).not.toHaveBeenCalled();
 fireEvent.click(screen.getByRole("button", { name: "Finish" }));
 await screen.findByRole("link", { name: "View Slack" }); expect(close).not.toHaveBeenCalled();
 expect(history).toHaveBeenCalledWith("token", "slack", "channels", 0);
});
it("preserves selected items on subsequent pages", async () => {
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValueOnce({ items: [], next_cursor: "next", total: 1 }).mockResolvedValueOnce({ items: [{ id: "hidden", label: "Later", selected: true, meta: null }], next_cursor: null, total: 1 });
 const save = vi.spyOn(connectorsApi, "setScope").mockRejectedValue(new Error("offline"));
 show(); await screen.findByText("Later"); fireEvent.click(screen.getByRole("button", { name: "Continue" }));
 await screen.findByRole("alert"); expect(save).toHaveBeenCalledWith("token", "slack", "channels", { mode: "pick", item_ids: ["hidden"] });
 expect(screen.getByText("Later")).toBeVisible();
});
it("EverythingOnly skips items and unsupported history", async () => {
 const load = vi.spyOn(connectorsApi, "scopeItems");
 const save = vi.spyOn(connectorsApi, "setScope").mockResolvedValue(connector.sources[0]);
 const close = show({ ...connector, sources: [{ ...connector.sources[0], scope_shape: "everything_only", history: { supported: false, reason: "starts_at_connect" } }] });
 expect(screen.queryByText("Only what I pick")).not.toBeInTheDocument();
 expect(screen.getByText(/Captures from the moment/)).toBeVisible();
 fireEvent.click(screen.getByRole("button", { name: "Continue" }));
 await screen.findByRole("link", { name: "View Slack" }); expect(close).not.toHaveBeenCalled(); expect(load).not.toHaveBeenCalled(); expect(save).toHaveBeenCalledWith("token", "slack", "channels", { mode: "everything" });
});
it("PickOnly offers no Everything and closing does not save", async () => {
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValue({ items: [], next_cursor: null, total: 0 });
 const save = vi.spyOn(connectorsApi, "setScope");
 const close = show({ ...connector, sources: [{ ...connector.sources[0], scope_shape: "pick_only" }] });
 await screen.findByText(/No items found/);
 expect(screen.queryByText("Everything I can see")).not.toBeInTheDocument();
 fireEvent.click(screen.getAllByRole("button", { name: /^Close$/ })[0]);
 expect(close).toHaveBeenCalled(); expect(save).not.toHaveBeenCalled();
});
it("hosted completion is verified and a refusal keeps the wizard open", async () => {
 sessionStorage.setItem("connector-pending:slack", "pending");
 const complete = vi.spyOn(connectorsApi, "complete").mockRejectedValue(new Error("sign_in_pending"));
 const close = show({ ...connector, status: "not_connected" });
 fireEvent.click(screen.getByRole("button", { name: "I’ve finished signing in" }));
 await screen.findByRole("alert");
 expect(complete).toHaveBeenCalledWith("token", "slack", "pending"); expect(close).not.toHaveBeenCalled();
});
it("sends onboarding return intent through the shared hosted sign-in", async () => {
 const connect = vi.spyOn(connectorsApi, "connect").mockResolvedValue({ authorize_url: "https://example.test/sign-in", expires_at: "later", pending_id: "pending" });
 vi.spyOn(window, "open").mockReturnValue(null);
 render(<QueryClientProvider client={new QueryClient()}><ConnectWizard connector={{ ...connector, status: "not_connected" }} returnTo="onboarding" onClose={vi.fn()} /></QueryClientProvider>);
 fireEvent.click(screen.getByRole("button", { name: "Sign in to Slack" }));
 await screen.findByRole("button", { name: "I’ve finished signing in" });
 expect(connect).toHaveBeenCalledWith("token", "slack", "onboarding");
 expect(sessionStorage.getItem("connector-return:slack")).toBe("onboarding");
});

it.each([0, 30] as const)("reopening partial setup preserves accepted %s days while importing and finishes remaining source", async (days) => {
 const partial = { ...connector, resume_step: "history" as const, sources: [
  { ...connector.sources[0], history_days: days, history_requested_at: "2026-10-30T00:00:00Z", setup_completed_at: null, status: "importing" as const },
  { ...connector.sources[0], key: "other", label: "Other" },
 ] };
 vi.mocked(pageReads.refreshPageSection).mockResolvedValue({ ...partial, account_label: null, connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } });
 const post = vi.spyOn(connectorsApi, "backfill").mockResolvedValue(partial.sources[1]);
 const close = show(partial);
 expect(pageReads.refreshPageSection).not.toHaveBeenCalled();
 await waitFor(() => expect(screen.getAllByRole("radio", { name: days === 0 ? "None" : "Last 30 days" })[0]).toBeChecked());
 fireEvent.click(screen.getByRole("button", { name: "Finish" }));
 await screen.findByRole("link", { name: "View Slack" }); expect(close).not.toHaveBeenCalled();
 expect(post).toHaveBeenCalledTimes(1);
 expect(post).toHaveBeenCalledWith("token", "slack", "other", 0);
});

it.each([true, false])("reconciles ambiguous failure (accepted=%s), preserving remaining choices", async (accepted) => {
 const initial = { ...connector, resume_step: "history" as const };
 let claimed = false;
 vi.mocked(pageReads.refreshPageSection).mockImplementation(async () => ({ ...initial, account_label: null, connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] }, sources: [{ ...initial.sources[0], history_days: claimed ? 30 : null, history_requested_at: claimed ? "2026-10-30T00:00:00Z" : null }] }));
 const post = vi.spyOn(connectorsApi, "backfill").mockImplementation(async () => { claimed = accepted; throw new Error("lost response"); });
 const close = show(initial);
 fireEvent.click(screen.getByRole("radio", { name: "Last 30 days" }));
 fireEvent.click(screen.getByRole("button", { name: "Finish" }));
 if (accepted) { await screen.findByRole("link", { name: "View Slack" }); expect(close).not.toHaveBeenCalled(); }
 else {
  await screen.findByRole("alert");
  expect(close).not.toHaveBeenCalled();
  expect(screen.getByRole("radio", { name: "Last 30 days" })).toBeChecked();
  post.mockResolvedValue(initial.sources[0]);
  fireEvent.click(screen.getByRole("button", { name: "Finish" }));
  await screen.findByRole("link", { name: "View Slack" }); expect(close).not.toHaveBeenCalled();
 }
 expect(post).toHaveBeenCalledTimes(accepted ? 1 : 2);
 expect(post).toHaveBeenLastCalledWith("token", "slack", "channels", 30);
});

it("expired pending sign-in can restart without completing the stale session", async () => {
 sessionStorage.setItem("connector-pending:slack", "stale");
 sessionStorage.setItem("connector-expiry:slack", "2000-01-01T00:00:00Z");
 const complete = vi.spyOn(connectorsApi, "complete");
 show({ ...connector, status: "not_connected" });
 fireEvent.click(screen.getByRole("button", { name: "I’ve finished signing in" }));
 await screen.findByText(/Sign-in expired/);
 expect(complete).not.toHaveBeenCalled();
 fireEvent.click(screen.getByRole("button", { name: "Start a new sign-in" }));
 expect(screen.getByRole("button", { name: "Sign in to Slack" })).toBeEnabled();
 expect(sessionStorage.getItem("connector-pending:slack")).toBeNull();
});
it("reopens the hosted sign-in link after closing and mounting again", () => {
 sessionStorage.setItem("connector-pending:slack", "pending");
 sessionStorage.setItem("connector-url:slack", "https://example.test/sign-in");
 const close = show({ ...connector, status: "not_connected" });
 fireEvent.click(screen.getAllByRole("button", { name: "Close" })[0]);
 expect(close).toHaveBeenCalled(); cleanup();
 show({ ...connector, status: "not_connected" });
 expect(screen.getByRole("link", { name: "Open sign-in again" })).toHaveAttribute("href", "https://example.test/sign-in");
});
it("a stale mixed-source history resume confirms unsupported scope before durable completion", async () => {
 const calendar = { ...connector.sources[0], key: "calendar", label: "Calendar", scope_shape: "everything_only" as const, scope: { mode: "everything" as const, count: null, noun: "calendars" }, history: { supported: false as const, reason: "automatic_window" } };
 let saved = { ...connector, resume_step: "history" as const, sources: [calendar, connector.sources[0]], account_label: null, connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } };
 vi.mocked(pageReads.refreshPageSection).mockImplementation(async () => saved);
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValue({ items: [], next_cursor: null, total: 0 });
 const scope = vi.spyOn(connectorsApi, "setScope").mockImplementation(async (_token, _provider, key) => {
  saved = { ...saved, sources: saved.sources.map(s => s.key === key && !s.history.supported ? { ...s, setup_completed_at: "2026-10-30T00:00:00Z" } : s) };
  return saved.sources.find(s => s.key === key)!;
 });
 const history = vi.spyOn(connectorsApi, "backfill").mockResolvedValue(connector.sources[0]);
 show(saved);
 fireEvent.click(screen.getByRole("button", { name: "Finish" }));
 await screen.findByRole("heading", { name: "Choose scope: Calendar" });
 expect(history).not.toHaveBeenCalled();
 expect(screen.queryByRole("link", { name: "View Slack" })).not.toBeInTheDocument();
 // Reopen at the server's corrected step, retaining the incomplete source.
 cleanup();
 show({ ...saved, resume_step: "scope" });
 fireEvent.click(screen.getByRole("button", { name: "Continue" }));
 await screen.findByRole("heading", { name: "Choose scope: Channels" });
 await screen.findByText("No items found. You can continue with nothing selected.");
 fireEvent.click(screen.getByRole("button", { name: "Continue" }));
 await screen.findByRole("button", { name: "Finish" });
 fireEvent.click(screen.getByRole("button", { name: "Finish" }));
 await screen.findByRole("link", { name: "View Slack" });
 expect(scope).toHaveBeenCalledWith("token", "slack", "calendar", { mode: "everything" });
 expect(saved.sources[0].setup_completed_at).not.toBeNull();
 expect(history).toHaveBeenCalledTimes(1);
 expect(history).toHaveBeenCalledWith("token", "slack", "channels", 0);
});

it("native single-source return enables durably and skips source selection", async () => {
 const patch = vi.spyOn(connectorsApi, "patchSource").mockResolvedValue(connector.sources[0]);
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValue({ items: [], next_cursor: null, total: 0 });
 show({ ...connector, resume_step: "sources", sources: [{ ...connector.sources[0], enabled: false }] });
 await screen.findByText(/No items found/);
 expect(patch).toHaveBeenCalledWith("token", "slack", "channels", { enabled: true });
 expect(screen.queryByText("Choose sources")).not.toBeInTheDocument();
});
it("explicit repair entry starts authorization even for needs attention", () => {
 render(<QueryClientProvider client={new QueryClient()}><ConnectWizard reconnect connector={{ ...connector, status: "needs_attention" }} onClose={vi.fn()} /></QueryClientProvider>);
 expect(screen.getByRole("button", { name: "Sign in to Slack" })).toBeVisible();
});
it("retries automatic source enable inline without losing setup", async () => {
 const patch = vi.spyOn(connectorsApi, "patchSource").mockRejectedValueOnce(new Error("offline")).mockResolvedValue(connector.sources[0]);
 vi.spyOn(connectorsApi, "scopeItems").mockResolvedValue({ items: [], next_cursor: null, total: 0 });
 show({ ...connector, resume_step: "sources" });
 fireEvent.click(await screen.findByRole("button", { name: "Try again" }));
 await screen.findByText(/No items found/);
 expect(patch).toHaveBeenCalledTimes(2);
 expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});
it("retries automatic toolkit scope inline and reaches Done", async () => {
 const source = { ...connector.sources[0], scope_shape: "everything_only" as const, history: { supported: false as const, reason: "starts_at_connect" } };
 const save = vi.spyOn(connectorsApi, "setScope").mockRejectedValueOnce(new Error("offline")).mockResolvedValue(source);
 show({ ...connector, key: "jira", name: "Jira", sources: [source] });
 fireEvent.click(await screen.findByRole("button", { name: "Try again" }));
 await screen.findByRole("link", { name: "View Jira" });
 expect(save).toHaveBeenCalledTimes(2);
 expect(save).toHaveBeenLastCalledWith("token", "jira", "channels", { mode: "everything" });
});
it("hosted single-source completion durably enables and automatically saves toolkit scope", async () => {
 const source = { ...connector.sources[0], scope_shape: "everything_only" as const, history: { supported: false as const, reason: "starts_at_connect" } };
 const c = { ...connector, key: "jira", name: "Jira", sources: [source] };
 sessionStorage.setItem("connector-pending:jira", "pending");
 vi.spyOn(connectorsApi, "complete").mockResolvedValue(c);
 const patch = vi.spyOn(connectorsApi, "patchSource").mockResolvedValue(source);
 const scope = vi.spyOn(connectorsApi, "setScope").mockResolvedValue(source);
 show({ ...c, status: "not_connected" });
 fireEvent.click(screen.getByRole("button", { name: "I’ve finished signing in" }));
 await screen.findByRole("link", { name: "View Jira" });
 expect(patch).toHaveBeenCalledWith("token", "jira", "channels", { enabled: true });
 expect(scope).toHaveBeenCalledWith("token", "jira", "channels", { mode: "everything" });
 expect(sessionStorage.getItem("connector-pending:jira")).toBeNull();
});
it("preserves two source selections through Back without enabling an unselected source", async () => {
 const patch = vi.spyOn(connectorsApi, "patchSource");
 vi.spyOn(connectorsApi, "scopeItems").mockImplementation(async (_token, _provider, key) => ({items:[{id:key+"-item",label:key+" item",selected:false,meta:null}],next_cursor:null,total:1}));
 const save = vi.spyOn(connectorsApi, "setScope").mockResolvedValue(connector.sources[0]);
 show({...connector,sources_total:3,sources_on:2,sources:[connector.sources[0],{...connector.sources[0],key:"chat",label:"Chat"},{...connector.sources[0],key:"mail",label:"Mail",enabled:false}]});
 fireEvent.click(await screen.findByRole("checkbox",{name:/channels item/}));
 fireEvent.click(screen.getByRole("button",{name:"Continue"}));
 fireEvent.click(await screen.findByRole("checkbox",{name:/chat item/}));
 fireEvent.click(screen.getByRole("button",{name:"Back"}));
 expect(await screen.findByRole("checkbox",{name:/channels item/})).toBeChecked();
 fireEvent.click(screen.getByRole("button",{name:"Continue"}));
 expect(await screen.findByRole("checkbox",{name:/chat item/})).toBeChecked();
 fireEvent.click(screen.getByRole("button",{name:"Continue"}));
 await waitFor(()=>expect(save).toHaveBeenCalledWith("token","slack","chat",{mode:"pick",item_ids:["chat-item"]}));
 expect(save).toHaveBeenCalledWith("token","slack","channels",{mode:"pick",item_ids:["channels-item"]});
 expect(patch).not.toHaveBeenCalled();
 expect(save.mock.calls.some(call=>call[2]==="mail")).toBe(false);
});
