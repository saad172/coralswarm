// @vitest-environment jsdom
import React, { useState } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ScopePicker } from "./ScopePicker";
import * as apiModule from "@/lib/api";
import { ConnectorDetail } from "./ConnectorDetail";
import { ConnectorSourceCard } from "./ConnectorSourceCard";
import { connectorsApi, type ScopeItemsPage, type SourceSummary, type ScopeSelection } from "@/lib/connectorsApi";
vi.mock("@/lib/providers", () => ({ useApp: () => ({ getAuthToken: () => "token", queryScope: "test", authReady: true }) }));
vi.mock("./wizard/ConnectWizard", () => ({ historyReason: () => "Unsupported", ConnectWizard: () => null }));
const source = { key: "channels", label: "Channels", enabled: true, paused: false, status: "active", scope_shape: "items", scope: { mode: "pick", count: 1, noun: "channels" }, history: { supported: false, reason: "unsupported" }, week: [], last_captured_at: null, history_days: null, history_requested_at: null, setup_completed_at: null, backfill: null } as SourceSummary;
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
it("preserves a changed mode when delayed scope hydration completes", async () => {
 let resolve!: (value: ScopeItemsPage) => void;
 vi.spyOn(connectorsApi, "scopeItems").mockImplementation(() => new Promise(r => { resolve = r; }));
 function Harness() { const [value, onChange] = useState<ScopeSelection>({mode:"pick"}); return <ScopePicker getAuthToken={() => "token"} provider="slack" source={source} value={value} onChange={onChange} />; }
 render(<Harness />);
 fireEvent.click(screen.getByRole("tab", { name: "Everything I can see" }));
 await act(async () => resolve({items:[{id:"a",label:"Alpha",meta:null,selected:true}],next_cursor:null,total:1}));
 expect(screen.getByRole("tab", { name: "Everything I can see" }).getAttribute("aria-selected")).toBe("true");
});
it("discards unsaved scope on close and uses latest persisted mode on reopening", async () => {
 vi.spyOn(connectorsApi,"scopeItems").mockResolvedValue({items:[],next_cursor:null,total:0});
 const client = new QueryClient({ defaultOptions:{queries:{retry:false}} });
 const view = (s: SourceSummary) => <QueryClientProvider client={client}><ConnectorSourceCard provider="slack" source={s} /></QueryClientProvider>;
 const {rerender}=render(view(source));
 await act(async () => fireEvent.click(screen.getByRole("button",{name:"Edit"})));
 fireEvent.click(screen.getByRole("tab", { name: "Everything I can see" }));
 fireEvent.click(screen.getByRole("button",{name:"Close"}));
 await act(async () => fireEvent.click(screen.getByRole("button",{name:"Edit"})));
 expect(screen.getByRole("tab", { name: "Only what I pick" }).getAttribute("aria-selected")).toBe("true");
 fireEvent.click(screen.getByRole("button",{name:"Close"}));
 rerender(view({...source,scope:{mode:"everything",count:null,noun:"channels"}}));
 await act(async () => fireEvent.click(screen.getByRole("button",{name:"Edit"})));
 expect(screen.getByRole("tab", { name: "Everything I can see" }).getAttribute("aria-selected")).toBe("true");
 client.clear();
});

it("refreshes capture results after a source mutation", async () => {
 let captured = false;
 const captures = vi.spyOn(connectorsApi, "captures");
 const read = vi.spyOn(apiModule, "apiRequest").mockImplementation(async (path) => {
  if (path !== "/v1/connectors/slack") throw new Error(`Unexpected mount GET ${path}`);
  return { model: "connector_detail", version: 1, computed_at: captured ? "2026-10-31T00:00:03Z" : "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
   payload: { key: "slack", name: "Slack", beta: false, status: "active", sources_on: 1, sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: null,
    sources: [{ ...source, paused: captured }], captures: { items: captured ? [{ atom_id: "new", source: "channels", title: "New capture", excerpt: "", author: null, captured_at: "2026-10-31T00:00:02Z", meta: {}, topics: [] }] : [], next_cursor: null },
    watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] }, topics: [], account_label: "Workspace", connected_at: null } } as never;
 });
 vi.spyOn(connectorsApi, "patchSource").mockImplementation(async () => { captured = true; return { ...source, paused: true }; });
 const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
 render(<QueryClientProvider client={client}><ConnectorDetail provider="slack" /></QueryClientProvider>);
 await screen.findByText("No captures yet.");
 fireEvent.click(screen.getByRole("button", { name: "Pause" }));
 await screen.findByText("New capture", { exact: false });
 expect(read).toHaveBeenCalledTimes(2);
 expect(captures).not.toHaveBeenCalled();
 client.clear();
});

it("marks the connector list and Home's attention row stale after a capture recovery, reading the detail once", async () => {
 let repaired = false;
 const detailReads = { n: 0 };
 vi.spyOn(apiModule, "apiRequest").mockImplementation(async (path, _token, init) => {
  if (path === "/v1/connectors/jira/recover" && init?.method === "POST") { repaired = true; return { problem: null } as never; }
  if (path === "/v1/connectors/jira/refresh" && init?.method === "POST") return { refresh_eta: null } as never;
  if (path !== "/v1/connectors/jira") throw new Error(`Unexpected request ${path}`);
  detailReads.n++;
  return { model: "connector_detail", version: 1, computed_at: repaired ? "2026-10-31T00:00:03Z" : "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
   payload: { key: "jira", name: "Jira", beta: false, status: repaired ? "active" : "needs_attention", sources_on: 1, sources_total: 1, last_captured_at: null, importing: null,
    problem: repaired ? null : { code: "capture_unavailable", source: null, since: "2026-10-30T00:00:00Z", params: {} }, resume_step: null,
    sources: [{ ...source, key: "issues", label: "Issues", scope_shape: "everything_only" }], captures: { items: [], next_cursor: null },
    watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] }, topics: [], account_label: "Workspace", connected_at: null } } as never;
 });
 const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
 client.setQueryData(["homeAttention", "test"], { has_content: true, attention: { provider: "jira" } });
 client.setQueryData(["connectors", "test"], []);
 render(<QueryClientProvider client={client}><ConnectorDetail provider="jira" /></QueryClientProvider>);
 fireEvent.click(await screen.findByRole("button", { name: "Retry capture check" }));
 await screen.findByText("Recovery check completed.");
 expect(client.getQueryState(["homeAttention", "test"])?.isInvalidated).toBe(true);
 expect(client.getQueryState(["connectors", "test"])?.isInvalidated).toBe(true);
 expect(detailReads.n).toBe(2);
 client.clear();
});

it.each([
 ["slack","channels","Channels","items","Captures all accessible channels for this connection."],
 ["github","repos","Repositories","items","Captures the currently granted repositories selected for this connection. Later repository grants require selection."],
 ["google","calendar","Calendar","everything_only","Captures calendar events available to this connected account, including the automatic history window."],
 ["jira","issues","Issues","everything_only","Captures issues across this connected workspace with its granted access."],
 ["google","gmail","Gmail","items","Captures mail available through this connected account’s Everything mode."],
])("explains Everything for %s/%s without the pick-only mail hint", async (provider,key,label,shape,explanation) => {
 vi.spyOn(connectorsApi,"scopeItems").mockResolvedValue({items:[],next_cursor:null,total:0});
 function Harness() { const [value,onChange]=useState<ScopeSelection>({mode:shape === "everything_only" ? "everything" : "pick"}); return <ScopePicker getAuthToken={()=>"token"} provider={provider} source={{...source,key,label,scope_shape:shape as SourceSummary["scope_shape"]}} value={value} onChange={onChange}/>; }
 await act(async()=>{render(<Harness/>);});
 if(shape !== "everything_only") fireEvent.click(screen.getByRole("tab",{name:"Everything I can see"}));
 expect(screen.getByText(explanation)).toBeTruthy();
 expect(screen.queryByText("Follow threads from the CoralSwarm add-in in Gmail or Outlook.")).toBeNull();
});
