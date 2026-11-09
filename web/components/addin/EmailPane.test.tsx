// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EmailPane, ingestBody, type EmailItem } from "./EmailPane";
import { Surface } from "./kit";
import type { ApiResult, EmailApi } from "@/lib/addin/emailTransport";

// Scenarios ported from extensions/outlook-addin/scripts/test-options.js (the
// plain-JS pane this component replaces): every option state and the exact wire
// bodies, plus the Gmail-voice copy.

const item: EmailItem = { itemId: "AAMk-item", conversationId: "AAQk-conv", subject: "Board packet", from: "Ada Lovelace" };
const toRestId = (id: string) => `rest:${id}`;
const ok = (data: unknown, status = 200): ApiResult => ({ status, ok: status < 400, data });
const fail = (status: number): ApiResult => ({ status, ok: false, data: null });
const WATCHED = "GET /v1/connectors/microsoft/watched/rest%3AAAQk-conv";
const PUT = "PUT /v1/connectors/microsoft/watched/rest%3AAAQk-conv";
const INGEST = "POST /v1/connectors/microsoft/ingest";

type Handler = (body?: unknown) => ApiResult | Promise<ApiResult>;
function setup(routes: Record<string, Handler>, over: Partial<EmailItem> = {}) {
  const wire: Array<{ key: string; body: unknown }> = [];
  const api: EmailApi = vi.fn(async (method, path, body) => {
    const key = `${method} ${path}`;
    wire.push({ key, body });
    const handler = routes[key];
    if (!handler) throw new Error(`unrouted ${key}`);
    return handler(body);
  });
  const toast = vi.fn();
  const onUnauthorized = vi.fn();
  const openToday = vi.fn();
  render(<Surface label="mail"><EmailPane item={{ ...item, ...over }} api={api} workspaceName="Acme ISD" toast={toast} onUnauthorized={onUnauthorized} toRestId={toRestId} openToday={openToday} /></Surface>);
  return { wire, toast, onUnauthorized, openToday, api, ingests: () => wire.filter((w) => w.key === INGEST) };
}

beforeEach(() => vi.clearAllMocks());
afterEach(cleanup);

describe("ingestBody: a field is sent iff its control is rendered", () => {
  it("checking sends neither", () => expect(ingestBody("r", "checking", "message", false)).toEqual({ item_ids: ["r"] }));
  it("fresh sends scope and follow", () => expect(ingestBody("r", "fresh", "message", false)).toEqual({ item_ids: ["r"], scope: "message", follow: false }));
  it("following sends follow only", () => expect(ingestBody("r", "following", "message", true)).toEqual({ item_ids: ["r"], follow: true }));
});

describe("fresh thread", () => {
  it("shows the Gmail rows: + row, thread row, scope radio, follow switch, Save footer", async () => {
    setup({ [WATCHED]: () => ok({ following: false }) });
    expect(await screen.findByRole("radio", { name: "Whole thread" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "Just this message" })).not.toBeChecked();
    expect(screen.getByRole("switch", { name: "Keep saving new replies" })).toBeChecked();
    expect(screen.getByRole("button", { name: "Save this thread to your workspace" })).toBeVisible();
    expect(screen.getByText("Board packet")).toBeVisible();
    expect(screen.getByText("Ada Lovelace")).toBeVisible();
    expect(screen.getByRole("button", { name: "Save to CoralSwarm" })).toBeVisible();
    expect(document.body.textContent).not.toMatch(/Grow|Change ocean/);
  });

  it("default save sends thread + follow:true, toasts the count, and turns the switch live", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: false }), [INGEST]: () => ok({ ingested: 4 }) });
    await screen.findByRole("radio", { name: "Whole thread" });
    fireEvent.click(screen.getByRole("button", { name: "Save to CoralSwarm" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Saved to CoralSwarm · 4 messages"));
    expect(t.ingests()[0].body).toEqual({ item_ids: ["rest:AAMk-item"], scope: "thread", follow: true });
    expect(await screen.findByText("In your workspace · following · 4 messages saved")).toBeVisible();
    expect(screen.queryByRole("radio")).not.toBeInTheDocument();
    fireEvent.click(screen.getByText("In your workspace · following · 4 messages saved"));
    expect(t.openToday).toHaveBeenCalled();
  });

  it("Just this message + switch off sends exactly that", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: false }), [INGEST]: () => ok({ ingested: 1 }) });
    fireEvent.click(await screen.findByRole("radio", { name: "Just this message" }));
    fireEvent.click(screen.getByRole("switch", { name: "Keep saving new replies" }));
    fireEvent.click(screen.getByRole("button", { name: "Save this thread to your workspace" }));
    await waitFor(() => expect(t.ingests()).toHaveLength(1));
    expect(t.ingests()[0].body).toEqual({ item_ids: ["rest:AAMk-item"], scope: "message", follow: false });
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Saved to CoralSwarm · 1 message"));
    expect(await screen.findByText("In your workspace · 1 message saved")).toBeVisible();
  });

  it("an already-saved thread is not an error", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: false }), [INGEST]: () => ok({ ingested: 0 }) });
    fireEvent.click(await screen.findByRole("button", { name: "Save to CoralSwarm" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Already in your workspace — nothing new to save."));
  });
});

describe("reopen on a followed thread", () => {
  it("shows the saved row and a live switch; no radio", async () => {
    setup({ [WATCHED]: () => ok({ following: true }) });
    expect(await screen.findByText("In your workspace · following")).toBeVisible();
    expect(screen.getByRole("switch", { name: "Keep saving new replies" })).toBeChecked();
    expect(screen.queryByRole("radio")).not.toBeInTheDocument();
  });

  it("flipping the switch PUTs and renders from the server's answer", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: true }), [PUT]: (body) => ok({ following: (body as { follow: boolean }).follow }) });
    fireEvent.click(await screen.findByRole("switch", { name: "Keep saving new replies" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Stopped following"));
    expect(t.wire.find((w) => w.key === PUT)?.body).toEqual({ follow: false });
    expect(screen.getByRole("switch", { name: "Keep saving new replies" })).not.toBeChecked();
    expect(screen.getByText("In your workspace")).toBeVisible();
  });

  it("a re-save after turning follow off sends follow:false and no scope (does not silently re-follow)", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: true }), [PUT]: () => ok({ following: false }), [INGEST]: () => ok({ ingested: 0 }) });
    fireEvent.click(await screen.findByRole("switch", { name: "Keep saving new replies" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Stopped following"));
    fireEvent.click(screen.getByRole("button", { name: "Save to CoralSwarm" }));
    await waitFor(() => expect(t.ingests()).toHaveLength(1));
    expect(t.ingests()[0].body).toEqual({ item_ids: ["rest:AAMk-item"], follow: false });
  });

  it("PUT 404 (never saved) falls back to the fresh knobs", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: true }), [PUT]: () => fail(404) });
    fireEvent.click(await screen.findByRole("switch", { name: "Keep saving new replies" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("This thread is not in your workspace yet — save it first."));
    expect(await screen.findByRole("radio", { name: "Whole thread" })).toBeVisible();
  });

  it("PUT 500 leaves the switch where the server last said", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: true }), [PUT]: () => fail(500) });
    fireEvent.click(await screen.findByRole("switch", { name: "Keep saving new replies" }));
    await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Could not change following (500). Try again in a moment."));
    expect(screen.getByRole("switch", { name: "Keep saving new replies" })).toBeChecked();
  });

  it("PUT 401 asks the shell to reconnect", async () => {
    const t = setup({ [WATCHED]: () => ok({ following: true }), [PUT]: () => fail(401) });
    fireEvent.click(await screen.findByRole("switch", { name: "Keep saving new replies" }));
    await waitFor(() => expect(t.onUnauthorized).toHaveBeenCalled());
  });
});

describe("the watched check never breaks the pane", () => {
  it.each([["a 500", () => fail(500)], ["a malformed body", () => ok({ nope: 1 })], ["a network error", () => { throw new Error("offline"); }]])("%s lands on fresh", async (_n, handler) => {
    setup({ [WATCHED]: handler as Handler });
    expect(await screen.findByRole("radio", { name: "Whole thread" })).toBeVisible();
  });

  it("no conversation id => fresh without any read", async () => {
    const t = setup({}, { conversationId: null });
    expect(await screen.findByRole("radio", { name: "Whole thread" })).toBeVisible();
    expect(t.api).not.toHaveBeenCalled();
  });

  it("a save inside the check window sends neither field, and the late 'not following' answer does not undo it", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const t = setup({ [WATCHED]: async () => { await gate; return ok({ following: false }); }, [INGEST]: () => ok({ ingested: 2 }) });
    fireEvent.click(await screen.findByRole("button", { name: "Save to CoralSwarm" }));
    await waitFor(() => expect(t.ingests()).toHaveLength(1));
    expect(t.ingests()[0].body).toEqual({ item_ids: ["rest:AAMk-item"] });
    await screen.findByText("In your workspace · following · 2 messages saved");
    release();
    await new Promise((r) => setTimeout(r, 20));
    expect(screen.getByText("In your workspace · following · 2 messages saved")).toBeVisible();
  });
});

describe("save error mapping", () => {
  const fresh = { [WATCHED]: () => ok({ following: false }) };
  async function save(result: () => ApiResult) {
    const t = setup({ ...fresh, [INGEST]: result });
    fireEvent.click(await screen.findByRole("button", { name: "Save to CoralSwarm" }));
    return t;
  }
  it("401 goes back to the start card via the shell", async () => { const t = await save(() => fail(401)); await waitFor(() => expect(t.onUnauthorized).toHaveBeenCalled()); expect(t.toast).not.toHaveBeenCalled(); });
  it("an unauthenticated transport result does too", async () => { const t = await save(() => ({ status: 0, ok: false, data: null, unauthenticated: true })); await waitFor(() => expect(t.onUnauthorized).toHaveBeenCalled()); });
  it("403/404 names the workspace", async () => { const t = await save(() => fail(403)); await waitFor(() => expect(t.toast).toHaveBeenCalledWith("CoralSwarm refused this for Acme ISD. Try changing your workspace.")); });
  it("other failures show the status", async () => { const t = await save(() => fail(502)); await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Could not save this thread (502).")); });
  it("a body without a count is unconfirmed, not success", async () => { const t = await save(() => ok({})); await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Couldn’t confirm this save. Try again.")); expect(screen.queryByText(/In your ocean/)).not.toBeInTheDocument(); });
  it("a network failure is reported", async () => { const t = await save(() => { throw new Error("offline"); }); await waitFor(() => expect(t.toast).toHaveBeenCalledWith("Couldn’t reach CoralSwarm. Try again in a moment.")); });
});
