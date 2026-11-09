import { describe, expect, it, vi } from "vitest";
import { createComposeAdapter, composeSupported, EXPIRES_PROPERTY, INTENT_ID_PROPERTY, isComposeAppointment, NONCE_PROPERTY, readComposeRecurring, validNonce } from "./composeIntent";
import type { OfficeApi, OfficeMailItem } from "./office";

const NONCE = "11111111-1111-4111-a111-111111111111";
const INTENT = "22222222-2222-4222-a222-222222222222";

function fixture(opts: { saveStatus?: string } = {}) {
  const data: Record<string, string> = { otherAddinSetting: "kept" };
  let saves = 0;
  const props = {
    get: (k: string) => data[k], set: (k: string, v: string) => { data[k] = v; }, remove: (k: string) => { delete data[k]; },
    saveAsync: vi.fn((cb: (r: { status: string; value: unknown }) => void) => { saves++; cb({ status: opts.saveStatus ?? "ok", value: undefined }); }),
  };
  const item = {
    itemType: "appointment", subject: { getAsync() {} },
    loadCustomPropertiesAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status: "ok", value: props }),
    saveAsync: () => { throw new Error("must never save the calendar item"); },
    sendAsync: () => { throw new Error("must never send"); },
  } as unknown as OfficeMailItem;
  const office = { AsyncResultStatus: { Succeeded: "ok" }, context: { mailbox: { item } } } as unknown as OfficeApi;
  return { data, props, item, office, adapter: createComposeAdapter(office, 50), saves: () => saves };
}

describe("compose detection", () => {
  it("a draft exposes subject.getAsync; a saved appointment has a string subject", () => {
    expect(isComposeAppointment({ itemType: "appointment", subject: { getAsync() {} } })).toBe(true);
    expect(isComposeAppointment({ itemType: "appointment", subject: "Standup" })).toBe(false);
    expect(composeSupported({ itemType: "appointment", subject: { getAsync() {} } })).toBe(false); // no CustomProperties
  });
  it("only random-uuid-shaped nonces are accepted", () => {
    expect(validNonce(NONCE)).toBe(true);
    expect(validNonce("csns_secret")).toBe(false);
  });
});

describe("marker writes", () => {
  it("stage writes nonce + intent id through the property bag only, preserving other add-in settings", async () => {
    const f = fixture();
    expect(f.saves()).toBe(0); // opening writes nothing
    await f.adapter.stage(NONCE, INTENT);
    expect(f.data[NONCE_PROPERTY]).toBe(NONCE); expect(f.data[INTENT_ID_PROPERTY]).toBe(INTENT);
    expect(f.data.otherAddinSetting).toBe("kept"); expect(f.saves()).toBe(1);
    expect(await f.adapter.read()).toEqual({ nonce: NONCE, intentId: INTENT, expiresAt: null });
  });
  it("stage keeps the expiry next to the marker, read returns it, remove clears it", async () => {
    const f = fixture();
    await f.adapter.stage(NONCE, INTENT, { expiresAt: "2099-01-01T00:00:00Z" });
    expect(f.data[EXPIRES_PROPERTY]).toBe("2099-01-01T00:00:00Z");
    expect(await f.adapter.read()).toEqual({ nonce: NONCE, intentId: INTENT, expiresAt: "2099-01-01T00:00:00Z" });
    await f.adapter.remove();
    expect(f.data[EXPIRES_PROPERTY]).toBeUndefined();
  });
  it("an unparseable expiry is never written or returned", async () => {
    const f = fixture();
    await f.adapter.stage(NONCE, INTENT, { expiresAt: "soon" });
    expect(f.data[EXPIRES_PROPERTY]).toBeUndefined();
    f.data[EXPIRES_PROPERTY] = "<script>";
    expect((await f.adapter.read())?.expiresAt).toBeNull();
  });
  it("replace overwrites a different marker (turn on again); without it that is refused", async () => {
    const f = fixture();
    await f.adapter.stage(NONCE, INTENT, { expiresAt: "2099-01-01T00:00:00Z" });
    const other = "33333333-3333-4333-a333-333333333333", otherIntent = "44444444-4444-4444-a444-444444444444";
    await expect(f.adapter.stage(other, otherIntent)).rejects.toThrow("different_marker");
    await f.adapter.stage(other, otherIntent, { replace: true, expiresAt: "2099-01-02T00:00:00Z" });
    expect(await f.adapter.read()).toEqual({ nonce: other, intentId: otherIntent, expiresAt: "2099-01-02T00:00:00Z" });
  });
  it("a definitive failure while replacing restores the previous marker and expiry", async () => {
    const f = fixture();
    await f.adapter.stage(NONCE, INTENT, { expiresAt: "2099-01-01T00:00:00Z" });
    f.props.saveAsync.mockImplementation((cb: (r: { status: string; value: unknown }) => void) => cb({ status: "failed", value: undefined }));
    await expect(f.adapter.stage("33333333-3333-4333-a333-333333333333", "44444444-4444-4444-a444-444444444444", { replace: true })).rejects.toMatchObject({ definitive: true });
    expect(f.data[NONCE_PROPERTY]).toBe(NONCE); expect(f.data[EXPIRES_PROPERTY]).toBe("2099-01-01T00:00:00Z");
  });
  it("remove clears the marker and nothing else", async () => {
    const f = fixture(); await f.adapter.stage(NONCE, INTENT); await f.adapter.remove();
    expect(f.data[NONCE_PROPERTY]).toBeUndefined(); expect(f.data[INTENT_ID_PROPERTY]).toBeUndefined();
    expect(f.data.otherAddinSetting).toBe("kept"); expect(await f.adapter.read()).toBeNull();
  });
  it("read tolerates a marker without an intent id", async () => {
    const f = fixture(); f.data[NONCE_PROPERTY] = NONCE;
    expect(await f.adapter.read()).toEqual({ nonce: NONCE, intentId: null, expiresAt: null });
  });
  it("a definitive Outlook failure rolls the local bag back and is flagged definitive", async () => {
    const f = fixture({ saveStatus: "failed" });
    await expect(f.adapter.stage(NONCE, INTENT)).rejects.toMatchObject({ definitive: true });
    expect(f.data[NONCE_PROPERTY]).toBeUndefined();
  });
  it("refuses to overwrite a different marker", async () => {
    const f = fixture(); f.data[NONCE_PROPERTY] = "33333333-3333-4333-a333-333333333333";
    await expect(f.adapter.stage(NONCE, INTENT)).rejects.toThrow("different_marker");
    expect(f.saves()).toBe(0);
  });
  it("rejects invalid values without touching the bag", async () => {
    const f = fixture(); await expect(f.adapter.stage("csns_secret", INTENT)).rejects.toThrow("invalid_intent");
    expect(f.saves()).toBe(0);
  });
  it("a switched draft aborts before writing", async () => {
    const f = fixture(); let loaded: ((r: { status: string; value: unknown }) => void) | undefined;
    (f.item as unknown as { loadCustomPropertiesAsync: unknown }).loadCustomPropertiesAsync = (cb: typeof loaded) => { loaded = cb; };
    const pending = f.adapter.stage(NONCE, INTENT);
    (f.office.context.mailbox as { item: unknown }).item = { ...f.item };
    loaded?.({ status: "ok", value: f.props });
    await expect(pending).rejects.toThrow("item_changed");
    expect(f.saves()).toBe(0);
  });
});

describe("recurrence hint", () => {
  const withRecurrence = (value: unknown, status = "ok") => ({
    AsyncResultStatus: { Succeeded: "ok" },
    context: { mailbox: { item: { recurrence: { getAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status, value }) } } } },
  }) as unknown as OfficeApi;
  it("true when the draft has a recurrence pattern, false for one-off or unsupported", async () => {
    expect(await readComposeRecurring(withRecurrence({ recurrenceType: "weekly" }))).toBe(true);
    expect(await readComposeRecurring(withRecurrence(null))).toBe(false);
    expect(await readComposeRecurring(withRecurrence({}, "failed"))).toBe(false);
    expect(await readComposeRecurring({ AsyncResultStatus: { Succeeded: "ok" }, context: { mailbox: { item: {} } } } as unknown as OfficeApi)).toBe(false);
  });
});

describe("a timed-out write", () => {
  it("keeps the adapter busy until Outlook's callback fires: no stale read, no second write", async () => {
    const f = fixture(); let finish: ((r: { status: string; value: unknown }) => void) | undefined;
    f.props.saveAsync.mockImplementation((cb: (r: { status: string; value: unknown }) => void) => { finish = cb; });
    await expect(f.adapter.stage(NONCE, INTENT)).rejects.toThrow("timeout");
    // Timed out, but the save is still pending: a read would see a bag that may miss the write.
    await expect(f.adapter.read()).rejects.toThrow("busy");
    await expect(f.adapter.stage(NONCE, INTENT)).rejects.toThrow("busy");
    await expect(f.adapter.remove()).rejects.toThrow("busy");
    finish?.({ status: "ok", value: undefined });
    expect(await f.adapter.read()).toEqual({ nonce: NONCE, intentId: INTENT, expiresAt: null }); // the write did land
  });
});
