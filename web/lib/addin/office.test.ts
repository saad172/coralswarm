// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadOffice, resetOfficeLoaderForTests, type OfficeApi } from "./office";

afterEach(() => { resetOfficeLoaderForTests(); delete window.Office; });

describe("loadOffice", () => {
  it("resolves inside an Office host", async () => {
    const office = { onReady: (cb: (info: { host?: string }) => void) => cb({ host: "Outlook" }) } as unknown as OfficeApi;
    window.Office = office;
    await expect(loadOffice()).resolves.toBe(office);
  });

  it("rejects when onReady reports no host (page opened in a plain browser)", async () => {
    window.Office = { onReady: (cb: (info: { host?: string }) => void) => cb({ host: undefined }) } as unknown as OfficeApi;
    await expect(loadOffice()).rejects.toThrow(/Office host/);
  });

  it("allows a retry after a failure", async () => {
    const ready = vi.fn((cb: (info: { host?: string }) => void) => cb({ host: undefined }));
    window.Office = { onReady: ready } as unknown as OfficeApi;
    await expect(loadOffice()).rejects.toThrow();
    ready.mockImplementation((cb) => cb({ host: "Outlook" }));
    await expect(loadOffice()).resolves.toBeDefined();
  });
});
