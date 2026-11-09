import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/preact";
import { useHostLayout } from "../layout";
import { installMcpHost } from "./fakeMcpHost";
import { installFakeOpenAi } from "./fakeOpenAi";

let host: ReturnType<typeof installMcpHost> | undefined;
afterEach(() => {
  cleanup();
  host?.close();
  host = undefined;
});

function setInnerWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
}

function stubMatchMedia(matching: Record<string, boolean>) {
  window.matchMedia = (query: string): MediaQueryList => ({
    matches: matching[query] ?? false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  });
}

function Probe() {
  const layout = useHostLayout();
  return (
    <div data-testid="probe">
      {[
        layout.width,
        layout.maxHeight ?? "null",
        layout.displayMode,
        layout.platform,
        layout.touch,
        layout.hover,
        layout.size,
        `${layout.safeArea.top}/${layout.safeArea.right}/${layout.safeArea.bottom}/${layout.safeArea.left}`,
      ].join("|")}
    </div>
  );
}

async function initialized() {
  await waitFor(() => expect(host?.packets.some((p) => p.method === "ui/notifications/initialized")).toBe(true));
}

describe("useHostLayout", () => {
  it("derives every field from the standard MCP Apps host context", async () => {
    host = installMcpHost({
      context: {
        displayMode: "pip",
        platform: "mobile",
        deviceCapabilities: { touch: true, hover: false },
        containerDimensions: { width: 390, maxHeight: 640 },
        safeAreaInsets: { top: 20, right: 0, bottom: 34, left: 0 },
      },
    });
    render(<Probe />);
    await initialized();
    await screen.findByText("390|640|pip|mobile|true|false|compact|20/0/34/0");
  });

  it("falls back to a legacy userAgent when there is no standard host, folding tablet into mobile+touch", async () => {
    installFakeOpenAi({ userAgent: { device: { type: "tablet" }, capabilities: { hover: false } } });
    render(<Probe />);
    // width/size come from the live browser fallback (no host at all);
    // platform/touch/hover come from the legacy userAgent translation.
    await screen.findByText(/\|null\|inline\|mobile\|true\|false\|/);
  });

  it("maps a legacy desktop-device host to platform web", async () => {
    installFakeOpenAi({ userAgent: { device: { type: "desktop" }, capabilities: { touch: false, hover: true } } });
    render(<Probe />);
    await screen.findByText(/\|null\|inline\|web\|false\|true\|/);
  });

  it("falls back to matchMedia/innerWidth when both the standard and legacy hosts are silent", async () => {
    setInnerWidth(390);
    stubMatchMedia({ "(pointer: coarse)": true, "(hover: hover)": false });
    render(<Probe />);
    await screen.findByText("390|null|inline|web|true|false|compact|0/0/0/0");
  });

  it("does not let a later legacy userAgent echo override a field the standard host already supplied", async () => {
    const openai = installFakeOpenAi({ userAgent: { device: { type: "mobile" }, capabilities: { touch: true, hover: false } } });
    host = installMcpHost({ context: { platform: "desktop", deviceCapabilities: { touch: false, hover: true } } });
    render(<Probe />);
    await initialized();
    await screen.findByText(/\|desktop\|false\|true\|/);
    // A late legacy echo (e.g. the host re-sending its OpenAI globals) must
    // not roll platform/touch/hover back to the legacy values above.
    openai.setGlobals({ userAgent: { device: { type: "mobile" }, capabilities: { touch: true, hover: false } } });
    await new Promise((r) => setTimeout(r, 10));
    expect(screen.getByTestId("probe").textContent).toMatch(/\|desktop\|false\|true\|/);
  });

  it("updates width on a later host-context-changed notification", async () => {
    host = installMcpHost({ context: { containerDimensions: { width: 1024 } } });
    render(<Probe />);
    await initialized();
    await screen.findByText(/^1024\|/);
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { width: 390 } });
    await screen.findByText(/^390\|/);
  });

  it("prefers a host-supplied width until this window's own resize fires, then prefers live innerWidth even over a later host update", async () => {
    setInnerWidth(1024);
    host = installMcpHost({ context: { containerDimensions: { width: 768 } } });
    render(<Probe />);
    await initialized();
    await screen.findByText(/^768\|/);

    setInnerWidth(390);
    await act(async () => {
      window.dispatchEvent(new Event("resize"));
    });
    await screen.findByText(/^390\|/);

    // A host update AFTER the resize must not un-do the live value.
    host.notify("ui/notifications/host-context-changed", { containerDimensions: { width: 900 } });
    await new Promise((r) => setTimeout(r, 10));
    expect(screen.getByTestId("probe").textContent).toMatch(/^390\|/);
  });

  it("never turns a host's current-iframe height into maxHeight (contextGlobals contract, exercised through the hook)", async () => {
    host = installMcpHost({ context: { containerDimensions: { width: 768, height: 179 } } });
    render(<Probe />);
    await initialized();
    await screen.findByText(/^768\|null\|/);
  });

  it("applies every safe-area inset, including the top one", async () => {
    const { safeAreaPadding } = await import("../layout");
    expect(safeAreaPadding({ top: 47, right: 0, bottom: 34, left: 0 })).toEqual({
      paddingTop: 47,
      paddingBottom: 34,
      paddingLeft: undefined,
      paddingRight: undefined,
    });
    expect(safeAreaPadding({ top: 0, right: 0, bottom: 0, left: 0 })).toEqual({});
  });

  it.each([
    [200, "compact"],
    [480, "regular"],
    [899, "regular"],
    [900, "wide"],
  ] as const)("classifies width %d as %s", async (width, tier) => {
    host = installMcpHost({ context: { containerDimensions: { width } } });
    render(<Probe />);
    await initialized();
    await screen.findByText(new RegExp(`\\|${tier}\\|`));
  });
});
