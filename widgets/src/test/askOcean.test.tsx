import { afterEach, describe, it, expect, vi } from "vitest";
import { act, render, screen, waitFor, fireEvent } from "@testing-library/preact";
import { AskOceanTemplate } from "../templates/AskOcean";
import { installFakeOpenAi } from "./fakeOpenAi";

import synthesizedFixture from "../../fixtures/ask_ocean.synthesized.json";
import emptyFixture from "../../fixtures/ask_ocean.empty.json";
import workingFixture from "../../fixtures/ask_ocean.working.json";
import atomFixture from "../../fixtures/get_atom.body_omitted.json";

// The preview count/arrangement tests below read the container width from
// `window.innerWidth` (no MCP host in these legacy-bridge tests).
const JSDOM_WIDTH = 1024;
function setViewportWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
}
afterEach(() => setViewportWidth(JSDOM_WIDTH));

describe("AskOceanTemplate", () => {
  it.each([
    [390, "stacked"],
    [768, "stacked"],
    [1280, "beside"],
  ] as const)("arranges the fullscreen sources rail %s-wide as %s", async (width, arrangement) => {
    setViewportWidth(width);
    installFakeOpenAi({ displayMode: "fullscreen", toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const split = document.querySelector("[data-sources]") as HTMLElement;
    expect(split.getAttribute("data-sources")).toBe(arrangement);
    expect(split.style.flexDirection).toBe(arrangement === "stacked" ? "column" : "row");
  });

  it.each([
    [true, 44],
    [false, null],
  ] as const)("sizes the inline expand control for touch=%s", async (touch, expected) => {
    installFakeOpenAi({
      toolInput: synthesizedFixture.toolInput,
      toolOutput: synthesizedFixture.toolOutput,
      userAgent: { device: { type: touch ? "mobile" : "desktop" }, capabilities: { touch, hover: !touch } },
    });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const expand = screen.getByRole("button", { name: "expand" });
    expect(expand.style.minHeight).toBe(expected === null ? "" : `${expected}px`);
    expect(expand.style.display).toBe("inline-flex");
    expect(document.querySelector("[data-card]")!.contains(expand)).toBe(true);
  });

  it("scrolls the answer under a host cap and leaves expand outside the scroller", async () => {
    installFakeOpenAi({
      maxHeight: 200,
      toolInput: synthesizedFixture.toolInput,
      toolOutput: synthesizedFixture.toolOutput,
    });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const body = document.querySelector("[data-scroll-body]") as HTMLElement;
    const expand = screen.getByRole("button", { name: "expand" });
    expect(body.getAttribute("data-scroll-body")).toBe("bounded");
    expect(body.contains(expand)).toBe(false);
    expect(document.querySelector("[data-card]")!.contains(expand)).toBe(true);
  });

  it("subtracts Card safe-area insets from the inline scroll budget", async () => {
    const openai = installFakeOpenAi({
      maxHeight: 200,
      toolInput: synthesizedFixture.toolInput,
      toolOutput: synthesizedFixture.toolOutput,
    });
    openai.setGlobals({ safeArea: { insets: { top: 47, right: 0, bottom: 34, left: 0 } } });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const body = document.querySelector("[data-scroll-body]") as HTMLElement;
    // 200 cap − 88 chrome − 47 top − 34 bottom.
    expect(body.style.maxHeight).toBe("31px");
    const expand = screen.getByRole("button", { name: "expand" });
    expect(body.contains(expand)).toBe(false);
    expect(document.querySelector("[data-card]")!.contains(expand)).toBe(true);
  });

  it("carries the host's top safe-area inset into the fullscreen shell", async () => {
    setViewportWidth(1280);
    const openai = installFakeOpenAi({ displayMode: "fullscreen", toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    openai.setGlobals({ safeArea: { insets: { top: 47, right: 0, bottom: 34, left: 0 } } });
    const { container } = render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const shell = container.firstElementChild as HTMLElement;
    expect(shell.style.paddingTop).toBe("47px");
    expect(shell.style.paddingBottom).toBe("34px");
  });

  it("bounds the fullscreen shell to the iframe viewport rather than height 100%", async () => {
    setViewportWidth(1280);
    installFakeOpenAi({ displayMode: "fullscreen", toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    const { container } = render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const shell = container.firstElementChild as HTMLElement;
    // jsdom's CSS.supports says no to dvh, so the vh fallback is what lands
    // here; either way it must never be the collapsing `100%`.
    expect(shell.style.height).toMatch(/^100(d?)vh$/);
    expect(shell.style.maxHeight).toBe(shell.style.height);
  });

  it("renders the synthesized answer with inline cite chips", async () => {
    installFakeOpenAi({ toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    render(<AskOceanTemplate />);
    expect(await screen.findByText(/Token bucket per API key/)).toBeTruthy();
    expect(screen.getByText(/answered from 5 sources/)).toBeTruthy();
  });

  it("expands a source INLINE on chip click without ever calling get_atom (not body_omitted)", async () => {
    const fake = installFakeOpenAi({ toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);

    const chip2 = screen.getByRole("button", { name: "2" });
    fireEvent.click(chip2);

    // The expanded card renders the full citation body inline.
    await waitFor(() => expect(document.querySelector(".csau-excerpt-text")?.textContent).toContain("one key did 40k/min for twelve minutes"));
    expect(fake.callToolCalls).toEqual([]);
  });

  it("calls get_atom ONLY for a citation with body_omitted: true, and only that one", async () => {
    const fake = installFakeOpenAi({
      toolInput: synthesizedFixture.toolInput,
      toolOutput: synthesizedFixture.toolOutput,
      callToolHandlers: {
        get_atom: (args) => (args.atom_id === atomFixture.atom_id ? atomFixture : undefined),
      },
    });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);

    // Chip 5 is the body_omitted citation.
    const chip5 = screen.getByRole("button", { name: "5" });
    fireEvent.click(chip5);

    await waitFor(() => {
      expect(fake.callToolCalls).toEqual([{ name: "get_atom", args: { atom_id: atomFixture.atom_id } }]);
    });
    // The full body from get_atom's fixture eventually renders.
    expect(await screen.findByText(/Weekly digest note: rate limiting follow-ups/)).toBeTruthy();

    // Collapse it, then expand a NON-body_omitted chip — get_atom must not
    // be called again.
    const close = screen.getByRole("button", { name: "Close" });
    fireEvent.click(close);
    const chip3 = screen.getByRole("button", { name: "3" });
    fireEvent.click(chip3);
    await waitFor(() => expect(document.querySelector(".csau-excerpt-text")?.textContent).toContain("Added X-RateLimit-Opt-In header gate"));
    expect(fake.callToolCalls.filter((c) => c.name === "get_atom")).toHaveLength(1);
  });

  it("renders Empty for a no_answer completion", async () => {
    installFakeOpenAi({ toolInput: emptyFixture.toolInput, toolOutput: emptyFixture.toolOutput });
    render(<AskOceanTemplate />);
    expect(await screen.findByText(/Nothing in your ocean matches that yet\./)).toBeTruthy();
  });

  it("renders Working while a legacy task_id is polling, then the answer once get_task_result completes", async () => {
    const fake = installFakeOpenAi({
      toolInput: workingFixture.toolInput,
      toolOutput: workingFixture.toolOutput,
      callToolHandlers: {
        get_task_result: () => synthesizedFixture.toolOutput,
      },
    });
    render(<AskOceanTemplate />);

    // Working covers the gap before the first poll resolves.
    expect(screen.getByText(/Waiting for your answer…/)).toBeTruthy();

    await waitFor(() => {
      expect(fake.callToolCalls.some((c) => c.name === "get_task_result")).toBe(true);
    });
    expect(await screen.findByText(/Token bucket per API key/)).toBeTruthy();
  });

  it("renders without window.openai at all (plain iframe / claude.ai host)", async () => {
    // No installFakeOpenAi() call — window.openai stays undefined. The
    // template still mounts and shows Working (no data reachable), rather
    // than throwing.
    render(<AskOceanTemplate />);
    expect(screen.getByText(/Waiting for your answer…/)).toBeTruthy();
  });

  it("restarts elapsed time when a later invocation returns to Working", async () => {
    const fake = installFakeOpenAi({ toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    try {
      vi.setSystemTime(Date.now() + 90_000);
      await act(async () => { fake.setGlobals({ toolOutput: null }); });
      await screen.findByText(/Waiting for your answer…/);
      await act(async () => { await vi.advanceTimersByTimeAsync(1_000); });
      expect(screen.getByText("1s")).toBeTruthy();
      expect(screen.queryByText("91s")).toBeNull();
    } finally {
      vi.clearAllTimers();
      vi.useRealTimers();
    }
  });

  it("resolves a 'failed' terminal poll result to the error state, not Working, and stops polling", async () => {
    const fake = installFakeOpenAi({
      toolInput: workingFixture.toolInput,
      toolOutput: workingFixture.toolOutput,
      callToolHandlers: {
        get_task_result: () => ({ status: "failed", error: { code: "synthesis_failed", message: "the ocean is unreachable" } }),
      },
    });
    render(<AskOceanTemplate />);
    expect(await screen.findByText(/the ocean is unreachable/)).toBeTruthy();
    expect(screen.queryByText(/Waiting for your answer…/)).toBeNull();
    // No repeat poll once terminal — isAskOceanWorking must not have
    // classified "failed" as "still working".
    expect(fake.callToolCalls.filter((c) => c.name === "get_task_result")).toHaveLength(1);
  });

  it("resolves a 'cancelled' terminal poll result to an explicit error state", async () => {
    installFakeOpenAi({
      toolInput: workingFixture.toolInput,
      toolOutput: workingFixture.toolOutput,
      callToolHandlers: {
        get_task_result: () => ({ status: "cancelled" }),
      },
    });
    render(<AskOceanTemplate />);
    expect(await screen.findByText("The request was cancelled.")).toBeTruthy();
  });

  it("catches a rejected callTool during polling into the explicit error state (no unhandled rejection)", async () => {
    installFakeOpenAi({
      toolInput: workingFixture.toolInput,
      toolOutput: workingFixture.toolOutput,
      callToolHandlers: {
        get_task_result: () => {
          throw new Error("network fell over");
        },
      },
    });
    render(<AskOceanTemplate />);
    expect(await screen.findByText(/network fell over/)).toBeTruthy();
  });

  it("clears the previous answer when a NEW working task starts in the same widget instance", async () => {
    const fake = installFakeOpenAi({ toolInput: synthesizedFixture.toolInput, toolOutput: synthesizedFixture.toolOutput });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);

    // A second ask_ocean call lands in the SAME widget instance — the host
    // pushes a new "working" envelope for a different task.
    const secondTaskId = "22222222-2222-4222-8222-222222222222";
    fake.setGlobals({ toolOutput: { status: "working", task_id: secondTaskId } });

    // The stale first answer must be gone — polling for the second task's
    // id must actually start (CodeRabbit round 1: a stale `resolved` blocks
    // it forever).
    await waitFor(() => {
      expect(screen.queryByText(/Token bucket per API key/)).toBeNull();
      expect(screen.getByText(/Waiting for your answer…/)).toBeTruthy();
    });

    // No get_atom/get_task_result handler was configured for this fixture's
    // second task — the widget just stays on Working, which is enough to
    // prove `resolved` was actually cleared rather than left stale.
    await waitFor(() => {
      expect(fake.callToolCalls.some((c) => c.name === "get_task_result" && c.args.task_id === secondTaskId)).toBe(true);
    });
  });
});
