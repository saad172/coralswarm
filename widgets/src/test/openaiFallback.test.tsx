import { describe, it, expect } from "vitest";
import { render, screen, waitFor } from "@testing-library/preact";
import { useToolOutput, useToolInput } from "../openai";

// No window.openai installed in this file at all — every test here exercises
// the NO-HOST fallback path (inline #cs-data / postMessage) directly.

function Probe() {
  const output = useToolOutput<{ hello?: string }>();
  const input = useToolInput<{ q?: string }>();
  return (
    <div>
      <span data-testid="output">{output?.hello ?? "none"}</span>
      <span data-testid="input">{input?.q ?? "none"}</span>
    </div>
  );
}

describe("useToolOutput/useToolInput no-host postMessage fallback", () => {
  it("ignores a message whose source is not window.parent", async () => {
    render(<Probe />);
    expect(screen.getByTestId("output").textContent).toBe("none");

    // A distinct `Window` to stand in for "some other frame that obtained a
    // reference to this one" — an iframe's contentWindow, never this
    // top-level test document's own `window`/`window.parent`.
    const iframe = document.createElement("iframe");
    document.body.appendChild(iframe);
    const foreignWindow = iframe.contentWindow;
    expect(foreignWindow).not.toBeNull();

    window.dispatchEvent(
      new MessageEvent("message", {
        data: { type: "cs:tool-output", toolOutput: { hello: "evil" } },
        source: foreignWindow,
      }),
    );

    // Give any (incorrect) state update a chance to land, then assert it
    // did not.
    await new Promise((r) => setTimeout(r, 20));
    expect(screen.getByTestId("output").textContent).toBe("none");
    iframe.remove();
  });

  it("accepts a message from window.parent and updates BOTH toolOutput and toolInput", async () => {
    render(<Probe />);

    window.dispatchEvent(
      new MessageEvent("message", {
        data: { type: "cs:tool-output", toolOutput: { hello: "world" }, toolInput: { q: "rate limits" } },
        source: window.parent,
      }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("output").textContent).toBe("world");
      expect(screen.getByTestId("input").textContent).toBe("rate limits");
    });
  });
});
