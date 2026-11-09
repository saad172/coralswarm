import { afterEach, describe, it, expect } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/preact";
import { SearchAtomsTemplate } from "../templates/SearchAtoms";
import { installFakeOpenAi } from "./fakeOpenAi";

import defaultFixture from "../../fixtures/search_atoms.default.json";
import emptyFixture from "../../fixtures/search_atoms.empty.json";

const JSDOM_WIDTH = 1024;
function setViewportWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
}
afterEach(() => setViewportWidth(JSDOM_WIDTH));

describe("SearchAtomsTemplate", () => {
  it("clamps the snippet to two lines and wraps the age tag at compact width", async () => {
    setViewportWidth(390);
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<SearchAtomsTemplate />);
    const snippet = (await waitFor(() => document.querySelector("[data-clamp]"))) as HTMLElement;
    expect(snippet.getAttribute("data-clamp")).toBe("2");
    expect(snippet.style.getPropertyValue("--csau-preview-lines")).toBe("2");
    expect(snippet.style.getPropertyValue("--csau-search-title-lines")).toBe("2");
  });

  it("keeps the single-line ellipsized snippet at regular width", async () => {
    setViewportWidth(768);
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<SearchAtomsTemplate />);
    const snippet = (await waitFor(() => document.querySelector("[data-clamp]"))) as HTMLElement;
    expect(snippet.getAttribute("data-clamp")).toBe("1");
    expect(snippet.style.getPropertyValue("--csau-preview-lines")).toBe("1");
    expect(snippet.style.getPropertyValue("--csau-search-title-lines")).toBe("1");
  });

  it("renders results with filter chip counts", async () => {
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<SearchAtomsTemplate />);
    expect(await screen.findByText(/6 corals match/)).toBeTruthy();
    expect(screen.getByText(/one key did 40k\/min/)).toBeTruthy();
  });

  it("filters results by category chip click", async () => {
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<SearchAtomsTemplate />);
    await screen.findByText(/6 corals match/);

    const slackChip = screen.getAllByRole("button").find((b) => b.textContent?.startsWith("Slack")) as HTMLElement;
    fireEvent.click(slackChip);

    expect(screen.getByText(/one key did 40k\/min/)).toBeTruthy();
    expect(screen.queryByText(/Agreed: token bucket keyed/)).toBeNull();
  });

  it("renders Empty when there are no results", async () => {
    installFakeOpenAi({ toolInput: emptyFixture.toolInput, toolOutput: emptyFixture.toolOutput });
    render(<SearchAtomsTemplate />);
    expect(await screen.findByText(/Nothing in your ocean matches that yet\./)).toBeTruthy();
  });
});
