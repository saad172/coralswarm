import { afterEach, describe, it, expect } from "vitest";
import { render, screen, waitFor } from "@testing-library/preact";
import { GetReefTemplate } from "../templates/GetReef";
import { installFakeOpenAi } from "./fakeOpenAi";

import defaultFixture from "../../fixtures/get_reef.default.json";
import emptyFixture from "../../fixtures/get_reef.empty.json";

const JSDOM_WIDTH = 1024;
function setViewportWidth(width: number) {
  Object.defineProperty(window, "innerWidth", { value: width, configurable: true });
}
afterEach(() => setViewportWidth(JSDOM_WIDTH));

describe("GetReefTemplate", () => {
  it.each([
    [390, "stacked", "minmax(0, 1fr)"],
    [768, "gutter", "70px minmax(0, 1fr)"],
  ] as const)("lays the timeline date out %d-wide as %s", async (width, mode, columns) => {
    setViewportWidth(width);
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<GetReefTemplate />);
    const row = (await waitFor(() => document.querySelector("[data-date-column]"))) as HTMLElement;
    expect(row.getAttribute("data-date-column")).toBe(mode);
    expect(row.style.gridTemplateColumns).toBe(columns);
  });

  it("renders the reef title, digest summary, and reached-by timeline", async () => {
    installFakeOpenAi({ toolInput: defaultFixture.toolInput, toolOutput: defaultFixture.toolOutput });
    render(<GetReefTemplate />);
    expect(await screen.findByText("Rate limiting for the public API")).toBeTruthy();
    expect(screen.getByText(/Requests are limited per API key/)).toBeTruthy();
    expect(screen.getByText(/Burst incident: one key at 40k\/min/)).toBeTruthy();
  });

  it("renders Empty for a reef with no digest, corals, or relationships yet", async () => {
    installFakeOpenAi({ toolInput: emptyFixture.toolInput, toolOutput: emptyFixture.toolOutput });
    render(<GetReefTemplate />);
    expect(await screen.findByText(/This reef hasn't settled yet\./)).toBeTruthy();
  });
});
