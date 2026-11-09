import { describe, it, expect, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { AskOceanTemplate } from "../templates/AskOcean";
import { SearchAtomsTemplate } from "../templates/SearchAtoms";
import { GetReefTemplate } from "../templates/GetReef";
import { RecentActivityTemplate } from "../templates/RecentActivity";
import { tokensFor } from "../theme";
import { installFakeOpenAi } from "./fakeOpenAi";
import synthesized from "../../fixtures/ask_ocean.synthesized.json";

describe("shared presentation widget adapters", () => {
  it.each([
    [AskOceanTemplate, "Waiting for your answer…"],
    [SearchAtomsTemplate, "Searching corals…"],
    [GetReefTemplate, "Loading reef…"],
    [RecentActivityTemplate, "Loading activity…"],
  ] as const)("reports truthful activity through the shared frame for %s", (Template, label) => {
    installFakeOpenAi({ theme: "dark" });
    render(<Template />);
    expect(screen.getByRole("status").textContent).toBe(label);
    const frame = document.querySelector<HTMLElement>(".csau-card-frame");
    expect(frame?.style.getPropertyValue("--csau-bg")).toBe(tokensFor("dark").bg);
    expect(frame?.style.getPropertyValue("--csau-font-body")).toBeTruthy();
  });

  it.each(["inline", "fullscreen"] as const)("preserves citation number and returns keyboard focus after closing %s evidence", async displayMode => {
    installFakeOpenAi({ displayMode, toolInput: synthesized.toolInput, toolOutput: synthesized.toolOutput });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    const source = screen.getByRole("button", { name: /^Open source 2:/ });
    source.focus();
    fireEvent.click(source);
    const heading = await screen.findByRole("heading", { level: 3 });
    expect(document.activeElement).toBe(heading);
    expect(document.querySelector(".csau-detail-number")?.textContent).toBe("Source 2");
    expect(document.querySelector(".csau-excerpt-text")?.textContent).toContain("one key did 40k/min for twelve minutes");
    expect(source.getAttribute("aria-pressed")).toBe("true");
    fireEvent.keyDown(heading, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("heading", { level: 3 })).toBeNull());
    expect(document.activeElement).toBe(source);
  });

  it("does not keep an old fetched body after switching to a missing source", async () => {
    const first = { ...synthesized.toolOutput.citation_details[0]!, atom_id: "omitted-one", body_omitted: true };
    const second = { ...synthesized.toolOutput.citation_details[1]!, atom_id: "omitted-two", body_omitted: true };
    const fetch = vi.fn((args: Record<string, unknown>) => args.atom_id === first.atom_id ? { content: "FIRST FETCHED BODY" } : undefined);
    installFakeOpenAi({ toolOutput: { ...synthesized.toolOutput, citation_details: [first, second] }, callToolHandlers: { get_atom: fetch } });
    render(<AskOceanTemplate />);
    await screen.findByText(/Token bucket per API key/);
    fireEvent.click(screen.getByRole("button", { name: /^Open source 1:/ }));
    expect(await screen.findByText("FIRST FETCHED BODY")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /^Open source 2:/ }));
    await waitFor(() => expect(screen.getByText("Content unavailable.")).toBeTruthy());
    expect(screen.queryByText("FIRST FETCHED BODY")).toBeNull();
    expect(fetch.mock.calls.map(([args]) => args.atom_id)).toEqual([first.atom_id, second.atom_id]);
    await act(async () => {});
  });
});
