// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, expect, it, vi } from "vitest";
import { TopicDigest, ReefDigestPanel } from "./TopicDigest";
import { readReefDigest } from "@/lib/reefDigestApi";
import { ApiError } from "@/lib/api";

vi.mock("@/lib/reefDigestApi", () => ({ readReefDigest: vi.fn() }));
const graph = "11111111-1111-4111-8111-111111111111", node = "22222222-2222-4222-8222-222222222222", atom = "33333333-3333-4333-8333-333333333333";
afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("shows a served digest with quiet attribution and links each excerpt to its item", async () => {
  vi.mocked(readReefDigest).mockResolvedValue({
    state: "served", summary: "Atlas is the onboarding rewrite, shipping in Q4.",
    excerpts: [{ atom_id: atom, excerpt: "We agreed Atlas ships in Q4.", contribution: "Sets the ship date" }],
    attribution: { model: "deepseek/deepseek-v4-flash", generated_at: "2026-10-03T12:00:00Z", input_count: 12 },
  });
  render(<TopicDigest identity="acct" getToken={() => "t"} nodeId={node} graphId={graph} narrowed />);
  expect(await screen.findByText("Atlas is the onboarding rewrite, shipping in Q4.")).toBeInTheDocument();
  expect(readReefDigest).toHaveBeenCalledWith("t", node, expect.any(AbortSignal));
  expect(screen.getByText(/Generated from 12 items · deepseek-v4-flash/)).toBeInTheDocument();
  expect(screen.getByText(/covers the whole topic/)).toBeInTheDocument();
  fireEvent.click(screen.getByText("Item excerpts · 1"));
  expect(screen.getByRole("link", { name: "Open item 1" })).toHaveAttribute("href", `/coral?id=${atom}&graph_id=${graph}`);
  expect(screen.getByText("Sets the ship date")).toBeInTheDocument();
});

it("says only 'unavailable' for a withheld digest, and whether one is being prepared", () => {
  const { rerender } = render(<ReefDigestPanel graphId={graph} view={{ state: "unavailable", regenerating: true }} />);
  expect(screen.getByText(/Summary unavailable\. A fresh summary is being prepared/)).toBeInTheDocument();
  rerender(<ReefDigestPanel graphId={graph} view={{ state: "unavailable", regenerating: false }} />);
  expect(screen.getByText(/Summary unavailable\. The sourced statements below are the record\./)).toBeInTheDocument();
  rerender(<ReefDigestPanel graphId={graph} view={{ state: "empty" }} />);
  expect(screen.getByText("No readable items to summarize yet.")).toBeInTheDocument();
});

it("renders the unattributed local-store digest without inventing attribution", () => {
  render(<ReefDigestPanel graphId={graph} view={{ state: "served", summary: "Local summary", excerpts: [], attribution: null }} />);
  expect(screen.getByText("Generated summary")).toBeInTheDocument();
  expect(screen.queryByText(/corals ·/)).not.toBeInTheDocument();
  expect(screen.queryByText(/Coral excerpts/)).not.toBeInTheDocument();
});

it("shows a loading skeleton, then a recoverable error confined to the panel", async () => {
  vi.mocked(readReefDigest).mockRejectedValueOnce(new ApiError("The topic summary is unavailable right now. Retry when the connection is restored.", 503))
    .mockResolvedValueOnce({ state: "empty" });
  render(<TopicDigest identity="acct" getToken={() => "t"} nodeId={node} graphId={graph} />);
  expect(screen.getByRole("status", { name: "Reading topic summary" })).toBeInTheDocument();
  expect(await screen.findByRole("alert")).toHaveTextContent("The topic summary is unavailable right now.");
  fireEvent.click(screen.getByRole("button", { name: "Retry summary" }));
  expect(await screen.findByText("No readable items to summarize yet.")).toBeInTheDocument();
});

it("does not read without a verified account", () => {
  render(<TopicDigest identity="acct" getToken={() => null} nodeId={node} graphId={graph} />);
  expect(readReefDigest).not.toHaveBeenCalled();
});
