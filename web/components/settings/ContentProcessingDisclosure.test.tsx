// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { CONTENT_PROCESSING_CARDS, ContentProcessingDisclosure } from "./ContentProcessingDisclosure";

afterEach(cleanup);

describe("ContentProcessingDisclosure", () => {
  it("states where indexing and answers are processed", () => {
    render(<ContentProcessingDisclosure />);
    expect(screen.getByText("where your content is processed")).toBeInTheDocument();
    expect(screen.getByText("Indexing & extraction")).toBeInTheDocument();
    expect(screen.getByText("Answer synthesis")).toBeInTheDocument();
    expect(screen.getAllByText(/zero.data.retention/i).length).toBeGreaterThanOrEqual(2);
  });

  it("offers no 'Local only' choice or retired-setting wording, and uses plain words", () => {
    const { container } = render(<ContentProcessingDisclosure />);
    const text = container.textContent ?? "";
    expect(text).not.toMatch(/local only|retired|sources.only|turn written answers/i);
    expect(text).not.toMatch(/\b(ocean|reef|coral)s?\b/i);
    expect(CONTENT_PROCESSING_CARDS).toHaveLength(2);
    expect(container.querySelector("button")).toBeNull();
  });
});
