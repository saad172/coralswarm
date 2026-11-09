// @vitest-environment jsdom
import React from "react";
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { TopicStrip } from "./TopicStrip";
import { connectorsApi } from "@/lib/connectorsApi";
import { apiRequest } from "@/lib/api";
vi.mock("@/lib/api", () => ({ apiRequest: vi.fn().mockResolvedValue({}) }));
afterEach(cleanup);
it("shows topic totals and recent counts", () => {
 render(<TopicStrip topics={[{ id: "a", label: "Design", total: 12, last_7_days: 3 }]} />);
 expect(screen.getByText(/Design/).textContent).toContain("12");
 expect(screen.getByText(/this week/).textContent).toContain("3");
});
it("uses the B5 source and cursor contract", async () => {
 await connectorsApi.captures("token", "google", { source: "gmail", cursor: "a+b" });
 expect(apiRequest).toHaveBeenCalledWith("/v1/connectors/google/captures?source=gmail&cursor=a%2Bb", "token");
});
it("disconnects through the unified provider endpoint", async () => {
 await connectorsApi.disconnect("token", "slack");
 expect(apiRequest).toHaveBeenCalledWith("/v1/connectors/slack", "token", { method: "DELETE" });
});
