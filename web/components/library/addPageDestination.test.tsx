// @vitest-environment jsdom
// /add's destination: ingest only lands in the caller's own writable ocean
// (`routes/ingest.rs` `is_own_writable_ocean`), whatever ocean is open.
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Graph } from "@/lib/api";

function graph(id: string, name: string, kind: Graph["kind"]): Graph {
  return { id, name, kind, is_primary: false, created_at: "2026-10-01T00:00:00Z", team_id: null, my_team_role: null } as Graph;
}

const mocks = vi.hoisted(() => ({
  ingest: vi.fn(),
  app: { token: "tok", currentGraph: null as Graph | null, graphs: [] as Graph[] },
}));

vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }));
vi.mock("@/lib/providers", () => ({ useApp: () => mocks.app }));
vi.mock("@/lib/engine/provider", () => ({ useEngine: () => null }));
vi.mock("@/components/library/AddRecentActivity", () => ({ AddRecentActivity: () => null }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { corals: { ingest: mocks.ingest } },
}));

import AddPage from "@/app/(app)/add/page";

const ORG = graph("g-org", "Acme commons", "org");
const MINE = graph("g-member", "Dana's ocean", "member");

beforeEach(() => {
  vi.clearAllMocks();
  mocks.ingest.mockResolvedValue({ atom_ids: ["a1"], jobs_enqueued: 1, chunks_created: 1 });
});
afterEach(cleanup);

async function paste() {
  fireEvent.change(screen.getByPlaceholderText(/paste notes/i), { target: { value: "We picked Postgres." } });
  fireEvent.click(screen.getByRole("button", { name: /release into/i }));
  await waitFor(() => expect(mocks.ingest).toHaveBeenCalled());
}

describe("/add destination", () => {
  it("posts an org member's paste to their own member ocean, not the open org ocean", async () => {
    mocks.app = { token: "tok", currentGraph: ORG, graphs: [ORG, MINE] };
    render(<AddPage />);
    expect(screen.getByRole("button", { name: /release into dana's ocean/i })).toBeInTheDocument();
    await paste();
    expect(mocks.ingest).toHaveBeenCalledWith("tok", expect.objectContaining({ graphId: "g-member" }));
  });

  it("keeps the open ocean when it is already the caller's own", async () => {
    const personal = graph("g-personal", "Personal", "personal");
    mocks.app = { token: "tok", currentGraph: personal, graphs: [personal] };
    render(<AddPage />);
    await paste();
    expect(mocks.ingest).toHaveBeenCalledWith("tok", expect.objectContaining({ graphId: "g-personal" }));
  });
});
