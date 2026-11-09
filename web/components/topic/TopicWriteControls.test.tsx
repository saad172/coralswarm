// @vitest-environment jsdom
// Write controls on topic curation + graph relationship review honour the
// server's `can_edit`, and a 404 from a write reads as "you can't change this".
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, type Reef } from "@/lib/api";
import { NO_EDIT_MESSAGE, NO_EDIT_TITLE, writeErrorMessage } from "@/lib/editAccess";
import { topicCandidatesQueryOptions } from "@/lib/appQueries";
import { makeQueryClient } from "@/lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { TopicMerge } from "./TopicMerge";
import { TopicCandidates } from "./TopicCandidates";
import { EdgeReviewActions } from "@/components/graph/GraphChrome";

vi.mock("@/lib/providers", () => ({
  useApp: () => ({ authReady: false, getAuthToken: () => "tok", queryScope: "test" }),
}));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { reefs: { merge: vi.fn(), linkAtom: vi.fn() } },
}));

const reef = (id: string, title: string): Reef => ({
  node_id: id, type: "entity", title, scope: "team", atom_count: 1,
  created_at: "2026-03-01T00:00:00Z", updated_at: "2026-03-01T00:00:00Z",
});
const KEEP = reef("keep-1", "Keep topic");
const DUP = reef("dup-1", "Duplicate topic");

afterEach(() => { cleanup(); vi.clearAllMocks(); });

describe("writeErrorMessage", () => {
  it("turns a 404 into the friendly can't-change message", () => {
    expect(writeErrorMessage(new ApiError("reef not found", 404), "x")).toBe(NO_EDIT_MESSAGE);
  });
  it("keeps other errors' own text, then the fallback", () => {
    expect(writeErrorMessage(new ApiError("boom", 500), "x")).toBe("boom");
    expect(writeErrorMessage("nope", "fallback")).toBe("fallback");
  });
  it("uses plain words only", () => {
    expect(`${NO_EDIT_TITLE} ${NO_EDIT_MESSAGE}`).not.toMatch(/ocean|reef|coral/i);
  });
});

describe("TopicMerge", () => {
  const view = (canEdit: boolean) =>
    render(<TopicMerge reef={KEEP} reefs={[KEEP, DUP]} token="tok" onMerged={() => {}} canEdit={canEdit} />);

  it("is usable when can_edit is true", () => {
    view(true);
    expect(screen.getByRole("combobox")).toBeEnabled();
    expect(screen.getByRole("combobox")).not.toHaveAttribute("title");
    expect(screen.queryByText(`${NO_EDIT_TITLE}.`)).not.toBeInTheDocument();
  });

  it("is disabled with a reason when can_edit is false, even with a pick", () => {
    view(false);
    const select = screen.getByRole("combobox");
    expect(select).toBeDisabled();
    expect(select).toHaveAttribute("title", NO_EDIT_TITLE);
    expect(screen.getByRole("button", { name: "Merge" })).toBeDisabled();
    expect(screen.getByText(`${NO_EDIT_TITLE}.`)).toBeInTheDocument();
  });

  it("shows the friendly message when the merge comes back 404", async () => {
    vi.mocked(api.reefs.merge).mockRejectedValue(new ApiError("reef not found", 404));
    view(true);
    fireEvent.change(screen.getByRole("combobox"), { target: { value: DUP.node_id } });
    fireEvent.click(screen.getByRole("button", { name: "Merge" }));
    expect(await screen.findByText(NO_EDIT_MESSAGE)).toBeInTheDocument();
  });
});

describe("TopicCandidates", () => {
  const cand = { atom_id: "a1", source_type: "slack", source_id: "s1", timestamp: null, participants: [], similarity: 0.9, title: "A related item", snippet: null };
  function view(canEdit: boolean) {
    const client = makeQueryClient();
    client.setQueryData(topicCandidatesQueryOptions({ getAuthToken: () => "tok", scope: "test" }, KEEP.node_id).queryKey, [cand]);
    return render(
      <QueryClientProvider client={client}>
        <TopicCandidates reef={KEEP} token="tok" onLinked={() => {}} canEdit={canEdit} />
      </QueryClientProvider>,
    );
  }

  it("lets you attach when can_edit is true", () => {
    view(true);
    expect(screen.getByRole("button", { name: "Attach" })).toBeEnabled();
  });

  it("disables Attach with a reason when can_edit is false", () => {
    view(false);
    const attach = screen.getByRole("button", { name: "Attach" });
    expect(attach).toBeDisabled();
    expect(attach).toHaveAttribute("title", NO_EDIT_TITLE);
    expect(screen.getByRole("button", { name: "Dismiss suggestion" })).toBeEnabled();
  });

  it("shows the friendly message when attach comes back 404", async () => {
    vi.mocked(api.reefs.linkAtom).mockRejectedValue(new ApiError("not found", 404));
    view(true);
    fireEvent.click(screen.getByRole("button", { name: "Attach" }));
    await waitFor(() => expect(screen.getByText(NO_EDIT_MESSAGE)).toBeInTheDocument());
  });
});

describe("EdgeReviewActions (graph relationship review)", () => {
  // The atlas and relationship lists carry no per-item can_edit, so the
  // buttons stay for a signed-in user and a refusal surfaces as the 404 message.
  it("shows Confirm and Reject for a suggested edge, with the refusal message when set", () => {
    render(
      <EdgeReviewActions edgeKey="e1" status="suggested" canReview
        state={{ busy: false, error: writeErrorMessage(new ApiError("nf", 404), "couldn’t confirm") }}
        onConfirm={() => {}} onReject={() => {}} />,
    );
    expect(screen.getByRole("button", { name: "Confirm" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Reject" })).toBeEnabled();
    expect(screen.getByText(NO_EDIT_MESSAGE)).toBeInTheDocument();
  });

  it("renders nothing without review access", () => {
    const { container } = render(
      <EdgeReviewActions edgeKey="e1" status="suggested" canReview={false} state={undefined} onConfirm={() => {}} onReject={() => {}} />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});
