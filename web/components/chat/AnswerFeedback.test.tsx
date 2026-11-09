// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { AnswerFeedback } from "./AnswerFeedback";
import {
  putMessageFeedback,
  clearMessageFeedback,
  MessageFeedbackDisabled,
} from "@/lib/messageFeedbackApi";
const admission = vi.hoisted(() => ({ key: "user:org", queryScope: "scope", ready: true, getToken: () => "token" }));
vi.mock("@/lib/useEvidenceAdmission", () => ({
  useEvidenceAdmission: () => admission,
}));
vi.mock("@/lib/messageFeedbackApi", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  putMessageFeedback: vi.fn(),
  clearMessageFeedback: vi.fn(),
}));
const mount = (props = {}) =>
  render(<AnswerFeedback conversationId="c" messageId="m" initial={null} {...props} />);
beforeEach(() => {
  vi.mocked(putMessageFeedback).mockImplementation(async (_t, _c, _m, value) => ({
    vote: value.vote,
    reasons: value.reasons ?? [],
    note: value.note ?? null,
  }));
});
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});
it("toggles up and clears it", async () => {
  mount();
  const up = screen.getByRole("button", { name: "Good answer" });
  fireEvent.click(up);
  await waitFor(() => expect(up).not.toBeDisabled());
  expect(up).toHaveAttribute("aria-pressed", "true");
  fireEvent.click(up);
  await waitFor(() => expect(clearMessageFeedback).toHaveBeenCalledWith("token", "c", "m"));
});
it("sends multiple reasons and a note, restores down votes", async () => {
  mount({ initial: { vote: "down", reasons: ["wrong"], note: "old" } });
  const down = screen.getByRole("button", { name: "Something's off" });
  expect(down).toHaveAttribute("aria-pressed", "true");
  fireEvent.click(down);
  fireEvent.click(screen.getByRole("button", { name: "Missing source" }));
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "check this" } });
  fireEvent.click(screen.getByRole("button", { name: "Send feedback" }));
  await waitFor(() =>
    expect(putMessageFeedback).toHaveBeenCalledWith("token", "c", "m", {
      vote: "down",
      reasons: ["wrong", "missing_source"],
      note: "check this",
    }),
  );
});
it("cancels an empty down vote and returns focus on Escape", async () => {
  mount();
  const down = screen.getByRole("button", { name: "Something's off" });
  fireEvent.click(down);
  await waitFor(() => expect(screen.getByRole("button", { name: "Cancel" })).not.toBeDisabled());
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  await waitFor(() => expect(clearMessageFeedback).toHaveBeenCalled());
  await waitFor(() => expect(down).not.toBeDisabled());
  fireEvent.click(down);
  fireEvent.keyDown(document, { key: "Escape" });
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(down).toHaveFocus();
});
it("rolls back on errors and allows a retry", async () => {
  vi.mocked(putMessageFeedback).mockRejectedValueOnce(new Error("offline"));
  mount();
  const up = screen.getByRole("button", { name: "Good answer" });
  fireEvent.click(up);
  await screen.findByText("Your feedback was not saved. Try again.");
  expect(up).toHaveAttribute("aria-pressed", "false");
  fireEvent.click(up);
  await waitFor(() => expect(up).not.toBeDisabled());
  expect(up).toHaveAttribute("aria-pressed", "true");
});
it("hides read-only and disabled feedback", async () => {
  const view = mount({ readOnly: true });
  expect(screen.queryByRole("button")).toBeNull();
  view.unmount();
  vi.mocked(putMessageFeedback).mockRejectedValueOnce(new MessageFeedbackDisabled());
  mount();
  fireEvent.click(screen.getByRole("button", { name: "Good answer" }));
  await waitFor(() => expect(screen.queryByRole("button")).toBeNull());
});
it('closes on outside interaction and retries the exact failed reason payload', async () => {
  mount({ initial: { vote: 'down', reasons: [], note: null } });
  const down = screen.getByRole('button', { name: "Something's off" });
  fireEvent.click(down); fireEvent.pointerDown(document.body);
  expect(screen.queryByRole('dialog')).toBeNull(); expect(down).toHaveFocus();
  fireEvent.click(down); fireEvent.click(screen.getByRole('button', { name: 'Outdated' }));
  vi.mocked(putMessageFeedback).mockRejectedValueOnce(new Error('offline'));
  fireEvent.click(screen.getByRole('button', { name: 'Send feedback' }));
  fireEvent.click(await screen.findByRole('button', { name: 'Retry feedback' }));
  await waitFor(() => expect(putMessageFeedback).toHaveBeenLastCalledWith('token', 'c', 'm', { vote: 'down', reasons: ['outdated'], note: null }));
});
it('resets state on a different conversation and ignores a late failure', async () => {
  let reject!: (error: Error) => void;
  vi.mocked(putMessageFeedback).mockReturnValueOnce(new Promise((_resolve, fail) => { reject = fail; }));
  const view = mount(); fireEvent.click(screen.getByRole('button', { name: 'Good answer' }));
  await waitFor(() => expect(putMessageFeedback).toHaveBeenCalled());
  view.rerender(<AnswerFeedback conversationId="other" messageId="m" initial={null} />);
  reject(new Error('late'));
  await waitFor(() => expect(screen.getByRole('button', { name: 'Good answer' })).toHaveAttribute('aria-pressed', 'false'));
  expect(screen.queryByText('Your feedback was not saved. Try again.')).toBeNull();
});

it("hides omitted capability on the first render", () => {
  mount({ initial: undefined });
  expect(screen.queryByRole("button")).toBeNull();
});
it("limits an ambiguous 404 to its message", async () => {
  vi.mocked(putMessageFeedback).mockRejectedValueOnce(new MessageFeedbackDisabled());
  mount();
  render(<AnswerFeedback conversationId="other" messageId="other" initial={null} />);
  fireEvent.click(screen.getAllByRole("button", { name: "Good answer" })[0]);
  await waitFor(() => expect(screen.getAllByRole("button", { name: "Good answer" })).toHaveLength(1));
});
it("fences a deferred conversation read started during a write", async () => {
  const { QueryClient, QueryClientProvider } = await import("@tanstack/react-query");
  const { render: rawRender, act } = await import("@testing-library/react");
  const { conversationKey } = await import("@/lib/appQueries");
  const client = new QueryClient();
  const key = conversationKey("scope", "c");
  const old = { messages: [{ id: "m", feedback: null }] };
  client.setQueryData(key, old);
  let finishWrite!: (value: { vote: "up"; reasons: []; note: null }) => void;
  vi.mocked(putMessageFeedback).mockReturnValueOnce(new Promise(resolve => { finishWrite = resolve; }));
  rawRender(<QueryClientProvider client={client}><AnswerFeedback conversationId="c" messageId="m" initial={null} /></QueryClientProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Good answer" }));
  await waitFor(() => expect(putMessageFeedback).toHaveBeenCalled());
  let finishRead!: (value: typeof old) => void;
  const read = client.fetchQuery({ queryKey: key, queryFn: () => new Promise<typeof old>(resolve => { finishRead = resolve; }) }).catch(() => {});
  const vote = { vote: "up" as const, reasons: [] as [], note: null };
  await act(async () => { finishWrite(vote); });
  await act(async () => { finishRead(old); await read; });
  expect(client.getQueryData(key)).toEqual(old);
  expect(client.getQueryState(key)?.isInvalidated).toBe(true);
  expect(screen.getByRole("button", { name: "Good answer" })).toHaveAttribute("aria-pressed", "true");
});

it("discards a pending write after an admission scope switch", async () => {
  let finish!: (value: { vote: "up"; reasons: []; note: null }) => void;
  vi.mocked(putMessageFeedback).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  const view = mount();
  fireEvent.click(screen.getByRole("button", { name: "Good answer" }));
  await waitFor(() => expect(putMessageFeedback).toHaveBeenCalled());
  admission.key = "other";
  admission.queryScope = "other";
  view.rerender(<AnswerFeedback conversationId="c" messageId="m" initial={null} />);
  finish({ vote: "up", reasons: [], note: null });
  await waitFor(() => expect(screen.getByRole("button", { name: "Good answer" })).toHaveAttribute("aria-pressed", "false"));
  admission.key = "user:org";
  admission.queryScope = "scope";
});
