import { ApiError } from "./api";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { guardedFetch } from "./transport";

export type FeedbackVote = "up" | "down";
export const FEEDBACK_REASONS = ["wrong", "missing_source", "outdated", "other"] as const;
export type FeedbackReason = (typeof FEEDBACK_REASONS)[number];

export interface MessageFeedback {
  vote: FeedbackVote;
  reasons: FeedbackReason[];
  note: string | null;
}

export class MessageFeedbackDisabled extends ApiError {
  constructor() {
    super("Answer feedback is not enabled.", 404);
  }
}

export function messageFeedbackPath(conversationId: string, messageId: string): string {
  return `/v1/conversations/${encodeURIComponent(conversationId)}/messages/${encodeURIComponent(messageId)}/feedback`;
}

async function send(token: string, conversationId: string, messageId: string, init: RequestInit): Promise<Response> {
  if (!token) throw new Error("Not authenticated");
  const res = await guardedFetch(
    messageFeedbackPath(conversationId, messageId),
    { ...init, cache: "no-store", headers: { ...init.headers, Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() } },
    { signal: init.signal ?? undefined },
  );
  if (res.status === 404) throw new MessageFeedbackDisabled();
  if (!res.ok) {
    let message = "Your feedback was not saved. Try again.";
    try {
      const body: unknown = await res.json();
      if (res.status === 422 && body && typeof body === "object" && typeof (body as { error?: unknown }).error === "string") {
        message = (body as { error: string }).error;
      }
    } catch {}
    throw new ApiError(message, res.status);
  }
  return res;
}

export async function putMessageFeedback(
  token: string,
  conversationId: string,
  messageId: string,
  feedback: { vote: FeedbackVote; reasons?: FeedbackReason[]; note?: string | null },
  signal?: AbortSignal,
): Promise<MessageFeedback> {
  const body = { vote: feedback.vote, reasons: feedback.reasons ?? [], note: feedback.note ?? null };
  const res = await send(token, conversationId, messageId, {
    method: "PUT",
    signal,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return (await res.json()) as MessageFeedback;
}

export async function clearMessageFeedback(
  token: string,
  conversationId: string,
  messageId: string,
  signal?: AbortSignal,
): Promise<void> {
  await send(token, conversationId, messageId, { method: "DELETE", signal });
}
