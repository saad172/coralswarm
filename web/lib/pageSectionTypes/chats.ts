import type { ActivityEvent, OAuthConnection, ConversationSummary } from "../api";
export interface ConversationListPayload { conversations: ConversationSummary[]; shared: ConversationSummary[] }
import { isConversationListPayload } from "./conversationListWire";
export function conversationList(payload: ConversationListPayload): ConversationSummary[] {
  if (!isConversationListPayload(payload)) throw new Error("Recent chats could not be verified.");
  return [...payload.conversations, ...payload.shared];
}

export interface ChatEmptyPayload {
  workspace_activity: import("./workspaceActivity").WorkspaceActivity | null;
  activity: ActivityEvent[];
  connections: OAuthConnection[] | null;
  connections_status: "pending" | "available" | "unavailable";
  teammate_count: number | null;
  own_conversation_count: number;
}
