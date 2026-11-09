import { parseScopedConversationDetail, type ScopedConversation, type ScopedConversationDetail } from "../scopedChatApi";
import { matchingLibraryScope, type LibraryScope } from "../libraryMemoryApi";
export type ConversationDetailPayload = Omit<ScopedConversationDetail, "conversation"> & {
  conversation: ScopedConversation | null;
  resolved_scope: LibraryScope | null;
};

/** Pure validation; never resolves selectors or issues a compatibility read. */
export function conversationDetailPayload(value: unknown, expectedId?: string): ConversationDetailPayload {
  if (!value || typeof value !== "object") throw new Error("This saved chat could not be verified.");
  const payload = value as ConversationDetailPayload;
  if (payload.conversation === null) {
    if (!Array.isArray(payload.messages) || payload.messages.length || payload.messages_truncated !== false ||
        payload.older_before_seq !== null || payload.resolved_scope !== null) throw new Error("This unavailable chat could not be verified.");
    return payload;
  }
  const id = expectedId ?? payload.conversation?.id;
  if (typeof id !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) throw new Error("This saved chat could not be verified.");
  parseScopedConversationDetail(payload, id);
  if (payload.conversation.scope_status === "resolved" ? !payload.resolved_scope || !payload.conversation.library_scope ||
      !matchingLibraryScope(payload.resolved_scope, payload.conversation.library_scope) : payload.resolved_scope !== null) {
    throw new Error("This saved chat's resolved scope could not be verified.");
  }
  return payload;
}
