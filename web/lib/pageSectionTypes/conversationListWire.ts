// Pure wire validation shared by the Stored reader and the budget recorder.
export function isConversationListPayload(payload: unknown): boolean {
  if (!payload || typeof payload !== "object") return false;
  const value = payload as { conversations?: unknown; shared?: unknown };
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  return [value.conversations, value.shared].every((list) => Array.isArray(list) && list.length <= 200 &&
    list.every((row: unknown) => !!row && typeof row === "object" &&
      typeof (row as { id?: unknown }).id === "string" && uuid.test((row as { id: string }).id) &&
      typeof (row as { title?: unknown }).title === "string"));
}
