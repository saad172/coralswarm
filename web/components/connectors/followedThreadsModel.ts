import { ApiError, type WatchedThread, type WatchedThreadsResponse } from "@/lib/api";

/** A missing or malformed list must never read as "nothing is followed". */
export function readWatchedPage(page: WatchedThreadsResponse): WatchedThreadsResponse {
  if (!page || !Array.isArray(page.threads) ||
      (page.next_cursor !== null && typeof page.next_cursor !== "string") ||
      page.threads.some((row) => !row || typeof row.thread_id !== "string" ||
        typeof row.following !== "boolean" ||
        (row.title !== null && typeof row.title !== "string") ||
        typeof row.saved_at !== "string" || !Number.isFinite(Date.parse(row.saved_at)))) {
    throw new Error("Invalid watched-thread response");
  }
  return page;
}

export function appendWatchedThreads(previous: WatchedThread[], incoming: WatchedThread[]): WatchedThread[] {
  const rows = new Map(previous.map((row) => [row.thread_id, row]));
  for (const row of incoming) rows.set(row.thread_id, row);
  return Array.from(rows.values());
}

export function followedThreadsError(error: unknown, action: "load" | "stop"): string {
  if (error instanceof ApiError && error.status === 404 && action === "load") {
    return "Followed threads aren't available on this backend yet. Try again after it updates.";
  }
  if (error instanceof ApiError && error.status === 401) {
    return "Your session expired. Sign in again to manage followed threads.";
  }
  if (error instanceof ApiError && error.status === 403) {
    return "Followed threads are available in your personal workspace.";
  }
  return action === "load"
    ? "Couldn't load followed threads. Try again in a moment."
    : "Couldn't stop following this thread. Try again in a moment.";
}
