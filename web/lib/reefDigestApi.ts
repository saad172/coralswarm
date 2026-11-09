import { ApiError } from "./api";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { guardedFetch } from "./transport";
import { parseReefDigest, type ReefDigestView } from "./graphSurfaces";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** `GET /v1/nodes/{id}/digest` — the attributed, reader-verified reef digest
 *  (#1357). The server serves it only when this reader may see every coral it
 *  was generated from; otherwise `unavailable` (and it may queue one deduped
 *  regeneration). Same fixed-credential, no-cache read as the Library panels. */
export async function readReefDigest(token: string, nodeId: string, signal: AbortSignal): Promise<ReefDigestView> {
  if (!token) throw new Error("Verify the current Library account.");
  if (!UUID.test(nodeId)) throw new Error("Choose one verified topic for its summary.");
  const response = await guardedFetch(`/v1/nodes/${nodeId}/digest`, {
    cache: "no-store",
    signal,
    headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() },
  }, { signal });
  if (!response.ok)
    throw new ApiError(
      response.status === 401 ? "Sign in again to read this topic summary."
        : response.status === 403 || response.status === 404 ? "This topic summary is unavailable or cannot be accessed."
          : "The topic summary is unavailable right now. Retry when the connection is restored.",
      response.status,
    );
  return parseReefDigest(await response.json());
}
