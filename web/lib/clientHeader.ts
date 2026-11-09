// Which first-party client is this request coming from?
//
// `X-CoralSwarm-Client` lets per-client usage be attributed server-side. This
// codebase only ever stamps `web`. The backend's allow-list
// (`backend/src/middleware/access_log.rs`) is wider because other clients
// stamped their own values.
// (The REMOTE MCP surface needs nothing here: it self-identifies from the
// connecting client's `User-Agent`, see
// `backend/src/mcp/auth.rs::extract_client_name`.)
//
// VALUE DISCIPLINE: the value is a closed set of build-time constants, never
// user input and never a free-form version string. A header that a caller can
// influence is a header the server can't trust, and the moment it carries a
// version it stops being a low-cardinality dimension. Version reporting, if it
// is ever wanted, is a separate header and a separate decision.

/** The request header every first-party client stamps. Header names are
 *  case-insensitive, so the backend matches any spelling. */
export const CLIENT_HEADER = "X-CoralSwarm-Client";

/** What THIS codebase can emit. Note what is absent: `cli`. The `cs` binary
 *  has no HTTP client, so no code path can produce that value and declaring it
 *  would just be a claim the tree doesn't back up. */
export type ClientName = "web";

/** This build's client name. */
export function clientName(): ClientName {
  return "web";
}
