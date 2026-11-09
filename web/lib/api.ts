// CoralSwarm API client.
// Talks to the Rust backend (cloud sync plane). Ocean vocabulary lives in the
// UI layer; the wire types keep the backend's names (graph / node / atom).

// The API base is resolved per call, not captured in a module-level const: it
// depends on which of our hosts the UI is served from, and a top-level `window`
// read would crash the build-time prerender. See lib/apiBase.ts; with no
// apex configured it resolves to `NEXT_PUBLIC_API_BASE`, exactly as the old
// constant did. It carries no tenancy — that rides the token's `org_id` claim.
import { apiBase } from "./apiBase";
// Every request out of this module identifies its client (see lib/clientHeader.ts).
// Set LAST on each header map so a caller-supplied `init.headers` can never
// override it — see lib/clientHeader.ts on why the value stays a closed set.
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { sameCredentialScope } from "./credentialScope";
// W1: every non-streaming request leaves through this one chokepoint, which
// owns concurrency, timeouts, retry, GET coalescing and the circuit breaker.
// `req`/`reqPaged` keep owning what a response MEANS (status handling, the 401
// refresh-and-retry, JSON parsing); transport.ts owns whether and when it goes
// out. See docs/plans/request-reliability-and-scale.md §6.1.
import { guardedFetch, TransportError, type GuardedFetchOptions } from "./transport";

export function getDeviceId(): string {
  if (typeof window === "undefined") return "web-ssr";
  let id = localStorage.getItem("cs_device_id");
  if (!id) {
    id = crypto.randomUUID();
    localStorage.setItem("cs_device_id", id);
  }
  return id;
}

// Optional "mint a fresh bearer" hook, registered by the auth provider. Clerk
// session tokens are short-lived (~60s) and webviews throttle the background
// refresh timer, so a request can race a just-expired token. On a 401 we refresh
// once and retry, which keeps the app usable without manual reloads.
type TokenRefresher = () => Promise<string | null>;
let tokenRefresher: TokenRefresher | null = null;
export function setTokenRefresher(fn: TokenRefresher | null): void {
  tokenRefresher = fn;
}

/** Account deletion locks the whole account: during the grace period every
 *  request answers 403 `account_deletion_pending`, while the purge runs 403
 *  `account_deletion_in_progress`, and afterwards 401 `account_deleted`. Rather
 *  than error on every screen, send the user to the page that explains it.
 *  The pages that handle the state themselves are exempt (no redirect loops). */
const DELETION_PAGES = ["/account/delete", "/account-deleted", "/org/deletion"];
function routeAccountDeletionState(status: number, code: string | null): void {
  if (typeof window === "undefined") return;
  const target =
    status === 403 && (code === "account_deletion_pending" || code === "account_deletion_in_progress")
      ? "/account/delete/pending"
      : status === 401 && code === "account_deleted"
        ? "/account-deleted"
        : // In-app org deletion (control 0387): the ACTIVE org is being
          // deleted, so every route refuses this org's token. The org
          // deletion page explains it (and lets the owner export or undo).
          status === 403 && (code === "org_deletion_pending" || code === "org_deletion_in_progress")
          ? "/org/deletion"
          : null;
  if (!target) return;
  const path = window.location.pathname;
  if (DELETION_PAGES.some((p) => path.startsWith(p))) return;
  window.location.assign(target);
}

async function req<T>(
  path: string,
  token: string | null,
  init?: RequestInit,
  retried = false,
  opts?: GuardedFetchOptions,
): Promise<T> {
  const headers: Record<string, string> = { ...(init?.headers as Record<string, string>) };
  if (init?.body) headers["Content-Type"] = "application/json";
  if (token) headers["Authorization"] = `Bearer ${token}`;
  headers[CLIENT_HEADER] = clientName();
  const res = await guardedFetch(path, { ...init, headers }, opts);
  if (!res.ok) {
    // Refresh only within the original subject/org/issuer. The global hook
    // may have moved to another account while this request was in flight.
    // Deliberately layered ABOVE the transport: a 401 is the server working
    // correctly, so it is not a transport-level retry and must not count
    // against the breaker or the attempt budget.
    if (res.status === 401 && !retried && token && tokenRefresher) {
      const fresh = await tokenRefresher();
      if (fresh && fresh !== token && sameCredentialScope(token, fresh)) {
        return req<T>(path, fresh, init, true, opts);
      }
    }
    let msg = `HTTP ${res.status}`;
    let code: string | null = null;
    let detail: string | null = null;
    let serverMessage: string | null = null;
    let errBody: Record<string, unknown> | null = null;
    let refusalReason: string | null = null;
    let available: number | null = null;
    try {
      const body = await res.json();
      errBody = body && typeof body === "object" ? body : null;
      msg = body.error ?? msg;
      // Structured error fields (org invites PR 2 fix): some routes (e.g.
      // `/v1/orgs/invites`) send a machine-readable `code`/`detail` alongside
      // `error` so a caller can key its copy off something more precise than
      // the free-text message — see `ApiError.code`/`.detail`.
      code = typeof body.code === "string" ? body.code : null;
      detail = typeof body.detail === "string" ? body.detail : null;
      serverMessage = typeof body.message === "string" ? body.message : null;
      refusalReason = typeof body.refusal_reason === "string" ? body.refusal_reason : null;
      available = typeof body.available === "number" ? body.available : null;
    } catch {
      /* ignore */
    }
    routeAccountDeletionState(res.status, code);
    const apiError = new ApiError(msg, res.status, code, detail, serverMessage, errBody);
    apiError.refusalReason = refusalReason;
    apiError.available = available;
    throw apiError;
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}


// The generic request helper, for typed clients that live in their own module
// (lib/connectorsApi.ts) rather than growing this file further.
export { req as apiRequest };

/** Result of a `req`-style GET whose body stays a bare array for backward
 *  compatibility, with pagination metadata riding on a response header
 *  instead (see `backend/src/routes/nodes.rs::list_nodes`, CS-reef-pagination). */
interface PagedResult<T> {
  data: T;
  hasMore: boolean;
}

/** Like `req`, but for paginated GET endpoints that report `X-Has-More` on
 *  the response instead of wrapping the body in an object — reads that
 *  header alongside the (still bare-array) JSON body. GET-only (no `init`
 *  needed) since that's the only shape these endpoints currently have. */
async function reqPaged<T>(
  path: string,
  token: string | null,
  retried = false,
  opts?: GuardedFetchOptions,
): Promise<PagedResult<T>> {
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  headers[CLIENT_HEADER] = clientName();
  const res = await guardedFetch(path, { headers }, opts);
  if (!res.ok) {
    if (res.status === 401 && !retried && token && tokenRefresher) {
      const fresh = await tokenRefresher();
      if (fresh && fresh !== token && sameCredentialScope(token, fresh)) {
        return reqPaged<T>(path, fresh, true, opts);
      }
    }
    let msg = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      msg = body.error ?? msg;
    } catch {
      /* ignore */
    }
    throw new ApiError(msg, res.status);
  }
  const data = (await res.json()) as T;
  const hasMore = res.headers.get("x-has-more") === "true";
  return { data, hasMore };
}

/** `GET /v1/me`'s body. */
export interface MeResponse {
  user_id: string;
  account_id: string;
  role: string;
  device_id: string;
  org_teams_enabled: boolean | null;
  active_graph_id: string | null;
  is_personal_org: boolean;
}

/** `GET /v1/bootstrap`'s body; see `api.bootstrap`. */
export interface BootstrapResponse {
  activities_enabled: boolean;
  gist_enabled: boolean;
  my_join_requests: MyJoinRequest[] | null;
  billing_banner: BillingSummary | null;
  me: MeResponse;
  graphs: Graph[];
  join_requests_count: { open: number } | null;
  /** Whether self-serve team sign-up (`POST /v1/orgs`) is on (A2 #1533,
   *  `ORG_SELFSERVE_SIGNUP_ENABLED`). Absent on an older backend: off. */
  selfserve_signup_enabled?: boolean;
  /** The caller's self-serve teams that aren't ready yet (issue #1540).
   *  Absent on a backend without it; the switcher then knows only the teams
   *  this browser started. */
  pending_orgs?: Array<{ org_id: string; status: string }>;
  /** Whether the ACTIVE org is a self-serve team org (#1540 review SF-a):
   *  the People page mounts Clerk's OrganizationProfile only for `false`.
   *  `null` when the server could not read it in time; absent on an older
   *  backend. */
  selfserve_org?: boolean | null;
}

/** Explicit opt-in; legacy clients keep BootstrapResponse unchanged. */
export interface TrialBootstrapResponse extends Omit<BootstrapResponse, "billing_banner"> {
  billing_banner_mode: "trial_v1";
  trial_banner: TrialBannerData | null;
}

/** A successful DB-only read. `trial: null` means no trial; an outer null
 * means the optional read was unavailable. Never a full BillingSummary. */
export interface TrialBannerData {
  kind: "trial_banner_v1";
  trial: BillingTrial | null;
}
export type TrialBannerMode = "trial_v1" | "legacy";

/** A team organization that blocks account deletion until the user leaves it. */
export interface DeleteAccountBlockingOrg {
  account_id: string;
  clerk_org_id: string | null;
  slug: string;
  role: string;
  /** The caller owns this org: they delete it rather than leave it. */
  owner?: boolean;
  /** In-app org deletion is available for this org (flag on, self-serve, owner). */
  can_delete_org?: boolean;
}

// ── In-app org deletion (control 0387) ──────────────────────────────────────

export type OrgDeletionState = "requested" | "grace" | "awaiting_approval" | "destroying" | "done" | "canceled";

export interface OrgDeletion {
  id: string;
  state: OrgDeletionState;
  /** Owner only: a locked-out member is not told who asked. */
  initiated_by?: "owner" | "unpaid" | "clerk_dashboard";
  /** Owner only: what happened to the subscription. */
  subscription?: string | null;
  /** ISO timestamp: when the 7-day grace ends and the data is deleted. */
  grace_ends_at: string;
  can_cancel: boolean;
  export_available: boolean;
  retrying: boolean;
}

/** `GET /v1/orgs/{org_id}/deletion` (404 while the feature is off). */
export interface OrgDeletionStatus {
  is_owner: boolean;
  eligible: boolean;
  blocked_reason: "not_owner" | "not_self_serve" | null;
  org_name: string | null;
  reverification_required: boolean;
  deletion: OrgDeletion | null;
}

/** `GET /v1/me/delete-account`. */
export interface DeleteAccountEligibility {
  eligible: boolean;
  email: string;
  reverification_required: boolean;
  blocking_organizations: DeleteAccountBlockingOrg[];
  shared_account_members: DeleteAccountSharedMember[];
  deletion: AccountDeletion | null;
}

/** Someone else who uses the caller's personal account. */
export interface DeleteAccountSharedMember {
  account_id: string;
  user_id: string;
  email: string | null;
  name: string | null;
  role: string;
}

export type AccountDeletionState = "grace" | "running" | "completed" | "cancelled";

/** The deletion run: a cancellable grace period, then the purge. */
export interface AccountDeletion {
  id: string;
  state: AccountDeletionState;
  /** ISO timestamp the purge starts (end of the grace period). */
  purge_after: string;
  can_cancel: boolean;
  current_step: string | null;
  steps: unknown[];
  retrying: boolean;
}

/** `POST /v1/me/delete-account` -> 202. */
export interface DeleteAccountStarted {
  id: string;
  status: "grace";
  deletion: AccountDeletion;
}

/** Cancel answers (`/v1/me/delete-account/cancel`, `/v1/account-deletion/cancel`). */
export interface DeleteAccountCancelled {
  status: "cancelled";
}

// ── Billing page (settings 3.7) ─────────────────────────────────────────────
// Every money field from the API is an integer in micro-USD (1 USD =
// 1_000_000) unless its name ends in `_cents`. Display with two decimals via
// `components/billing/money.ts`.

export interface BillingAutoReload {
  enabled: boolean;
  threshold_micro_usd: number | null;
  amount_micro_usd: number | null;
}

export interface BillingExtraUsage {
  enabled: boolean;
  balance_micro_usd: number;
  monthly_limit_micro_usd: number | null;
  spent_this_period_micro_usd: number;
  /** Org only; `null` for a personal account. */
  member_cap_seat_multiple: number | null;
  auto_reload: BillingAutoReload;
}

/** `GET /v1/billing/summary` (admin-only for an org; members get 403). */
export interface BillingSummary {
  account_kind: "org" | "personal";
  plan: { code: string; name: string } | null;
  seats: { used: number; total: number };
  credits: { allowance_used: number; allowance_total: number; resets_at: string | null };
  extra_usage: BillingExtraUsage;
  subscription: {
    status: string | null;
    has_billing_account: boolean;
    renews_at: string | null;
    cancel_at_period_end: boolean;
    cancel_at: string | null;
  };
  payment_method: BillingPaymentMethod | null;
  /** A self-serve team org's free-credit trial (#1521); `null` (or absent on
   *  an older backend) for every other account. */
  trial?: BillingTrial | null;
}

/** `GET /v1/billing/summary` → `trial` (`routes/billing_manage.rs::SummaryTrial`). */
export interface BillingTrial {
  /** `checkout_open`, `trialing`, `converting`, `converted`, `abandoned` or
   *  `canceled` (`billing::org_trial::TrialState`). */
  state: string;
  seats: number;
  credits_total: number;
  credits_used: number;
  /** When the 90-day backstop converts the trial. */
  ends_at: string | null;
  /** The conversion charge is waiting on Stripe's retries (card declined). */
  payment_failed: boolean;
  /** One seat's monthly price once the trial converts, in US cents. Absent
   *  on an older backend, or `null` when the tier could not be read. */
  seat_price_usd_cents?: number | null;
  /** The monthly credits one seat gets on the first charge. */
  seat_monthly_credits?: number | null;
}

// ── Self-serve team org sign-up (A2 #1533) ──────────────────────────────────

/** `POST /v1/orgs` body. `seats` is at least 2; both URLs must be on this
 *  app's own origin. */
export interface OrgCreateRequest {
  name: string;
  seats: number;
  success_url: string;
  cancel_url: string;
}

/** The setup states `GET /v1/orgs/{org_id}/provisioning` reports. */
export type OrgSetupState =
  | "awaiting_payment"
  | "payment_abandoned"
  | "canceled"
  | "provisioning"
  | "ready"
  | "failed";

/** `POST /v1/orgs` 201 (or 200 with `replayed: true`). */
export interface OrgCreateResult {
  org_id: string;
  status: OrgSetupState;
  /** Stripe Checkout to send the creator to. `null` when the Checkout is
   *  already complete (then `status` says what happened), or when "resume
   *  payment" issued a newer one (`resume_payment: true`: call
   *  `trialCheckout`). */
  checkout_url: string | null;
  session_id: string | null;
  /** `false`: the creator already had a trial, so this Checkout charges the
   *  seats today. `null` once the Checkout is past. */
  trial: boolean | null;
  seats: number;
  trial_credits: number;
  trial_days: number;
  resume_payment: boolean;
  replayed: boolean;
}

/** `GET /v1/orgs/{org_id}/provisioning`, and the retry's 202 body. */
export interface OrgSetupStatus {
  org_id: string;
  status: OrgSetupState;
  /** The job's next step while `provisioning`; `queued` before the job row exists. */
  step: string | null;
  /** `payment`: a payment is still settling. `update`: waiting for a newer server build. */
  waiting_for: "payment" | "update" | null;
  retryable: boolean;
  /** Set while `awaiting_payment`: until when "resume payment" works. */
  resume_payment_until: string | null;
  message: string;
  /** The seats this sign-up is buying: what "resume payment" sends.
   *  `null` when the server has no sign-up record for the org (an older
   *  backend omits the field). */
  seats?: number | null;
}

/** `POST /v1/orgs/{org_id}/trial-checkout` 200. */
export interface OrgTrialCheckout {
  checkout_url: string;
  session_id: string;
  trial: boolean;
  seats: number;
  trial_credits: number;
  trial_days: number;
}

/** The default payment method, of any type. Card fields are present for `card`
 *  only; `bank_name` for `us_bank_account`; `email` for `link`. */
export interface BillingPaymentMethod {
  type: string;
  brand?: string;
  last4?: string;
  exp_month?: number;
  exp_year?: number;
  bank_name?: string;
  email?: string;
}

/** `GET /v1/billing/seats/quote`. Cents are signed: a reduce prorates to a credit. */
export interface SeatQuote {
  tier: string;
  action: "add" | "reduce";
  quantity: number;
  current_quantity: number;
  new_quantity: number;
  currency: string;
  proration_cents: number;
  next_invoice_total_cents: number;
  recurring_cents_after: number;
  next_invoice_at: string | null;
}

/** `PUT /v1/billing/extra-usage` body. Omitted fields keep their stored value. */
export interface ExtraUsageUpdate {
  enabled: boolean;
  monthly_limit_micro_usd?: number;
  member_cap_seat_multiple?: number;
  auto_reload?: { enabled: boolean; threshold_micro_usd?: number; amount_micro_usd?: number };
}

export interface ExtraUsageResult {
  extra_usage: BillingExtraUsage;
  limits?: Record<string, unknown>;
}

export interface CheckoutSession {
  checkout_url: string;
  session_id: string;
}

export class ApiError extends Error {
  status: number;
  /** The backend's machine-readable `body.code`, when present — e.g.
   *  `"seat_purchase_failed"`. Distinct from `message` (`body.error`, which
   *  legacy callers already keyed error-mapping off of, sometimes on the
   *  literal error string rather than a dedicated code) — kept for backward
   *  compat, this is additive. `null`/`undefined` when the body carried none. */
  code?: string | null;
  /** The backend's optional machine-readable `body.detail` — a refinement of
   *  `code` (e.g. `"card_declined"` alongside `code: "seat_purchase_failed"`). */
  detail?: string | null;
  /** The backend's human-readable `body.message`, when the route sends one
   *  next to its `error` code (e.g. `/v1/orgs/members/{user_id}`). */
  serverMessage?: string | null;
  /** The parsed JSON error body, for routes that attach structured extras
   *  (e.g. delete-account's 409 `organizations[]` / `members[]`). */
  body?: Record<string, unknown> | null;
  /** The 402 body's `refusal_reason` (`credits_frozen`, `free_plan`,
   *  `extra_usage_off`, `member_cap_reached`, `monthly_limit_reached`,
   *  `extra_balance_exhausted`). Absent on older backends; callers must
   *  fall back to the generic out-of-credits copy. */
  refusalReason?: string | null;
  /** `seats_in_use`'s `available` count: how many seats are free to give back. */
  available?: number | null;
  constructor(
    message: string,
    status: number,
    code?: string | null,
    detail?: string | null,
    serverMessage?: string | null,
    body?: Record<string, unknown> | null,
  ) {
    super(message);
    this.body = body;
    this.status = status;
    this.code = code;
    this.detail = detail;
    this.serverMessage = serverMessage;
  }
}

/** 401, 403 and 404 refuse the credential or the scope. Anything else is availability, not lost access. */
export const scopeRefused = (error: unknown): error is ApiError =>
  error instanceof ApiError && (error.status === 401 || error.status === 403 || error.status === 404);

/** The complement for revalidation: a timeout, a dropped connection, a 5xx or
 * a 429 says nothing about whether the reader may still see what is on
 * screen, so it keeps the last verified result. Anything else — a refusal, a
 * response that failed verification, an account-changed guard — fails closed. */
export const transientFailure = (error: unknown): boolean =>
  error instanceof TransportError || (error instanceof ApiError && !scopeRefused(error));

// ── SSE streaming (cs-ask-chat-a) ───────────────────────────────────────────
export function parseSseFrame(frame: string): { event: string; data: unknown } | null {
  let event = "message";
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
  }
  if (dataLines.length === 0) return null;
  try {
    return { event, data: JSON.parse(dataLines.join("\n")) };
  } catch {
    return null;
  }
}

/** Dispatch one parsed SSE frame (`event:` + `data:` lines) to the handlers. */
function dispatchSseFrame(frame: string, h: ChatStreamHandlers): void {
  const sse = parseSseFrame(frame);
  if (!sse) return;
  const { event, data: parsed } = sse;
  switch (event) {
    case "meta":
      h.onMeta?.(parsed as ChatMeta);
      break;
    case "delta": {
      const d = parsed as { text?: string; replace?: boolean };
      if (d.replace) {
        h.onReplace?.(d.text ?? "");
      } else {
        h.onDelta?.(d.text ?? "");
      }
      break;
    }
    case "action":
      h.onAction?.(parsed as ChatAction);
      break;
    // Ask v2 (ADR-0011 §B8): structured cross-ocean contradiction pass, emitted
    // as its own event before `final`. The backend wraps the array in an object:
    // `{ contradictions: [{ topic, positions: [...] }] }` — unwrap it.
    case "contradictions":
      h.onContradictions?.(
        (parsed as { contradictions?: Contradiction[] }).contradictions ?? [],
      );
      break;
    case "final":
      h.onFinal?.(parsed as ChatFinal);
      break;
    // A source the answer just cited, pushed the moment its `[n]` marker is
    // emitted rather than waiting for `final` — `n` is the 1-based display
    // number the marker in the prose carries. The payload is the same shape as
    // a `citation_details` entry, minus the reef/ocean enrichment that `final`
    // adds; `final` still delivers the authoritative list and replaces these.
    case "citation":
      h.onCitation?.(parsed as StreamedCitation);
      break;
    case "error":
      h.onError?.((parsed as { error?: string }).error ?? "stream error");
      break;
    // `status` (e.g. `{"stage":"retrieving"}`) is an optional lightweight
    // heartbeat frame the backend may emit before `meta` so the client sees
    // bytes during retrieval. Surface it if a caller opted in; otherwise fall
    // through to the default below — this switch has no `default` case, so
    // any event name this client doesn't recognize yet (this one, or a future
    // addition) is silently ignored rather than throwing.
    case "status":
      h.onStatus?.((parsed as { stage?: string }).stage ?? null);
      break;
  }
}

/** POST a streaming query and drive the handlers as `meta → delta* → final`
 * (or `meta → final` in retrieval-only mode) frames arrive. Resolves when the
 * stream closes; rejects (and calls `onError`) if the request fails to start. */
/** Ask v2 stream request body (ADR-0011). `graph_ids` replaces the single
 *  `graph_id`; a one-element array is valid. `model` is a stable tier keyword
 *  ("fast"|"smart"|"max") the backend maps to an allowlisted id. */
export interface AskStreamBody {
  text: string;
  /** Omitted on a follow-up inherits the thread's server-held cutoff; changing it needs `scope_action: "replace"`. */
  temporal?: TemporalSelection;
  library_scope?: LibraryScope;
  scope_action?: "replace";
  graph_ids: string[];
  /** "private" mutually-exclusive mode, or "shared" multi-ocean (ADR-0011 §A3). */
  mode?: "shared" | "private";
  conversation_id?: string;
  model?: string;
  style?: "concise" | "detailed" | "sources-only";
  /** "fast" or "thorough" — maps to top_k + context size server-side. */
  depth?: "fast" | "thorough";
  /** Source/connector filter (already supported by the backend). */
  source_type?: string;
  /** Reasoning-effort level for the answer prose: "off" | "low" | "medium" |
   *  "high" (two-model-world amendment, 2026-09-03 — the dial applies to the
   *  Smart tier only). Validated server-side, tier-aware, by the backend's
   *  `resolve_effort`: forced to unset on the Fast tier and forced to "high"
   *  on the Max tier regardless of what is sent here. Wired from the
   *  composer's model+effort control (`ChatComposer.tsx`), which omits this
   *  field itself whenever Fast is selected — see `effortForWire` in
   *  `lib/askScope.ts`. */
  effort?: "off" | "low" | "medium" | "high";
}

async function askStream(
  token: string | null,
  body: AskStreamBody,
  h: ChatStreamHandlers,
  retried = false,
): Promise<void> {
  let res: Response;
  try {
    res = await fetch(`${apiBase()}/v1/queries/stream`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/event-stream",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        [CLIENT_HEADER]: clientName(),
      },
      body: JSON.stringify(body),
      signal: h.signal,
    });
  } catch (e) {
    // An abort (Stop/cancel, ADR-0011 §G26) is intentional — not an error.
    if (e instanceof DOMException && e.name === "AbortError") throw e;
    const msg = e instanceof Error ? e.message : "network error";
    h.onError?.(msg, 0);
    throw new ApiError(msg, 0);
  }
  // Refresh the bearer once on an auth failure and retry (expired Clerk
  // token), mirroring `req()`'s 401 handling above — this hand-rolled
  // streaming fetch otherwise bypasses that shared recovery path entirely, so
  // an ask fired with a stale token would 401 with no way to recover short of
  // reloading. Safe to do before touching `res.body`: no stream bytes have
  // been consumed yet.
  if (res.status === 401 && !retried && token && tokenRefresher) {
    const fresh = await tokenRefresher();
    if (fresh && fresh !== token) {
      return askStream(fresh, body, h, true);
    }
  }
  if (!res.ok || !res.body) {
    let msg = `HTTP ${res.status}`;
    let refusalReason: string | null = null;
    try {
      const b = await res.json();
      msg = b.error ?? msg;
      refusalReason = typeof b.refusal_reason === "string" ? b.refusal_reason : null;
    } catch {
      /* ignore */
    }
    h.onError?.(msg, res.status, refusalReason);
    const failure = new ApiError(msg, res.status);
    failure.refusalReason = refusalReason;
    throw failure;
  }

  // Observe the `final` frame so the caller can distinguish a clean close from a
  // dropped stream (ADR-0011 §G27 — disconnect retry/resume).
  let gotFinal = false;
  const observed: ChatStreamHandlers = {
    ...h,
    onFinal: (f) => {
      gotFinal = true;
      h.onFinal?.(f);
    },
  };

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx: number;
      // Frames are separated by a blank line (\n\n).
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        if (frame.trim()) dispatchSseFrame(frame, observed);
      }
    }
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") throw e;
    // Mid-stream transport drop — surface as a resumable disconnect.
    h.onDisconnect?.();
    return;
  }
  if (buf.trim()) dispatchSseFrame(buf, observed);
  // Stream closed without ever delivering `final` (and not aborted): the
  // connection was dropped. Let the UI offer retry/resume rather than hang.
  if (!gotFinal) h.onDisconnect?.();
}

// ── Wire types ────────────────────────────────────────────────────────────────
/** Ocean visibility level (ADR-0006). `"member"` (member-private-oceans PR 1)
 *  is an owner-only ocean living inside an ORG's tenant database — private
 *  from coworkers via the application, but encrypted under the org DEK, not
 *  server-blind. Never render it with the "on-device" copy reserved for the
 *  genuinely server-blind consumer-plane `"personal"` kind (see
 *  `lib/askScope.ts`). `"channel"` (Stage 2, shared-channel fan-out) does not
 *  exist yet and must not be added here until something creates it. */
export type OceanKind = "personal" | "team" | "org" | "member";
export interface Graph {
  id: string;
  name: string;
  /** "personal" (private), "team" (shared group), or "org" (company commons). */
  kind: OceanKind;
  /** True for the single personal ocean that is this account's primary home. */
  is_primary: boolean;
  created_at: string;
  /** The id every `/v1/teams/*` route (`api.teams.*`) expects — DISTINCT from
   *  `id` above, which is the ocean's own id (`graphs.id`). `id` and
   *  `team_id` are two separate primary keys (`teams.id` vs. `graphs.id`,
   *  `backend/migrations_control/0022_teams.sql:35-42`); sending `id` where
   *  a route wants `team_id` 404s ("team not found") even though the
   *  workspace plainly exists — this is exactly the bug that made the
   *  workspace Members panel show "team not found" for every shared
   *  workspace. `null` for every ocean whose `kind !== "team"`, and — real,
   *  if unexpected — `null` even for a `kind === "team"` ocean if its
   *  `teams` row is somehow missing; callers must treat that case as an
   *  error, never fire a `/v1/teams/{team_id}/...` request with
   *  `null`/`undefined` spliced into the path. */
  team_id: string | null;
  /** The CALLER's role in the team `team_id` identifies — `"admin"` or
   *  `"member"`. `null` in two DISTINCT cases: `team_id` itself is `null`
   *  (not a team ocean, or its `teams` row is somehow missing), or the
   *  caller is not an ACTIVE member of that team (a departed member reads
   *  `null` too, same as never having joined). Only ever non-null off
   *  `GET /v1/graphs` — every other endpoint that returns a `Graph` has no
   *  caller to compute a role for and always sends `null` here. Nothing
   *  consumes this yet; the workspace-card kebab menu (workspace-manage
   *  design doc) is the first reader, landing in a follow-up PR. */
  my_team_role: "admin" | "member" | null;
}
export type ReefType = "entity" | "decision" | "thread" | "reference";
export type EntityKind = "person" | "org" | "topic" | "project";
export interface EvidenceCounts {
  semantic_atom_count?: number | null;
  source_support_count?: number | null;
  unmapped_legacy_chunk_count?: number | null;
}
export interface LibraryTopic extends EvidenceCounts {
  node_id: string;
  title: string;
  entity_type: EntityKind;
  graph_id: string;
  graph_name: string;
  atom_count: number;
  last_happened_at: string | null;
  updated_at: string;
}
export interface LibraryPage {
  topics: LibraryTopic[];
  page: number;
  page_size: number;
  total: number;
}
export interface LibraryLineage {
  node_id: string;
  title: string;
  graph_id: string;
  graph_name: string;
}
export interface LibraryDecisionClaim {
  node_id: string;
  title: string;
  created_at: string;
}
export interface CandidateCoral {
  atom_id: string;
  source_type: string;
  source_id: string;
  timestamp: string | null;
  participants: string[];
  similarity: number;
  /** The candidate coral's own headline, non-empty (title-always contract) —
   *  the card's primary line. Never the raw atom/source id. */
  title: string;
  /** Short, whitespace-collapsed excerpt of the atom's content (best-effort —
   *  null when no content is available, e.g. content lives only in R2). Falls
   *  back to `excerpt` server-side. */
  snippet: string | null;
}
/** LLM digest of a reef: a high-level summary across its corals + per-coral the
 * excerpt relevant to this reef. `unavailable` when there's no LLM (free tier). */
export interface ReefDigest {
  summary: string | null;
  excerpts: { atom_id: string; excerpt: string; contribution?: string | null }[];
  unavailable?: boolean;
  /** #1357: a miss queued one deduped background regeneration. */
  regenerating?: boolean;
  /** #1357: present on every served Postgres digest (absent on the local store). */
  attribution?: { receipt_id: string; model: string; generated_at: string; input_count: number; purpose?: string };
  cached?: boolean;
}
export interface SupersessionEdge {
  superseder_id: string;
  superseded_id: string;
  superseder_title: string;
  superseded_title: string;
}
export interface Reef {
  node_id: string;
  // The backend wire shape uses `type` (mapped from `nodes.type` column);
  // do not rename to `node_type` — that's the internal Rust struct field name.
  type: ReefType;
  title: string;
  scope: string;
  atom_count: number;
  created_at: string;
  updated_at: string;
  entity_type?: EntityKind | null;
}
/** Reef drill-through detail (`GET /v1/nodes/{id}`, fix/get-node-by-id).
 *  Deliberately NOT folded into `Reef` above: `Reef` is the list-row shape
 *  (`GET /v1/nodes` / `listAll`), which is always fetched already scoped to
 *  one ocean and genuinely never carries `graph_id` on the wire — adding it
 *  there as required would be a lie about every list row, and as optional
 *  would let a list consumer silently read `undefined`. This is the other
 *  shape: single-reef fetch by id alone, authorized by reachability rather
 *  than a caller-supplied graph_id, so the response has to say which ocean
 *  the reef lives in. A page addressed only by node id (topic/session/meeting
 *  deep links) resolves its ocean from here, never from ambient session
 *  state — same pattern as `CoralDetail` below. */
export interface ReefDetail extends Reef, EvidenceCounts {
  graph_id: string;
  /** Server-derived: `true` iff `PATCH /v1/nodes/{id}` would NOT 403 for this
   *  caller right now. GET authorizes by reachability (`node_owned_by_account`
   *  is broader than that); PATCH authorizes by ownership against the
   *  caller's active account (`node_owned_by_account_scoped`,
   *  `backend/src/routes/nodes.rs`'s `patch_node_impl_scoped`) — a stricter,
   *  DIFFERENT predicate. A team-ocean reef reachable via promotion can GET
   *  200 and PATCH 403 in the same request. This field is that PATCH
   *  predicate, computed server-side and handed to the client so the UI
   *  never has to (and never should) reconstruct it from `Graph.kind` or any
   *  other client-side guess — the backend is the only source of truth for
   *  what it will itself accept. */
  can_edit: boolean;
  entity_type_sticky?: boolean;
  last_happened_at?: string | null;
  lineage?: LibraryLineage[];
  decisions?: LibraryDecisionClaim[];
}
/** True reef/coral/source totals for an ocean (`GET /v1/graphs/{id}/stats`),
 *  independent of `/v1/nodes` pagination state (CS-reef-nav-counts) — powers
 *  the Reefs page tab-bar counts, which must always reflect the whole ocean,
 *  not just whatever page(s) of reefs the client has fetched so far. */
export interface OceanStats {
  reef_total: number;
  reef_counts: Record<ReefType, number>;
  atom_count: number;
  source_count: number;
}
export interface Coral {
  atom_id: string;
  source_id: string;
  source_type: string;
  timestamp: string;
  graph_id: string;
  created_at: string;
  /** Truncated content incl. the "{sender} in #{channel} · {when}" header when
   * the atom has inline content, else the persisted `excerpt` — so an R2-only
   * atom (no inline content) still gets a real preview instead of `null`.
   * Used to render a human label + provenance. */
  preview?: string | null;
  /** GUARANTEED non-empty (title-always-present PR — backend
   * `services::atom_title::display_title`): a real headline, or a
   * deterministic fallback derived from the excerpt/body, or (as a last
   * resort) a "{Source label} · {date}" placeholder. Never render an
   * "Untitled …" string of your own — this field is never empty. */
  title: string;
  /** The atom's persisted excerpt (`atoms.excerpt`), when set. Nullable —
   * `title`/`preview` above are the fields to render; this is exposed for
   * callers that want the raw value. */
  excerpt?: string | null;
  /** Structured provenance (migration 0034): resolved channel + author for chat
   * corals. Null for non-chat / un-backfilled sources. Lets the list render a
   * human title without an extra detail fetch. */
  channel_name?: string | null;
  author_name?: string | null;
}
/** One coral inside a source group (`GET /v1/graphs/{id}/sources`). */
export interface SourceCoral {
  atom_id: string;
  /** Full stored source_id (incl. :chunk:N) — the unit promote-source matches. */
  source_id: string;
  /** See `Coral.preview` — same inline-else-excerpt fallback. */
  preview?: string | null;
  /** GUARANTEED non-empty — see `Coral.title`. */
  title: string;
  /** See `Coral.excerpt`. */
  excerpt?: string | null;
  timestamp: string | null;
}
/** One channel-level rollup row (`GET /v1/graphs/{id}/source-groups`,
 * perf/library-source-groups) — everything the Sources list row needs,
 * already folded by `channel_key` server-side (grouped SQL, not the client
 * `groupSources` fold over `list_sources`'s per-source-instance array this
 * replaces). Sorted `last_ts DESC NULLS LAST` by the backend. */
export interface SourceGroupRow {
  channel_key: string;
  channel_label: string | null;
  source_type: string;
  coral_count: number;
  instance_count: number;
  reef_count: number;
  first_ts: string | null;
  last_ts: string | null;
}
/** One channel group's full detail (`GET
 * /v1/graphs/{id}/source-groups/{channel_key}`) — the whole group's
 * metadata plus `source_ids` (every full source_id in the group, for
 * promote-source) and `reefs` (every topic it settled into, already
 * titled/typed — no separate node↔atom edge lookup needed), both unpaged,
 * alongside ONE PAGE of `corals` (newest first). 404s with `{"error":
 * "source group not found"}` for an unknown `channel_key`. */
export interface SourceGroupDetail {
  channel_key: string;
  channel_label: string | null;
  source_type: string;
  coral_count: number;
  instance_count: number;
  reef_count: number;
  first_ts: string | null;
  last_ts: string | null;
  source_ids: string[];
  reefs: { node_id: string; type: ReefType; title: string }[];
  corals: SourceCoral[];
  page: number;
  page_size: number;
  has_more: boolean;
}
/** Coral drill-through detail (`GET /v1/atoms/{id}`). */
export interface CoralDetail {
  atom_id: string;
  graph_id: string;
  source_type: string;
  source_id: string;
  /** Decrypted body text. `null` when the atom only has an R2 blob and the
   * route can't decrypt (no DEK / no R2). */
  content: string | null;
  participants: string[];
  timestamp: string | null;
  created_at: string;
  scope: string;
  fts_language: string;
  /** Structured provenance (backend migration 0034). Source-specific: chat corals
   * carry channel + author; non-chat / un-backfilled corals leave these null. */
  channel_id: string | null;
  channel_name: string | null;
  author_id: string | null;
  author_name: string | null;
  /** GUARANTEED non-empty — see `Coral.title`. */
  title: string;
  /** The atom's persisted excerpt (`atoms.excerpt`), when set. */
  excerpt: string | null;
  linked_reefs: Array<{ node_id: string; type: ReefType; title: string }>;
}
/** One coral row inside a reef (`GET /v1/nodes/{id}/atoms`). */
export interface ReefAtom {
  atom_id: string;
  source_type: string;
  source_id: string;
  timestamp: string | null;
  participants: string[];
}
/** `POST /v1/nodes/{id}/promote` response (feat/promote-async-batch). The
 *  reef is created/linked into the target ocean synchronously; its corals
 *  are copied/linked by a background job — see `promote`'s own doc comment
 *  and `api.jobs.get`. */
export interface PromoteResponse {
  source_node_id: string;
  target_graph_id: string;
  target_node_id: string;
  created: boolean;
  corals_requested: number;
  /** Poll `api.jobs.get(token, job_id)` for progress. `null` only when
   *  `corals_requested` is 0 — nothing for a job to do, already complete. */
  job_id: string | null;
}
/** One reef↔coral link from the W3 bulk edge contract. Deliberately narrower
 *  than {@link ReefAtom}: at 20,000 drained rows, fields no list view reads
 *  are pure payload. No production caller as of perf/library-source-groups
 *  (Library · Sources' own drain of this, `graphs.nodeAtomsAll`, was
 *  removed — the new `source-groups/{channel_key}` detail route returns
 *  `reefs` pre-joined instead); still exercised directly by
 *  `lib/requestBudget.test.ts`'s budget-regression tests on the drain
 *  itself, which is why `nodeAtomsAll`/`nodeAtomsPage` weren't removed too. */
export interface GraphEdge {
  node_id: string;
  atom_id: string;
  source_type: string;
  timestamp: string | null;
}
/** Exact, server-computed edge facets (W3). Used instead of client-derived
 *  counts whenever the drain budget bound — see `graphs.nodeAtomsAll`. */
export interface GraphEdgeFacets {
  total_edges: number;
  by_source_type: { source_type: string; count: number }[];
  by_week: { week: string; count: number }[];
}
/** A retrieval source returned in retrieval-only mode (free/offline tier). In
 *  Ask v2 every source carries its owning ocean (`graph_id` + optional name) so
 *  the UI can render a per-ocean badge (ADR-0011 §B7). */
export interface AskSource {
  atom_id: string;
  source_type: string;
  /** The cited coral's own headline, non-empty (title-always contract) — the
   *  citation card's primary line. Never the raw atom/source id. */
  title: string;
  content: string;
  timestamp: string | null;
  /** Owning ocean (ADR-0011 §B7) — the backend emits these per source on the
   *  retrieval-only `final` event and persists them on the turn. Optional for
   *  back-compat with pre-v2 backends. */
  ocean_id?: string | null;
  ocean_name?: string | null;
  pending_suggestion?: PendingSuggestion | null;
}
/** Per-citation provenance for synthesized answers, so each `[n]` chip / cited
 *  coral row knows its ocean (ADR-0011 §B7). The backend emits this on `final`
 *  alongside the bare `citations` id list; the UI falls back to its local coral
 *  map when absent. */
export interface CitationDetail {
  atom_id: string;
  graph_id: string;
  graph_name?: string | null;
  source_type?: string | null;
  /** The cited coral's own headline, non-empty (title-always contract) — the
   *  cite chip's / cited-coral row's primary line. Never the raw atom id. */
  title: string;
  /** Synthesized-mode snippet (backend field `content`); `preview` is the
   *  retrieval-only alias. Either renders the cited coral without a raw UUID. */
  content?: string | null;
  preview?: string | null;
  timestamp?: string | null;
  /** High-signal review Stage 3b: this citation's reef is an endpoint of a
   *  pending suggestion the answer leaned on. The chat's relationship cards
   *  are the one-tap resolve surface; this is the awareness marker. */
  pending_suggestion?: PendingSuggestion | null;
}

export interface PendingSuggestion {
  edge_id: string;
  prompt: string;
  confirm_url: string;
  reject_url: string;
}
/** A citation pushed mid-stream, the instant its `[n]` marker is written.
 *  Carries the display number so the client can slot it in without waiting for
 *  the ordered `final` list — markers are renumbered server-side, so `n` is
 *  already contiguous and 1-based. */
export interface StreamedCitation extends CitationDetail {
  n: number;
}
/** One side of a structured cross-ocean contradiction (ADR-0011 §B8). */
export interface ContradictionPosition {
  ocean_id: string;
  ocean_name: string;
  /** 1-based index into the answer's citation list. */
  citation_index: number;
  claim: string;
}
/** A topic on which two or more in-scope oceans disagree (ADR-0011 §B8). */
export interface Contradiction {
  topic: string;
  positions: ContradictionPosition[];
}
/** An in-scope ocean whose retrieval failed/timed out — answered from the rest
 *  and flagged (ADR-0011 §G25). */
export interface UnreachableOcean {
  ocean_id: string;
  ocean_name: string;
}
/** Cloud retained Ask uses exact passage locators; never treat them as coral IDs. */
export interface ScopedAskRequest {
  text: string;
  /** Omitted on a follow-up inherits the thread's server-held cutoff; changing it needs `scope_action: "replace"`. */
  temporal?: TemporalSelection;
  library_scope?: LibraryScope;
  conversation_id?: string;
  scope_action?: "replace";
  model?: "fast" | "smart" | "max";
  candidate_limit?: number;
  style?: "concise" | "detailed" | "sources-only";
}
export interface ScopedAskOutcome {
  graph_id: string;
  semantic_atom_id: string;
  kind: "fact" | "decision" | "action" | "observation";
  statement: string;
  statement_truncated: boolean;
  source_item_id: string;
  source_version_id: string;
  source_version_is_current: boolean;
  occurred_at: string | null;
  captured_at: string;
  representation: string;
  confidence: number | null;
  synthetic_demo: boolean;
  citations: LibraryCitation[];
}
export interface ScopedAskResult {
  mode: "synthesized" | "retrieval_only";
  answer: string | null;
  text: string;
  library_scope: LibraryScope;
  previous_library_scope: LibraryScope | null;
  scope_changed: boolean;
  temporal: TemporalSelection;
  previous_temporal: TemporalSelection | null;
  temporal_changed: boolean;
  /** Present only for historical selections; null for ordinary Ask. */
  temporal_state: TemporalStateResult | null;
  retained_citations: LibraryCitation[];
  outcomes: ScopedAskOutcome[];
  legacy_evidence: {kind:"legacy_chunk";graph_id:string;atom_id:string;representation:"unmapped_legacy_chunk";content:string;content_truncated:boolean;occurred_at:string|null;captured_at:string}[];
  contains_synthetic_demo_evidence: boolean;
  support_status: "complete";
  /** `temporal_unresolved`: historical evidence exists but the recorded state is unknown/incomplete. */
  answer_status: "supported" | "no_evidence" | "temporal_unresolved";
  disagreement_status: "not_resolved";
  truncated: boolean;
  truncation_reason: string | null;
  conversation_id: string;
  message_id: string;
  query_id: string;
}

export interface AskResult {
  /** Discriminates the cloud (synthesized) vs free/offline (retrieval-only) tier,
   *  vs a synthesized-tier request whose LLM call failed after retry and
   *  degraded to citations without prose (`"sources"` — retrieval succeeded,
   *  only generation didn't; distinct from the intentional `retrieval_only`
   *  free/offline tier). */
  mode?: "synthesized" | "retrieval_only" | "sources";
  /** Prose answer in synthesized mode; `null` in retrieval-only and sources mode. */
  answer: string | null;
  /** True when this ocean *could* synthesize but its egress policy ("Local
   * only") forbids cloud egress — distinct from the tier simply having no LLM. */
  egress_blocked?: boolean;
  citations: string[]; // atom UUIDs
  /** Synthesized mode: per-citation display payloads (human snippet + provenance)
   * so the client renders cited corals without a raw atom UUID. */
  citation_details?: AskSource[];
  /** Present in retrieval-only mode: the ranked source corals with content. */
  sources?: AskSource[];
  contradiction_hints: unknown[];
  /** `synthesize_ms` is absent in retrieval-only mode. */
  timing: { embed_ms: number; retrieve_ms: number; synthesize_ms?: number; walk?: TraversalWalk };
  request_id: string;
  /** Inferred-unconfirmed relationship edges the answer used — surfaced inline so
   * the asker can confirm/reject them in place. */
  pending_confirmations?: PendingConfirmation[];
}
/** A relationship edge between two reefs in the knowledge graph. `predicate` is a
 * snake_case id (e.g. "parent_of") meant to be humanized for display. `provenance`
 * is JSON: stated edges carry `{atom_ids:[...]}`, inferred edges carry
 * `{rule, source_edge_ids:[...]}`. `src_title`/`dst_title` may be null. */
export interface RelationshipEdge {
  edge_id: string;
  graph_id: string;
  src_node_id: string;
  dst_node_id: string;
  src_title: string | null;
  dst_title: string | null;
  predicate: string;
  origin: string;
  status: string;
  confidence: number;
  provenance: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  impact?: ReviewImpact;
  queued_at?: string | null;
  deadline_at?: string | null;
}
/** One keyset page of relationship edges — `GET /v1/graphs/{id}/relationships`. */
export interface RelationshipEdgePage {
  edges: RelationshipEdge[];
  has_more: boolean;
  next_cursor: string | null;
}
/** One endpoint of a merge-review pair, as the card renders it. */
export interface MergeReviewEndpoint {
  node_id: string;
  title: string;
  /** Corals linked to this reef — the default "which one survives" signal. */
  atom_count: number;
}
/** One standing merge-review item: a pair the resolver deliberately declined
 * to decide either way (judgement graph). `confidence` is the model's own
 * uncertainty; `expires_at` is when the question lapses if nobody answers it.
 * Rows whose reefs have since disappeared are dropped server-side, so both
 * endpoints are always present here. */
export interface MergeReviewItem {
  judgement_id: string;
  a: MergeReviewEndpoint;
  b: MergeReviewEndpoint;
  confidence: number | null;
  created_at: string;
  expires_at: string | null;
}
/** One keyset page of merge reviews — `GET /v1/graphs/{id}/merge-review`.
 * Empty in every `JUDGEMENT_GRAPH_WRITES` mode but `on`: with the flag off
 * nothing could retire a resolved row, so the whole surface goes quiet. */
export interface MergeReviewPage {
  items: MergeReviewItem[];
  has_more: boolean;
  next_cursor: string | null;
}
/** Sidebar-badge totals — `GET /v1/review/count`. */
export interface ReviewCount {
  decisions: number;
  suggested_edges: number;
  /** Live merge reviews in this ocean; always 0 while the judgement graph's
   * write flag is off. */
  merge_reviews: number;
}
/** Atlas (per-ocean knowledge graph) wire types — `GET /v1/graphs/{id}/atlas`.
 * The backend returns *semantics* (node type, reef kind, source platform); the
 * client maps those to hues / glyphs / radii. Node ids: the ocean hub is the
 * literal "ocean"; reefs/decisions/corals use their UUID; sources are
 * "src:<platform>"; people are "person:<key>". */
export type AtlasNodeType = "ocean" | "reef" | "decision" | "coral" | "source" | "person";
export interface AtlasNode {
  id: string;
  type: AtlasNodeType;
  /** `null` when unresolved — only on `type: "person"` when `resolved` is
   * false (the field is always present on the wire; serde serializes
   * `Option<String>::None` as JSON `null`, not an omitted key). The backend
   * emits the label from author_name alone: raw provider ids (Slack user
   * ids, GUIDs, ...) may still appear in a person node's `id` (a stable
   * "person:<author_id>" key) but are never used as the display label. */
  label: string | null;
  /** Reef subtype (entity/thread/reference) — only on `type: "reef"`. */
  reef_kind?: ReefType;
  /** Linked-coral count — on reef/decision. */
  count?: number;
  /** Source platform (e.g. "slack") — on coral & source nodes. */
  source_type?: string;
  /** The "src:<platform>" node this coral cites — on coral. */
  src_id?: string;
  /** Parent reef id (first membership) for layout seeding — on coral; may be null. */
  reef_id?: string | null;
  /** Authoring person node id — on coral; optional. */
  person_id?: string | null;
  /** Decision timestamp (node updated_at) — on decision. */
  date?: string;
  /** Ocean visibility level — only on the ocean hub. */
  kind?: OceanKind;
  /** Whether this is the account's primary ocean — only on the ocean hub. */
  is_primary?: boolean;
  /** Whether the author's display name resolved — only on `type: "person"`. */
  resolved?: boolean;
  /** The ocean this reef/decision was promoted (copied) in from, or `null`
   * when native to this ocean — on reef/decision only. */
  promoted_from_graph_id?: string | null;
  /** True on the ocean owner's self reef (`graphs.self_node_id`). */
  is_self?: boolean;
}
/** Review state of a reef↔reef typed edge as the atlas emits it (`rejected`
 * edges are never emitted). */
export type AtlasEdgeStatus = "confirmed" | "suggested";
export type AtlasEdgeOrigin = "stated" | "inferred" | "derived";
export interface AtlasLink {
  a: string;
  b: string;
  /** ocean-reef · ocean-decision · reef-coral · supports · coral-source ·
   * authored · member · reef-reef · decision-reef · co-occurs */
  kind: string;
  /** Predicate id for reef↔reef typed edges (ADR-0008), else absent. */
  predicate?: string;
  /** Relationship edge id — stated/inferred typed edges only. */
  edge_id?: string;
  /** `confirmed` draws as a fact, `suggested` as tentative — typed edges only. */
  status?: AtlasEdgeStatus;
  /** `stated` / `inferred` for persisted currents; `derived` for
   * co-occurrence and decision anchors. */
  origin?: AtlasEdgeOrigin;
  /** Engine confidence 0..1 — persisted typed edges only. */
  confidence?: number;
  /** Shared-coral count on derived currents. */
  weight?: number;
  /** Owner→topic stated spoke. Not a layout edge. */
  owner?: boolean;
  /** 1-based rank among a decision's anchors (title containment, then
   * most-specific). Drawn at ≤2 unless "show all anchors" is on. */
  anchor_rank?: number;
}
export interface AtlasGraph {
  ocean: { id: string; name: string; kind: OceanKind; is_primary: boolean };
  /** Ocean owner reef, if bound. */
  self_node_id?: string | null;
  /** Threshold the server actually used for `co-occurs` links. */
  cooccur?: number;
  nodes: AtlasNode[];
  links: AtlasLink[];
  /** Honest scale flags: `atoms` true when the coral sample hit the page cap. */
  truncated: { atoms: boolean };
}
/** An inferred-unconfirmed relationship edge an answer relied on (returned inline
 * on an AskResult). `prompt` is a ready human string ("Harper parent of Casey — is
 * this correct?"); `confirm_url`/`reject_url` are convenience links to the
 * confirm/reject endpoints (the UI calls `api.relationships` directly). */
export interface PendingConfirmation {
  edge_id: string;
  subject: string;
  predicate: string;
  object: string;
  confidence: number;
  prompt: string;
  confirm_url: string;
  reject_url: string;
}

// ── Conversational Ask (cs-ask-chat-a) ──────────────────────────────────────
/** A conversation sidebar row. `mine` is false for conversations shared into
 * the ocean by another member (read-only for the caller). */
export interface ConversationSummary {
  id: string;
  graph_id: string;
  title: string;
  pinned: boolean;
  shared: boolean;
  mine: boolean;
  created_at: string;
  updated_at: string;
  // ── Ask v2 scope descriptor (ADR-0011 §C11). Optional for back-compat with
  // pre-v2 single-ocean rows (which the backfill maps to mode:"shared"). ──
  /** Mode-locked for the conversation's lifetime (D14). */
  mode?: ScopeMode;
  /** The conversation's current ocean set (D13). */
  ocean_ids?: string[];
  /** Resolved ocean badges for the sidebar row (D12).
   *
   *  Keys are `ocean_id`/`ocean_name`, matching what the backend actually
   *  emits (`conv_json` in backend/src/routes/conversations.rs) and the
   *  identical field on `AskFinal` below. This was previously typed
   *  `{id, name}`, which nothing ever satisfied — the mismatch stayed
   *  invisible only because no screen rendered a saved conversation's badges
   *  until the chat history list landed. */
  oceans?: { ocean_id: string; ocean_name: string }[];
}
/** A conversation's persisted scope mode (ADR-0011 §C). */
export type ScopeMode = "shared" | "private";
/** One persisted turn. Assistant turns carry the full render payload so a
 * reopened conversation re-renders identically. */
export interface ConversationMessageItem {
  id: string;
  seq: number;
  role: "user" | "assistant";
  text: string;
  citations: string[];
  sources: AskSource[];
  mode: "synthesized" | "retrieval_only" | "sources" | null;
  timing: { embed_ms?: number; retrieve_ms?: number; synthesize_ms?: number; walk?: TraversalWalk };
  created_at: string;
  /** Per-turn scope snapshot (ADR-0011 §C11) — what this turn actually queried,
   *  so history stays truthful when the conversation's scope changes later. */
  scope_mode?: ScopeMode;
  scope_ocean_ids?: string[];
  /** Per-citation ocean provenance for a reopened synthesized turn (ADR-0011). */
  citation_details?: CitationDetail[];
  contradictions?: Contradiction[];
}
export interface ConversationDetail {
  conversation: ConversationSummary;
  messages: ConversationMessageItem[];
}
/** SSE `meta` event — arrives first, before any token. */
export interface ChatMeta {
  conversation_id: string;
  new_conversation: boolean;
  title: string;
  /** In-scope oceans the backend ran this turn against (ADR-0011) — id + name. */
  oceans?: { ocean_id: string; ocean_name: string }[];
  /** `atom_id → ocean_id` for every citable source, so each `[n]` chip / cited
   *  coral can be badged with its ocean (ADR-0011 §B7). Also echoed on `final`. */
  source_oceans?: Record<string, string>;
  /** Oceans that errored/timed out for this turn — partial-failure degradation
   *  (ADR-0011 §G25). The answer is from the reachable oceans. */
  unreachable_oceans?: UnreachableOcean[];
}
/** SSE `final` event — arrives last, with citations + timing (+ sources in
 * retrieval-only mode, or in `"sources"` mode — a synthesized-tier turn whose
 * LLM call failed after retry, so the server degrades to citations without
 * prose (`answer`/`text` stays empty) rather than dropping the connection. */
export interface ChatFinal {
  library_scope?: LibraryScope;
  previous_library_scope?: LibraryScope | null;
  scope_changed?: boolean;
  temporal?: TemporalSelection;
  previous_temporal?: TemporalSelection | null;
  temporal_changed?: boolean;
  temporal_state?: TemporalStateResult | null;
  retained_citations?: LibraryCitation[];
  outcomes?: ScopedAskOutcome[];
  legacy_evidence?: ScopedAskResult["legacy_evidence"];
  answer_status?: ScopedAskResult["answer_status"];
  disagreement_status?: "not_resolved";
  mode: "synthesized" | "retrieval_only" | "sources";
  citations: string[];
  /** Per-citation display payloads (human snippet + provenance) so the client
   *  renders cited corals without a raw atom UUID (#142), parallel to
   *  `citations`. The ocean badge is resolved from `source_oceans` below
   *  (ADR-0011 §B7). */
  citation_details?: CitationDetail[];
  /** Structured cross-ocean contradictions (ADR-0011 §B8). In practice these
   *  arrive as their own `contradictions` SSE event before `final`; the backend
   *  does NOT repeat them here, so this stays optional. */
  contradictions?: Contradiction[];
  /** Honesty guard (ADR-0011 §G24): nothing cleared the relevance floor, so the
   *  prose is a truthful "I don't have anything on this in [scope]" — the UI
   *  offers to widen scope rather than trusting confident prose. (Backend field
   *  name: `honesty_guard`.) */
  honesty_guard?: boolean;
  /** Authoritative prose after a post-stream cite rewrite; replaces appended deltas. */
  text?: string;
  /** `atom_id → ocean_id` map for per-citation ocean badges (ADR-0011 §B7),
   *  echoed here so a synthesized turn can badge its citations. */
  source_oceans?: Record<string, string>;
  /** Oceans that failed for this turn (ADR-0011 §G25), echoed on `final` too. */
  unreachable_oceans?: UnreachableOcean[];
  /** User-voice follow-up chips for this answer (synthesized mode only;
   *  ephemeral — never persisted). 0–3 entries, may be absent. */
  suggestions?: string[];
  sources?: AskSource[];
  egress_blocked?: boolean;
  timing: { embed_ms: number; retrieve_ms: number; synthesize_ms?: number; walk?: TraversalWalk };
  conversation_id: string;
  new_conversation: boolean;
  title: string;
  message_id: string | null;
  /** Reefs and currents this question's graph walk produced. Always sent
   *  when retrieval ran (including shadow mode); `merged` says whether those
   *  hops actually entered the answer. Also piggy-backed on persisted
   *  `timing.walk` so a reopened chat can redraw it. */
  traversal_walk?: TraversalWalk;
}

export interface WalkReef {
  id: string;
  title: string;
  hop: number;
}

export interface WalkCurrent {
  src: string;
  dst: string;
  predicate: string;
  derived: boolean;
}

export interface TraversalWalk {
  merged: boolean;
  seeds: WalkReef[];
  hops: WalkReef[];
  currents: WalkCurrent[];
}
/** A button on an inline action card. `op` selects which backend call the
 * client makes; `params` carries its arguments. */
export interface ChatActionButton {
  label: string;
  op:
    | "save_context"
    | "confirm_candidate"
    | "reject_candidate"
    | "merge_reefs"
    | "dismiss_merge"
    | "confirm_relationship"
    | "reject_relationship"
    | "dismiss";
  style?: "primary" | "ghost";
  params?: Record<string, string>;
}
/** SSE `action` event — an inline action-card proposal (save context, resolve a
 * review, confirm an inferred relationship). The assistant proposes; the user
 * confirms via a button. */
export interface ChatAction {
  id: string;
  kind:
    | "save_context"
    | "decision_candidate"
    | "merge_suggestion"
    | "relationship_confirm";
  title: string;
  detail?: string | null;
  buttons: ChatActionButton[];
}
/** Callbacks for `api.ask.stream`. All optional; `onError` also fires if the
 * request fails to start. */
export interface ChatStreamHandlers {
  onMeta?: (m: ChatMeta) => void;
  onDelta?: (text: string) => void;
  /** Terminal cite-correction: replace streamed prose instead of appending. */
  onReplace?: (text: string) => void;
  onAction?: (a: ChatAction) => void;
  /** Structured contradictions (ADR-0011 §B8), if emitted as a discrete event. */
  onContradictions?: (c: Contradiction[]) => void;
  /** A cited source, delivered mid-answer as its marker streams. */
  onCitation?: (c: StreamedCitation) => void;
  onFinal?: (f: ChatFinal) => void;
  /** `status` carries the HTTP code on a start failure (e.g. 429) so the UI can
   *  show a friendly rate-limit state (ADR-0011 §G28). On a 402,
   *  `refusalReason` is the body's `refusal_reason` (null on older backends). */
  onError?: (message: string, status?: number, refusalReason?: string | null) => void;
  /** The stream dropped before `final` (ADR-0011 §G27) — offer retry/resume. */
  onDisconnect?: () => void;
  /** Optional `status` SSE frame (e.g. "retrieving") a caller may emit before
   *  `meta` to signal progress during retrieval. Unused today — the parser
   *  tolerates the frame either way; wire this up if/when the UI grows a
   *  loading-stage indicator. */
  onStatus?: (stage: string | null) => void;
  signal?: AbortSignal;
}

/** A possible-duplicate entity-reef pair to review (CS-422). */
export interface MergeSuggestion {
  a: { node_id: string; title: string; atom_count: number };
  b: { node_id: string; title: string; atom_count: number };
  reason: string;
}
export interface EscrowRecoveryEvent {
  id: string;
  recovered_user_id: string;
  initiated_by: string | null;
  // Identity snapshotted server-side AT RECOVERY TIME (added post PR #176) so
  // departed members can still be named — null on rows written before this
  // enrichment shipped, or if the lookup missed. `initiated_by_email` is typed
  // here for future use; it is not rendered yet.
  recovered_user_email: string | null;
  initiated_by_email: string | null;
  created_at: string;
}
// ── Usage metering (usage-metering PR12) ────────────────────────────────────
// `GET /v1/usage/summary` — backs the org Usage/Billing pages and the personal
// Usage page (`backend/src/routes/usage.rs`). RAW NUMBERS ONLY: no
// pre-formatted currency strings, no server-computed percentages — the admin
// contract in this repo puts presentation decisions in the client, and the
// mock this replaces (pre-baked `pct`/`"$182 · 289 asks"` strings) is exactly
// what this contract must not reproduce. Field names mirror the backend
// struct names verbatim (`UsageSummaryResponse` et al.) so there is one
// vocabulary between the two, not a translation layer to keep in sync.
export interface UsagePlanSummary {
  code: string;
  name: string;
  /** TOTAL allowance, already multiplied by `seats` for a seat-priced plan
   *  (`COALESCE(credits_override, plans.credits * COALESCE(seats, 1))`).
   *  Do NOT multiply by `seats` again. */
  credits: number;
  /** Purchased seats for a seat-priced plan (`team_standard`,
   *  `team_premium`); `null` for a flat plan (`free`, `pro`,
   *  `founder_comp`). `null` means "flat plan", never "unknown" — the
   *  schema rejects a seat-priced grant with no seat count. */
  seats: number | null;
  /** PER-SEAT allowance, for rendering "3,000 × 4 seats". Don't derive it
   *  as `credits / seats`.
   *
   *  `null` for a flat plan, AND `null` when the grant carries an absolute
   *  override — because then a per-seat figure would contradict `credits`
   *  rather than explain it. So whenever this is non-null,
   *  `credits_per_seat * seats === credits` holds, and a client never has
   *  to ask whether an override is in play: if it's null, don't render a
   *  breakdown. */
  credits_per_seat: number | null;
  refresh_kind: string;
  granted_at: string;
}
export interface UsageAxisSpend {
  /** `credit_ledger.entry_kind` — "usage_job" | "usage_query" | "usage_journal"
   *  (`workers::billing_derivation`'s `ENTRY_KIND_*` constants). Render the
   *  label client-side; don't assume this list is exhaustive or ordered. */
  axis: string;
  credits: number;
  cost_usd: number;
  count: number;
}
export interface UsageSpendSummary {
  total_credits: number;
  total_cost_usd: number;
  by_axis: UsageAxisSpend[];
}
export interface UsageWorkspaceSpend {
  /** `null` exactly when `unattributed` is true. */
  graph_id: string | null;
  name: string | null;
  cost_usd: number;
  /** True for the bucket of `llm_usage` rows with no `graph_id` at all — show
   *  this, never drop it (see the backend module doc's "Unattributed
   *  workspace bucket"). */
  unattributed: boolean;
}
export interface UsageAskerStat {
  /** `null` for system/worker-authored activity with no acting user. */
  user_id: string | null;
  email: string | null;
  asks: number;
}
/** `GET /v1/billing/me` — the caller's own allowance, in credits only (never dollars). */
export interface BillingMe {
  account_kind: "team_org" | "personal";
  plan: { code: string; is_free: boolean };
  plan_credits: null | {
    used: number;
    total: number;
    remaining: number;
    period_start: string;
    resets_at: string;
  };
  extra_usage: {
    status: "unavailable" | "off" | "on";
    scope: null | "member_cap" | "monthly_limit";
    used_credits: number;
    limit_credits: null | number;
    percent_used: null | number;
  };
  blocked: {
    blocked: boolean;
    reason:
      | null
      | "free_plan"
      | "credits_frozen"
      | "extra_usage_off"
      | "member_cap_reached"
      | "monthly_limit_reached"
      | "extra_balance_exhausted";
  };
}
export interface UsageSummary {
  period_start: string;
  period_end: string;
  balance_credits: number;
  /** `null` is expected and real today — plan seeding (PR10) hasn't shipped
   *  in this tree yet, so no account has a `grants` row. Render "no plan
   *  assigned yet", never a fabricated default. */
  plan: UsagePlanSummary | null;
  credit_conversion_usd_per_credit: number | null;
  spend: UsageSpendSummary;
  by_workspace: UsageWorkspaceSpend[];
  top_askers: UsageAskerStat[];
  /** `COUNT(atoms)` ingested this period, by ingestion time (not the source
   *  event's own timestamp) — scoped like `by_workspace`: this account's
   *  graphs when personal, every graph the caller can see when org. */
  atoms_ingested: number;
  /** Whether `credit_ledger` has ANY row for this account, ever (not scoped
   *  to this period) — the honest-empty-state signal: a brand new / never
   *  metered account renders differently from one that's just quiet this
   *  month, without guessing from a zero. */
  has_ledger_history: boolean;
  /** The account's oldest `credit_ledger` row, ever; `null` exactly when
   *  `has_ledger_history` is false. Nothing before it was ever billed, so
   *  when it falls INSIDE the reporting window the ledger-derived tiles cover
   *  less of that window than `by_workspace` does and a zero means "not
   *  billed yet", not "nothing happened". `lib/usageSummary.ts` turns this
   *  into the disclosure; never render it raw. */
  ledger_coverage_start: string | null;
}

export interface QueryHistoryItem {
  id: string;
  query_text: string;
  answer_summary: string;
  citation_count: number;
  input_tokens: number;
  output_tokens: number;
  graph_id: string;
  created_at: string;
}
export interface ActivityEvent {
  id: string;
  event_type: string;
  graph_id: string | null;
  payload: Record<string, unknown>;
  user_id: string;
  created_at: string;
}
export interface TeamMembership {
  account_id: string;
  owner_user_id: string;
  role: string;
  plan_tier: string;
  joined_at: string;
}
// Today-journal PR D. Field names/casing mirror `backend/src/routes/journal.rs`
// exactly (see that file's module doc for the full response contract).
export interface JournalEntry {
  graph_id: string;
  graph_name: string;
  /** 1-3 sentence LLM-compiled prose. RENDER AS PLAIN TEXT — never
   *  `dangerouslySetInnerHTML`/markdown; this is LLM output over untrusted
   *  captured content, same rule as `AnswerMarkdown`'s citation handling. */
  summary: string;
  /** Opaque compile metadata (atom_count, per-source counts, asks,
   *  interactions, system events) — shaped by the compile worker, not
   *  rendered directly today. */
  stats: Record<string, unknown>;
  compiled_at: string;
}
export interface JournalAggregateBlock {
  source_type: string;
  title: string;
  atom_count: number;
}
export interface JournalAggregate {
  atom_count: number;
  asks: number;
  blocks: JournalAggregateBlock[];
}
// Zoomable journal (ticket 17), day/week/month tiers — `granularity=day|week|
// month` on the same `GET /v1/journal` endpoint. Field names mirror
// `backend/src/routes/journal.rs`'s `tier_periods_response` doc comment
// exactly. Distinct from `JournalEntry` (the hour tier) because the tier
// shape carries `tz`/`recompile_count`/`open` per entry that the hour tier
// doesn't.
export interface JournalTierEntry {
  graph_id: string;
  graph_name: string;
  /** Day tier: bounded bullet list (one workstream per line, newline-
   *  separated). Week/month tier: 2-4 sentence prose. Either way, RENDER AS
   *  PLAIN TEXT ONLY — same rule as `JournalEntry.summary`; see
   *  `lib/journal.ts`'s `splitBullets`/`parseBulletLine` for the hardened
   *  newline-split list renderer. Never markdown/HTML. */
  summary: string;
  stats: Record<string, unknown>;
  /** IANA zone name for a closed period's compile; `null` on a currently
   *  OPEN period's live-composed entry. */
  tz: string | null;
  /** `null` on a live-composed open-period entry (no stored row yet). */
  compiled_at: string | null;
  recompile_count: number | null;
  /** True for the live composition of the currently-open period (no stored
   *  rollup row yet); false for a closed, compiled period. */
  open: boolean;
}
export interface JournalPeriod {
  period_start: string;
  period_end: string;
  /** True when this is the period containing "now" — the live composition,
   *  not (necessarily) a closed/compiled one; mirrors each of its `entries[]`
   *  own `open` flag. */
  open: boolean;
  entries: JournalTierEntry[];
}
export type JournalGranularity = "hour" | "day" | "week" | "month";
export interface JournalTierResponse {
  granularity: JournalGranularity;
  periods: JournalPeriod[];
}

// The uncapped hour drill-down (ticket 16) — `GET /v1/journal/hour/
// {hour_start}/items`. Field names mirror `journal_hour_items`'s response
// JSON exactly.
export interface JournalHourItemAtom {
  id: string;
  graph_id: string;
  graph_name: string;
  source_type: string;
  excerpt: string | null;
  group: string | null;
  occurred_at: string;
}
export interface JournalHourItemActivity {
  id: string;
  graph_id: string;
  graph_name: string;
  event_type: string;
  label: string;
  created_at: string;
}
export interface JournalHourItemAsk {
  id: string;
  graph_id: string;
  graph_name: string;
  question: string;
  citation_count: number;
  created_at: string;
}
export interface JournalHourItemsResponse {
  hour_start: string;
  atoms: JournalHourItemAtom[];
  activity: JournalHourItemActivity[];
  asks: JournalHourItemAsk[];
  /** FULL per-kind counts for the hour — unaffected by pagination, the "N of
   *  M shown" source. */
  totals: { atoms: number; activity: number; asks: number };
  has_more: boolean;
  next_cursor: string | null;
}

export interface JournalHour {
  hour_start: string;
  entries: JournalEntry[];
  aggregate: JournalAggregate;
}
export interface JournalResponse {
  hours: JournalHour[];
}
export interface Member {
  email: string;
  role: string;
  user_id: string;
  joined_at: string;
}

// ── Named-team roster & governance (CS-094 follow-up, org-team-membership
// design doc) — a SINGLE team ocean's (`kind: "team"`) active membership,
// distinct from `Member`/`teams.members` above (which is the whole account). ─

/** A named-team role. Every roster/mutation endpoint below round-trips
 *  exactly these two strings; the backend 422s anything else. */
export type TeamRole = "admin" | "member";

/** One row of a team ocean's active roster (`GET /v1/teams/{team_id}/members`).
 *  `name` is frequently `null` — control-plane `users.name` is optional — so
 *  render `name ?? email`, never a blank. */
export interface TeamMemberRow {
  user_id: string;
  name: string | null;
  email: string;
  role: TeamRole;
  joined_at: string;
}

/** `POST /v1/teams/{team_id}/members` response — the row the add created. */
export interface TeamMemberAdded {
  member_id: string;
  user_id: string;
  role: TeamRole;
}

/** `PATCH /v1/teams/{team_id}/members/{user_id}` response. `changed: false`
 *  is a genuine no-op (the role already matched — idempotent, no audit event
 *  written), not a failure; render it as "already {role}", not an error. */
export interface TeamMemberRoleUpdate {
  member_id: string;
  user_id: string;
  role: TeamRole;
  changed: boolean;
}

/** `PATCH /v1/teams/{team_id}` response (rename). `changed: false` is a
 *  genuine no-op — the (trimmed) name already matched — not a failure;
 *  `name` echoes the trimmed request, not a re-read of the row, so treat a
 *  non-throwing result as success and re-fetch the graph list rather than
 *  read fields off it for display. */
export interface TeamRenameResult {
  team_id: string;
  name: string;
  changed: boolean;
}

/** `POST /v1/teams/{team_id}/restore` response — the small, additive
 *  unarchive counterpart to `archive`'s 204. `changed: false` is a genuine
 *  no-op (the workspace wasn't archived to begin with), not a failure. */
export interface TeamRestoreResult {
  team_id: string;
  archived: false;
  changed: boolean;
}

/** One row of `GET /v1/teams/archived` (`api.teams.listArchived`) — the read
 *  half of archive/restore, and the only thing that makes `restore`
 *  REACHABLE at all (every other read, `GET /v1/graphs` included, filters
 *  `archived_at IS NULL`, so nothing else in this API ever surfaces a team's
 *  id once it's archived).
 *
 *  `expires_at`/`restorable` are computed SERVER-SIDE (`team_restore_window`,
 *  `backend/src/routes/teams.rs`) from the one 120-day constant, evaluated at
 *  request time — never stored, and never to be recomputed here. Render
 *  `expires_at` for the countdown and read `restorable` verbatim to decide
 *  whether to offer Restore; a `false` here means the window has closed,
 *  full stop, regardless of what a client-side date diff might suggest.
 *
 *  Gated on ACCOUNT-level authority (`account_members.role IN ('owner',
 *  'admin')` — the account owner on a personal account, the org admin on an
 *  org one), NOT "team admin or org admin" like `archive`/`restore`
 *  themselves — see those two calls' own doc comments. A 403 here is the
 *  EXPECTED answer for most viewers (a team admin, a plain member, an org
 *  member who isn't the org admin) and must render as "no section", never an
 *  error. */
export interface ArchivedTeam {
  team_id: string;
  graph_id: string;
  name: string;
  archived_at: string;
  expires_at: string;
  restorable: boolean;
}

/** `membership_events` action kinds (`GET /v1/teams/{team_id}/events`).
 *  `cascade_removed` is the automated offboard sweep — its row always carries
 *  a null `actor_user_id` (see {@link MembershipEvent}), never a person.
 *
 *  `left` is deliberately DISTINCT from `removed` rather than folded into it:
 *  `removed` has always meant "an admin acted on someone else", so collapsing
 *  self-departure into it would make every future audit read of `removed`
 *  ambiguous. Keep them separate here for the same reason.
 *
 *  `archived` / `restored` / `renamed` are workspace-level, not person-level:
 *  the backend sets `target_user_id` equal to `actor_user_id` on those rows
 *  (mirroring `created`) because there is no other target, and leaves both
 *  role columns null. Renderers must not read them as "X did something to Y".
 *
 *  Mirrors the `action` CHECK in `migrations_control/0125_membership_events`
 *  and its tenant twin `0032`. */
export type MembershipEventAction =
  | "created"
  | "added"
  | "removed"
  | "role_changed"
  | "cascade_removed"
  | "left"
  | "archived"
  | "restored"
  | "renamed";

/** One row of a team's membership audit history, newest first.
 *
 *  `actor_user_id` is `null` exactly for the automated offboard cascade
 *  (`action: "cascade_removed"`, fired when an org member is revoked
 *  elsewhere and this team loses them as a side effect) — render that as a
 *  system action ("Automatically removed — offboarded from the
 *  organization"), never as a blank actor or a crash. Every other action
 *  always carries a real actor. */
export interface MembershipEvent {
  id: string;
  team_id: string;
  actor_user_id: string | null;
  target_user_id: string;
  action: MembershipEventAction;
  old_role: TeamRole | null;
  new_role: TeamRole | null;
  created_at: string;
}

/** `GET /v1/orgs/members` — the org-wide directory the add-member picker
 *  draws candidates from (org context only; 403 outside one). `role` here is
 *  the ACCOUNT/org role (owner/admin/member, or a Clerk `"org:*"` string) —
 *  do not confuse with a {@link TeamRole}, which is per-team.
 *
 *  `pending`/`invited_by`/`invited_at` are admin-only and best-effort: the
 *  backend derives them from Clerk, so they read `null` (not an error) when
 *  Clerk is briefly unreachable — a non-admin caller never gets these fields
 *  at all (org invites PR 1, `backend/src/routes/org_invites.rs`).
 *
 *  `connected_providers`/`connect_nudged_at` are also admin-only (CS-436,
 *  org-onboarding plan §3 item 49, D21): `connected_providers` is `[]`
 *  (never `null`) when this member has connected no platform with their own
 *  grant — the "approved, never connected" state D21 says must never be
 *  silent to the admin. `connect_nudged_at` is the last time the
 *  member-connect nudge worker emailed this member about it, or `null` if
 *  it never has. */
export interface OrgDirectoryMember {
  user_id: string;
  name: string | null;
  email: string;
  role: string;
  pending?: boolean | null;
  invited_by?: { user_id: string; name: string | null } | null;
  invited_at?: string | null;
  connected_providers?: string[];
  connect_nudged_at?: string | null;
  /** Admin-shaped rows only: true when this row is `accounts.owner_user_id`. */
  is_owner?: boolean;
}

/** `POST /v1/orgs/invites` response. `email_sent: false` means the seat was
 *  purchased and the account created, but the sign-in email didn't go out —
 *  the caller must offer to resend or share the link by hand. */
export interface OrgInviteResult {
  user_id: string;
  clerk_user_id: string;
  email: string;
  role: string;
  pending: boolean;
  email_sent: boolean;
}

/** `GET`/`POST /v1/orgs/slots` (CS-435, org-onboarding plan §3 item 7,
 *  D1/D7/D15) — the account's slot pool, derived fresh on every read, never
 *  stored (`backend/src/billing/slot_pool.rs`'s own module doc). `slot`, not
 *  `seat`: `seat` stays reserved for team-ocean access, this is billing —
 *  see D15. */
export interface SlotPoolResult {
  purchased: number;
  used: number;
  reserved: number;
  available: number;
  tier_breakdown: { tier: string; seats: number }[];
  /** CS-442 (item 40, D8) — one entry per ACTIVE seat tier, each tier's own
   *  purchased/used/reserved/available. Additive alongside `tier_breakdown`
   *  (unchanged, `purchased` only, under the ledger's own name `seats`) —
   *  the top-level fields above stay the account-WIDE total. */
  tiers: { code: string; purchased: number; used: number; reserved: number; available: number }[];
}

/** `POST /v1/join-requests` (CS-437, org-onboarding plan §3 items 4 + 14,
 *  D3/D9/D10) — `"sent_to_admin"` whatever actually happened server-side
 *  (freshly queued, already had an open request, silently suppressed by a
 *  durable rejection, blocked from auto-add by a tombstone, or fallen out
 *  of auto-add because the pool was empty). Never a count, never "full",
 *  never "declined" — see the backend route's own doc comment.
 *
 *  `"added"` (CS-447, item 9) is the ONE other answer: the three auto-add
 *  gates all passed and the caller is a member RIGHT NOW. It is not a
 *  disclosure — it states a fact about the caller's own membership, which
 *  they can see for themselves on the next load — and it cannot be used to
 *  probe an org's slot count, because auto-add is off for every org by
 *  default. Telling an auto-added member "sent to your admin" would simply
 *  be false, and the waiting banner (item 39, which reads OPEN requests)
 *  would never appear to correct it. */
export interface JoinRequestSentResult {
  status: "sent_to_admin" | "added";
}

/** `GET /v1/me/join-requests` item — the requester's own open requests.
 *  Item 39's waiting-state banner reads this: no count, no ETA, nothing
 *  about the pool. */
export interface MyJoinRequest {
  id: string;
  org: { clerk_org_id: string; name: string | null };
  requested_at: string;
}

/** `GET /v1/orgs/join-requests` item — the admin queue, open requests first
 *  then recently decided ones (`include_decided`). `evidence` is a free-form
 *  bag (verified email, tombstone hit, chosen tier, `pool_full`) rendered
 *  for the admin, never queried by shape client-side. */
export interface JoinRequest {
  id: string;
  user: { user_id: string; name: string | null; email: string | null };
  source: "self_request" | "pool_full" | "unverified_domain" | "tombstoned_returning" | "matched_not_member";
  evidence: Record<string, unknown>;
  requested_at: string;
  decided_at: string | null;
  decided_by: string | null;
  decision: "approved" | "rejected" | null;
}

/** `GET /v1/orgs/connector-health` (CS-446, org-onboarding plan §3 item 16,
 *  D16) — one CURRENT connector degradation an admin can act on.
 *
 *  `provider`/`capability`/`condition` are machine-readable keys, never
 *  prose: the wording lives in `components/connectors/connectorHealthModel
 *  .ts` beside the component that renders it, the same split
 *  `reconsentModel.ts` makes against `missing_scopes`. The backend ships no
 *  sentences, so a copy change is not a backend deploy.
 *
 *  Today exactly one shape reaches this: `google_chat` /
 *  `domain_directory` / `backing_member_departed` — the member whose
 *  Google grant carried `directory.readonly` left, so the org dropped from
 *  D16's rung 1 to rung 2. `backing_email` is whose grant it was, captured
 *  at the moment they left; it is what makes the notice actionable rather
 *  than merely true. */
export interface ConnectorHealthItem {
  provider: string;
  capability: string;
  condition: string;
  backing_email: string | null;
  first_observed_at: string;
  last_observed_at: string;
  detail: Record<string, unknown>;
}

/** One still-present member named by a {@link DirectoryAttribution} — CS-448. */
export interface DisconnectedMember {
  email: string;
  /** From the connector audit log (migration 0193), read-only. */
  disconnected_at: string;
}

/** CS-448 — **attribution, not a degradation.**
 *
 *  Nobody in this org is granting directory access, and these members, who
 *  are STILL HERE, disconnected Google Chat and have not reconnected. Item
 *  16 records the case where the backing member LEAVES, because a departure
 *  has no Reconnect button. A disconnect does — and item 49 already shows
 *  an admin that a member hasn't connected a tool — so nothing new is
 *  recorded here; what was missing was that the two facts are the same
 *  fact, and this payload is the join.
 *
 *  No `condition` field, deliberately: that vocabulary belongs to migration
 *  0243's `CHECK`, which admits exactly one value and grows only with a
 *  migration. This is derived on every read and stored nowhere, so it takes
 *  no value from that domain. `provider`/`capability` are shared because
 *  they key both payloads to the same connector tile. */
export interface DirectoryAttribution {
  provider: string;
  capability: string;
  /** Newest disconnect first. Never empty when the field is present. */
  members: DisconnectedMember[];
}

/** `GET /v1/orgs/connector-health` — what is recorded, and what is merely
 *  derived, in one read.
 *
 *  Two members rather than one array because they are different kinds of
 *  claim: `degradations` are durable rows with a lifecycle an admin can
 *  watch resolve; `directory_attribution` is a view of right now that
 *  exists only for the length of the response. `null` covers all three of
 *  "somebody backs it", "we cannot tell" and "nobody ever did" — the
 *  surface renders nothing for each, so it never needs them apart. */
export interface ConnectorHealthResponse {
  degradations: ConnectorHealthItem[];
  directory_attribution: DirectoryAttribution | null;
}

/** `GET /v1/orgs/domains` item (CS-443, org-onboarding plan §3 item 37,
 *  D11) — one domain this org has claimed, verified or not, and which
 *  proof (if either) verified it.
 *
 *  `method` is not cosmetic. Founder ruling, 2026-09-22: DNS verification
 *  arms BOTH the D2 guest rule and D16 rung 2 — and therefore auto-add
 *  (CS-447/CS-449) — while email verification arms the guest rule only. A
 *  domain reads as able to arm auto-add exactly when `verified && method
 *  === "dns_txt"`; `components/org/autoAddModel.ts`'s `domainArmsAutoAdd`
 *  is the one place that check is made — `autoAddDomainState` (the toggle's
 *  own gate) and `components/org/domainVerificationModel.ts`'s `domainStatus`
 *  (CS-450, the claim/verify admin surface) both call it, so neither can
 *  drift from the other about which domains are armed. */
export interface OrgDomain {
  id: string;
  domain: string;
  verified: boolean;
  method: "dns_txt" | "email" | null;
  verified_at: string | null;
  claimed_at: string;
  dns_record_name: string;
  dns_record_value: string;
  confirmation_email: string | null;
  confirmation_sent_at: string | null;
}

/** `GET /v1/orgs/domains` — every claimed domain, wrapped in an envelope
 *  (unlike `joinRequests.list`'s bare array) matching the backend's own
 *  `OrgDomainListResponse` (`backend/src/routes/org_domains.rs`). */
export interface OrgDomainListResponse {
  domains: OrgDomain[];
}

/** `POST /v1/orgs/domains/{id}/confirmation`'s 202 body — the backend sends
 *  only `{"status": "sent"}` (`backend/src/routes/org_domains.rs::
 *  request_confirmation`), nothing else; the caller already knows which
 *  domain and address it asked for. Not exported (code review, CS-450 fix
 *  round): its only caller (`orgs.domains.requestConfirmation` below)
 *  awaits and discards the resolved value entirely — this exists purely to
 *  type `req<T>`'s generic, not for anything outside this module to read. */
interface OrgDomainConfirmationSent {
  status: string;
}

/** `GET`/`PUT /v1/orgs/auto-add` (CS-447, org-onboarding plan §3 item 9,
 *  D1/D11/D16) — the admin-facing toggle, and the `seat_tiers.code` it
 *  spends against. Never 404s for an org that has not set one — reads as
 *  `{ enabled: false, tier: "standard" }`, per the backend route's own
 *  "off by default, structurally" doc section. There is web UI for this
 *  setting for the first time as of CS-449 (item 38) —
 *  `components/org/AutoAddSetting.tsx`. */
export interface AutoAddSettings {
  enabled: boolean;
  tier: string;
}

/** One queued join request `GET /v1/orgs/auto-add/queued-preview` (CS-449,
 *  org-onboarding plan §3 item 38, D11 "ask once") reports as a would-be
 *  auto-add — oldest first, the same order `admitQueued` expects its
 *  `request_ids` back in. */
export interface AutoAddQueuedPasser {
  request_id: string;
  email: string;
  name: string | null;
  requested_at: string;
}

/** `GET /v1/orgs/auto-add/queued-preview` — everyone currently queued who
 *  would pass all three auto-add gates (domain/pool/tombstone) if admitted
 *  right now, and the pool state the backend read at THAT moment.
 *
 *  `free_slots`/`would_admit` are ADVISORY, not a promise: the backend
 *  deliberately does not pre-reserve here — `admitQueued` reserves each
 *  slot for real, one at a time, so the true count by the time an admin
 *  clicks can be higher (another path freed slots) or lower (another path
 *  spent them) than what this read saw. Treat them as an estimate the
 *  outcome may honestly beat or miss, never as a figure to reconcile
 *  against and call an error — see `components/org/autoAddModel.ts`'s
 *  `planAskOnceDialog` and `summarizeAdmitOutcome`. */
export interface AutoAddQueuedPreview {
  tier: string;
  free_slots: number;
  would_admit: number;
  passers: AutoAddQueuedPasser[];
}

/** `POST /v1/orgs/auto-add/admit-queued` — the one outcome of the D11 "ask
 *  once" dialog's single button. `not_admitted[].reason` was confirmed
 *  2026-09-22 by reading `FallReason::label()` in
 *  `backend/src/services/org_auto_add.rs` directly as the ten labels
 *  `components/org/autoAddModel.ts`'s `FALL_REASON_LABELS` maps verbatim
 *  (eight `FallReason` values plus `pool_full`/`unavailable`, which
 *  `admit-queued` emits itself) — that set may grow (e.g. a label
 *  distinguishing "the cap stopped this one" from "the pool itself was
 *  empty") now that `max_admit` (see `admitQueued` below) lets a request
 *  fall short of the pool ON PURPOSE. That map's own fallback renders an
 *  unrecognised label readably rather than leaking the raw enum at an
 *  admin — see its own doc comment for why a WRONG key is more dangerous
 *  than a missing one, and why a new key is only ever added once the exact
 *  string is confirmed against the backend source, never guessed. */
export interface AutoAddAdmitResult {
  admitted: string[];
  not_admitted: { request_id: string; reason: string }[];
}

/** `POST /v1/orgs/join-requests/{id}/approve` response. `slot: "purchased"`
 *  means the pool was full and Stripe was charged for one more seat (D9) —
 *  surfaced to the admin who just approved, never to the requester. `tier`
 *  (CS-442, item 40) echoes the `seat_tiers.code` this member was actually
 *  provisioned at. */
export interface JoinRequestDecision {
  user_id: string;
  slot: "reserved" | "purchased";
  tier: string;
}

/** `GET /v1/billing/seat-tiers` (CS-442, org-onboarding plan §3 item 40,
 *  D8/D15) — the tier choices for the approval row's slot-tier control (and
 *  slot purchase generally). `code` is the tier keyword (`"standard"`,
 *  never bare "seat" in copy — D15). Named `SeatTier` after the Stripe/
 *  backend `seat_tiers` table this mirrors — the type name isn't
 *  user-facing copy, only the rendered word is, and that stays "slot" (see
 *  the members page). */
export interface SeatTier {
  code: string;
  credits: number;
  price_usd_cents: number;
}

export interface OAuthConnection {
  provider: string;
  provider_workspace_id: string | null;
  scope: string;
  expires_at: string | null;
  connected_at: string;
  /** CS-431 (org-onboarding-from-the-invite.md §3 item 34) — scopes this
   *  backend currently requests for `provider` that are absent from `scope`
   *  above. Computed only for `"slack"` (against `slack_scopes()`) and
   *  `"google_chat"` (against Chat's `data_scope()`); every other provider
   *  always sends `[]`. Always `[]` when `scopes_unknown` is `true` — never
   *  guessed together. See `components/connectors/reconsentModel.ts`. */
  missing_scopes: string[];
  /** CS-431 — `true` when the stored `scope` string cannot be trusted to
   *  answer "is anything missing" at all (e.g. a Slack workspace migrated
   *  from a legacy path that never recorded a real scope string). Render
   *  nothing rather than guess. */
  scopes_unknown: boolean;
  /** When a sync pass recorded a definite revocation on this row
   *  (`connectors::credential_health`): the connection is dead until the
   *  user reconnects, which clears it. `null` for a live row. Optional only
   *  for older fixtures; the backend always sends it. */
  credential_invalid_at?: string | null;
  /** The recorded reason code (e.g. `google_invalid_grant`); `null` exactly
   *  when `credential_invalid_at` is. */
  credential_invalid_reason?: string | null;
  /** `true` only for the caller's own per-member grant (`owner_user_id` is
   *  the caller). `false` for an account-wide row, whoever connected it.
   *  Under D21 only `true` means "you connected this". Optional for older
   *  fixtures and backends; treat a missing value as not yours. */
  mine?: boolean;
}

/** Personal mailbox follow state. Unfollowing preserves the saved content
 * and the watched row; these endpoints never remove messages. */
export type MailboxProvider = "google" | "microsoft";
export interface WatchedThread {
  thread_id: string;
  following: boolean;
  title: string | null;
  saved_at: string;
}
export interface WatchedThreadsResponse {
  threads: WatchedThread[];
  next_cursor: string | null;
}

/** `GET /v1/addin/pair/info` — the Microsoft pairing approval page's picker data. */
export interface AddinPairInfo {
  kind: string;
  workspaces: { account_id: string; name: string; kind: "personal" | "org" }[];
}

/** One live Gmail Add-on personal access token (`addin_tokens`, migration
 *  0210 — `backend/src/routes/addin.rs`). The PAT itself is NEVER on this
 *  wire shape: it is written into the pairing row exactly once, retrieved
 *  exactly once by Apps Script's `pair/poll`, and stored only as a SHA-256
 *  hash thereafter. These fields are all a revoke UI can (and needs to) know.
 *
 *  `last_used_at` is `null` for a token that has been minted but never
 *  presented — a real state (the user approved the pairing, then never tapped
 *  "Save to CoralSwarm"), NOT a missing field, so it renders as "never"
 *  rather than being hidden. */
export interface AddinToken {
  id: string;
  name: string;
  created_at: string;
  last_used_at: string | null;
}

// ── Slack connect onboarding (cs-slack-onboarding) ─────────────────────────
// One server-derived payload, rendered by both the in-app checklist and the
// Slack-side DM/App Home surfaces — see docs/connectors/slack-onboarding.md
// §3. Two independent client-side derivations of "is this set up" would drift
// the first time a rule changes, so the client renders this, it never
// recomputes it.
/** Step 1 — is the bot in any channel. NOT derived from `slack_watched_channels`
 *  (a live `conversations.list` call instead — see the doc's §4). `state:
 *  "unavailable"` is NOT `count: 0`: a failed/rate-limited fetch must never
 *  render as "the bot is in no channels", which reads as a setup failure and
 *  sends a correctly-configured user to redo finished work.
 *
 *  A DISCRIMINATED UNION, for the same reason `SlackOnboardingStatus` is one:
 *  the server genuinely sends `count: null` on the unavailable arm
 *  (`connectors::slack_onboarding`'s `count: Option<u32>`, set to `None`
 *  there), so declaring `count: number` was a lie the compiler then enforced
 *  in the wrong direction — `known && data.channels.count > 0` type-checked
 *  precisely because the type claimed a number that isn't there. Splitting on
 *  `state` means reaching `count` at all requires narrowing first. */
export type SlackOnboardingChannels =
  | SlackOnboardingChannelsOk
  | SlackOnboardingChannelsUnavailable;

export interface SlackOnboardingChannelsOk {
  state: "ok";
  count: number;
  /** Capped at 5 by the server; `count` is authoritative. */
  names: string[];
  error: null;
}

export interface SlackOnboardingChannelsUnavailable {
  state: "unavailable";
  /** No count exists — the roster fetch itself failed. This is the field that
   *  must never be read as a zero. */
  count: null;
  names: string[];
  error: string | null;
}
/** Step 2 — has a destination actually been chosen. `routed.count <=
 *  channels.count` is normal, not a shortfall: an unrouted channel still
 *  ingests via the account-level default, so render this as "not yet chosen",
 *  never as broken. */
export interface SlackOnboardingRouted {
  count: number;
  fanout: number;
}
/** Step 3 — has anything actually been captured. `first_at`/`last_at` are
 *  null when `atom_count === 0`. */
export interface SlackOnboardingCapture {
  atom_count: number;
  first_at: string | null;
  last_at: string | null;
}
export interface SlackOnboardingSkipReason {
  reason: string;
  count: number;
}
/** Huddle health off the `slack_huddle_captures` ledger. `skipped` with a
 *  reason is the NORMAL case, not an error — most huddles are too short for
 *  Slack to summarise. Render as information, never as a failure count. */
export interface SlackOnboardingHuddles {
  seen: number;
  ingested: number;
  skipped: number;
  /** Max 3 entries. */
  top_skip_reasons: SlackOnboardingSkipReason[];
}
/** Coverage of the verified-email identity matcher. Org plane only — always
 *  null on personal, and must not be rendered there. */
export interface SlackOnboardingIdentity {
  identified: number;
  total: number;
}
/** Member step 1 — does CoralSwarm know which Slack account is this person.
 *  `link_method` is `"verified_email"` for a match nobody had to perform, so
 *  the client can say "linked automatically" rather than crediting the member
 *  with an action they never took. */
export interface SlackOnboardingMemberLink {
  linked: boolean;
  slack_user_id: string | null;
  link_method: string | null;
}
/** Member step 2 — how many of THIS member's channels the bot is also in.
 *
 *  A DISCRIMINATED UNION for the same reason `SlackOnboardingChannels` is
 *  one: `count` genuinely is null on two of the three arms, and the two
 *  non-`ok` arms mean different things.
 *
 *  `"unlinked"` is not `"unavailable"` and neither is `0`: with no confirmed
 *  identity we never made the call at all (the question doesn't apply yet),
 *  and an `"unavailable"` intersection means one of the two live lookups was
 *  lost. Rendering either as zero tells a member the bot is in none of their
 *  channels when nobody looked. */
export type SlackOnboardingMyChannels =
  | { state: "ok"; count: number; containers: SlackOnboardingMyContainer[] }
  | { state: "unavailable"; count: null; containers: null }
  | { state: "unlinked"; count: null; containers: null };
/** One container this member and the agent are both in — the unit the backfill
 *  offer is made per (chat-per-user plan §4.2).
 *
 *  Carries no destination, deliberately: under plan §2.1 every container fans
 *  out into each linked member's own private ocean, so every entry here is a
 *  valid offer and the ocean is always the asking member's own. The union
 *  above keeps `containers: []` ("we looked; the agent is in none of yours")
 *  distinguishable from `containers: null` ("we could not look") — collapsing
 *  those is the same absence-rendered-as-an-answer bug the three-way state
 *  exists to prevent. */
export interface SlackOnboardingMyContainer {
  id: string;
  name: string;
}
/** `POST /v1/connectors/slack/backfill`.
 *
 *  Partial success is a real outcome, so it is modelled as one: `queued` and
 *  `skipped` are both lists, and `skipped` carries the reason per channel —
 *  most often "you're not in this channel", which is what a page listing a
 *  channel the person has since left produces. A UI that renders only
 *  `queued.length` would silently tell someone their history is coming when
 *  some of it is not.
 *
 *  `window` is the server's own sentence about what it will fetch. It is sent
 *  rather than composed client-side because the bound is the PLATFORM's
 *  retention, capped by ours, and only the server knows both. */
export interface SlackBackfillResult {
  queued: { channel_id: string }[];
  skipped: { channel_id: string; reason: string }[];
  window: string;
}
/** Org-MEMBER viewers only — null for personal accounts, for org
 *  owners/admins, and whenever nothing is connected. See
 *  docs/connectors/slack-onboarding.md §3.1. */
export interface SlackOnboardingMember {
  linked: SlackOnboardingMemberLink;
  my_channels: SlackOnboardingMyChannels;
  /** Scoped to the caller's OWN member ocean, not the account. */
  my_capture: SlackOnboardingCapture;
}
/** `GET /v1/connectors/slack/onboarding`.
 *
 *  A DISCRIMINATED UNION on `connected`, because the server genuinely sends
 *  every section as `null` when nothing is connected
 *  (`SlackOnboardingStatus::not_connected`). Declaring those sections
 *  non-nullable and relying on callers to check `connected` first happens to
 *  work today — the one caller guards — but it is a convention the compiler
 *  cannot enforce, so the first caller that reads `data.channels` without the
 *  guard gets an undefined dereference and NO type error. Modelling the two
 *  shapes separately makes the guard load-bearing: narrowing on
 *  `connected === true` is the only way to reach the sections at all. */
export type SlackOnboardingStatus =
  | SlackOnboardingConnected
  | SlackOnboardingNotConnected;

export interface SlackOnboardingNotConnected {
  connected: false;
  workspace_id: null;
  workspace_name: null;
  plane: "personal" | "org";
  channels: null;
  routed: null;
  capture: null;
  huddles: null;
  identity: null;
  /** Carried on THIS arm deliberately — the connectors page has to choose
   *  between a Connect button and "an org admin needs to connect this" before
   *  anything is connected, and this is the only signal it has at that
   *  moment. See `SlackOnboardingConnected["viewer_role"]` for the null case. */
  viewer_role: SlackViewerRole;
  /** Every member section is derived FROM a connected workspace, so there is
   *  nothing to report here — same reason the sections above are null. */
  member: null;
}

/** The caller's own role on this account.
 *
 *  `null` only ever comes off the Slack App Home renderer, which resolves an
 *  ACCOUNT for its viewer but never a ROLE and does not serialise this
 *  payload — `GET /v1/connectors/slack/onboarding` always sends a string.
 *  Modelled as nullable anyway rather than lying: the server's own type is
 *  `Option<String>`, and a fabricated `"owner"` would be a claim about a
 *  person nobody checked. Every client branch treats null as "not a member",
 *  which is the safe direction (it shows the read-only account view, never a
 *  member's private slice).
 *
 *  Left open as `string` beyond the three known values: roles are an
 *  `account_members` column, not a closed client-side enum, and a new one
 *  must not fail to parse. */
export type SlackViewerRole = string | null;

export interface SlackOnboardingConnected {
  connected: true;
  workspace_id: string | null;
  workspace_name: string | null;
  plane: "personal" | "org";
  channels: SlackOnboardingChannels;
  routed: SlackOnboardingRouted;
  capture: SlackOnboardingCapture;
  huddles: SlackOnboardingHuddles;
  /** Org plane only — always null on personal. */
  identity: SlackOnboardingIdentity | null;
  viewer_role: SlackViewerRole;
  /** Org-MEMBER viewers only. Null for personal accounts and org
   *  owners/admins — `deriveSteps` narrows on its presence, so reaching a
   *  member field at all requires checking. */
  member: SlackOnboardingMember | null;
}

/* ── Slack connector management (read-only) ─────────────────────────────────
   `GET /v1/connectors/slack/channels`. The steady-state "what is CoralSwarm
   actually reading" view, as opposed to the onboarding status above which
   answers "is this set up yet". */

/** How far a fan-out destination actually reaches right now, resolved by the
 *  same backend code ingest routes with (`resolve_org_fanout_destinations`).
 *
 *  The two numbers partition the account's member roster, so the roster size
 *  is `mapped_members + unmapped_members` — there is deliberately no third
 *  total field to disagree with its own halves.
 *
 *  `mapped_members === 0` is NOT an empty state: it means Slack messages are
 *  being fanned out to nobody and thrown away, silently, with no error
 *  anywhere. Render it as the problem it is. */
export interface SlackFanoutReach {
  /** People whose messages actually land somewhere: a confirmed Slack
   *  identity AND a resolvable private ocean. Equals the number of
   *  destination oceans. */
  mapped_members: number;
  /** People skipped — no confirmed Slack identity, or no private ocean yet.
   *  Never guessed from display names. */
  unmapped_members: number;
}

/** Channel-less Slack corals, split by whether the server can prove which
 *  workspace they came from. One number would have been a claim the data
 *  doesn't support: an atom with neither `workspace_id` nor `channel_id`
 *  (written before the provenance columns existed) is claimed identically by
 *  every connected workspace, so the copy must not name one. */
export interface SlackUnattributedAtoms {
  /** Stamped with THIS workspace, but no channel — attributable to the
   *  workspace this page describes, just not to any row on it. */
  this_workspace: number;
  /** Neither column set. Belongs to this ACCOUNT; which Slack workspace
   *  produced it is unknowable from the row. */
  account_legacy: number;
}

/** One ocean this channel's corals are ACTUALLY sitting in, and how many.
 *
 *  Present tense, unlike `SlackChannelDestination`'s future tense: re-routing
 *  never moves already-captured corals, so the two legitimately disagree. */
export interface SlackLandedOcean {
  ocean_name: string;
  /** `graphs.kind` verbatim: `"personal" | "team" | "org" | "member"`. Passed
   *  through unmapped, so an unrecognised kind must render as itself rather
   *  than fall into a wrong bucket. */
  ocean_kind: string;
  atom_count: number;
}

/** Where one channel's content lands NEXT (see `SlackChannelRow.landed_in`
 *  for where it already is).
 *
 *  `kind: "default"` is the NO-ROW case and is completely normal: a channel
 *  nobody explicitly routed still ingests, via the account-level default. It
 *  means "nobody has chosen", never "broken".
 *
 *  **But `kind` alone never says where anything goes.** On a personal account
 *  the default is one named ocean (`ocean_name`); on an ORG account it is a
 *  live fan-out to each MAPPED member's private ocean, with the shared org
 *  ocean receiving nothing (`fanout`). Rendering `default` as a bare "your
 *  account default" is what made a fanning-out-to-one-person workspace read
 *  as healthy in production on 2026-09-09. Derive the label with
 *  `destinationLabel` in `components/connectors/slackChannelModel.ts` rather
 *  than re-deciding it per renderer. */
export interface SlackChannelDestination {
  kind: "single" | "fanout" | "default";
  /** The single destination ocean's name: the picked ocean when
   *  `kind === "single"`, or the account's own primary ocean when
   *  `kind === "default"` on the PERSONAL plane. Null for every fan-out
   *  shape, and null if the graph no longer resolves. */
  ocean_name: string | null;
  /** How far the fan-out reaches, when the destination IS a fan-out:
   *  `kind === "fanout"`, or `kind === "default"` on the org plane. Null for
   *  single-ocean destinations — and always null for an org MEMBER viewer,
   *  who is not shown roster counts about other people. */
  fanout: SlackFanoutReach | null;
  /** **This channel's next message reaches NO ocean at all** — ingested,
   *  fanned out to an empty set, discarded, with no error raised anywhere.
   *
   *  Sent to EVERY viewer, including an org member whose `fanout` is null:
   *  masking the counts left a member's row reading "still ingesting" while
   *  their own messages were being dropped. One boolean about this channel
   *  names nobody and counts nobody. Never derive this from `fanout` on the
   *  client — for a member `fanout` is null and this is the only signal. */
  dropping: boolean;
  /** When a human picked this destination. Null when `kind === "default"`. */
  chosen_at: string | null;
  /** The stored `display_name` disagrees with the live channel name — that
   *  column is written once at pick time and never updates, so a renamed
   *  channel would otherwise show its old name forever. */
  stale_name: boolean;
}

export interface SlackChannelHuddles {
  seen: number;
  ingested: number;
  skipped: number;
  /** RAW per-channel reason strings, deliberately ungrouped — they're
   *  free-form prose today, and bucketing them needs a stable code first.
   *  Per channel there are few enough that grouping isn't needed at all. */
  reasons: string[];
}

export interface SlackChannelRow {
  channel_id: string;
  /** From the live Slack API, not the stored copy — see `stale_name`. */
  name: string;
  destination: SlackChannelDestination;
  last_activity_at: string | null;
  atom_count: number;
  /** Which oceans this channel's corals are ACTUALLY in, biggest share first
   *  — the current-state companion to `destination`'s future state. Empty
   *  exactly when `atom_count === 0`… except for an org MEMBER viewer, who
   *  only ever sees oceans they own: for them these entries do NOT sum to
   *  `atom_count`, and an empty list next to a non-zero count means "none of
   *  these are yours", not "nothing landed". */
  landed_in: SlackLandedOcean[];
  huddles: SlackChannelHuddles;
}

/** `state: "unavailable"` is NOT an empty `rows` array — a failed
 *  `conversations.list` must render as "couldn't check", never as "the bot is
 *  in no channels". Same rule as the onboarding status.
 *
 *  `state: "unlinked"` — an org member with no confirmed Slack identity: the
 *  row set is scoped to "channels you're in" and cannot be known until they
 *  link. Rows are always empty; render the linking prompt, never "no
 *  channels". */
export interface SlackChannelList {
  state: "ok" | "unavailable" | "unlinked";
  error: string | null;
  rows: SlackChannelRow[];
}

export interface SlackChannelsResponse {
  /** Slack team id (`T…`). Half of a Slack deep link —
   *  `https://app.slack.com/client/{workspace_id}/{channel_id}` — and the only
   *  way to say WHICH workspace this table describes, since an account may
   *  have more than one connected. */
  workspace_id: string;
  workspace_name: string | null;
  plane: "personal" | "org";
  /** Org members deliberately receive a RESTRICTED row set (only channels
   *  feeding their own member-private ocean) — the filter is server-side, so
   *  this field is for labelling the view, never for hiding anything. */
  viewer_role: "owner" | "admin" | "member";
  channels: SlackChannelList;
  /** Slack corals that carry no `channel_id` at all and so appear in NO row —
   *  older ingest paths and seeded history predate the structured provenance
   *  columns. Stated rather than hidden so the rows' counts don't quietly
   *  understate what was captured.
   *
   *  Null — never a zeroed object — when it wasn't computed: the channel list
   *  is `unavailable`/`unlinked`, or the viewer is an org member (it counts
   *  content outside their own row set). Zeros mean it really is zero. */
  unattributed_atom_count: SlackUnattributedAtoms | null;
}

/* ── Slack channel actions (write half): re-route, stop watching, impact ────
   `PUT/DELETE/GET .../channels/{channel_id}[/destination|/impact]`
   (backend/src/routes/slack.rs — `set_channel_destination`,
   `stop_watching_channel`, `channel_impact`). Mutating siblings of the
   read-only management view above; see that module's own header
   (`connectors::slack_channel_actions`) for the two founder decisions the
   UI must never soften: re-routing moves future content ONLY (already
   -captured atoms never move), and "stop watching" really calls Slack's
   `conversations.leave` — the bot actually departs the channel. */

/** What a human is choosing, wire-tagged on `kind` to match
 *  `DestinationPickBody` (`backend/src/routes/slack.rs`) exactly:
 *  `{"kind":"single","graph_id":"…"}` / `{"kind":"fanout"}` /
 *  `{"kind":"default"}`. */
export type SlackDestinationPick =
  | { kind: "single"; graph_id: string }
  | { kind: "fanout" }
  | { kind: "default" };

/** The destination shape `set_channel_destination`/`channel_impact` actually
 *  render (`destination_json` in routes/slack.rs) — narrower than
 *  {@link SlackChannelDestination}: there is no `"default"` arm here, because
 *  both endpoints report the destination content ACTUALLY resolves to right
 *  now (`resolve_channel_destinations` always picks a real single ocean or a
 *  fan-out, even with no explicit row — see `ChannelDestination`'s own doc
 *  comment), not "did a human choose one". */
/** `seated_oceans` is the SAME quantity as `SlackFanoutReach.mapped_members`
 *  (one destination ocean per mapped member, deduplicated by person) under a
 *  different wire name — the two endpoints were built months apart. Left
 *  unaligned deliberately: renaming either is a wire change on a response
 *  shape `SetSlackChannelDestinationResponse` also uses, which is a write-path
 *  edit and not this read-side PR's business. What IS aligned is the WORDING:
 *  both surfaces render through `fanoutDestinationLabel` in
 *  `components/connectors/slackChannelModel.ts`, so the table row and the
 *  dialog it opens can no longer describe the same fan-out differently. */
export type SlackDestinationImpact =
  | { kind: "single"; graph_id: string; ocean_name: string | null }
  | { kind: "fanout"; seated_oceans: number; unmapped_members: number };

/** `seat_result` on `set_channel_destination`'s response — `Some` only when
 *  the pick was `single` against a `kind = "team"` ocean (the auto-seat step
 *  ran). The routing change itself is never rolled back on a seat failure —
 *  see `SetDestinationOutcome`'s doc comment — so `error` here means "routing
 *  succeeded, seating didn't", not "nothing happened". */
export type SlackSeatResult = { seated: number; unresolved: number } | { error: string };

export interface SetSlackChannelDestinationResponse {
  destination: SlackDestinationImpact;
  seat_result: SlackSeatResult | null;
}

/** `left` is always true on success (an `Err` throws instead) — `already_left`
 *  distinguishes a real `conversations.leave` from the idempotent no-op when
 *  the bot was already out of the channel. Either way the routing row is gone
 *  (`watch_row_deleted`) and the bot is not reading this channel any more. */
export interface StopWatchingSlackChannelResponse {
  left: true;
  already_left: boolean;
  watch_row_deleted: boolean;
}

/** `GET .../impact` — real numbers for the confirmation dialog in front of
 *  EITHER action (`channel_impact` in routes/slack.rs). `atom_count` +
 *  `current_destination` describe what's already captured and where it
 *  stays, always. `prospective_team_seats` only appears when the caller
 *  passed `target_graph_id` for a `kind = "team"` ocean this account owns —
 *  `null` otherwise (never zeroed), and it can itself be an `error` when the
 *  roster walk failed rather than a clean `{identified, total}`. */
export interface SlackChannelImpact {
  atom_count: number;
  current_destination: SlackDestinationImpact;
  prospective_team_seats: { identified: number; total: number } | { error: string } | null;
}

// ── Google Chat watched-space connector (gsuite Train G4) ──────────────────
// The container-archetype destination picker for `space_type: "SPACE"` Chat
// rooms (`routes::google_chat.rs`), plus the mailbox archetype's opt-in for
// `GROUP_CHAT`/`DIRECT_MESSAGE` spaces (G4.5) — both come back from the SAME
// `GET .../spaces` call (`spaces_response` in the backend), one entry per
// live Chat space this member can currently see, merged against this
// account's local watch/opt-in rows.

/** Where a CONTAINER space's (`space_type === "SPACE"`) messages land, or why
 *  they don't yet — `destination_json` in `routes/google_chat.rs`.
 *
 *  `"not_connected"`: the CoralSwarm Chat app has never been added to this
 *  space (D6) — there is nothing to route yet, and `POST .../destination`
 *  404s with an actionable message if attempted; a space in this state must
 *  be shown as "add the app in Google Chat first", not offered a picker.
 *
 *  `"unrouted"`: the app IS added, but no human has picked a destination yet
 *  — the RESTING state for a newly added space (`mode IS NULL`). It ingests
 *  nothing until routed, and a re-add after removal always returns here
 *  (re-adding never resurrects a previous pick) — render this as clearly
 *  actionable, never as "finished". */
export type GoogleChatDestination =
  | { kind: "not_connected" }
  | { kind: "unrouted" }
  | { kind: "single"; graph_id: string }
  | { kind: "fanout" };

/** Whether, and where, THIS caller has personally opted a `GROUP_CHAT`/
 *  `DIRECT_MESSAGE` space's content into their own private ocean
 *  (`mailbox_optin_json` in `routes/google_chat.rs`). There is no
 *  destination to pick for this archetype — content always lands in the
 *  opted-in member's own primary ocean, never a shared or fanned-out one,
 *  and only the CALLING member's own opt-in is ever reported here. */
export type GoogleChatMailboxOptin =
  | { opted_in: false }
  | { opted_in: true; active: boolean; graph_id: string };

/** One live Google Chat space, merged against this account's local
 *  `google_chat_watched_spaces`/mailbox-opt-in rows (`spaces_response`).
 *  `destination` is present only for `space_type === "SPACE"`;
 *  `mailbox_optin` only for `"GROUP_CHAT"`/`"DIRECT_MESSAGE"` — the two
 *  archetypes are mutually exclusive per space, never both, never neither. */
export interface GoogleChatSpace {
  /** The bare Chat resource suffix (e.g. `"AAAAAAAAAAA"`), never the
   *  `"spaces/…"`-prefixed full name — every route under
   *  `/v1/connectors/google/chat/spaces/{space_id}` expects this bare form. */
  space_id: string;
  display_name: string | null;
  space_type: "SPACE" | "GROUP_CHAT" | "DIRECT_MESSAGE";
  history_state: string;
  connected?: boolean;
  active?: boolean;
  destination?: GoogleChatDestination;
  mailbox_optin?: GoogleChatMailboxOptin;
}

export interface GoogleChatSpacesResponse {
  spaces: GoogleChatSpace[];
}

/** What a human is choosing for a CONTAINER space, wire-tagged on `kind` to
 *  match `DestinationPickBody` (`routes/google_chat.rs`) exactly — narrower
 *  than {@link SlackDestinationPick}: there is no `"default"` arm here (a
 *  Chat space has no account-default fallback; an un-picked space just stays
 *  unrouted). */
export type GoogleChatDestinationPick = { kind: "single"; graph_id: string } | { kind: "fanout" };

/** Best-effort roster-seating preview computed right after a destination
 *  pick (`SeatingDisclosure` in `connectors/google_chat.rs`). `available:
 *  false` means the roster read itself failed, or no connected member's
 *  credential could be borrowed to read it — every other field is
 *  MEANINGLESS (not a real zero) in that case and must render as
 *  "unavailable", never as a count. */
export interface GoogleChatSeatingDisclosure {
  available: boolean;
  total_humans: number;
  matched: number;
  unmapped: number;
  group_memberships: number;
}

/** `POST .../destination`'s response. `newly_routed: false` means the pick
 *  was a NO-OP: the space was already routed (the server-side UPDATE is
 *  scoped to `mode IS NULL`), so `destination` below reflects whatever it
 *  was routed to BEFORE this call, not the pick just submitted. Callers must
 *  gate any "routed to X" success copy on `newly_routed === true` — the
 *  connector UI never calls this for an already-routed space in the first
 *  place (see `googleChatModel.ts`'s space-status derivation), but the
 *  response shape does not stop a stale client from doing so. */
export interface SetGoogleChatDestinationResponse {
  newly_routed: boolean;
  destination: GoogleChatDestination;
  history_off: boolean;
  seating: GoogleChatSeatingDisclosure;
}

/** `POST .../optin`'s response (G4.5) — echoes the row just written. */
export interface GoogleChatMailboxOptinResult {
  space_id: string;
  display_name: string | null;
  space_type: string;
  graph_id: string;
  active: boolean;
  history_off: boolean;
}

// ── GitHub connector (PR A: sync plumbing) ──────────────────────────────────
// docs/connectors/github.md §3.3–3.4, §3.6, §3.8. One GitHub App, two install
// paths (org / personal, §3.1) sharing the same `oauth_tokens` shape; the
// install path only decides the CREDENTIAL. Destination is chosen PER REPO
// at selection time (contract I2), independent of which path produced it —
// never re-derived from the collaborator roster, since a public repo's
// roster understates its audience (§3.4).

/** One `GET /v1/connectors/github/installs` row — an App installation on a
 *  GitHub org or a personal account. */
export interface GithubInstall {
  /** A decimal string, not a number — GitHub ids exceed the safe integer
   *  range and the backend serializes them as strings. */
  installation_id: string;
  account_login: string;
  account_type: "Organization" | "User";
  connected_at: string;
  /** The App was uninstalled or suspended upstream — sync is paused, every
   *  atom already ingested is kept (§3.8: never purge). */
  suspended: boolean;
  /** Repos granted via `installation_repositories.added` since the last
   *  selection save (§3.3) — they land PAUSED with a notification, never an
   *  automatic backfill, until someone visits this page and saves a
   *  selection for them. */
  pending_selection: number;
}

export interface GithubInstallsResponse {
  installs: GithubInstall[];
}

/** A repo's CURRENT destination, as already saved — `team_id`/`team_name`
 *  present only for `kind: "team"`. Distinct from
 *  {@link GithubRepoDestinationPick}, which is what a save submits (and
 *  which additionally allows creating a brand-new team). */
export interface GithubRepoDestination {
  kind: "personal" | "team" | "org";
  team_id?: string;
  team_name?: string;
}

/** Per-object-type backfill progress (§3.6), each 0–100 or `null` before
 *  that object type's crawl has started for this repo, newest-to-oldest —
 *  render as "PRs 40%, issues 0%". The connector captures no repository
 *  source code (2026-10-27), so there is no files/code progress. */
export interface GithubRepoProgress {
  prs: number | null;
  issues: number | null;
  releases: number | null;
  commits: number | null;
}

export interface GithubRepo {
  /** `null` for a repo that's been granted but never selected — there is no
   *  watch row yet, so `suggested` (not `active`) is what the selection page
   *  must default the toggle to for this row. */
  row_id: string | null;
  repo_id: number;
  owner: string;
  name: string;
  full_name: string;
  default_branch: string;
  /** GitHub's visibility string, passed through unchanged. */
  visibility: string;
  archived: boolean;
  fork: boolean;
  size_kb: number;
  active: boolean;
  /** The server's default for a never-selected row: non-archived, non-fork
   *  repos default ON (§3.3). Meaningless once `row_id` is non-null — `active`
   *  is the live value at that point. */
  suggested: boolean;
  paused_reason: string | null;
  /** Non-null exactly when sync is paused and content is being kept — render
   *  "sync stopped since {date}" (§3.8), never a purge/removal implication. */
  sync_stopped_at: string | null;
  destination: GithubRepoDestination | null;
  progress: GithubRepoProgress | null;
  last_synced_at: string | null;
  /** How this repo's most recent backfill tick failed, `null` when it is not
   *  currently failing (`github_watched_repos.sync_error_code`, migration
   *  `0250`/`0102`). Cleared by the next tick that makes progress, so it
   *  answers "is this repo failing right now", never "has it ever failed".
   *
   *  A CLASSIFICATION, never the worker's error text — internal error detail
   *  must not leave through an API response, so the backend maps every
   *  failure onto a closed set and keeps the verbatim message in
   *  `jobs.error_message` and its own logs.
   *
   *  Typed as an OPEN string rather than a closed union: the
   *  backend's set can gain a member without a web release, and the one
   *  outcome that is not allowed is for a new code to render as nothing. See
   *  `syncErrorCopy` in `components/connectors/githubRepoModel.ts`, which
   *  falls back to generic copy for a code this build has never heard of.
   *
   *  Load-bearing because none of the other fields can say it:
   *  `last_synced_at` staying null on a first backfill is indistinguishable
   *  from "still working". Both prod installs once rendered as `backfilling`
   *  with a healthy counter line while their jobs were dead. */
  sync_error_code: string | null;
  sync_error_at: string | null;
}

/** `destinations.teams[].is_admin` gates the EXISTING-team option per §3.4's
 *  role table — only a team the caller already admins is offered; creating a
 *  brand-new repo-mirrored team is always available regardless (see
 *  `GithubRepoDestinationPick`'s `{kind:"team",create:true}` arm) — the
 *  founder's own gate note reads "the team options need a team admin (or
 *  create a new team)". `destinations.org.allowed` gates the org option
 *  (CoralSwarm org admin only); a non-admin who submits it anyway gets the
 *  403 documented on `setGithubRepos` below. `org.non_collaborator_count`
 *  backs the §3.4 disclosure line ("N org members, M of whom are not
 *  collaborators") — `null` when it couldn't be computed (a roster read
 *  failed), which must render as "unavailable", never as zero. */
export interface GithubDestinationOptions {
  personal: true;
  teams: { id: string; name: string; is_admin: boolean }[];
  org: { allowed: boolean; member_count: number; non_collaborator_count: number | null };
}

export interface GithubReposResponse {
  repos: GithubRepo[];
  destinations: GithubDestinationOptions;
}

/** What a save submits for one repo's destination — wire-tagged on `kind` to
 *  match the backend's `DestinationPick` body. Three of the four arms mirror
 *  {@link GithubRepoDestination} exactly; the fourth,
 *  `{kind:"team",create:true}`, is §3.4's "a new repo-mirrored team ocean
 *  created with the repo's mapped collaborators seated" — it has no
 *  `team_id` because the team doesn't exist yet. */
export type GithubRepoDestinationPick =
  | { kind: "personal" }
  | { kind: "team"; team_id: string }
  | { kind: "team"; create: true }
  | { kind: "org" };

export interface GithubRepoSelection {
  repo_id: number;
  active: boolean;
  destination: GithubRepoDestinationPick;
}

export interface GithubReposPutBody {
  installation_id: string;
  selections: GithubRepoSelection[];
}

/** The `error` string `PUT /v1/connectors/github/repos` sends on a 403 when
 *  a non-admin submits `{kind:"org"}` for any repo (§3.4: "a non-admin who
 *  picks the org ocean is refused and offered the other two"). Unlike most
 *  of this client's error bodies, this one is a machine code, not a
 *  sentence — render it via a fixed client-side message, and re-offer
 *  personal/team for whichever row(s) the submission had set to org, rather
 *  than surfacing the raw string. */
export const GITHUB_ORG_DESTINATION_REQUIRES_ADMIN = "org_destination_requires_admin";

// A decision reef awaiting human review (CS-069).
export interface DecisionCandidate {
  node_id: string;
  graph_id: string;
  title: string;
  rationale: string | null;
  confidence: number | null;
  decided_at: string | null;
  created_at: string;
  source_atom_ids: string[];
  /** Why this queued (high-signal review): structural blast radius. */
  impact?: ReviewImpact;
  pending_since?: string | null;
  /** When the deadline sweep will auto-resolve it if unreviewed. */
  deadline_at?: string | null;
}

/** One keyset page of decision candidates — `GET /v1/candidates`. */
export interface DecisionCandidatePage {
  items: DecisionCandidate[];
  has_more: boolean;
  next_cursor: string | null;
}

/** Why a review item qualified — rendered as the card's consequence line. */
export interface ReviewImpact {
  degree: number;
  corals: number;
  contradiction: boolean;
  /** Edge cards only: recent ask answers mentioning an endpoint. */
  recent_asks?: number;
  sample_ask?: string | null;
}

// ── Outlook ingest workflows (CS-118) ──────────────────────────────────────────
/** Match criteria for an auto-ingest rule. AND across populated fields, OR within
 * each (e.g. from_domains:[acme.com] + subject_contains:[invoice,po]). All string
 * matching is case-insensitive on the backend. */
export interface WorkflowFilter {
  from_addresses?: string[];
  from_domains?: string[];
  subject_contains?: string[];
  recipient_addresses?: string[];
  recipient_domains?: string[];
  categories?: string[];
  folders?: string[];
  has_attachments?: boolean | null;
  /** Server-computed display label per entry of `folders`, keyed by the raw
   * folder value (e.g. `{"inbox": "Inbox"}`). Well-known keys always resolve;
   * a raw Graph folder id from an older workflow is simply absent — fall back
   * to the local `FOLDER_LABELS` map / "other folder" placeholder. Additive:
   * absent entirely on older backends. */
  folder_labels?: Record<string, string>;
}
export interface Workflow {
  id: string;
  name: string;
  enabled: boolean;
  filter: WorkflowFilter;
  created_at: string;
}

// ── Microsoft Teams ingest (CS-119) ────────────────────────────────────────────
export interface MsTeam {
  id: string;
  display_name: string;
}
export interface MsChannel {
  id: string;
  display_name: string;
  membership_type?: string | null;
}
export interface MsChat {
  id: string;
  display_name: string;
  chat_type?: string | null;
}
export interface MsWatched {
  id: string;
  kind: string;
  team_id: string;
  channel_id: string;
  chat_id: string;
  display_name: string;
  created_at: string;
  /** `"delayed"` when the shared Teams doorbell is polling. Absent while push is live. */
  capture?: string | null;
}
export type MsOnboardingResult =
  | { status: "active"; watch_id: string; created: boolean; capture_since: string | null }
  | { status: "connect_required"; authorization_url: string };

export interface MsIngestBody {
  kind: "channel" | "chat";
  team_id?: string;
  channel_id?: string;
  chat_id?: string;
  display_name?: string;
}

// ── Meeting transcript provenance (raw ⟷ corrected) ─────────────────────────
/** One surface_form → canonical correction applied to a meeting's raw
 *  transcript. `status` flips between "applied" and "reverted" via the
 *  revert/restore endpoints; a reverted correction leaves the raw word as-is. */
export interface TranscriptCorrection {
  correction_id: string;
  /** The verbatim Whisper text that was corrected. */
  surface_form: string;
  /** What the system thinks it actually was. */
  canonical: string;
  /** The entity reef this correction resolved to, when any. */
  node_id: string | null;
  confidence: number | null;
  status: "applied" | "reverted";
  /** "name" (typo/name cleanup) or "speaker" (speaker relabel). Optional —
   *  older rows predate the distinction. */
  kind?: "name" | "speaker";
  /** For "speaker" corrections scoped to a single segment: the 1-based
   *  occurrence of `surface_form` in the transcript this targets. null for
   *  an all-occurrences correction. Optional — older rows predate this. */
  occurrence?: number | null;
}
/** An entity auto-merge performed during extraction: `merged_title` was folded
 *  into `kept_title`. Undoable via `api.merges.undo`. */
export interface EntityMerge {
  merge_id: string;
  kept_node_id: string;
  kept_title: string;
  merged_title: string;
  created_at: string;
}
/** A brand-new entity reef this meeting created (for the "is this right?" check). */
export interface NewEntity {
  node_id: string;
  title: string;
  created_at: string;
}
/** Lifecycle of the async post-meeting synthesis job. `pending`/`running` mean a
 *  job is (or will be) working; `done` has a `summary`; `failed` carries a
 *  `summary_error`; `blocked` means synthesis needs cloud egress the account
 *  hasn't opted into for this meeting (a per-meeting opt-in unblocks it). */
export type MeetingSummaryStatus = "pending" | "running" | "done" | "failed" | "blocked";

/** One decision the meeting reached. `ts_ms` anchors it to a transcript segment
 *  (nearest by `start_ms`) for cite-chip deep-linking; null when unanchored. */
export interface MeetingSummaryDecision {
  text: string;
  ts_ms: number | null;
}
/** A follow-up owed after the meeting. `owner` is null when unassigned. */
export interface MeetingSummaryActionItem {
  text: string;
  owner: string | null;
  ts_ms: number | null;
}
/** The machine-written synthesis: a short overview paragraph plus the decisions
 *  reached and action items owed. Present only when `summary_status === "done"`. */
export interface MeetingSummaryBody {
  overview: string;
  decisions: MeetingSummaryDecision[];
  action_items: MeetingSummaryActionItem[];
}
/** A speaker-attributed transcript turn with timestamps, used to render a
 *  timestamped transcript and to resolve cite-chip deep-links. */
export interface MeetingSegment {
  speaker: string;
  start_ms: number;
  end_ms: number | null;
  text: string;
}

/** Post-meeting summary (`GET /v1/meetings/summary`): the verbatim raw transcript,
 *  the corrected ("what we think it is") transcript, and the provenance of every
 *  correction / merge / new entity so the user can revert what was wrong. Any of
 *  the text fields may be null (e.g. before extraction completes / free tier).
 *  `title` / `occurred_at` are best-effort (derived server-side) and may be
 *  null — the frontend falls back to deriving a label from provenance.
 *
 *  Also carries the async machine synthesis: `summary_status` drives the UI
 *  (poll while pending/running), `summary` holds the overview/decisions/action
 *  items when done, `summary_model` credits the model, `summary_error` explains a
 *  failure, and `segments` (when present) is the timestamped transcript. Meetings
 *  ingested before this feature report `summary_status: "pending"` with null
 *  `summary`/`segments`. */
/** Recorded transcript inventory. Unknown event metadata stays null; created_at is capture time. */
export interface MeetingListItem {
  source_id: string;
  title: string | null;
  occurred_at: string | null;
  participants: string[] | null;
  created_at: string;
  duration_ms: number | null;
  summary_status: string;
}

export interface MeetingsPage {
  items: MeetingListItem[];
  next_cursor: string | null;
  has_more: boolean;
}

/** Exact evidence in an immutable meeting transcript version. Offsets count
 * Unicode scalar values (Array.from(text)), with an exclusive end. */
export interface MeetingEvidencePassage {
  passage_id: string;
  quote: string;
  start_char: number;
  end_char: number;
  start_ms: number | null;
  end_ms: number | null;
  speaker: string | null;
}

export interface MeetingSemanticAtom {
  atom_id: string;
  kind: "fact" | "decision" | "action" | "observation";
  statement: string;
  passages: MeetingEvidencePassage[];
  nodes: { node_id: string; graph_id: string; title: string; type: string }[];
}

export interface MeetingSourceVersion {
  version_id: string;
  captured_at: string;
  is_legacy_baseline: boolean;
  is_current: boolean;
}

/** Captured service identity; transport describes how it reached CoralSwarm.
 * Legacy sources without verified metadata report null. */
export interface SourceOrigin {
  service_id: string;
  transport: "native" | "composio" | "synthetic";
  content_kind: string;
  capture_surface: string | null;
  connection: { kind: "oauth_grant" | "connector_install"; id: string } | null;
  container: { kind: string; id: string; provider_namespace: string; label: string | null } | null;
}

export type CaptureCapability = "available" | "unsupported" | "unknown";

/** Declared code support for this capture surface, independent of connection
 * health or corpus completeness; unknown is not unsupported. */
export interface CaptureContract {
  history: CaptureCapability;
  updates: CaptureCapability;
  deletes: CaptureCapability;
  reconnect: CaptureCapability;
}

export interface MeetingEvidenceVersion {
  source_item_id?: string;
  source_type?: string;
  provider_namespace?: string;
  source_id?: string;
  /** Immutable metadata captured with this version; absent on older APIs. */
  origin?: SourceOrigin | null;
  capture_contract?: CaptureContract;
  representation?: string;
  source_metadata?: Record<string, unknown>;
  occurred_at?: string | null;
  participants?: string[];
  extraction_version?: string;
  version_id: string;
  captured_at: string;
  is_legacy_baseline: boolean;
  raw_text: string | null;
  text_status: "available" | "unavailable";
  extraction_status: "not_started" | "pending" | "running" | "done" | "failed" | "blocked";
  extraction_error: string | null;
  can_retry: boolean;
  atoms: MeetingSemanticAtom[];
  atoms_next_cursor: string | null;
}

export interface MeetingEvidencePage {
  graph_id: string;
  source_id: string;
  versions: MeetingSourceVersion[];
  next_cursor: string | null;
  selected_version: MeetingEvidenceVersion | null;
}

export interface SourceIdentity {
  source_item_id: string;
  source_type: string;
  provider_namespace: string;
  source_id: string;
  /** Current item metadata, independent of a selected version's origin.
   * Optional for compatibility with older API responses. */
  origin?: SourceOrigin | null;
  capture_contract?: CaptureContract;
}
export interface SourceEvidencePage extends Omit<MeetingEvidencePage, "source_id"> {
  source: SourceIdentity;
  source_item_id: string;
}
export interface SourceItem extends SourceIdentity {
  graph_id: string;
  current_version_id: string | null;
  mapped_version_id: string | null;
  updated_at: string;
  captured_at: string | null;
  occurred_at: string | null;
  representation: string | null;
  source_metadata: Record<string, unknown>;
  extraction_status: MeetingEvidenceVersion["extraction_status"] | null;
  is_legacy_baseline?: boolean;
}
export interface SourceItemsPage {
  graph_id: string;
  items: SourceItem[];
  next_cursor: string | null;
}

/** Server-validated evidence selectors. An echoed/resolved scope is UI context,
 * never an authorization token; retrieval must validate it again. */
export interface LibraryScope {
  graph_ids: string[];
  /** Defaults to all_retained. Exact version selection normalizes to all_retained. */
  outcome_versions?: "current" | "all_retained" | null;
  service_id?: string | null;
  connection?: SourceOrigin["connection"];
  container?: { kind: string; id: string; provider_namespace: string } | null;
  content_kind?: string | null;
  collection?: "meetings" | "agent_sessions" | "work_sessions" | null;
  source_item_id?: string | null;
  source_version_id?: string | null;
  node_id?: string | null;
}
export interface LibrarySearchRequest {
  scope: LibraryScope;
  query?: string;
  page_size?: number;
  sources_after?: string | null;
  outcomes_after?: string | null;
  nodes_after?: string | null;
}
export interface LibraryCursorPage<T> { items: T[]; total: number; next_cursor: string | null }
export interface LibraryFacet {
  key: string;
  value: string | Record<string, unknown>;
  label: string | null;
  source_count: number;
}
export interface LibrarySource extends SourceIdentity {
  graph_id: string;
  current_captured_at: string | null;
  current_extraction_status: string | null;
  selected_extraction_status: string | null;
  title: string | null;
  current_version_id: string | null;
  selected_version_id: string | null;
  origin: SourceOrigin | null;
  version_origin: SourceOrigin | null;
  representation: string | null;
  occurred_at: string | null;
  captured_at: string | null;
  updated_at: string;
  semantic_atom_count: number;
  legacy_chunk_count: number;
  synthetic_demo: boolean;
  session?: LibrarySessionKey | null;
}
export interface LibraryEvidenceCounts {
  source_support_count: number;
  semantic_atom_count: number;
  unmapped_legacy_chunk_count: number;
}
export interface LibraryCurrentCapture {
  source_count: number;
  current_version_count: number;
  unavailable_version_count: number;
  pending_version_count: number;
  incomplete_version_count: number;
  current_semantic_atom_count: number;
  latest_captured_at: string | null;
  /** Extraction readiness for current captures, never effective validity. */
  status: "ready" | "pending" | "incomplete" | "unavailable";
}
export interface LibraryNode {
  graph_id: string;
  current_capture: LibraryCurrentCapture;
  node_id: string;
  type: string;
  entity_type: string | null;
  title: string | null;
  contribution: LibraryEvidenceCounts;
  wider_context: LibraryEvidenceCounts;
  latest_captured_at: string | null;
  unresolved_disagreement: boolean;
  current_understanding_status: "evidence_only" | "unresolved_disagreement";
  synthetic_demo: boolean;
}
export interface LibraryCitation {
  kind: "source_passage";
  graph_id: string;
  source_item_id: string;
  source_version_id: string;
  passage_id: string;
  semantic_atom_id: string;
  start_char: number;
  end_char: number;
  quote: string;
  quote_truncated: boolean;
  representation: string;
  occurred_at: string | null;
  captured_at: string;
}
export interface LibraryOutcome {
  graph_id: string;
  source_version_is_current: boolean;
  semantic_atom_id: string;
  kind: "fact" | "decision" | "action" | "observation";
  statement: string;
  source_item_id: string;
  source_version_id: string;
  origin: SourceOrigin | null;
  occurred_at: string | null;
  captured_at: string;
  nodes: { node_id: string; title: string | null; type: string }[];
  citations: LibraryCitation[];
  nodes_truncated: boolean;
  citations_truncated: boolean;
  session?: LibrarySessionKey | null;
}
export interface LibrarySearchResponse {
  scope: LibraryScope;
  sources: LibraryCursorPage<LibrarySource>;
  outcomes: LibraryCursorPage<LibraryOutcome>;
  nodes: LibraryCursorPage<LibraryNode>;
  prominent_work: LibraryNode[];
  facets: { services: LibraryFacet[]; content_kinds: LibraryFacet[]; connections: LibraryFacet[]; containers: LibraryFacet[]; unknown_origin_count: number; truncated: boolean };
}
export type LibraryFacetDimension = "services" | "content_kinds" | "connections" | "containers";
export interface LibraryFacetRequest {
  scope: LibraryScope;
  dimension: LibraryFacetDimension;
  option_query?: string;
  page_size?: number;
  after?: string | null;
}
export interface LibraryFacetPage {
  scope: LibraryScope;
  dimension: LibraryFacetDimension;
  items: LibraryFacet[];
  total: number;
  next_cursor: string | null;
  truncated: boolean;
}
export interface LibraryCapturedContext {
  revision: 1 | null;
  session_id: string | null; session_kind: string | null; session_title: string | null;
  project: string | null; project_key: string | null;
  repo_path: string | null; remote_sanitized: string | null; branch: string | null;
  hostname: string | null; harness_version: string | null; model: string | null;
  agent_name: string | null; platform: string | null; platform_detected: string | null;
  task: string | null; subagent_name: string | null;
  parent_session_id: string | null; root_session_id: string | null;
  lineage_status: "captured" | "partial" | "unknown";
}
export interface LibraryCapturePassage {
  graph_id: string; source_item_id: string; source_version_id: string;
  passage_id: string; ordinal: number; start_char: number; end_char: number;
  text: string; text_truncated: boolean; citation_label: string;
}
export interface LibraryCaptureVersion extends SourceIdentity {
  graph_id: string; source_version_id: string; source_version_is_current: boolean;
  title: string | null; representation: string; origin: SourceOrigin | null;
  captured_at: string; source_event_at: string | null; extraction_status: string;
  participants: string[]; participants_truncated: boolean;
  context_status: "captured_reported" | "historical_partial" | "unavailable";
  context: LibraryCapturedContext;
  outcomes: { total: number; items: LibraryOutcome[]; truncated: boolean };
  passages: { total: number; items: LibraryCapturePassage[]; truncated: boolean };
}
export interface LibraryHistoryRequest {
  temporal?: TemporalSelection;
  scope: LibraryScope; session_id?: string | null; provider_namespace?: string | null; include_subagents?: boolean;
  page_size?: number; after?: string | null;
}
/** The one captured session a history read resolved to. */
export interface LibrarySessionKey { graph_id: string; provider_namespace: string; session_id: string }
export interface LibrarySessionCandidate extends LibrarySessionKey {
  version_count: number; first_captured_at: string; last_captured_at: string; title: string | null;
}
/** A subagent session admitted by a source pointer inside the resolved (graph, namespace).
 * `parent_session_id` is the member it points to; null when only the root pointer admitted it. */
export interface LibrarySubagentSession { session_id: string; parent_session_id: string | null; version_count: number }
/** Retained captures in another namespace that name a member but cannot prove it; counts only. */
export interface LibrarySubagentExclusion { provider_namespace: string; version_count: number }
export interface LibrarySubagentTree {
  sessions: LibrarySubagentSession[]; sessions_truncated: boolean; excluded: LibrarySubagentExclusion[]; excluded_truncated: boolean;
}
export interface LibraryHistoryResponse {
  temporal_state?: TemporalStateResult | null;
  scope: LibraryScope; selection: "retained_capture_history";
  session: LibrarySessionKey | null;
  /** Present only when `include_subagents` was requested; servers before COR-7 omit it. */
  subagents?: LibrarySubagentTree | null;
  total: number; versions: LibraryCaptureVersion[];
  next_cursor: string | null; truncated: boolean;
}
/** 409: the session id names several (workspace, namespace) sessions; nothing was read. */
export interface LibrarySessionAmbiguity {
  selection: "ambiguous_session"; scope: LibraryScope; session_id: string; candidates: LibrarySessionCandidate[];
}
export type TemporalSelection = {mode: "ordinary"; revision: 1} | {
  mode: "historical"; revision: 1; state_key: string; effective_at: string; as_known_at: string;
};
export interface TemporalStateRecord {
  event_id: string; operation: "assert" | "transition" | "correct" | "withdraw";
  value: string | null; valid_from: string | null; valid_until: string | null;
  known_at: string; evidence_observed_at: string;
  recording_method: "human_curated_v1" | "temporal_extraction_v1";
  semantic_kind: string; interpretation: "curated_declared_validity";
  epistemic_status: "human_interpretation" | "recorded_interpretation";
  predecessor_event_id: string | null; relation_status: "not_applicable" | "recorded_link" | "unavailable";
  citation: LibraryCitation;
}
/** POST /v1/library/temporal/state */
export interface TemporalStateRequest { scope: LibraryScope; temporal: Extract<TemporalSelection, {mode: "historical"}> }
export interface TemporalStateResponse { scope: LibraryScope; temporal_state: TemporalStateResult }
export interface TemporalStateResult {
  mode: "recorded_evidence_state"; temporal: TemporalSelection;
  status: "supported" | "disputed" | "unknown" | "unsupported" | "incomplete";
  history_coverage: "explicit_records_only";
  state_basis: "declared_interpretations_not_verified_provider_state";
  values: {value: string; source_count: number; event_ids: string[]; citations: LibraryCitation[]}[];
  records: TemporalStateRecord[]; truncated: boolean; parallel_corrections: boolean;
  contradictions_resolved: false;
}
/** `POST /v1/library/resolve-scope`: the authorized, validated scope and
 * nothing else. Totals for a scope come from `/v1/library/search`. */
export interface LibraryScopeResponse {
  scope: LibraryScope;
}
export interface LibraryNeighborhood {
  scope: LibraryScope;
  node_id: string;
  items: {
    edge_id: string;
    predicate: string;
    origin: "stated" | "inferred";
    status: "confirmed" | "suggested";
    confidence: number;
    reviewed_by: string | null;
    src: { node_id: string; title: string | null; type: string };
    dst: { node_id: string; title: string | null; type: string };
    support: ({ source_item_id: string; source_version_id: string; source_version_is_current: boolean } | { kind: "legacy_chunk"; graph_id: string; atom_id: string })[];
    support_truncated: boolean;
  }[];
  next_cursor: string | null;
  truncated: boolean;
}
export interface NodeMeetingAtomsPage {
  graph_id: string;
  node_id: string;
  items: {
    atom_id: string;
    kind: MeetingSemanticAtom["kind"];
    statement: string;
    source_id: string;
    source_version_id: string;
    source_item_id?: string;
    source_type?: string;
    provider_namespace?: string;
    representation?: string;
    is_legacy_baseline?: boolean;
    captured_at: string;
    passages: MeetingEvidencePassage[];
  }[];
  next_cursor: string | null;
}

export interface MeetingSummary {
  source_id: string;
  title: string | null;
  occurred_at: string | null;
  raw_text: string | null;
  corrected_text: string | null;
  corrections: TranscriptCorrection[];
  merges: EntityMerge[];
  new_entities: NewEntity[];
  summary_status: MeetingSummaryStatus;
  summary: MeetingSummaryBody | null;
  summary_model: string | null;
  summary_error: string | null;
  segments: MeetingSegment[] | null;
}

/** One AI coding-agent session (Claude Code, etc.) captured via `add_context`'s
 *  `session_id` input (coding-session capture Phase 1/3/4) — `GET /v1/sessions`'s
 *  list row shape, byte-for-byte the same fields the `list_sessions` MCP tool
 *  returns. `project_key`/`repo_path`/`hostname`/`harness_version`/`model`
 *  are all best-effort provenance and may be null; `title` is not (see below). */
export interface CodingSessionSummary {
  session_id: string;
  /** The WIRE kind (backend `routes::ingest::SessionKind`) — deliberately
   *  not the derived `atoms.source_type`. One table (`coding_sessions`) holds
   *  every kind, so the row itself has to say which it is.
   *
   *  Read it as a plain string, never a union narrowed at the type level: a
   *  row written by a newer deploy must still render rather than break the
   *  list, which is the same rule the backend's own `CodingSession::
   *  session_kind` doc states. */
  session_kind: string;
  /** Always a non-empty string — the backend guarantees this via
   *  `services::atom_title::display_session_title` (title-always-present
   *  PR): a real `coding_sessions.title` when present, else the display
   *  title of the session's checkpoint, else `"{project_key} · {date}"`,
   *  else a kind-labelled `"{Kind} session · {date}"`. Never
   *  null and never depends on an async sweep having run. Still handled
   *  defensively at render sites — never trust a wire contract blindly for
   *  "never empty". */
  title: string;
  project_key: string | null;
  repo_path: string | null;
  /** Sanitized git remote (`github.com/org/repo`). Distinct from `repo_path`
   *  (a local working-directory path). Either form is a valid `repo` filter. */
  remote_sanitized?: string | null;
  hostname: string | null;
  harness_version: string | null;
  model: string | null;
  /** Last-reported git branch on the session row. Per-checkpoint history
   *  still rides each coral's chunk header. */
  branch?: string | null;
  started_at: string;
  last_activity_at: string;
  /** Under the default `roots_only=true` listing this is the TREE-WIDE
   *  count for an agent session (the root's own corals plus every
   *  subagent's); per-session otherwise. */
  coral_count: number;
  /** Agent identity (`session_kind === "agent"`, migration 0219). All null
   *  on coding / work / meeting sessions and on rows written before the
   *  columns existed, so every field is optional AND nullable. */
  agent_name?: string | null;
  /** Canonical platform slug (`claude-code`, `cursor`, `codex`, ...). */
  platform?: string | null;
  /** What the request's User-Agent said the harness was, when it named one.
   *  Kept separate from `platform` so a declared value is never rewritten. */
  platform_detected?: string | null;
  /** `platform_detected == null || platform_detected === platform`. `false`
   *  means the harness the corals came from disagreed with what the agent
   *  declared — render an "unverified" marker, never hide the row. */
  platform_verified?: boolean;
  task?: string | null;
  /** The top of the agent tree this session belongs to (itself for a root). */
  root_session_id?: string | null;
  /** Set on a subagent: the session that spawned it. */
  parent_session_id?: string | null;
  /** The name a subagent declared for itself; its effective `agent_name` is
   *  inherited from the root. */
  subagent_name?: string | null;
  /** Sessions rolled up under this root (any depth). Only computed under
   *  `roots_only=true`; `0` otherwise. */
  subagent_count?: number;
}

/** One coral captured under a coding session (`GET /v1/sessions/{id}`'s `corals`
 *  entries) — lightweight, no content; link to `/coral?id=` for the full text. */
export interface CodingSessionCoral {
  atom_id: string;
  source_id: string;
  timestamp: string;
  created_at: string;
  /** Always a non-empty string (title-always-present PR) — the backend runs
   *  this through `services::atom_title::display_title` regardless of
   *  whether the async titling sweep has reached this atom yet: a real
   *  `atoms.title` when present, else derived from `excerpt`, else a
   *  `"{Source} · {date}"` label. Never the raw `source_id`, and never
   *  null. Render defensively anyway — never trust a wire contract blindly
   *  for "never empty". */
  title: string;
  /** The persisted excerpt (`atoms.excerpt`), when available — the raw
   *  short-form text `title` may itself be derived from. `null`/absent for
   *  an atom with no excerpt yet; distinct from `title`, which is always
   *  present. */
  excerpt?: string | null;
}

/** `GET /v1/sessions/{session_id}` — one session's metadata plus its corals in
 *  chronological (oldest-first) order. `corals_truncated` is set when the
 *  session has more corals than the route returns (see the backend's
 *  `SESSION_DETAIL_CORAL_CAP`). */
export interface CodingSessionDetail extends CodingSessionSummary {
  corals: CodingSessionCoral[];
  corals_truncated: boolean;
  /** Every session rolled up under this one (root agent sessions only;
   *  empty for children and for non-agent sessions). Each entry's
   *  `coral_count` is that subagent's OWN count. */
  subagents?: CodingSessionSubagent[];
  /** Set when `subagents` is a prefix of the tree (the backend's
   *  `SESSION_DETAIL_SUBAGENT_CAP`); page the rest through
   *  `GET /v1/sessions?parent_session_id=` / `roots_only=false`. */
  subagents_truncated?: boolean;
}

/** One subagent row of `GET /v1/sessions/{id}`'s `subagents` array. */
export interface CodingSessionSubagent {
  session_id: string;
  parent_session_id: string | null;
  subagent_name: string | null;
  task: string | null;
  title: string | null;
  started_at: string;
  last_activity_at: string;
  coral_count: number;
}

export interface CodingSessionsPage {
  sessions: CodingSessionSummary[];
  has_more: boolean;
  next_cursor: string | null;
}

/** The wire kinds the Library facets ask for (backend
 *  `routes::ingest::SessionKind`). An unknown value is a 400 from
 *  `GET /v1/sessions`, never a silently-ignored filter. */
export const SESSION_KIND_CODING = "coding";
export const SESSION_KIND_AGENT = "agent";

/** Server-side session list filters shared by `list` / `listAgent`
 *  / `facets`. Every field is optional; AND together on the server. Also the
 *  exact shape `facets` re-derives its dimension vocabularies from — see
 *  `SessionFacets`'s doc on why the current selection has to ride along with
 *  a facets request, not just a plain list one. */
export type SessionListFilters = {
  project?: string;
  agentName?: string;
  platform?: string;
  task?: string;
  harness?: string;
  model?: string;
  repo?: string;
  branch?: string;
};

function applySessionListFilters(params: URLSearchParams, args: SessionListFilters) {
  if (args.project) params.set("project", args.project);
  if (args.agentName) params.set("agent_name", args.agentName);
  if (args.platform) params.set("platform", args.platform);
  if (args.task) params.set("task", args.task);
  if (args.harness) params.set("harness", args.harness);
  if (args.model) params.set("model", args.model);
  if (args.repo) params.set("repo", args.repo);
  if (args.branch) params.set("branch", args.branch);
}

export interface AgentSessionsPage {
  sessions: CodingSessionSummary[];
  has_more: boolean;
  next_cursor: string | null;
}

/** One value of one facet dimension, with the row count it carries under the
 *  CURRENT filter selection (see `SessionFacets` below). */
export interface FacetValue {
  value: string;
  count: number;
}

/** `GET /v1/sessions/facets`'s response — the Library session filter bar's
 *  (`components/library/SessionFilterBar.tsx`) server-computed vocabulary,
 *  replacing "chip values come from the rows loaded so far". Faceted-search
 *  semantics: each dimension is computed with every OTHER active filter
 *  applied but NOT its own, so narrowing one dropdown can change what every
 *  other dropdown offers — the frontend re-fetches on every selection
 *  change (`lib/sessionFacets.ts`'s `useSessionFacets`), sending the current
 *  selection right back as this same route's filter params.
 *
 *  A dimension's list is not guaranteed to include the value currently
 *  selected in THAT dimension — the other active filters can exclude every
 *  row carrying it. Callers must render the selection at the top of its own
 *  menu regardless (`buildFacetMenuValues` in
 *  `components/library/sessionFilterHelpers.ts`), not treat its absence as
 *  "no longer a valid choice". */
export interface SessionFacets {
  facets: Record<"agent_name" | "platform" | "project" | "harness" | "model" | "repo" | "branch", FacetValue[]>;
  /** Per-dimension cap on the backend's aggregation. A dimension whose list
   *  is exactly this long may be truncated — the bar shows a "search to
   *  narrow" line rather than implying the shown values are exhaustive. */
  cap: number;
}

/** D6/D7's fixed outcome vocabulary (`docs/plans/capture/02-b2-retract-fixes.md`
 *  — `services::deletion::outcome` on the backend). Exactly these three
 *  strings; nothing else is ever written to `retraction_audit_log.outcome`. */
export type RetractionOutcome = "removed" | "out_of_window" | "not_a_member";

/** One `retraction_audit_log` row (D7's per-`(ocean, surface)` identity) —
 *  `DELETE /v1/graphs/{id}/corals/{atom}`'s `surface_outcomes` entries and
 *  `GET /v1/retractions{,/…}`'s `rows` entries are byte-for-byte this shape. */
export interface RetractionRow {
  graph_id: string;
  is_personal_ocean: boolean;
  source_type: string;
  source_id: string;
  atom_id: string | null;
  /** Free text on the wire (`"content"`, `"meeting_transcript"`,
   *  `"coding_session"`, `"journal_rollup"` today) — not a fixed union,
   *  since D7's own schema comment says new surfaces are expected over time. */
  surface: string;
  outcome: RetractionOutcome;
  /** Ids of derived artifacts left standing despite this outcome (D7's
   *  `survivors[]`) — always empty for `outcome: "removed"` today. */
  survivors: string[];
}

/** `DELETE /v1/graphs/{id}/corals/{atom}`'s response body — the D6 UI's
 *  synchronous read of what one retract call reached, keyed by the SAME
 *  `retraction_id` `GET /v1/retractions/{id}` can re-fetch later (e.g. after
 *  a page reload, once the content itself — an atom, or FM-6's whole
 *  `coding_sessions` row — may already be gone). */
export interface DeletionCascadeOutcome {
  retraction_id: string;
  atoms_deleted: { graph_id: string; atom_id: string }[];
  deleted_reef_ids: { graph_id: string; node_id: string }[];
  propagated_graph_ids: string[];
  surface_outcomes: RetractionRow[];
}

/** One `retraction_id` group, as `GET /v1/retractions`/`GET
 *  /v1/retractions/{id}`/`GET /v1/retractions/session/{session_id}` render
 *  it — every row ONE retract call produced, reached and unreached together
 *  (D6: "in the SAME list, not a success state with a footnote"). */
export interface RetractionEntry {
  retraction_id: string;
  retracted_at: string | null;
  /** Who made THIS retract call — `null` for a `system`/`webhook` actor.
   *  No name resolution on the wire (same posture as `graph_id`: the caller
   *  resolves identity itself); use `is_own` below rather than comparing
   *  this to a locally-held user id, since the caller may not have one
   *  handy at every render site. */
  actor_user_id: string | null;
  /** `true` iff the CALLER is this entry's own actor. D8 lets an org admin
   *  read a shared-ocean entry that ISN'T theirs (`retraction-history`'s
   *  admin visibility) — this is what lets the UI say "you" only when
   *  that's actually true, rather than presenting every visible entry as
   *  the viewer's own act. */
  is_own: boolean;
  /** `true` when this entry's `rows` were capped
   *  (`MAX_ROWS_PER_RETRACTION_GROUP` on the backend) — a bulk
   *  `delete_subject`/`delete_source` call can produce far more (ocean,
   *  surface) rows than any UI list should render at once. When set, `rows`
   *  is a PREFIX of the real set, not the whole thing. */
  truncated: boolean;
  rows: RetractionRow[];
}

export interface RetractionsPage {
  entries: RetractionEntry[];
  page: number;
  page_size: number;
  /** The filter the backend actually applied, echoed on EVERY response
   *  including an empty one — "nothing matched the filter" and "nothing at
   *  all" are different facts and must never render the same. */
  unreached_only: boolean;
}

// ── API surface ─────────────────────────────────────────────────────────────
/** `GET /v1/jobs/{id}` — a generic background job's status/progress
 *  (`backend/src/routes/jobs.rs`). Deliberately kind-agnostic: this is the
 *  one polling shape shared by every job kind (`PromoteNode` today, more
 *  later), NOT the meeting-synthesis-specific {@link MeetingSummaryStatus}
 *  above, which is its own dedicated state machine with a richer body. */
export type JobRunState = "pending" | "running" | "done" | "failed" | "dead";

export interface PublicationProcessing {
  state: "processing" | "ready" | "attention";
  stages: { stage: "copying" | "building" | "checking"; total: number; completed: number; failed: number; skipped: number }[];
}
export interface SourceConnection {
  id: string; source_graph_id: string; target_graph_id: string; label: string;
  mode: "once" | "live"; paused: boolean; source_type: string; source_id: string;
  current_job_id: string | null; last_checked_at: string | null; error_message: string | null;
  has_pending_evidence: boolean; processing: PublicationProcessing | null;
}

export interface JobStatus {
  processing?: PublicationProcessing;
  id: string;
  kind: string;
  status: JobRunState;
  /** `null` means this job kind reports no granular progress signal — an
   *  indeterminate/spinner state, never "0 of unknown". */
  progress_current: number | null;
  progress_total: number | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

/** "enabled": CoralSwarm turned on Teams "Record and transcribe automatically".
 *  "manual": someone in the meeting must start transcription in Teams. */
export type AutoTranscription = "enabled" | "manual";

/** Outlook compose capture: the intent behind the nonce an unsaved draft carries. */
export type OutlookIntentStatus = {
  status: "pending_save" | "active" | "stopped" | "expired" | "error";
  error_code: string | null;
  /** When a pending intent stops waiting. Not returned today (the create response has it); read if the API adds it. */
  expires_at?: string | null;
  recurring: boolean;
  next_occurrence_start: string | null;
  auto_transcription: AutoTranscription | null;
  manual_reason: "recurring" | "not_organizer" | "missing_scope" | "policy" | "already_started" | "unknown" | null;
};

export const api = {
  composio: {
    toolkits: (t: string) =>
      req<{ toolkits: import("../components/connectors/composioModel").ComposioToolkitSummary[] }>(`/v1/connectors/composio`, t),
    status: (t: string, service: import("../components/connectors/composioModel").ComposioToolkit) =>
      req<import("../components/connectors/composioModel").ComposioStatus>(`/v1/connectors/composio/${service}`, t),
    connect: (t: string, service: import("../components/connectors/composioModel").ComposioToolkit) =>
      req<{ connection_id: string; redirect_url: string }>(`/v1/connectors/composio/${service}/connect`, t, { method: "POST", body: "{}" }),
    verify: (t: string, service: import("../components/connectors/composioModel").ComposioToolkit, id: string) =>
      req<{ connection_id: string; status: string; provider_namespace: string; self_check: string; message: string | null; capture_id: string; graph_id: string }>(`/v1/connectors/composio/${service}/connections/${encodeURIComponent(id)}/verify`, t, { method: "POST", body: "{}" }),
    disconnect: (t: string, service: import("../components/connectors/composioModel").ComposioToolkit, id: string) =>
      req<{ connection_id: string; status: string; revoked: boolean }>(`/v1/connectors/composio/${service}/connections/${encodeURIComponent(id)}/disconnect`, t, { method: "POST", body: "{}" }),
  },
  outlookCaptureSchedule: {
    /** Reopened draft that carries only the nonce (no intent id): same body as `status`, plus the id. */
    byNonce: (t: string, nonce: string, signal?: AbortSignal) =>
      req<OutlookIntentStatus & { intent_id: string }>(`/v1/outlook-capture-schedule/by-nonce/${encodeURIComponent(nonce)}`, t, { signal }, true),
    create: (t: string) =>
      req<{ intent_id: string; nonce: string; expires_at: string }>("/v1/outlook-capture-schedule/intents", t, { method: "POST", body: "{}" }, true),
    status: (t: string, intentId: string, signal?: AbortSignal) =>
      req<OutlookIntentStatus>(`/v1/outlook-capture-schedule/intents/${encodeURIComponent(intentId)}`, t, { signal }, true),
    cancel: (t: string, intentId: string) =>
      req<void>(`/v1/outlook-capture-schedule/intents/${encodeURIComponent(intentId)}`, t, { method: "DELETE" }, true),
  },
  sourceConnections: {
    create: (t: string, graphId: string, args: { source_id: string; target_graph_id: string; keep_connected: boolean; channel: boolean; include_history: boolean }) =>
      req<{ connection_id: string; job_id: string | null; mode: "once" | "live" }>(`/v1/graphs/${graphId}/source-connections`, t, { method: "POST", body: JSON.stringify(args) }),
    list: (t: string, graphId: string, before?: string) => req<{ connections: SourceConnection[]; next_cursor: string | null }>(`/v1/graphs/${graphId}/source-connections${before ? `?before=${encodeURIComponent(before)}` : ""}`, t),
    update: (t: string, graphId: string, id: string, action: "pause" | "resume" | "retry") =>
      req<{ id: string; paused: boolean }>(`/v1/graphs/${graphId}/source-connections/${id}`, t, { method: "PATCH", body: JSON.stringify({ action }) }),
  },
  auth: {
    sendCode: (email: string) =>
      req<void>("/auth/email", null, {
        method: "POST",
        body: JSON.stringify({ email, device_id: getDeviceId() }),
      }),
    verify: (email: string, code: string) =>
      req<{ token: string; user_id: string; account_id: string }>("/auth/verify", null, {
        method: "POST",
        body: JSON.stringify({ email, code, device_id: getDeviceId() }),
      }),
    logout: (token: string) => req<void>("/auth/logout", token, { method: "POST" }),
  },
  // `routes::email_prefill` (CodeRabbit round 1, #1005): resolves the
  // unmatched-recipient nudge email's opaque `?pt=` signup token to the
  // address it was minted for. Called from `app/signup/page.tsx` BEFORE the
  // signup form renders, so the address itself never appears in a URL —
  // unauthenticated, same as `auth.sendCode`/`auth.verify` above, because
  // the caller has no CoralSwarm account yet either.
  emailPrefill: (token: string) =>
    req<{ email: string | null }>(`/v1/email/prefill?t=${encodeURIComponent(token)}`, null),
  // Resolved caller identity (active account id, role) — under Clerk the
  // frontend only holds the org id/slug, not the internal account UUID.
  /** `org_teams_enabled` is a SERVER CAPABILITY, not a preference: the backend's
   *  `ORG_TEAMS_ENABLED` env flag decides whether creating a shared workspace
   *  under an org will succeed, and the client has no other way to know. Three
   *  states, and the distinction matters — `null` means "not in an org, so the
   *  flag is irrelevant here", `false` means "in an org and the flag is off, so
   *  creation WILL 403", `true` means it will work. Gate the create affordance
   *  on this rather than on the mere presence of an org. */
  /** `active_graph_id` is the caller's last-selected ocean IN THIS ACCOUNT
   *  (claim-based tenancy decision 8, migration `0201_user_active_graph`) —
   *  the server-side replacement for the per-browser `cs_graph_id`
   *  localStorage key. `null` means "never reported one here", which is the
   *  normal state for every user's first boot after that migration and for any
   *  build with no control pool; it is NOT an error.
   *
   *  ADVISORY, NOT A GRANT. There is no foreign key behind it (`graphs` is
   *  content-plane and, for an org, lives in that org's own tenant database),
   *  so it may name an ocean that has since been deleted or whose access was
   *  revoked. Never open it directly — pass it through
   *  `lib/activeOcean.pickActiveOcean`, which intersects it with the reachable
   *  list `graphs.list` returns. That intersection is the gate. */
  /** `is_personal_org` (control migration 0208): true when the account bound
   *  to this request is a PERSONAL ORG — a Clerk organization that stands for
   *  one person's own ocean, not a real organization. Clerk's own `orgId`
   *  cannot tell the two apart (a personal org is an ordinary Clerk org in
   *  every way Clerk cares about), so this is the only signal the client has.
   *  `false` on any lookup failure, matching the server's fail-safe direction
   *  (see `routes::me::get_me`'s doc comment) — that degrades to today's
   *  behavior (treat as an ordinary org), never to a new failure mode. See
   *  `lib/providers.tsx`'s `isPersonalOrg` for the one boot-time consumer. */
  me: (t: string) => req<MeResponse>("/v1/me", t),
  /** `GET /v1/bootstrap` — the app shell's boot read in one request (see
   *  `backend/src/routes/bootstrap.rs`). Each part is byte-for-byte the body
   *  of its own endpoint: `me` is `GET /v1/me`, `graphs` is `GET /v1/graphs`,
   *  and `join_requests_count` is `GET /v1/orgs/join-requests/count` or `null`
   *  where that endpoint does not answer 200 (non-admin, personal account).
   *  A backend older than this route answers 404; `lib/bootData.ts` falls
   *  back to the separate reads then. */
  bootstrap: (t: string) => req<BootstrapResponse>("/v1/bootstrap", t),
  embed: (t: string, texts: string[]) =>
    req<{ embeddings: number[][]; model: string }>("/v1/embed", t, {
      method: "POST",
      body: JSON.stringify({ texts }),
    }),
  // Boot-time IANA timezone report (Today-journal tier feature, migration
  // `0118_users_timezone`) — `Intl.DateTimeFormat().resolvedOptions().
  // timeZone` on the browser side; the server validates it (`chrono_tz`) and
  // only writes when it actually changed, so this is safe to call on every
  // boot. See `web/lib/providers.tsx`'s boot sequence for the one call site.
  reportTimezone: (t: string, timezone: string) =>
    req<{ accepted: boolean }>("/v1/me/timezone", t, {
      method: "PUT",
      body: JSON.stringify({ timezone }),
    }),
  // The write half of `me().active_graph_id` (migration
  // `0201_user_active_graph`). Records the ocean the user is standing in, per
  // (user, account), so the selection survives a second browser/device and an
  // org switch.
  //
  // Same posture as `reportTimezone` above and for the same reason — it is
  // called from the boot sequence: the server validates the id's SHAPE, ignores
  // a malformed one with `200 {"accepted": false}` rather than erroring, and
  // no-ops an unchanged value, so this is safe to call on every boot. It
  // deliberately does NOT check that the ocean is reachable (that check would
  // expire the moment access changed); reachability is decided client-side by
  // `lib/activeOcean.pickActiveOcean` on every boot instead.
  reportActiveOcean: (t: string, graphId: string) =>
    req<{ accepted: boolean }>("/v1/me/active-graph", t, {
      method: "PUT",
      body: JSON.stringify({ graph_id: graphId }),
    }),
  // The caller's passphrase-wrapped private-ocean key blob (server-blind),
  // for cross-device enrollment (ADR-0007 D5). get() 404s when not enrolled.
  privateKey: {
    get: (t: string) => req<{ blob: string }>("/v1/me/private-key", t),
    put: (t: string, blob: string) =>
      req<{ ok: boolean }>("/v1/me/private-key", t, {
        method: "PUT",
        body: JSON.stringify({ blob }),
      }),
    del: (t: string) => req<{ ok: boolean }>("/v1/me/private-key", t, { method: "DELETE" }),
  },
  // Delete account (Settings → Account danger zone). Backend: PR #1476.
  // POST errors (`ApiError.code`): 400 `confirmation_mismatch`; 403
  // `session_required` | `reverification_required`; 409 `team_org_membership`
  // (+ body.organizations[]) | `shared_personal_account` (+ body.members[]).
  // Cancel errors: 409 `too_late`, 404 `no_pending_deletion`.
  meDeleteAccount: {
    get: (t: string) => req<DeleteAccountEligibility>("/v1/me/delete-account", t),
    post: (t: string, confirmEmail: string) =>
      req<DeleteAccountStarted>("/v1/me/delete-account", t, {
        method: "POST",
        body: JSON.stringify({ confirm_email: confirmEmail }),
      }),
    /** Signed-in cancel during the grace period. */
    cancel: (t: string) => req<DeleteAccountCancelled>("/v1/me/delete-account/cancel", t, { method: "POST" }),
  },
  // In-app org deletion (control 0387). These name the org in the path and
  // work with the org's own token during grace (the only routes that do) or
  // with a personal-session token. Owner only, except the status read.
  orgDeletion: {
    get: (t: string, orgId: string) =>
      req<OrgDeletionStatus>(`/v1/orgs/${encodeURIComponent(orgId)}/deletion`, t),
    request: (t: string, orgId: string, confirmName: string) =>
      req<{ deletion: OrgDeletion }>(`/v1/orgs/${encodeURIComponent(orgId)}/deletion`, t, {
        method: "POST",
        body: JSON.stringify({ confirm_name: confirmName }),
      }),
    cancel: (t: string, orgId: string) =>
      req<{ status: "canceled" }>(`/v1/orgs/${encodeURIComponent(orgId)}/deletion/cancel`, t, { method: "POST" }),
    /** The owner's export during grace, as a file. */
    exportFile: async (t: string, orgId: string): Promise<Blob> => {
      const headers: Record<string, string> = { Authorization: `Bearer ${t}`, [CLIENT_HEADER]: clientName() };
      const res = await guardedFetch(`/v1/orgs/${encodeURIComponent(orgId)}/deletion/export`, { headers });
      if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        let code: string | null = null;
        try {
          const body = await res.json();
          msg = body.error ?? msg;
          code = typeof body.code === "string" ? body.code : null;
        } catch {
          /* ignore */
        }
        throw new ApiError(msg, res.status, code);
      }
      return res.blob();
    },
  },
  // Unauthenticated cancel from the emailed link; the token is single-use.
  // Adds 400 `invalid_token` to the cancel errors above.
  accountDeletion: {
    cancelWithToken: (token: string) =>
      req<DeleteAccountCancelled>("/v1/account-deletion/cancel", null, {
        method: "POST",
        body: JSON.stringify({ token }),
      }),
  },
  // Per-account preferences (D11). Opaque JSON object; the settings screen owns
  // the shape (e.g. { llm_mode, connectors: {...} }).
  // Usage-metering PR12: real credits/spend, for the active tenancy context
  // (org or personal — tenancy travels in the Clerk token's `org_id` claim,
  // never in the host and never in a header of our own).
  usage: {
    summary: (t: string) => req<UsageSummary>("/v1/usage/summary", t),
  },

  // Org-escrow recovery (D6). All blobs are base64. Shamir shares of the recovery
  // private key are admin-held out-of-band and NEVER sent here — only the public
  // key, per-user wrapped blobs, and the audit log touch the server.
  escrow: {
    // Recovery public key + ceremony metadata. `get` 404s when not set up.
    getRecoveryPubkey: (t: string) =>
      req<{ pubkey: string; threshold: number; admin_count: number }>(
        "/v1/escrow/recovery-pubkey",
        t,
      ),
    putRecoveryPubkey: (t: string, pubkey: string, threshold: number, adminCount: number) =>
      req<{ ok: boolean }>("/v1/escrow/recovery-pubkey", t, {
        method: "PUT",
        body: JSON.stringify({ pubkey, threshold, admin_count: adminCount }),
      }),
    // A member stores their own ocean key wrapped to the recovery public key.
    putWrappedKey: (t: string, wrappedKey: string) =>
      req<{ ok: boolean }>("/v1/escrow/wrapped-key", t, {
        method: "PUT",
        body: JSON.stringify({ wrapped_key: wrappedKey }),
      }),
    // Admin fetches a member's wrapped key during recovery. 404s if absent.
    getWrappedKey: (t: string, userId: string) =>
      req<{ wrapped_key: string }>(`/v1/escrow/wrapped-key?user_id=${userId}`, t),
    // Admin re-delivers a recovered key (re-wrapped under a recovery passphrase)
    // to a member's device-trust slot so they can re-enroll.
    putRecoveredKey: (t: string, userId: string, blob: string) =>
      req<{ ok: boolean }>("/v1/escrow/recovered-key", t, {
        method: "PUT",
        body: JSON.stringify({ user_id: userId, blob }),
      }),
    // Audit trail of recovery events (admin only).
    listRecoveryLog: (t: string) =>
      req<{ events: EscrowRecoveryEvent[] }>("/v1/escrow/recovery-log", t),
    logRecovery: (t: string, recoveredUserId: string) =>
      req<{ ok: boolean }>("/v1/escrow/recovery-log", t, {
        method: "POST",
        body: JSON.stringify({ recovered_user_id: recoveredUserId }),
      }),
  },
  graphs: {
    list: (t: string) => req<Graph[]>("/v1/graphs", t),
    // kind defaults to "personal"; pass "org" to create the account's single
    // organization ocean, or "member" for a secondary member-private ocean
    // under an org account (the backend refuses "personal" there — see
    // lib/askScope.ts / components/Shell.tsx's newOcean()). Team oceans are
    // created through the teams API.
    create: (t: string, name: string, kind: OceanKind = "personal") =>
      req<Graph>("/v1/graphs", t, {
        method: "POST",
        body: JSON.stringify({ name, kind }),
      }),
    // Un-paginated: the newest 100 atoms only. Kept for existing callers
    // (Atlas's own recency sample, etc.) — the Reefs workbench's Raw material
    // view uses `atomsAll` below instead, which needs the full corpus.
    atoms: (t: string, graphId: string) =>
      req<Coral[]>(`/v1/graphs/${graphId}/atoms`, t),
    // Paginated (CS-reefs-raw-material-all-time): same response shape as
    // `atoms` (bare array), `hasMore` read off `X-Has-More` — mirrors
    // `reefs.list`/`nodes.rs::list_nodes`'s convention. `pageSize` defaults
    // to 200, the backend's max.
    atomsPage: (t: string, graphId: string, page = 1, pageSize = 200) =>
      reqPaged<Coral[]>(`/v1/graphs/${graphId}/atoms?page=${page}&page_size=${pageSize}`, t),
    // Fetches every atom in the ocean by paging through `atomsPage` until
    // `hasMore` is false (mirrors `reefs.listAll`). Raw material → Corals
    // mode needs the FULL corpus so search/type-facet filtering isn't
    // silently limited to the newest slice (a single-type-recent-window
    // ocean looked like "filtering doesn't work" before this existed).
    // `onPage` lets the caller render already-fetched atoms immediately
    // instead of waiting for the whole corpus.
    atomsAll: async (t: string, graphId: string, onPage?: (soFar: Coral[]) => void): Promise<Coral[]> => {
      const all: Coral[] = [];
      let page = 1;
      // Raised 200 → 1,000 alongside the backend's ceiling (W3, §6.3). Atom
      // rows here are metadata only — content lives in R2 behind
      // `content_blob_key` — so a 1,000-row page stays small.
      const pageSize = 1000;
      for (;;) {
        const { data, hasMore } = await api.graphs.atomsPage(t, graphId, page, pageSize);
        all.push(...data);
        onPage?.(all);
        // Sanity backstop mirroring the backend's own SOURCES_ATOM_CAP —
        // not an expected ceiling for the "a few thousand atoms" oceans
        // this targets, just a guard against an unbounded loop.
        if (!hasMore || all.length >= 20000) break;
        page += 1;
      }
      return all;
    },
    // ── W3: the paginated edge contract ────────────────────────────────────
    // docs/plans/request-reliability-and-scale.md §6.3.
    //
    // perf/library-source-groups removed this drain's one production caller
    // (Library · Sources — see `sourceGroups`/`sourceGroup` below, whose
    // detail route returns `reefs` pre-joined instead). Kept anyway:
    // `lib/requestBudget.test.ts` pins the `/node-atoms` drain's OWN budget
    // contract directly (not through any page — see that file's header on
    // why it measures loaders, not screens), so removing this out from under
    // it would silently delete a live regression test's subject rather than
    // its usage. If a future PR removes those tests too, this can go with
    // them — see `nodeAtomsAll`'s own comment.
    nodeAtomsPage: (t: string, graphId: string, page = 1, pageSize = 1000, signal?: AbortSignal) =>
      reqPaged<GraphEdge[]>(
        `/v1/graphs/${graphId}/node-atoms?page=${page}&page_size=${pageSize}`,
        t,
        false,
        { signal },
      ),
    // Exact, server-computed facet counts. These exist because the drain
    // below is BUDGETED: above the budget the client holds only a slice of
    // the edges, so facets derived from what it drained would be silently
    // wrong rather than merely incomplete. Same "no production caller, kept
    // for the drain it backstops" status as `nodeAtomsAll` below.
    nodeAtomFacets: (t: string, graphId: string, signal?: AbortSignal) =>
      req<GraphEdgeFacets>(`/v1/graphs/${graphId}/node-atoms?facets=true`, t, undefined, false, { signal }),
    /**
     * Drain edges up to a budget.
     *
     * `page_size` 1,000 × 20 pages = 20,000 edges. Below the budget the
     * workbench behaves exactly as before — everything in memory, instant
     * client-side sort/filter/facets. At the budget the drain STOPS and
     * `complete` comes back false, which is the caller's signal that any
     * facet it derives locally is partial and must say so.
     *
     * One code path at every ocean size: the only thing that varies is how
     * many pages were drained (plan §4.5). There is deliberately no second
     * "large ocean" mode to diverge from this one.
     */
    nodeAtomsAll: async (
      t: string,
      graphId: string,
      onPage?: (soFar: GraphEdge[]) => void,
      maxPages = 20,
      pageSize = 1000,
      signal?: AbortSignal,
    ): Promise<{ edges: GraphEdge[]; complete: boolean }> => {
      const all: GraphEdge[] = [];
      for (let page = 1; page <= maxPages; page++) {
        // The drain is a LOOP of independent requests, so an AbortController
        // held by the caller aborts at most the one in flight — the next
        // iteration would happily start another. Check the signal between
        // pages, and thread it into each request so the in-flight one is
        // cancelled too.
        if (signal?.aborted) return { edges: all, complete: false };
        const { data, hasMore } = await api.graphs.nodeAtomsPage(
          t, graphId, page, pageSize, signal,
        );
        all.push(...data);
        onPage?.(all);
        if (!hasMore) return { edges: all, complete: true };
      }
      // Budget bound: there is more, and we stopped on purpose.
      return { edges: all, complete: false };
    },
    // ── source-groups (perf/library-source-groups) ─────────────────────────
    // Replaces the old `sources` (`GET /v1/graphs/{id}/sources`) client call
    // for the Library · Sources screen: one server-aggregated rollup per
    // channel instead of the client folding every raw source instance
    // itself, plus (`sourceGroup`) a per-channel detail fetch that carries
    // its own `reefs`/`source_ids` — no more separate `reefs.listAll` +
    // `nodeAtomsAll` drain just to title the "settled into" chips. See
    // `backend/src/routes/graphs.rs`'s `list_source_groups`/
    // `get_source_group`.
    sourceGroups: (t: string, graphId: string, signal?: AbortSignal) =>
      req<{ groups: SourceGroupRow[] }>(`/v1/graphs/${graphId}/source-groups`, t, undefined, false, { signal }),
    sourceGroup: (
      t: string,
      graphId: string,
      channelKey: string,
      params: { page?: number; pageSize?: number } = {},
      signal?: AbortSignal,
    ) => {
      const qs = new URLSearchParams();
      if (params.page) qs.set("page", String(params.page));
      if (params.pageSize) qs.set("page_size", String(params.pageSize));
      const q = qs.toString();
      return req<SourceGroupDetail>(
        `/v1/graphs/${graphId}/source-groups/${encodeURIComponent(channelKey)}${q ? `?${q}` : ""}`,
        t,
        undefined,
        false,
        { signal },
      );
    },
    // True reef/coral/source totals for the Reefs page tab bar
    // (CS-reef-nav-counts) — see `OceanStats`.
    stats: (t: string, graphId: string, signal?: AbortSignal) =>
      req<OceanStats>(`/v1/graphs/${graphId}/stats`, t, undefined, false, { signal }),
    // The per-ocean knowledge graph (nodes + links) for the Graph canvas.
    atlas: (
      t: string,
      graphId: string,
      opts?: { cooccur?: number; anchors?: "all" | "top2" },
    ) => {
      const qs = new URLSearchParams();
      if (opts?.cooccur != null) qs.set("cooccur", String(opts.cooccur));
      if (opts?.anchors === "all") qs.set("anchors", "all");
      const q = qs.toString();
      return req<AtlasGraph>(`/v1/graphs/${graphId}/atlas${q ? `?${q}` : ""}`, t);
    },
  },
  library: {
    list: (
      t: string,
      params: {
        graphId?: string;
        entityType?: EntityKind;
        q?: string;
        page?: number;
        pageSize?: number;
      } = {},
      signal?: AbortSignal,
    ) => {
      const qs = new URLSearchParams();
      if (params.graphId) qs.set("graph_id", params.graphId);
      if (params.entityType) qs.set("entity_type", params.entityType);
      if (params.q) qs.set("q", params.q);
      if (params.page) qs.set("page", String(params.page));
      if (params.pageSize) qs.set("page_size", String(params.pageSize));
      const q = qs.toString();
      return req<LibraryPage>(`/v1/library${q ? `?${q}` : ""}`, t, undefined, false, { signal });
    },
  },
  reefs: {
    // Single-reef drill-through (fix/get-node-by-id, `GET /v1/nodes/{id}`),
    // authorized by reachability rather than a caller-supplied graph_id —
    // the response carries `graph_id`, the reef's OWN ocean, so a page
    // addressed only by node id can resolve which ocean to query without
    // ever trusting ambient session state (see `ReefDetail` above; mirrors
    // `api.corals.get`/`CoralDetail` below). 404 body is
    // `{"error": "reef not found or access denied"}` when the reef doesn't
    // exist or this caller can't reach it.
    get: (t: string, nodeId: string) => req<ReefDetail>(`/v1/nodes/${nodeId}`, t),
    // Paginated (CS-reef-pagination): `page` defaults to 1, `pageSize` to 20
    // (matches the backend's own default). The response body stays a bare
    // array — `hasMore` is read off the `X-Has-More` response header rather
    // than a wrapped object, so older API consumers parsing a plain array
    // aren't broken (see `backend/src/routes/nodes.rs::list_nodes`).
    list: (t: string, graphId: string, type?: ReefType, page = 1, pageSize = 20, signal?: AbortSignal) =>
      reqPaged<Reef[]>(
        `/v1/nodes?graph_id=${graphId}${type ? `&node_type=${type}` : ""}&page=${page}&page_size=${pageSize}`,
        t,
        false,
        { signal },
      ),
    // Fetches every reef in the ocean by paging through `list` until
    // `hasMore` is false — for callers that need the *complete* set rather
    // than one page to browse (the merge/supersession pickers and the
    // reef-detail page's own lookup-by-id on `reef/page.tsx`).
    listAll: async (t: string, graphId: string, type?: ReefType, signal?: AbortSignal): Promise<Reef[]> => {
      const all: Reef[] = [];
      let page = 1;
      // 1,000 is the backend's page_size ceiling, raised from 100 by W3
      // (`nodes.rs`'s `page_size.clamp(1, 1000)`,
      // docs/plans/request-reliability-and-scale.md §6.3). Raising the server
      // ceiling alone changes nothing — the client has to ASK for the bigger
      // page — which is why the two move together. 3,440 reefs: 35 round
      // trips → 4.
      const pageSize = 1000;
      for (;;) {
        // Same shape as `graphs.nodeAtomsAll`'s drain loop: a loop of
        // independent requests, so a caller's `AbortController` only cancels
        // the ONE in flight unless every iteration also checks it — see that
        // function's own comment (this is what W4 threads a react-query
        // `signal` into, for the Library Sources list).
        //
        // UNLIKE `nodeAtomsAll`, this is a COMPLETE-SET loader with no
        // `{complete: boolean}` in its return type — every existing caller
        // (`TopicSupersession`/`TopicMerge`'s candidate list, the Library
        // Sources query, `reef/page.tsx`'s lookup-by-id) treats a resolved
        // `Reef[]` as the WHOLE roster. Silently returning `all` here handed
        // those callers a partial roster with nothing distinguishing it from
        // a genuinely small ocean — a merge picker could omit real
        // candidates, `reef/page.tsx` could report "not found" for a reef
        // that exists past the aborted page. Throw instead, matching what
        // `guardedFetch`/`dispatch` already throws for an abort mid-fetch
        // (`TransportError("aborted", …)`, `lib/transport.ts`) one line below
        // — so an abort here and an abort there are the same shape to every
        // caller, and (for the react-query caller) a rejection rather than a
        // stale success is what lets the query be treated as cancelled
        // rather than resolved with wrong data.
        if (signal?.aborted) throw new TransportError("aborted", "/v1/nodes", "reefs.listAll aborted mid-drain");
        const { data, hasMore } = await api.reefs.list(t, graphId, type, page, pageSize, signal);
        all.push(...data);
        if (!hasMore) break;
        page += 1;
      }
      return all;
    },
    // `signal` (W1) lets a caller genuinely CANCEL, not merely ignore the
    // result. The Reefs workbench fans this out once per reef, so an unmount
    // mid-fan-out has to give the transport its capacity back — otherwise
    // every abandoned page leaves orphaned requests queued ahead of whatever
    // the user navigated to.
    atoms: (t: string, nodeId: string, signal?: AbortSignal) =>
      req<ReefAtom[]>(`/v1/nodes/${nodeId}/atoms`, t, undefined, false, { signal }),
    // Rename / re-type a reef (curation). PATCH /v1/nodes/{id}; omitted fields
    // are left unchanged. Returns the updated reef.
    update: (t: string, nodeId: string, patch: { title?: string; type?: ReefType; entity_type?: EntityKind }) =>
      req<Reef>(`/v1/nodes/${nodeId}`, t, {
        method: "PATCH",
        body: JSON.stringify({
          ...(patch.title !== undefined ? { title: patch.title } : {}),
          ...(patch.type !== undefined ? { type: patch.type } : {}),
          ...(patch.entity_type !== undefined ? { entity_type: patch.entity_type } : {}),
        }),
      }),
    // Suggested corals to add to a reef (curation): vector-similar atoms not yet
    // linked. 400s when the reef has no atoms to seed retrieval from.
    candidates: (t: string, nodeId: string, limit = 10) =>
      req<CandidateCoral[]>(`/v1/nodes/${nodeId}/candidates?limit=${limit}`, t),
    // LLM digest: high-level summary of the reef + per-coral relevant excerpts.
    digest: (t: string, nodeId: string, title: string) =>
      req<ReefDigest>(`/v1/nodes/${nodeId}/digest?title=${encodeURIComponent(title)}`, t),
    // Entity resolution: merge `dupId` into `keepId` (dup's corals re-pointed,
    // dup deleted). For combining duplicate reefs of the same thing.
    merge: (t: string, keepId: string, dupId: string) =>
      req<{ ok: boolean; corals_moved: number }>(`/v1/nodes/${keepId}/merge`, t, {
        method: "POST",
        body: JSON.stringify({ dup_id: dupId }),
      }),
    // Auto-dedup sweep: merge duplicate entity reefs across an ocean (name
    // variants from transcription). Returns how many groups/reefs were merged.
    dedup: (t: string, graphId: string) =>
      req<{ merged_groups: number; reefs_removed: number }>(
        `/v1/graphs/${graphId}/dedup-entities`,
        t,
        { method: "POST" },
      ),
    // (Standing merge-suggestions listing removed 2026-08-05 — duplicates
    // surface via in-chat cards + the workbench; dismiss/merge actions stay.)
    // "Keep separate": never suggest merging this pair again.
    dismissMerge: (t: string, graphId: string, a: string, b: string) =>
      req<{ ok: boolean }>(`/v1/graphs/${graphId}/merge-suggestions/dismiss`, t, {
        method: "POST",
        body: JSON.stringify({ a, b }),
      }),
    // Link an existing coral to a reef (acts on a candidate).
    linkAtom: (t: string, nodeId: string, atomId: string) =>
      req<{ linked: boolean }>(`/v1/nodes/${nodeId}/atoms`, t, {
        method: "POST",
        body: JSON.stringify({ atom_id: atomId }),
      }),
    // Supersession edges (curation): which reef this one replaces / is replaced by.
    supersessions: (t: string, nodeId: string) =>
      req<{ as_superseder: SupersessionEdge[]; as_superseded: SupersessionEdge[] }>(
        `/v1/nodes/${nodeId}/supersessions`,
        t,
      ),
    // Mark `oldNodeId` as superseded BY `supersededBy` (the newer/correct reef).
    supersede: (t: string, oldNodeId: string, supersededBy: string) =>
      req<{ superseder_id: string; superseded_id: string }>(
        `/v1/nodes/${oldNodeId}/supersede`,
        t,
        { method: "POST", body: JSON.stringify({ superseded_by: supersededBy }) },
      ),
    // Remove the supersession edge between two reefs (either direction).
    unsupersede: (t: string, nodeId: string, otherId: string) =>
      req<void>(`/v1/nodes/${nodeId}/supersede/${otherId}`, t, { method: "DELETE" }),
    // Promote every reef from one source instance into a shared ocean (D8).
    // withCorals=true also copies the source's corals into the target ocean
    // (drill-through/retrieval/citations work there); false = reef-only.
    promoteSource: (t: string, graphId: string, sourceId: string, targetGraphId: string, withCorals = true) =>
      req<{ connection_id: string; job_id: string | null; mode: "once" | "live" }>(
        `/v1/graphs/${graphId}/promote-source`,
        t,
        { method: "POST", body: JSON.stringify({ source_id: sourceId, target_graph_id: targetGraphId, with_corals: withCorals }) },
      ),
    // Promote a whole coding session — its container row, every coral captured
    // under it, and the reefs those corals grew — into a shared ocean. Not
    // expressible as promoteSource: a session spans one source_id per
    // checkpoint, and promote-source cannot carry the coding_sessions row, so
    // the session would arrive as corals its Sessions page can't see. Corals
    // always travel (an empty promoted session isn't a useful state), hence no
    // withCorals knob. `session_copied` is false on a cross-plane (org-tenant)
    // promote, where the container row can't cross yet.
    promoteSession: (t: string, graphId: string, sessionId: string, targetGraphId: string) =>
      req<{
        session_id: string;
        target_graph_id: string;
        session_copied: boolean;
        corals_copied: number;
        corals_linked: number;
        /** False when only reef-attached corals travelled (cross-plane), so
         *  checkpoints that never grew a reef stayed behind. */
        corals_complete: boolean;
        reefs_found: number;
        promoted_count: number;
      }>(
        `/v1/graphs/${graphId}/promote-session`,
        t,
        { method: "POST", body: JSON.stringify({ session_id: sessionId, target_graph_id: targetGraphId }) },
      ),
    // Publish a reef into a wider shared ocean (ADR-0006). One-way, copy-on-promote.
    // `coralIds` selects a subset of the reef's corals to copy along with it —
    // omit it (or pass undefined) to copy every coral (backward compatible,
    // also what a widening re-promote wants); pass `[]` for a reef-only
    // promote with no corals.
    //
    // Async-promote contract (feat/promote-async-batch): the reef itself is
    // published synchronously by THIS call, but copying/linking its corals
    // now runs as a background job — poll `api.jobs.get` on the returned
    // `job_id` for progress (`corals_copied` is GONE from this response; it
    // implied synchronous completion, which is no longer true). `job_id` is
    // `null` only when `corals_requested` is 0 — nothing for a job to do, so
    // there is nothing to poll and the promote is already complete.
    promote: (t: string, nodeId: string, targetGraphId: string, coralIds?: string[]) =>
      req<PromoteResponse>(`/v1/nodes/${nodeId}/promote`, t, {
        method: "POST",
        body: JSON.stringify({
          target_graph_id: targetGraphId,
          ...(coralIds !== undefined ? { coral_ids: coralIds } : {}),
        }),
      }),
  },
  corals: {
    get: (t: string, atomId: string) =>
      req<CoralDetail>(`/v1/atoms/${atomId}`, t),
    ingest: (
      t: string,
      args: {
        graphId: string;
        content: string;
        sourceType?: string;
        sourceId?: string;
        participants?: string[];
        timestamp?: string;
        scope?: string;
        /** Verbatim, pre-correction/pre-relabel transcript (transcript sources).
         *  Preserved so the meeting summary can show RAW ⟷ "what we think it is". */
        rawContent?: string;
        /** Recording length in ms (transcript sources). */
        durationMs?: number;
        /** Speaker-attributed, timestamped transcript turns (transcript sources).
         *  Lets the cloud anchor decision/action-item timestamps to segments.
         *  Best-effort — omitted when the local record has no segments. */
        segments?: MeetingSegment[];
      },
    ) =>
      req<{ atom_ids: string[]; jobs_enqueued: number; chunks_created: number }>(
        "/v1/ingest/atom",
        t,
        {
          method: "POST",
          body: JSON.stringify({
            graph_id: args.graphId,
            source_type: args.sourceType ?? "note",
            source_id: args.sourceId ?? `web-${Date.now()}`,
            content: args.content,
            participants: args.participants ?? [],
            timestamp: args.timestamp ?? new Date().toISOString(),
            scope: args.scope ?? "private",
            ...(args.rawContent !== undefined ? { raw_content: args.rawContent } : {}),
            ...(args.durationMs !== undefined ? { duration_ms: args.durationMs } : {}),
            ...(args.segments !== undefined ? { segments: args.segments } : {}),
          }),
        },
      ),
  },
  transcript: {
    // Conservative name/typo cleanup of a meeting transcript before the naming
    // panel + ingest. The backend fixes names + clear typos using the ocean's
    // known names and preserves everything else (incl. "You:/Speaker N:" line
    // prefixes), so the downstream relabel logic keeps working unchanged.
    cleanup: (token: string, args: { graphId: string; transcript: string }) =>
      req<{ transcript: string }>("/v1/transcript/cleanup", token, {
        method: "POST",
        body: JSON.stringify({ graph_id: args.graphId, transcript: args.transcript }),
      }),
  },
  sources: {
    items: (t: string, args: { graphId: string; before?: string; sourceType?: string; atomId?: string }, signal?: AbortSignal) => {
      const params = new URLSearchParams({ graph_id: args.graphId });
      if (args.before) params.set("before", args.before);
      if (args.sourceType) params.set("source_type", args.sourceType);
      if (args.atomId) params.set("atom_id", args.atomId);
      return req<SourceItemsPage>(`/v1/sources/items?${params}`, t, undefined, false, { signal });
    },
    evidence: (t: string, args: { graphId: string; sourceItemId: string; versionId?: string; before?: string; atomId?: string; atomsAfter?: string }, signal?: AbortSignal) => {
      const params = new URLSearchParams({ graph_id: args.graphId, source_item_id: args.sourceItemId });
      if (args.versionId) params.set("version_id", args.versionId);
      if (args.before) params.set("before", args.before);
      if (args.atomId) params.set("atom_id", args.atomId);
      if (args.atomsAfter) params.set("atoms_after", args.atomsAfter);
      return req<SourceEvidencePage>(`/v1/sources/evidence?${params}`, t, undefined, false, { signal });
    },
    retryExtraction: (t: string, args: { graphId: string; sourceItemId: string; versionId: string }) =>
      req<{ accepted: boolean }>("/v1/sources/evidence/retry", t, {
        method: "POST",
        body: JSON.stringify({ graph_id: args.graphId, source_item_id: args.sourceItemId, version_id: args.versionId }),
      }),
  },
  meetings: {
    atomsForNode: (t: string, args: { graphId: string; nodeId: string; before?: string }, signal?: AbortSignal) => {
      const params = new URLSearchParams({ graph_id: args.graphId });
      if (args.before) params.set("before", args.before);
      return req<NodeMeetingAtomsPage>(`/v1/nodes/${encodeURIComponent(args.nodeId)}/semantic-atoms?${params}`, t, undefined, false, { signal });
    },
    list: (t: string, args: { graphId: string; limit?: number; cursor?: string }, signal?: AbortSignal) => {
      const params = new URLSearchParams({ graph_id: args.graphId });
      if (args.limit !== undefined) params.set("limit", String(args.limit));
      if (args.cursor) params.set("cursor", args.cursor);
      return req<MeetingsPage>(`/v1/meetings?${params}`, t, undefined, false, { signal });
    },
    evidence: (t: string, args: { graphId: string; sourceId: string; versionId?: string; before?: string; atomId?: string; atomsAfter?: string }, signal?: AbortSignal) => {
      const params = new URLSearchParams({ graph_id: args.graphId, source_id: args.sourceId });
      if (args.versionId) params.set("version_id", args.versionId);
      if (args.before) params.set("before", args.before);
      if (args.atomId) params.set("atom_id", args.atomId);
      if (args.atomsAfter) params.set("atoms_after", args.atomsAfter);
      return req<MeetingEvidencePage>(`/v1/meetings/evidence?${params}`, t, undefined, false, { signal });
    },
    retryExtraction: (t: string, args: { graphId: string; sourceId: string; versionId: string }) =>
      req<{ accepted: boolean }>("/v1/meetings/evidence/retry", t, {
        method: "POST",
        body: JSON.stringify({ graph_id: args.graphId, source_id: args.sourceId, version_id: args.versionId }),
      }),
    // Post-meeting summary for one ingested transcript (keyed by its source_id):
    // the verbatim raw transcript vs the corrected one, plus the provenance of
    // corrections / merges / new entities the user can revert.
    summary: (t: string, graphId: string, sourceId: string) =>
      req<MeetingSummary>(
        `/v1/meetings/summary?graph_id=${encodeURIComponent(graphId)}&source_id=${encodeURIComponent(sourceId)}`,
        t,
      ),
    // (Re-)enqueue machine synthesis for one meeting, resetting `summary_status`
    // to pending. `allowCloud: false` is a plain retry (from `failed`);
    // `allowCloud: true` is the per-meeting egress opt-in (from `blocked`).
    generateSummary: (
      t: string,
      args: { graphId: string; sourceId: string; allowCloud?: boolean },
    ) =>
      req<{ ok: boolean; summary_status: MeetingSummaryStatus }>(
        "/v1/meetings/summary/generate",
        t,
        {
          method: "POST",
          body: JSON.stringify({
            graph_id: args.graphId,
            source_id: args.sourceId,
            ...(args.allowCloud !== undefined ? { allow_cloud: args.allowCloud } : {}),
          }),
        },
      ),
  },
  // AI coding-agent session capture — read surface (coding-session capture
  // Phase 4). Cloud-only (Postgres).
  sessions: {
    list: (
      t: string,
      args: {
        graphId: string;
        project?: string;
        /** Wire session kind — `"coding"` for the Coding sessions facet.
         *  Filtered SERVER-SIDE, and it
         *  has to be: this route is keyset-paginated over one table holding
         *  every kind, so narrowing a fetched page in the browser would show
         *  an empty facet whenever the first page happens to hold none of the
         *  wanted kind — an empty list that is not evidence of an empty
         *  ocean. Omit it to get every kind, as before. */
        kind?: string;
        since?: string;
        limit?: number;
        cursor?: string;
      } & SessionListFilters,
      signal?: AbortSignal,
    ) => {
      const params = new URLSearchParams({ graph_id: args.graphId });
      if (args.kind) params.set("kind", args.kind);
      if (args.since) params.set("since", args.since);
      if (args.limit !== undefined) params.set("limit", String(args.limit));
      if (args.cursor) params.set("cursor", args.cursor);
      applySessionListFilters(params, args);
      return req<CodingSessionsPage>(`/v1/sessions?${params.toString()}`, t, undefined, false, { signal });
    },
    /** `GET /v1/sessions/facets` — the filter bar's vocabulary for one
     *  session kind, computed server-side over every matching row rather
     *  than just the page(s) fetched so far. Takes the SAME optional filter
     *  params as `list`/`listAgent` (send the current selection
     *  so each dimension is narrowed by every OTHER active filter, per
     *  {@link SessionFacets}'s doc) plus `roots_only`/`parent_session_id`
     *  for the Agent-sessions facet, which lists roots only by default. */
    facets: (
      t: string,
      args: {
        graphId: string;
        kind?: string;
        rootsOnly?: boolean;
        parentSessionId?: string;
      } & SessionListFilters,
      signal?: AbortSignal,
    ) => {
      const params = new URLSearchParams({ graph_id: args.graphId });
      if (args.kind) params.set("kind", args.kind);
      if (args.rootsOnly !== undefined) params.set("roots_only", String(args.rootsOnly));
      if (args.parentSessionId) params.set("parent_session_id", args.parentSessionId);
      applySessionListFilters(params, args);
      return req<SessionFacets>(`/v1/sessions/facets?${params.toString()}`, t, undefined, false, { signal });
    },
    /** The Agent-sessions facet's read. Same route, `kind=agent`; ROOT
     *  sessions only by default (each with a tree-wide `coral_count` and a
     *  `subagent_count`), or one agent's direct children when
     *  `parentSessionId` is given. `platform` is canonicalized server-side,
     *  so any spelling of a platform name is a valid filter. */
    listAgent: (
      t: string,
      args: {
        graphId: string;
        parentSessionId?: string;
        rootsOnly?: boolean;
        limit?: number;
        cursor?: string;
      } & SessionListFilters,
      signal?: AbortSignal,
    ) => {
      const params = new URLSearchParams({ graph_id: args.graphId, kind: SESSION_KIND_AGENT });
      if (args.parentSessionId) params.set("parent_session_id", args.parentSessionId);
      if (args.rootsOnly !== undefined) params.set("roots_only", String(args.rootsOnly));
      if (args.limit !== undefined) params.set("limit", String(args.limit));
      if (args.cursor) params.set("cursor", args.cursor);
      applySessionListFilters(params, args);
      return req<AgentSessionsPage>(`/v1/sessions?${params.toString()}`, t, undefined, false, { signal });
    },
    get: (t: string, graphId: string, sessionId: string) =>
      req<CodingSessionDetail>(
        `/v1/sessions/${encodeURIComponent(sessionId)}?graph_id=${encodeURIComponent(graphId)}`,
        t,
      ),
  },
  // Retract (deletion routes, §7.3) — the D6 UI's write side. `coral`
  // retracts ONE atom (plus any promoted copy elsewhere in the account);
  // `run_cascade` mints ONE `retraction_id` shared by every row that single
  // call produces, so a single checkpoint's retract can legitimately span
  // several oceans (propagation) under one report.
  deletion: {
    coral: (t: string, graphId: string, atomId: string) =>
      req<DeletionCascadeOutcome>(
        `/v1/graphs/${encodeURIComponent(graphId)}/corals/${encodeURIComponent(atomId)}`,
        t,
        { method: "DELETE" },
      ),
  },
  // D6 UI's read of `retraction_audit_log` (`routes::retractions`, built
  // separately from B2 unit 02/#718). No account/graph scoping on the wire —
  // the backend's RLS policy carries the full D8 visibility rule (author,
  // every ocean; admin of the ocean a row belongs to; personal rows
  // author-only), so every call here is simply "what can I see".
  retractions: {
    /** `unreachedOnly` is `RetractionHistory`'s "Not fully removed" filter,
     *  applied SERVER-side (`?unreached_only=`) — see
     *  `services::retraction_audit::list_retraction_ids_page`'s own doc for
     *  why filtering the loaded page client-side would be an
     *  absence-renders-as-success bug. The response echoes
     *  `unreached_only` back, so an empty `entries` can always be told
     *  apart from an empty history. */
    list: (t: string, page = 1, pageSize = 20, unreachedOnly = false) =>
      req<RetractionsPage>(
        `/v1/retractions?page=${page}&page_size=${pageSize}${unreachedOnly ? "&unreached_only=true" : ""}`,
        t,
      ),
    get: (t: string, retractionId: string) =>
      req<RetractionEntry>(`/v1/retractions/${encodeURIComponent(retractionId)}`, t),
    /** Every retraction group that ever touched ONE coding session, newest
     *  first — independent review finding 3: a session retracted checkpoint
     *  by checkpoint mints one `retraction_id` PER checkpoint, so the
     *  post-retract report has to read ALL of them back, not just the call
     *  that finally emptied the session. Backs `SessionRetracted`'s report
     *  (`web/app/(app)/session/page.tsx`) both synchronously (right after
     *  the retract that ends the session) and on re-hydration (reload / a
     *  link opened later). */
    forSession: (t: string, sessionId: string) =>
      req<{ entries: RetractionEntry[] }>(
        `/v1/retractions/session/${encodeURIComponent(sessionId)}`,
        t,
      ),
  },
  // Revert / restore an individual transcript correction (surface_form → canonical).
  // Reverting keeps the raw Whisper word; restoring re-applies the correction.
  transcriptCorrections: {
    revert: (t: string, correctionId: string) =>
      req<{ correction_id: string; status: string }>(
        `/v1/transcript-corrections/${correctionId}/revert`,
        t,
        { method: "POST" },
      ),
    restore: (t: string, correctionId: string) =>
      req<{ correction_id: string; status: string }>(
        `/v1/transcript-corrections/${correctionId}/restore`,
        t,
        { method: "POST" },
      ),
  },
  // Speaker relabels (cs-uxp3c): best-effort cloud mirror of an on-device
  // speaker rename, so the ocean's transcript view reflects the new name too.
  speakerRelabels: {
    create: (
      t: string,
      args: { graphId: string; sourceId: string; surfaceForm: string; canonical: string; occurrence?: number | null },
    ) =>
      req<{
        correction_id: string;
        surface_form: string;
        canonical: string;
        node_id: string | null;
        confidence: number | null;
        status: string;
        kind: string;
        occurrence: number | null;
      }>("/v1/transcript-corrections", t, {
        method: "POST",
        body: JSON.stringify({
          graph_id: args.graphId,
          source_id: args.sourceId,
          surface_form: args.surfaceForm,
          canonical: args.canonical,
          occurrence: args.occurrence ?? null,
        }),
      }),
  },
  // Undo an entity auto-merge (split the wrongly-merged entity back out).
  merges: {
    undo: (t: string, mergeId: string) =>
      req<{ merge_id: string; status: string }>(`/v1/merges/${mergeId}/undo`, t, {
        method: "POST",
      }),
  },
  ask: {
    runScoped: (t: string, body: ScopedAskRequest) =>
      req<ScopedAskResult>("/v1/queries", t, {method:"POST",body:JSON.stringify(body)}),
    run: (t: string, graphId: string, text: string) =>
      req<AskResult>("/v1/queries", t, {
        method: "POST",
        body: JSON.stringify({ text, graph_id: graphId }),
      }),
    history: (t: string, graphId?: string) =>
      req<{ items: QueryHistoryItem[] }>(
        `/v1/queries${graphId ? `?graph_id=${graphId}` : ""}`,
        t,
      ),
    /** Streaming conversational query (ADR-0011). Pass `graphIds` (one or more
     * oceans) + the scope `mode`, optional `model`/`style`/`depth`/`sourceType`/
     * `effort`. Omit `conversationId` to start a new conversation (id arrives
     * in `meta`). See `AskStreamBody.effort`'s doc comment for where `effort`
     * is driven from on the composer side. */
    stream: (
      t: string | null,
      opts: {
        text: string;
        libraryScope?: LibraryScope;
        scopeAction?: "replace";
        graphIds: string[];
        mode?: ScopeMode;
        conversationId?: string;
        model?: string;
        style?: "concise" | "detailed" | "sources-only";
        depth?: "fast" | "thorough";
        sourceType?: string;
        effort?: "off" | "low" | "medium" | "high";
      },
      h: ChatStreamHandlers,
    ) =>
      askStream(
        t,
        {
          text: opts.text,
          ...(opts.libraryScope ? {library_scope:opts.libraryScope} : {}),
          ...(opts.scopeAction ? {scope_action:opts.scopeAction} : {}),
          graph_ids: opts.graphIds,
          ...(opts.mode ? { mode: opts.mode } : {}),
          ...(opts.conversationId ? { conversation_id: opts.conversationId } : {}),
          ...(opts.model ? { model: opts.model } : {}),
          ...(opts.style ? { style: opts.style } : {}),
          ...(opts.depth ? { depth: opts.depth } : {}),
          ...(opts.sourceType ? { source_type: opts.sourceType } : {}),
          ...(opts.effort ? { effort: opts.effort } : {}),
        },
        h,
      ),
  },
  conversations: {
    // Ask v2 (ADR-0011 §C12): omit `graphId` to list ALL of the caller's
    // conversations across oceans (de-filtered sidebar). Passing it preserves
    // the legacy single-ocean filter.
    list: (t: string, graphId?: string, q?: string) => {
      const params = new URLSearchParams();
      if (graphId) params.set("graph_id", graphId);
      if (q) params.set("q", q);
      const qs = params.toString();
      return req<{ conversations: ConversationSummary[]; shared: ConversationSummary[] }>(
        `/v1/conversations${qs ? `?${qs}` : ""}`,
        t,
      );
    },
    get: (t: string, id: string) =>
      req<ConversationDetail>(`/v1/conversations/${id}`, t),
    rename: (t: string, id: string, title: string) =>
      req<ConversationSummary>(`/v1/conversations/${id}`, t, {
        method: "PATCH",
        body: JSON.stringify({ title }),
      }),
    setPinned: (t: string, id: string, pinned: boolean) =>
      req<ConversationSummary>(`/v1/conversations/${id}`, t, {
        method: "PATCH",
        body: JSON.stringify({ pinned }),
      }),
    setShared: (t: string, id: string, shared: boolean) =>
      req<ConversationSummary>(`/v1/conversations/${id}`, t, {
        method: "PATCH",
        body: JSON.stringify({ shared }),
      }),
    remove: (t: string, id: string) =>
      req<{ ok: boolean }>(`/v1/conversations/${id}`, t, { method: "DELETE" }),
  },
  activity: {
    list: (t: string, graphId?: string, eventType?: string) => {
      const q = new URLSearchParams();
      if (graphId) q.set("graph_id", graphId);
      if (eventType) q.set("event_type", eventType);
      const qs = q.toString();
      return req<ActivityEvent[]>(`/v1/activity${qs ? `?${qs}` : ""}`, t);
    },
  },
  journal: {
    /** `GET /v1/journal` — the hourly work journal (Today-journal PR D).
     *  `from`/`to` are RFC3339 instants, `to` exclusive, span capped at 48h
     *  server-side (`backend/src/routes/journal.rs`). */
    list: (t: string, from: string, to: string) =>
      req<JournalResponse>(
        `/v1/journal?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
        t,
      ),
    /** `GET /v1/journal?granularity=day|week|month` — the zoomable journal's
     *  tier reads (ticket 17). `from`/`to` are RFC3339 instants, `to`
     *  exclusive; per-tier span caps are server-side (day 62d/week 26w/
     *  month 12mo — `backend/src/routes/journal.rs`). Byte-identical to
     *  `list` above when `granularity` is omitted/`"hour"`, which is why that
     *  case keeps its own narrower `JournalResponse`-typed method rather than
     *  folding into this one. */
    listTier: (t: string, granularity: "day" | "week" | "month", from: string, to: string) =>
      req<JournalTierResponse>(
        `/v1/journal?granularity=${granularity}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
        t,
      ),
    /** `GET /v1/journal/hour/{hour_start}/items` — the uncapped "everything
     *  this hour" drill-down (ticket 16). `hourStart` is an RFC3339 UTC hour
     *  boundary. Keyset-paginated (`cursor`/`limit`, default 50/max 200);
     *  `graphId` narrows to one reachable ocean. */
    hourItems: (
      t: string,
      hourStart: string,
      opts?: { graphId?: string; cursor?: string; limit?: number },
    ) => {
      const qs = new URLSearchParams();
      if (opts?.graphId) qs.set("graph_id", opts.graphId);
      if (opts?.cursor) qs.set("cursor", opts.cursor);
      if (opts?.limit) qs.set("limit", String(opts.limit));
      const suffix = qs.toString();
      return req<JournalHourItemsResponse>(
        `/v1/journal/hour/${encodeURIComponent(hourStart)}/items${suffix ? `?${suffix}` : ""}`,
        t,
      );
    },
  },
  candidates: {
    /** One keyset page of pending decision candidates, newest first. Pass the
     *  previous response's `next_cursor` back verbatim for the next page
     *  (server clamps `limit` to 1..200, default 50). Before this was keyset-
     *  paginated, "load more" re-asked with a bigger `limit` and the queue was
     *  unreachable past the 200 clamp. */
    list: (t: string, opts?: { limit?: number; cursor?: string }) => {
      const qs = new URLSearchParams();
      // `!== undefined`, not truthiness — `limit: 0` must reach the server so
      // its own clamp decides, matching the relationship list's convention.
      if (opts?.limit !== undefined) qs.set("limit", String(opts.limit));
      if (opts?.cursor !== undefined) qs.set("cursor", opts.cursor);
      const suffix = qs.toString();
      return req<DecisionCandidatePage>(
        `/v1/candidates${suffix ? `?${suffix}` : ""}`,
        t,
      );
    },
    // Optional inline edits applied on accept (CS-069b).
    confirm: (
      t: string,
      nodeId: string,
      edits?: { title?: string; rationale?: string; decided_at?: string },
    ) =>
      req<{ node_id: string }>(`/v1/nodes/${nodeId}/confirm`, t, {
        method: "POST",
        body: JSON.stringify(edits ?? {}),
      }),
    // Optional rejection reason: "wrong" | "not_a_decision" | "duplicate".
    reject: (t: string, nodeId: string, reason?: string) =>
      req<{ node_id: string }>(`/v1/nodes/${nodeId}/reject`, t, {
        method: "POST",
        body: JSON.stringify(reason ? { reason } : {}),
      }),
  },
  relationships: {
    // One keyset page of relationship edges in a graph's knowledge graph —
    // stated edges + inferred-unconfirmed edges awaiting human review. Pass
    // the previous response's `next_cursor` back verbatim for the next page
    // (server clamps `limit` to 1..200, default 100).
    list: (
      graphId: string,
      t: string,
      status = "suggested",
      opts?: { limit?: number; cursor?: string; predicate?: string },
    ) => {
      const qs = new URLSearchParams({ status });
      // `!== undefined`, not truthiness: `limit: 0` must reach the server (it
      // clamps to 1), and empty strings should hit server validation.
      if (opts?.limit !== undefined) qs.set("limit", String(opts.limit));
      if (opts?.cursor !== undefined) qs.set("cursor", opts.cursor);
      if (opts?.predicate !== undefined) qs.set("predicate", opts.predicate);
      return req<RelationshipEdgePage>(
        `/v1/graphs/${graphId}/relationships?${qs.toString()}`,
        t,
      );
    },
    confirm: (edgeId: string, t: string) =>
      req<{ edge_id: string; status: string }>(
        `/v1/relationships/${edgeId}/confirm`,
        t,
        { method: "POST" },
      ),
    reject: (edgeId: string, t: string) =>
      req<{ edge_id: string; status: string }>(
        `/v1/relationships/${edgeId}/reject`,
        t,
        { method: "POST" },
      ),
  },
  mergeReview: {
    /** One keyset page of the standing merge-review queue: pairs the entity
     *  resolver declined to decide on its own. Oldest first — the item that
     *  has waited longest is also the one closest to lapsing. Pass the
     *  previous response's `next_cursor` back verbatim for the next page
     *  (server clamps `limit` to 1..200, default 100).
     *
     *  Rejected through `api.reefs.dismissMerge`, which is the same
     *  keep-separate ruling made anywhere else. CONFIRM is `confirm` below,
     *  NOT `api.reefs.merge`: that one is node-anchored and 404s on any ocean
     *  the caller's active account does not own (a reachable team ocean), so
     *  it could not drain this queue. */
    list: (graphId: string, t: string, opts?: { limit?: number; cursor?: string }) => {
      const qs = new URLSearchParams();
      // `!== undefined`, not truthiness — same reason as relationships.list.
      if (opts?.limit !== undefined) qs.set("limit", String(opts.limit));
      if (opts?.cursor !== undefined) qs.set("cursor", opts.cursor);
      const q = qs.toString();
      return req<MergeReviewPage>(
        `/v1/graphs/${graphId}/merge-review${q ? `?${q}` : ""}`,
        t,
      );
    },
    /** Resolve one card as "same reef": merges `dupId` into `keepId` and
     *  supersedes the card's UNSURE judgement with a human POSITIVE.
     *  `judgementId` names the card being resolved — a 404 means it is no
     *  longer open (already resolved, expired, or re-adjudicated) and the
     *  caller should drop it. */
    confirm: (
      graphId: string,
      t: string,
      body: { judgementId: string; keepId: string; dupId: string },
    ) =>
      req<{ ok: boolean; corals_moved: number }>(
        `/v1/graphs/${graphId}/merge-review/confirm`,
        t,
        {
          method: "POST",
          body: JSON.stringify({
            judgement_id: body.judgementId,
            keep_id: body.keepId,
            dup_id: body.dupId,
          }),
        },
      ),
  },
  review: {
    /** The sidebar badge's totals as three server-side COUNTs — replaces
     *  fetching full candidate + edge rows on every route navigation. */
    count: (graphId: string, t: string) =>
      req<ReviewCount>(`/v1/review/count?graph_id=${graphId}`, t),
  },
  teams: {
    list: (t: string) => req<TeamMembership[]>("/v1/teams", t),
    /** Account-level members — everyone in the account, NOT the members of one
     *  team ocean (use `roster` below for that). A per-team member listing
     *  endpoint now exists (`GET /v1/teams/{team_id}/members`, see `roster`);
     *  this one is unchanged and stays account-wide — it is also the source
     *  the Members surface's add-member picker draws from on the PERSONAL
     *  plane (there is no org-directory equivalent outside an org context). */
    members: (t: string, signal?: AbortSignal) =>
      req<Member[]>("/v1/teams/members", t, undefined, false, { signal }),
    /** Create a named team, which creates its team ocean and enrols the caller
     *  in one transaction (graph → team → team key → creator envelope →
     *  creator membership).
     *
     *  Used to be PERSONAL PLANE ONLY, hard-403ing any org context. That
     *  blanket refusal is now CONDITIONAL on the backend's `ORG_TEAMS_ENABLED`
     *  flag (org-team-membership design doc, "the org gate"): flag on, an org
     *  caller gets the org key model (no per-member envelope — see
     *  `routes/teams.rs`'s module doc) and this succeeds exactly like the
     *  personal path; flag off, it still 403s with "named teams are not yet
     *  available in organization workspaces". `GET /v1/me` now publishes
     *  which state the flag is in as `org_teams_enabled` (see that call's doc
     *  comment) — callers should gate the create affordance on THAT rather
     *  than attempt-and-discover-the-403. This call itself still just
     *  attempts the write and surfaces whatever 403 message comes back
     *  verbatim if it's called anyway (a stale read of the flag, a direct API
     *  call, etc.), same as any other write. NOTE this is NOT `POST
     *  /v1/graphs`: that route rejects kind='team' outright. */
    create: (t: string, name: string) =>
      req<{ team_id: string; graph_id: string }>("/v1/teams", t, {
        method: "POST",
        body: JSON.stringify({ name }),
      }),
    invite: (t: string, email: string, role: "member" | "owner" = "member") =>
      req<unknown>("/v1/teams/invite", t, {
        method: "POST",
        body: JSON.stringify({ email, role }),
      }),
    // ── One team ocean's roster + governance (Members surface) ─────────────
    /** Active roster of ONE team ocean (`kind: "team"`) — see the module note
     *  above `members` for how this differs from it. Readable by any active
     *  member of the team, or (org context) an org admin — 403 otherwise.
     *  `name` may be null; render `name ?? email`. */
    roster: (t: string, teamId: string) =>
      req<TeamMemberRow[]>(`/v1/teams/${teamId}/members`, t),
    /** Add someone already in the caller's account/org to this team.
     *  Requires the caller be a team admin or org admin (403 otherwise). The
     *  backend enforces the membership-subset invariant server-side and
     *  answers 422 ("target user is not a member of this account/organization")
     *  when `userId` isn't already an account/org member — surface that
     *  verbatim; it names exactly what to do (invite them to the org/account
     *  first). 409 if already an active member. */
    addMember: (t: string, teamId: string, userId: string, role: TeamRole = "member") =>
      req<TeamMemberAdded>(`/v1/teams/${teamId}/members`, t, {
        method: "POST",
        body: JSON.stringify({ user_id: userId, role }),
      }),
    /** Change a member's role. Team-admin/org-admin only (403 otherwise).
     *  409 "cannot demote the last admin — assign another admin first" is a
     *  real guard, not a transient failure — surface it as guidance. A 200
     *  with `changed: false` means the role already matched; that is success,
     *  not a no-op error. */
    updateMemberRole: (t: string, teamId: string, userId: string, role: TeamRole) =>
      req<TeamMemberRoleUpdate>(`/v1/teams/${teamId}/members/${userId}`, t, {
        method: "PATCH",
        body: JSON.stringify({ role }),
      }),
    /** Remove someone ELSE from the roster (soft-remove; TK rotation is
     *  deferred, CS-104). Team-admin/org-admin only — for the caller removing
     *  THEMSELF, call `leave` below instead. 409 "cannot remove the last
     *  admin — promote another member first" when the target is the sole
     *  admin; 404 if already departed/not found. */
    removeMember: (t: string, teamId: string, userId: string) =>
      req<void>(`/v1/teams/${teamId}/members/${userId}`, t, { method: "DELETE" }),
    /** Leave a team ocean voluntarily — the caller removing their OWN
     *  membership. Wire-identical to `removeMember` (same route, `userId` is
     *  the caller's own id); kept as its own named call so a call site reads
     *  as intent ("I'm leaving" vs. "I'm removing someone else"). Unlike
     *  `removeMember`, this needs NO admin rights at all — the backend
     *  permits a self-removal from any active member (team-ocean lifecycle
     *  management's "leave a workspace" item; see `remove_team_member`'s doc
     *  comment in `routes/teams.rs`). Still 409 "cannot remove the last admin
     *  — promote another member first" if the caller is the team's sole
     *  admin — leaving is exempt from the admin GATE, not from that guard. */
    leave: (t: string, teamId: string, userId: string) =>
      req<void>(`/v1/teams/${teamId}/members/${userId}`, t, { method: "DELETE" }),
    /** Paginated membership audit history, newest first (`page_size` clamped
     *  1..50 server-side, default 20). Team-admin/org-admin only — same
     *  authorization as the roster WRITES, not a public roster view. A plain
     *  member's 403 here means "nothing to show", not a fetch error; the UI
     *  should hide this section for them rather than render an ErrorNote. */
    events: (t: string, teamId: string, page = 1, pageSize = 20) =>
      req<MembershipEvent[]>(`/v1/teams/${teamId}/events?page=${page}&page_size=${pageSize}`, t),
    /** Rename a team ocean. Admin-only (team admin or org admin, mirroring
     *  every other roster-governance route above). Idempotent: renaming to
     *  the (trimmed) name it already has answers 200 with `changed: false`,
     *  not an error — treat that as success. */
    rename: (t: string, teamId: string, name: string) =>
      req<TeamRenameResult>(`/v1/teams/${teamId}`, t, {
        method: "PATCH",
        body: JSON.stringify({ name }),
      }),
    /** Archive a team ocean — REVERSIBLE, and its content (topics, corals,
     *  sources) is preserved, not deleted. Admin-only. Idempotent: archiving
     *  an already-archived workspace answers 204 same as a fresh archive, not
     *  an error. UI copy must call this "archive", never "delete", to match
     *  these semantics. See `restore` below for the unarchive counterpart. */
    archive: (t: string, teamId: string) => req<void>(`/v1/teams/${teamId}`, t, { method: "DELETE" }),
    /** Restore (unarchive) a team ocean — the small, additive counterpart to
     *  `archive` above. Admin-only, same team-admin-or-org-admin check.
     *  Idempotent: restoring a workspace that isn't currently archived
     *  answers 200 with `changed: false`, not an error. */
    restore: (t: string, teamId: string) =>
      req<TeamRestoreResult>(`/v1/teams/${teamId}/restore`, t, { method: "POST" }),
    /** Archived team oceans for the caller's ACCOUNT — see {@link ArchivedTeam}
     *  for the authorization gate and why a 403 is an expected, quiet answer
     *  rather than an error. This is what makes `restore` above reachable at
     *  all: nothing else in this API surfaces a `team_id` once it's archived. */
    listArchived: (t: string) => req<ArchivedTeam[]>("/v1/teams/archived", t),
  },
  // ── Join-request queue (CS-437, org-onboarding plan §3 items 4 + 14,
  //    `backend/src/routes/org_join_requests.rs`, D3/D9/D10) — the
  //    non-admin-initiated path into an org: a signed-in user who is not
  //    yet a member asks to join, and an admin approves or rejects. No UI
  //    consumes this yet (items 5/11/39/40 build it); this is the typed
  //    client surface only. ──────────────────────────────────────────────
  joinRequests: {
    /** Always `202` with the same `"sent_to_admin"` body, whatever actually
     *  happened server-side — never disclose more client-side either.
     *  `token`: the opaque `?t=` value from a "join this org" email's link
     *  (CS-440, org-onboarding plan §3 item 29, D19) — optional and
     *  additive, every existing caller omits it and keeps working
     *  unchanged. An invalid/expired/mismatched token is never surfaced
     *  here either: the backend silently falls back to an ordinary
     *  self-request and still answers 202. */
    create: (t: string, org: string, token?: string) =>
      req<JoinRequestSentResult>("/v1/join-requests", t, {
        method: "POST",
        body: JSON.stringify(token ? { org, token } : { org }),
      }),
  },
  /** `GET /v1/me/join-requests` — the caller's own OPEN requests (item 39's
   *  waiting-state banner). Not nested under `me` above: `api.me` is
   *  already a bare function (`GET /v1/me`), called at 9+ existing sites as
   *  `api.me(token)` — turning it into a namespace object would be a
   *  breaking rename of every one of them for no behavioral gain. */
  myJoinRequests: (t: string) => req<MyJoinRequest[]>("/v1/me/join-requests", t),
  // ── Org member directory (org-team-membership design doc, decision #10) ──
  // Org context only — the source the Members surface's add-member picker
  // draws from when the active workspace is inside an organization (see
  // `teams.members`'s doc comment for the personal-plane equivalent).
  orgs: {
    members: (t: string) => req<OrgDirectoryMember[]>("/v1/orgs/members", t),
    // ── Org invite lifecycle (org invites PR 1, `backend/src/routes/org_invites.rs`) ──
    // Custom invite system, NOT Clerk's `organization.inviteMembers` — adding
    // a member purchases their seat and creates the Clerk account server-side,
    // so this has to be a backend call, not a client-side Clerk SDK call.
    invites: {
      /** 201 on success. Throws `ApiError` whose `message` is the backend's
       *  `error` string — see the field doc above and the callers that map
       *  `seat_purchase_failed` / `billing_unavailable` / `already_a_member`
       *  to calm copy.
       *
       *  `allow_purchase: false` (what the web always sends) forbids the
       *  invite from buying a seat: with none free the backend answers 409
       *  `code: "no_free_seat"` and creates nothing, and the caller prices
       *  the seat through the quote → confirm dialog. Omitted, the backend
       *  defaults to `true` for older clients. */
      create: (t: string, body: { email: string; role: "member" | "admin"; name?: string; allow_purchase?: boolean }) =>
        req<OrgInviteResult>("/v1/orgs/invites", t, { method: "POST", body: JSON.stringify(body) }),
      /** Re-sends the sign-in email for a still-pending invite. 409
       *  `not_pending` once the invitee has signed in. */
      resend: (t: string, userId: string) =>
        req<{ email_sent: boolean }>(`/v1/orgs/invites/${userId}/resend`, t, { method: "POST" }),
      /** Revokes a standing invite and releases its seat. 400
       *  `cannot_revoke_self`/`cannot_revoke_owner`; 409 `not_pending`. */
      revoke: (t: string, userId: string) =>
        req<{ revoked: boolean; user_deleted: boolean }>(`/v1/orgs/invites/${userId}`, t, { method: "DELETE" }),
    },
    // ── Active-member management (settings 2.2, `backend/src/routes/org_members.rs`) ──
    // Admin-only. `userId` is the control-plane id from `members` above.
    member: {
      /** Removes a member from the org right away. 400 `cannot_remove_self`/
       *  `cannot_remove_owner`; 404 `member_not_found`; 409 `last_admin`. */
      remove: (t: string, userId: string) =>
        req<{ removed: boolean; user_id: string }>(`/v1/orgs/members/${encodeURIComponent(userId)}`, t, {
          method: "DELETE",
        }),
      /** Switches a member between admin and member. 400 `cannot_change_own_role`/
       *  `cannot_change_owner_role`/`invalid_role`; 409 `last_admin`. */
      setRole: (t: string, userId: string, role: "admin" | "member") =>
        req<{ role: string; changed: boolean }>(`/v1/orgs/members/${encodeURIComponent(userId)}`, t, {
          method: "PATCH",
          body: JSON.stringify({ role }),
        }),
    },
    // ── Slot pool (CS-435, `backend/src/routes/org_slots.rs`) ──────────────
    // `get` is any org member (matches the members page's own read bar — see
    // that route's own module doc); `purchase` is admin-only, 403 for a
    // personal org.
    slots: {
      get: (t: string) => req<SlotPoolResult>("/v1/orgs/slots", t),
      /** `idempotencyKey`: a FRESH client-generated value per distinct
       *  purchase attempt, reused only when retrying the SAME attempt (a
       *  dropped response, a timeout) — see the backend route's own doc
       *  comment on `PurchaseSlotsRequest.idempotency_key` for the accepted
       *  narrow gap this protects against. */
      purchase: (t: string, body: { tier: string; quantity: number; idempotencyKey: string }) =>
        req<SlotPoolResult>("/v1/orgs/slots", t, {
          method: "POST",
          body: JSON.stringify({ tier: body.tier, quantity: body.quantity, idempotency_key: body.idempotencyKey }),
        }),
      /** Give back free seats of one tier. Same key discipline as `purchase`. */
      reduce: (t: string, body: { tier: string; quantity: number; idempotencyKey: string }) =>
        req<SlotPoolResult>("/v1/orgs/slots/reduce", t, {
          method: "POST",
          body: JSON.stringify({ tier: body.tier, quantity: body.quantity, idempotency_key: body.idempotencyKey }),
        }),
    },
    // ── Admin side of the join-request queue (CS-437) — mirrors `invites`/
    //    `slots` above: admin-only, 403 for a personal org. ────────────────
    joinRequests: {
      /** `includeDecidedDays`: also return recently-decided requests from
       *  that many days back, appended AFTER the open queue. Omitted ⇒ open
       *  requests only. */
      list: (t: string, includeDecidedDays?: number) =>
        req<JoinRequest[]>(
          `/v1/orgs/join-requests${includeDecidedDays ? `?include_decided=${includeDecidedDays}` : ""}`,
          t,
        ),
      /** D3's Clerk-first sequence, server-side — 200 once a real Clerk +
       *  local membership exists. `tier` is validated against `seat_tiers`
       *  and recorded on the decision row; see the backend route's own doc
       *  comment for what item 40 still owes on actually applying it. */
      approve: (t: string, id: string, tier: string) =>
        req<JoinRequestDecision>(`/v1/orgs/join-requests/${id}/approve`, t, {
          method: "POST",
          body: JSON.stringify({ tier }),
        }),
      /** 204. Silent and durable (D10) — no email, no notice to the
       *  requester, and a later self-request from the same person for this
       *  org is suppressed forever. */
      reject: (t: string, id: string) => req<void>(`/v1/orgs/join-requests/${id}/reject`, t, { method: "POST" }),
      /** CS-438 (D13) — the open count alone, for the members-page/rail
       *  badge. Cheaper than `list(t).length`: no rows, no evidence
       *  payloads, just the number. */
      count: (t: string) => req<{ open: number }>("/v1/orgs/join-requests/count", t),
    },
    /** `GET /v1/orgs/connector-health` (CS-446, item 16, D16) — every
     *  CURRENT connector degradation for this org, oldest first; `[]` when
     *  everything is healthy.
     *
     *  Admin-only on the server (`require_org_admin` + the personal-org
     *  bar), so a non-admin or a personal-plane caller gets 403 rather than
     *  an empty list. Callers gate on `useOrgAdmin` first and treat any
     *  failure as "show nothing" — a connectors page must not grow an
     *  error banner because an org-admin read it was never meant to make
     *  came back 403.
     *
     *  The read is not free of side effects by design: the backend
     *  re-derives whether each degradation is still true and closes the
     *  ones that are fixed (`services::connector_health::
     *  refresh_open_degradations`), so this never shows a problem an admin
     *  has already solved. */
    connectorHealth: (t: string) =>
      req<ConnectorHealthResponse>("/v1/orgs/connector-health", t),
    /** `/v1/orgs/domains*` (CS-443 the backend, item 37, D11; CS-450 the web
     *  UI) — admin-only, 403 for a personal org, same bar `invites`/`slots`/
     *  `joinRequests` above hold. See `backend/src/routes/org_domains.rs`
     *  for the wire shapes and every status code below — read it directly
     *  rather than trusting a paraphrase here, per that route module's own
     *  header comment about the two proofs not being interchangeable. */
    domains: {
      /** Every claimed domain, its verified state, and (D11) which method
       *  proved it. Read for two reasons: this screen's own list, AND
       *  whether auto-add has a domain to arm against
       *  (`components/org/autoAddModel.ts`'s `autoAddDomainState`). */
      list: (t: string) => req<OrgDomainListResponse>("/v1/orgs/domains", t),
      /** `POST /v1/orgs/domains` — claim a domain. `200` whether newly
       *  claimed or already claimed by this org (idempotent on the same
       *  string); the row carries the DNS TXT record name/value to publish
       *  either way. `400`'s message is `services::org_domains::
       *  DomainRejection::message` — written to show an admin verbatim, not
       *  a code to re-map. `409` is this org's own 25-domain cap, also
       *  written to show verbatim (it names the number). */
      claim: (t: string, domain: string) =>
        req<OrgDomain>("/v1/orgs/domains", t, { method: "POST", body: JSON.stringify({ domain }) }),
      /** `DELETE /v1/orgs/domains/{id}` — `204`. Any outstanding email
       *  confirmation link for it stops working immediately; nothing else
       *  is undone (a domain another org later claims starts its own,
       *  unrelated proof). */
      remove: (t: string, id: string) => req<void>(`/v1/orgs/domains/${id}`, t, { method: "DELETE" }),
      /** `POST /v1/orgs/domains/{id}/verify` — check the DNS TXT record
       *  now. `200` on an already-verified row is a no-op echo (it does NOT
       *  re-check DNS or change `method` — once verified by either proof,
       *  this route never touches the row again).
       *
       *  `409` and `502` are NOT the same fact and this surface must never
       *  render them the same way (`components/org/domainVerificationModel.ts`'s
       *  `dnsCheckFailure`, and that route's own header comment, "'Could not
       *  ask' is not 'not published'"): `409` is a resolver that answered —
       *  either with no matching record yet, or with a match that a
       *  DIFFERENT org already verified first (D16/CS-447, one domain
       *  verifies for exactly one org, globally) — both cases are worth
       *  acting on and the backend's own message names which one happened.
       *  `502` is a resolver we could not reach AT ALL, which says nothing
       *  about the record — telling an admin "not published" on a `502`
       *  would send them to break a record that was already fine. */
      verify: (t: string, id: string) => req<OrgDomain>(`/v1/orgs/domains/${id}/verify`, t, { method: "POST" }),
      /** `POST /v1/orgs/domains/{id}/confirmation` — mail a confirmation
       *  link to `email`, which MUST be at the claimed domain (`400`
       *  otherwise, server-checked — the whole proof rests on that). `202`
       *  sent. `409` is either "already verified" or "sent very recently,
       *  try again shortly" — the message names which. `502` is the email
       *  provider refusing the send — nothing was consumed, retry. `503`
       *  means email sending is not configured on THIS DEPLOYMENT — a
       *  deployment fact, not a retryable admin mistake (the body carries no
       *  domain-specific detail for it, `{"error":"service unavailable"}`,
       *  so `domainVerificationModel.ts`'s `confirmationFailure` supplies
       *  its own copy rather than showing that literally) — DNS stays the
       *  route that still works. */
      requestConfirmation: (t: string, id: string, email: string) =>
        req<OrgDomainConfirmationSent>(`/v1/orgs/domains/${id}/confirmation`, t, {
          method: "POST",
          body: JSON.stringify({ email }),
        }),
    },
    // ── Auto-add (CS-447 the setting; CS-449 item 38 the "ask once" dialog
    //    on top of it, D1/D11/D16) — admin-only on BOTH verbs of every
    //    route here, same as `domains` above: this decides whether
    //    strangers are added to the org, which is not an ordinary member's
    //    read. ──────────────────────────────────────────────────────────
    autoAdd: {
      get: (t: string) => req<AutoAddSettings>("/v1/orgs/auto-add", t),
      /** 400 `"unknown or inactive seat tier"` for a `tier` that isn't a
       *  live `seat_tiers.code`. */
      put: (t: string, body: { enabled: boolean; tier: string }) =>
        req<AutoAddSettings>("/v1/orgs/auto-add", t, { method: "PUT", body: JSON.stringify(body) }),
      /** The D11 "ask once" preview — everyone currently queued who would
       *  pass right now, oldest first, and the (advisory) pool state that
       *  produced that list. See `AutoAddQueuedPreview`'s own doc comment
       *  for why `free_slots`/`would_admit` are not a promise. */
      queuedPreview: (t: string) => req<AutoAddQueuedPreview>("/v1/orgs/auto-add/queued-preview", t),
      /** Corrected contract (code review on PR #1041 — an earlier round
       *  sent only the oldest `would_admit` of the passers shown, which
       *  broke the drain for everyone left out; see
       *  `components/org/autoAddModel.ts`'s `askOnceRequestIds` for the
       *  full account). `requestIds`: EVERY passer the ask-once dialog
       *  showed, oldest first — never a narrower slice. `maxAdmit`: the
       *  ceiling the button's own words promise ("Admit the N oldest"),
       *  `askOnceMaxAdmit`'s own number; `0` is valid (the "none-fit"
       *  click: admit nobody, but still patch everyone sent to `pool_full`
       *  so the drain collects them). The backend admits at most
       *  `maxAdmit`, oldest first, and patches every remaining id in
       *  `requestIds` to `pool_full` in the same call. */
      admitQueued: (t: string, requestIds: string[], maxAdmit: number) =>
        req<AutoAddAdmitResult>("/v1/orgs/auto-add/admit-queued", t, {
          method: "POST",
          body: JSON.stringify({ request_ids: requestIds, max_admit: maxAdmit }),
        }),
    },
  },
  // ── Billing (CS-442, org-onboarding plan §3 item 40, D8/D15) ───────────────
  // Merge coordination: CS-441 (the approval-queue UI, built in parallel)
  // merged first (#1027) and already defines `api.billing.seatTiers` and
  // `SeatTier` below — this branch's own duplicate definition (same shape,
  // `SeatTierResult`) is dropped in favor of theirs, since the shipped UI
  // already calls it.
  billing: {
    /** The tier choices for the approval row's slot-tier control (and slot
     *  purchase generally) — CS-442's counterpart to this story, landing
     *  independently and possibly after it. NOT guaranteed to exist on the
     *  backend yet: every caller MUST treat a failed/404 read as
     *  `["standard"]` alone and stay silent about it — never surface an
     *  error for this specific fetch, per this story's own brief. */
    seatTiers: (t: string) => req<SeatTier[]>("/v1/billing/seat-tiers", t),
    /** The signed-in user's own credits and extra usage (credits only, no dollars). */
    me: (t: string) => req<BillingMe>("/v1/billing/me", t),
    /** Plan, seats, credits, extra usage, renewal, card. Org: admin-only. */
    summary: (t: string) => req<BillingSummary>("/v1/billing/summary", t),
    /** Stripe Billing Portal. 409 `no_billing_account`, 503
     *  `portal_not_configured`, 502 `billing_unavailable`. */
    portal: (t: string, returnUrl: string) =>
      req<{ url: string }>("/v1/billing/portal", t, {
        method: "POST",
        body: JSON.stringify({ return_url: returnUrl }),
      }),
    /** Stripe Checkout for the first subscription: `pro` (personal) or
     *  a seat tier such as `standard` (org, one seat). */
    subscribe: (t: string, body: { tier: string; successUrl: string; cancelUrl: string }) =>
      req<CheckoutSession>("/v1/billing/checkout/subscription", t, {
        method: "POST",
        body: JSON.stringify({ tier: body.tier, success_url: body.successUrl, cancel_url: body.cancelUrl }),
      }),
    /** Price a seat change. The proration lands on the next invoice. */
    seatQuote: (t: string, p: { quantity: number; action: "add" | "reduce"; tier?: string }) =>
      req<SeatQuote>(
        `/v1/billing/seats/quote?quantity=${p.quantity}&tier=${encodeURIComponent(p.tier ?? "standard")}&action=${p.action}`,
        t,
      ),
    extraUsage: {
      update: (t: string, body: ExtraUsageUpdate) =>
        req<ExtraUsageResult>("/v1/billing/extra-usage", t, { method: "PUT", body: JSON.stringify(body) }),
      /** One-time top-up through Stripe Checkout; $5 to $1,000 in whole cents. */
      topUp: (t: string, body: { amountMicroUsd: number; successUrl: string; cancelUrl: string }) =>
        req<CheckoutSession>("/v1/billing/extra-usage/topup", t, {
          method: "POST",
          body: JSON.stringify({
            amount_micro_usd: body.amountMicroUsd,
            success_url: body.successUrl,
            cancel_url: body.cancelUrl,
          }),
        }),
    },
  },
  // ── Self-serve team org sign-up (A2 #1533, C′ #1521) ──────────────────────
  // Every route here is called with the creator's CURRENT session (personal,
  // or another org they already belong to), never a token naming the new
  // org: the binding gate refuses that org until it is ready. All of them
  // 404 while the backend's `ORG_SELFSERVE_SIGNUP_ENABLED` is unset.
  orgSelfserve: {
    /** `POST /v1/orgs`. `idempotencyKey` is minted once per (name, seats)
     *  attempt and resent on every retry, so a repeat returns the same org
     *  and the same Checkout (`replayed: true`). That guarantee is what makes
     *  the transport's ambiguous-status retry safe here. */
    create: (t: string, body: OrgCreateRequest, idempotencyKey: string) =>
      req<OrgCreateResult>(
        "/v1/orgs",
        t,
        { method: "POST", body: JSON.stringify(body), headers: { "Idempotency-Key": idempotencyKey } },
        false,
        { retryNonIdempotent: true },
      ),
    /** `GET /v1/orgs/{org_id}/provisioning` (owner or admin of that org). */
    status: (t: string, orgId: string) =>
      req<OrgSetupStatus>(`/v1/orgs/${encodeURIComponent(orgId)}/provisioning`, t),
    /** `POST /v1/orgs/{org_id}/provisioning/retry` → 202 with the status body. */
    retry: (t: string, orgId: string) =>
      req<OrgSetupStatus>(`/v1/orgs/${encodeURIComponent(orgId)}/provisioning/retry`, t, { method: "POST" }),
    /** `POST /v1/orgs/{org_id}/trial-checkout`: "resume payment", a new
     *  Checkout for an org still within 24 h of its first one (#1521). */
    trialCheckout: (t: string, orgId: string, body: { seats: number; success_url: string; cancel_url: string }) =>
      req<OrgTrialCheckout>(`/v1/orgs/${encodeURIComponent(orgId)}/trial-checkout`, t, {
        method: "POST",
        body: JSON.stringify(body),
      }),
  },
  // ── Connector OAuth: PERSONAL plane only ───────────────────────────────────
  // These build browser-navigation URLs, not fetches. They used to be pinned to
  // a `personalApiBase()` that forced `api.<apex>` regardless of the active
  // org, back when an org's requests went to `<slug>.api.<apex>` and a flow
  // started there would bounce back to the wrong origin (`redirect_uri` comes
  // from a single-valued `BASE_URL`, and the final hop from a single-valued
  // `FRONTEND_URL`). With claim-based tenancy there is one API host for every
  // tenant, so that function collapsed into `apiBase()` and was deleted rather
  // than aliased — a helper still named "personal" that no longer forces the
  // personal plane is a false affordance.
  //
  // THE PERSONAL-PLANE RESTRICTION IS ENFORCED SERVER-SIDE, and always was —
  // the URL never enforced it. `mint_state`, which `mintState` below calls
  // immediately BEFORE this URL is ever navigated to, runs
  // `reject_org_oauth_management` / `reject_org_gmail_management`
  // (backend/src/routes/oauth.rs:116,165), whose shared 403 body (`:139`) is
  // "connector authorization is available in your personal workspace only;
  // switch out of the organization workspace to manage it". `callback` then
  // re-checks independently via `google_org_callback_allowed`. It is a
  // per-provider matrix rather than a blanket refusal — Slack, Gmail and
  // Microsoft are each admissible under an org behind their own connector
  // flag — which is one more reason the frontend must not try to encode the
  // rule in a hostname. An org user gets exactly the response they got
  // before, from exactly the same guard.
  oauth: {
    listConnections: (t: string) =>
      req<OAuthConnection[]>("/v1/oauth/connections", t),
    disconnect: (t: string, provider: string) =>
      req<{ status: string }>(`/v1/oauth/connections/${provider}`, t, {
        method: "DELETE",
      }),
    // Mint a server-signed `state` token (CS-110) bound to the authenticated
    // account. Call this before starting a connect/admin-consent flow and pass
    // the returned token as `state` — the account is established server-side from
    // the session, never from a client-supplied parameter.
    //
    // `surface` (gsuite Train G2/G4 — `MintStateParams` in `routes/oauth.rs`):
    // optionally requests that the minted state embed a Google surface
    // (`"gmail"` | `"chat"` | `"meet"`) as its `sfc` claim, alongside this
    // caller's own `uid` — both of which `authorize`'s non-Calendar arm then
    // requires the state to carry (`require_verified_surface_state`).
    // Omitted (every caller before this one) mints byte-identical to before
    // this parameter existed — the backend treats a `surface`-less state as
    // `(None, Calendar)`, same as always. Calendar's own connect flows
    // (Settings · Capture, the Connectors tile) now pass `"calendar"`
    // explicitly (Google Calendar org lift) so an ORG member's mint is gated
    // on the org's Calendar flag; a personal workspace behaves identically
    // either way. NEVER meaningful for a non-Google provider; harmless to pass
    // regardless (the route is provider-blind and only reads it to pick
    // which org gate applies, see `mint_state`'s own doc comment).
    mintState: (t: string, surface?: string) =>
      req<{ state: string; expires_in: number }>(
        `/v1/oauth/state${surface ? `?surface=${encodeURIComponent(surface)}` : ""}`,
        t,
      ),
    // One-button "Connect Google Workspace": which Google surfaces this
    // workspace may connect right now. Each surface is gated by its own org
    // flag (personal workspaces: Calendar always, the rest by env default);
    // reveals only the caller's own workspace's state.
    googleWorkspaceSurfaces: (t: string) =>
      req<{ surfaces: { surface: "calendar" | "gmail" | "chat" | "meet"; enabled: boolean }[] }>(
        "/v1/oauth/google/workspace",
        t,
      ),
    // Always pass a state minted via `mintState` immediately before
    // navigating; without it the backend falls back to an unattributed random
    // UUID and the callback can't tie the token to an account.
    //
    // `surface` (same gsuite Train G2/G4 param, read by `authorize`'s
    // `AuthorizeParams`): selects which Google surface's scope to request —
    // `"calendar"` (the default when omitted), `"gmail"`, `"chat"`, or
    // `"meet"`. Omitted, this builds the EXACT URL every existing caller
    // already builds (a caller that omits it, e.g. a pre-lift Calendar
    // connect, still works unchanged).
    // Ignored by every non-Google provider. Pass the SAME surface string you
    // minted the state with above — `authorize` 400s a Chat/Gmail/Meet
    // request whose state does not declare this exact surface.
    authorizeUrl: (provider: string, state: string, surface?: string) =>
      `${apiBase()}/v1/oauth/${provider}/authorize?state=${encodeURIComponent(state)}${
        surface ? `&surface=${encodeURIComponent(surface)}` : ""
      }`,
    // Org-wide admin consent: a tenant admin authorizes CoralSwarm for the whole
    // company, so employees can connect even when per-user consent is blocked.
    adminConsentUrl: (provider: string, state: string) =>
      `${apiBase()}/v1/oauth/${provider}/admin-consent?state=${encodeURIComponent(state)}`,
  },
  connectors: {
    // Options train: all saved mailbox threads, including unfollowed rows.
    // The cursor is opaque; never derive it from a thread id or timestamp.
    watchedThreads: (t: string, provider: MailboxProvider, cursor?: string) =>
      req<WatchedThreadsResponse>(
        `/v1/connectors/${provider}/watched?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
        t,
      ),
    setThreadFollow: (t: string, provider: MailboxProvider, threadId: string, follow: boolean) =>
      req<{ following: boolean }>(
        `/v1/connectors/${provider}/watched/${encodeURIComponent(threadId)}`,
        t,
        { method: "PUT", body: JSON.stringify({ follow }) },
      ),
    // Pull-based "sync now": fetch recent items from the provider and ingest them.
    sync: (t: string, provider: string) =>
      req<{ ingested: number; fetched: number }>(
        `/v1/connectors/${provider}/sync`,
        t,
        { method: "POST" },
      ),
    // Slack connect onboarding status (docs/connectors/slack-onboarding.md §3):
    // live bot-membership + routing + capture + huddle state, server-derived so
    // the in-app checklist and the Slack-side surfaces never drift apart.
    slackOnboarding: (t: string) =>
      req<SlackOnboardingStatus>("/v1/connectors/slack/onboarding", t),
    // The backfill offer's accept path (chat-per-user plan §4.2). ONE request
    // carries every channel the member picked, deliberately: a POST per row
    // would be the O(rows) fan-out scripts/check_no_unbounded_fanout.sh
    // forbids, and someone in forty channels would feel it.
    //
    // 202 with a per-channel breakdown, not a bare ok: a channel the caller
    // has since left is `skipped` with its reason rather than failing the
    // whole request, because a stale page listing is the ordinary case.
    requestSlackBackfill: (
      t: string,
      channelIds: string[],
      windowDays?: number,
    ) =>
      req<SlackBackfillResult>("/v1/connectors/slack/backfill", t, {
        method: "POST",
        body: JSON.stringify({
          channel_ids: channelIds,
          ...(windowDays === undefined ? {} : { window_days: windowDays }),
        }),
      }),
    // Steady-state management view: one row per channel the bot is actually in,
    // with where it lands and whether huddles are being saved there.
    slackChannels: (t: string) =>
      req<SlackChannelsResponse>("/v1/connectors/slack/channels", t),
    // Real numbers for the confirmation dialog in front of EITHER channel
    // action below — call this BEFORE rendering a confirmation, never just to
    // discard the numbers. `targetGraphId` previews a prospective team-ocean
    // pick's seat count (only meaningful for a `kind: "team"` ocean; omit it
    // for every other pick).
    slackChannelImpact: (t: string, channelId: string, targetGraphId?: string) =>
      req<SlackChannelImpact>(
        `/v1/connectors/slack/channels/${encodeURIComponent(channelId)}/impact${
          targetGraphId ? `?target_graph_id=${encodeURIComponent(targetGraphId)}` : ""
        }`,
        t,
      ),
    // Re-route (or clear) a channel's destination. Moves FUTURE content only —
    // already-captured atoms never move, re-tag, or re-embed (see
    // `SlackDestinationPick`'s doc comment) — the UI must say so at the moment
    // of the decision, not just note it somewhere after the fact.
    setSlackChannelDestination: (t: string, channelId: string, pick: SlackDestinationPick) =>
      req<SetSlackChannelDestinationResponse>(
        `/v1/connectors/slack/channels/${encodeURIComponent(channelId)}/destination`,
        t,
        { method: "PUT", body: JSON.stringify(pick) },
      ),
    // Stop watching a channel. NOT a soft toggle: this actually calls Slack's
    // `conversations.leave` — the bot departs the channel for real, and
    // re-enabling requires re-inviting it in Slack afterward. `409` means the
    // workspace predates the `channels:leave` scope (see
    // `StopWatchingSlackChannelResponse`'s module doc) — the UI must surface
    // that as its own actionable state, never a generic failure.
    stopWatchingSlackChannel: (t: string, channelId: string) =>
      req<StopWatchingSlackChannelResponse>(
        `/v1/connectors/slack/channels/${encodeURIComponent(channelId)}`,
        t,
        { method: "DELETE" },
      ),
    // Google Chat watched-space picker (gsuite Train G4 PRs 3-5). Live
    // `spaces.list` under the CALLER's OWN connected `google_chat` token,
    // merged with local watch/opt-in state — see `GoogleChatSpace`'s own
    // doc comment for what each field means and why the two archetypes
    // never overlap. 404s when Google Chat isn't connected for this member
    // — callers treat that as "not connected".
    googleChatSpaces: (t: string) =>
      req<GoogleChatSpacesResponse>("/v1/connectors/google/chat/spaces", t),
    // Destination pick for a CONTAINER (`space_type: "SPACE"`) space.
    // UPDATE-only server-side: 404s with an actionable message ("space not
    // connected — add the CoralSwarm Chat app to it first") when the app has
    // never been added to this space — callers must surface that message
    // verbatim rather than a generic failure. Never call this against a
    // space whose `destination.kind !== "unrouted"` — see
    // `SetGoogleChatDestinationResponse`'s own doc comment on why that would
    // silently no-op rather than re-route.
    setGoogleChatSpaceDestination: (t: string, spaceId: string, pick: GoogleChatDestinationPick) =>
      req<SetGoogleChatDestinationResponse>(
        `/v1/connectors/google/chat/spaces/${encodeURIComponent(spaceId)}/destination`,
        t,
        { method: "POST", body: JSON.stringify(pick) },
      ),
    // Mailbox archetype (G4.5) — opt the CALLING member's own `GROUP_CHAT`/
    // `DIRECT_MESSAGE` space into their own private ocean. There is no
    // destination to pass; there is only ever one. 409 when Google Chat
    // isn't connected yet for this member.
    optInGoogleChatMailboxSpace: (t: string, spaceId: string) =>
      req<GoogleChatMailboxOptinResult>(
        `/v1/connectors/google/chat/spaces/${encodeURIComponent(spaceId)}/optin`,
        t,
        { method: "POST" },
      ),
    // Opts the calling member back out. 404 when they had never opted in
    // (already out, or never in — the row is provably gone either way).
    optOutGoogleChatMailboxSpace: (t: string, spaceId: string) =>
      req<{ active: false }>(
        `/v1/connectors/google/chat/spaces/${encodeURIComponent(spaceId)}/optin`,
        t,
        { method: "DELETE" },
      ),
    // GitHub connector (PR A) — docs/connectors/github.md §3.3–3.4, §3.8.
    // Every live installation this account can manage (org path + personal
    // path both land here, same `connector_installs` index).
    githubInstalls: (t: string) =>
      req<GithubInstallsResponse>("/v1/connectors/github/installs", t),
    // The selection page's read: every repo the App is currently granted for
    // one installation, plus this account's actual destination options
    // (which existing teams the caller admins, whether the org ocean is
    // open to them). Not paginated — an installation's grant is the whole
    // list, same as Slack's `conversations.list` page.
    githubRepos: (t: string, installationId: string) =>
      req<GithubReposResponse>(
        `/v1/connectors/github/repos?installation_id=${encodeURIComponent(installationId)}`,
        t,
      ),
    // One PUT for the whole table — every row's active/destination state at
    // once, not per-repo. 403 with `error: "org_destination_requires_admin"`
    // (see `GITHUB_ORG_DESTINATION_REQUIRES_ADMIN`) when any selection picks
    // `{kind:"org"}` and the caller isn't a CoralSwarm org admin; nothing in
    // the submission is saved in that case — the caller must resubmit with
    // personal/team in place of org for the offending row(s).
    setGithubRepos: (t: string, body: GithubReposPutBody) =>
      req<GithubReposResponse>("/v1/connectors/github/repos", t, {
        method: "PUT",
        body: JSON.stringify(body),
      }),
    // Pauses an installation (App uninstall from this side, effectively) —
    // never deletes ingested content (§3.8). Every repo under it keeps
    // whatever it already captured and shows "sync stopped since <date>".
    disconnectGithubInstall: (t: string, installationId: string) =>
      req<{ status: string }>(
        `/v1/connectors/github/installs/${encodeURIComponent(installationId)}`,
        t,
        { method: "DELETE" },
      ),
  },
  // Outlook auto-ingest workflows (CS-118). Microsoft-only today.
  workflows: {
    list: (t: string) =>
      req<{ workflows: Workflow[] }>("/v1/connectors/microsoft/workflows", t),
    create: (t: string, name: string, filter: WorkflowFilter) =>
      req<Workflow>("/v1/connectors/microsoft/workflows", t, {
        method: "POST",
        body: JSON.stringify({ name, filter }),
      }),
    setEnabled: (t: string, id: string, enabled: boolean) =>
      req<{ ok: boolean }>(`/v1/connectors/microsoft/workflows/${id}`, t, {
        method: "PATCH",
        body: JSON.stringify({ enabled }),
      }),
    remove: (t: string, id: string) =>
      req<{ ok: boolean }>(`/v1/connectors/microsoft/workflows/${id}`, t, {
        method: "DELETE",
      }),
  },
  // Microsoft Teams ingest (CS-119): browse + opt-in channels/chats.
  msTeams: {
    // Enrollment stays bound to this bearer. A 401 must not replay it with a
    // refreshed token from a newly selected workspace; the UI offers retry.
    onboarding: (t: string, ticket: string) =>
      req<MsOnboardingResult>("/v1/connectors/microsoft/teams/onboarding", t, {
        method: "POST",
        body: JSON.stringify({ ticket }),
      }, true),
    teams: (t: string) =>
      req<{ teams: MsTeam[] }>("/v1/connectors/microsoft/teams", t),
    channels: (t: string, teamId: string) =>
      req<{ channels: MsChannel[] }>(
        `/v1/connectors/microsoft/teams/channels?team_id=${encodeURIComponent(teamId)}`,
        t,
      ),
    chats: (t: string) =>
      req<{ chats: MsChat[] }>("/v1/connectors/microsoft/teams/chats", t),
    ingest: (t: string, body: MsIngestBody) =>
      req<{ id: string }>("/v1/connectors/microsoft/teams/ingest", t, {
        method: "POST",
        body: JSON.stringify(body),
      }),
    watched: (t: string) =>
      req<{ watched: MsWatched[] }>("/v1/connectors/microsoft/teams/watched", t),
    removeWatched: (t: string, id: string) =>
      req<{ ok: boolean }>(`/v1/connectors/microsoft/teams/watched/${id}`, t, {
        method: "DELETE",
      }),
    syncTranscripts: (t: string) =>
      req<{ ingested: number; fetched: number }>(
        "/v1/connectors/microsoft/teams/transcripts/sync",
        t,
        { method: "POST" },
      ),
    connectMeeting: (t: string, return_path: string) =>
      req<{ authorization_url: string }>("/v1/connectors/microsoft/teams/transcripts/connect", t,
        { method: "POST", body: JSON.stringify({ return_path }) }, true),
    meetingContext: (t: string, target: { teams_meeting_id: string; chat_id: string }, signal?: AbortSignal) =>
      req<{ meeting_id: string }>("/v1/connectors/microsoft/teams/transcripts/meeting-context", t,
        { method: "POST", body: JSON.stringify(target), signal }, true),
    meetingStatus: (t: string, target: { join_url: string } | { teams_meeting_id: string; chat_id: string } | { capture_id: string }, signal?: AbortSignal) =>
      req<{
        meeting_id: string; title: string | null; occurrence_start: string; occurrence_end: string; availability_window_open: boolean; connection_required: boolean;
        /** Absent on older backends: treat as "manual". */
        auto_transcription?: AutoTranscription;
        /** Teams pane: recording or transcription is on in the meeting chat. Absent = false. */
        recording_active?: boolean;
        capture: null | { capture_id: string; status: "active" | "stopped"; last_checked_at: string | null; last_retrieved_at: string | null; retrieved_transcript_count: number; last_error_code: string | null };
      }>("/v1/connectors/microsoft/teams/transcripts/meeting-status", t,
        { method: "POST", body: JSON.stringify(target), signal }, true),
    meetingCaptures: (t: string, signal?: AbortSignal) =>
      req<{ captures: { capture_id: string; meeting_id: string; created_at: string; status: "active" }[] }>(
        "/v1/connectors/microsoft/teams/transcripts/captures", t, { signal }, true),
    captureMeeting: (t: string, target: { join_url: string } | { meeting_id: string; chat_id?: string } | { meeting_ticket: string }) =>
      req<{ meeting_id: string; capture_id: string; status: "waiting_for_transcript"; auto_transcription?: AutoTranscription }>(
        "/v1/connectors/microsoft/teams/transcripts/capture", t,
        { method: "POST", body: JSON.stringify(target) }, true),
    stopMeetingSelection: (t: string, capture_id: string) =>
      req<{ status: "revoked" }>("/v1/connectors/microsoft/teams/transcripts/capture", t,
        { method: "DELETE", body: JSON.stringify({ capture_id }) }, true),
    stopMeetingCapture: (t: string, meeting_id: string) =>
      req<{ status: "revoked" }>("/v1/connectors/microsoft/teams/transcripts/capture", t,
        { method: "DELETE", body: JSON.stringify({ meeting_id }) }, true),
    syncMeeting: (t: string, target: { join_url: string } | { meeting_id: string }) =>
      req<{ meeting_id: string; ingested: number; fetched: number }>(
        "/v1/connectors/microsoft/teams/transcripts/sync-meeting",
        t,
        { method: "POST", body: JSON.stringify(target) },
        true,
      ),
  },
  // Generic background-job polling (`GET /v1/jobs/{id}`, feat/promote-async-batch).
  // Works for every job `kind`, not just `PromoteNode` — a job kind that never
  // reports granular progress leaves `progress_current`/`progress_total` `null`
  // (never `0`; see `JobStatus`'s own doc comment).
  jobs: {
    get: (t: string, jobId: string) => req<JobStatus>(`/v1/jobs/${jobId}`, t),
  },
  // Gmail Add-on pairing (backend/src/routes/addin.rs) — the AUTHENTICATED
  // half only. `pair/start` and `pair/poll` are called by Apps Script itself
  // (unauthenticated, `UrlFetchApp`), never from this web app.
  addin: {
    approve: (t: string, code: string) =>
      req<{ approved: boolean }>("/v1/addin/pair/approve", t, {
        method: "POST",
        body: JSON.stringify({ code }),
      }),
    /** Microsoft add-in pairing (Teams + Outlook, `kind=microsoft`): what the
     *  approving user may bind this code to. `GET /v1/addin/pair/info?code=`.
     *  The Gmail flow never calls this. */
    pairInfo: (t: string, code: string) =>
      req<AddinPairInfo>(`/v1/addin/pair/info?code=${encodeURIComponent(code)}`, t),
    /** Microsoft add-in approval: same route as Gmail's, plus the workspace the
     *  user picked in the browser (`account_id`). */
    approveMicrosoft: (t: string, code: string, accountId: string) =>
      req<{ approved: boolean }>("/v1/addin/pair/approve", t, {
        method: "POST",
        body: JSON.stringify({ code, account_id: accountId }),
      }),
    /** The caller's OWN live add-on tokens — never anyone else's, and never a
     *  revoked one (the backend filters `revoked_at IS NULL` in the query, so
     *  a revoked token is simply absent rather than flagged).
     *
     *  400 in an org context, like `approve` above: add-on tokens are
     *  personal-plane only (`addin_tokens.owner_user_id` is NOT NULL). Call
     *  sites must gate on the personal plane rather than rendering that 400 —
     *  see `components/connectors/GmailAddonTokens.tsx`. */
    listTokens: (t: string) => req<{ tokens: AddinToken[] }>("/v1/addin/tokens", t),
    /** 204 on success (`req` resolves to `undefined` for a 204 body); 404
     *  covers "no such id", "already revoked" AND "belongs to someone else"
     *  identically — see `revoke_token`'s own doc comment on why those three
     *  are deliberately indistinguishable. */
    revokeToken: (t: string, id: string) =>
      req<void>(`/v1/addin/tokens/${encodeURIComponent(id)}`, t, { method: "DELETE" }),
  },
};
