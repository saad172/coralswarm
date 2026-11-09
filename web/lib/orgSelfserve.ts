// Self-serve team sign-up: the pure half of the create form, the Stripe
// return page and the trial banner. Every decision the screens make lives
// here so it can be unit tested without rendering anything:
//
//   * create-form validation (mirrors A2's `validate_name` / seat bounds);
//   * the per-attempt `Idempotency-Key` and where it is remembered;
//   * plain-words copy for every error code A2 and #1521 return;
//   * the setup state machine: `GET /v1/orgs/{org}/provisioning` → what the
//     return page shows, which buttons it offers and whether it keeps polling;
//   * the poll backoff;
//   * the Checkout copy #1521 says the form must show;
//   * the trial banner from `GET /v1/billing/summary` → `trial`.
//
// Wire shapes are in lib/api.ts (`OrgCreateResult`, `OrgSetupStatus`,
// `BillingTrial`). Backend contract: PR #1533 (A2) and #1521 (C′).

import { ApiError, type BillingTrial, type OrgSetupStatus } from "./api";

// ── Limits (A2 `orgs_create.rs`, #1521 `billing::org_trial`) ────────────────

export const MIN_SEATS = 2;
export const MAX_SEATS = 1_000;
export const DEFAULT_SEATS = 2;
export const NAME_MIN_CHARS = 2;
export const NAME_MAX_CHARS = 120;
export const TRIAL_CREDITS = 3_000;
export const TRIAL_DAYS = 90;
export const SUPPORT_EMAIL = "person@example.com";

/** A2 refuses a team name that reads like a personal org in Clerk's picker. */
const PERSONAL_ORG_NAME_SUFFIX = "'s organization";

// ── Create form ─────────────────────────────────────────────────────────────

/** Trim and collapse runs of whitespace: names that differ only in spacing
 *  are the same sign-up (and the same idempotency key). */
export function normalizeOrgName(raw: string): string {
  // NFKC first, as A2 does, so the length counted here is the server's.
  return raw.normalize("NFKC").trim().replace(/\s+/g, " ");
}

export interface CreateFormErrors {
  name?: string;
  seats?: string;
}

export type CreateFormResult =
  | { ok: true; name: string; seats: number }
  | { ok: false; errors: CreateFormErrors };

export const NAME_ERROR = `Use ${NAME_MIN_CHARS} to ${NAME_MAX_CHARS} characters, with at least one letter or number.`;
/** Control and invisible format characters (zero-width spaces, direction
 *  marks): A2 refuses them, and a person can't see what to remove. */
export const NAME_HIDDEN_CHARS_ERROR =
  "The name has hidden characters in it, like zero-width spaces or text-direction marks, often from copying and pasting. Type the name in again.";
/** A2's `invalid_name` doesn't say which rule failed, so its copy names both. */
export const NAME_SERVER_ERROR = `Use ${NAME_MIN_CHARS} to ${NAME_MAX_CHARS} characters, with at least one letter or number and no hidden characters (like zero-width spaces).`;
export const NAME_PERSONAL_ERROR = "Names ending in “'s organization” are kept for personal accounts. Pick another name.";
export const SEATS_ERROR = `Pick between ${MIN_SEATS} and ${MAX_SEATS.toLocaleString("en-US")} seats.`;

/** Client-side mirror of A2's checks, so the common mistakes never make a
 *  round trip. The server stays the authority (its 400s map to the same copy
 *  in `createErrorView`). */
export function validateCreateForm(input: { name: string; seats: string }): CreateFormResult {
  const errors: CreateFormErrors = {};
  const name = normalizeOrgName(input.name);
  const chars = [...name].length;
  if (/[\p{Cc}\p{Cf}]/u.test(name)) {
    errors.name = NAME_HIDDEN_CHARS_ERROR;
  } else if (chars < NAME_MIN_CHARS || chars > NAME_MAX_CHARS || !/[\p{L}\p{N}]/u.test(name)) {
    errors.name = NAME_ERROR;
  } else if (name.toLowerCase().endsWith(PERSONAL_ORG_NAME_SUFFIX)) {
    errors.name = NAME_PERSONAL_ERROR;
  }
  const seatsText = input.seats.trim();
  const seats = /^\d+$/.test(seatsText) ? Number(seatsText) : NaN;
  if (!Number.isInteger(seats) || seats < MIN_SEATS || seats > MAX_SEATS) errors.seats = SEATS_ERROR;
  if (errors.name || errors.seats) return { ok: false, errors };
  return { ok: true, name, seats };
}

// ── The sign-up attempt (idempotency) ───────────────────────────────────────

/** One "create this team" attempt. `key` is the `Idempotency-Key`: minted
 *  once per (name, seats) and resent on every retry, so a double click, a
 *  reload or a dropped response gets the same org and the same Checkout back
 *  instead of a second org. A2 answers 409 `idempotency_key_reused` if one key
 *  is sent with a different name or seat count, so changing either mints a
 *  new attempt. `orgId` is filled in once the server has answered. */
export interface SignupAttempt {
  key: string;
  name: string;
  seats: number;
  orgId?: string;
}

export type KV = Pick<Storage, "getItem" | "setItem" | "removeItem">;

/** Where one signed-in person's attempts live. Keyed by user id, so a second
 *  person signing in on the same browser never sees (or resumes, or clears)
 *  the first person's sign-up. */
export interface AttemptStore {
  kv: KV;
  user: string;
}

/** localStorage, not sessionStorage: the attempt has to survive the trip to
 *  Stripe and back, and a person who closes the tab and comes back tomorrow
 *  to "resume payment" should still get the same org. */
const ATTEMPTS_PREFIX = "cs_team_signup_attempts:";
/** Newest first; more than one, so starting a second team doesn't forget a
 *  first one that is still being paid for or set up. */
export const MAX_STORED_ATTEMPTS = 5;

/** This browser's attempt store for `userId`, or `null` (no user yet, or no
 *  storage): the return URL carries the attempt then. */
export function signupStore(userId: string | null | undefined): AttemptStore | null {
  if (!userId) return null;
  try {
    return typeof window === "undefined" ? null : { kv: window.localStorage, user: userId };
  } catch {
    return null;
  }
}

function parseAttempt(v: unknown): SignupAttempt | null {
  if (!v || typeof v !== "object") return null;
  const a = v as Partial<SignupAttempt>;
  if (typeof a.key !== "string" || typeof a.name !== "string" || typeof a.seats !== "number") return null;
  return { key: a.key, name: a.name, seats: a.seats, orgId: typeof a.orgId === "string" ? a.orgId : undefined };
}

/** Every stored attempt for this user, newest first. */
export function readAttempts(store: AttemptStore | null): SignupAttempt[] {
  if (!store) return [];
  try {
    const raw = store.kv.getItem(ATTEMPTS_PREFIX + store.user);
    if (!raw) return [];
    const list: unknown = JSON.parse(raw);
    if (!Array.isArray(list)) return [];
    return list.map(parseAttempt).filter((a): a is SignupAttempt => a !== null);
  } catch {
    return [];
  }
}

function writeAttempts(store: AttemptStore | null, list: SignupAttempt[]): void {
  if (!store) return;
  try {
    if (list.length === 0) store.kv.removeItem(ATTEMPTS_PREFIX + store.user);
    else store.kv.setItem(ATTEMPTS_PREFIX + store.user, JSON.stringify(list.slice(0, MAX_STORED_ATTEMPTS)));
  } catch {
    // Storage full or blocked: the return URL carries the attempt too.
  }
}

/** The newest attempt (what the form fills itself in from). */
export function readAttempt(store: AttemptStore | null): SignupAttempt | null {
  return readAttempts(store)[0] ?? null;
}

export function attemptByKey(store: AttemptStore | null, key: string): SignupAttempt | null {
  return readAttempts(store).find((a) => a.key === key) ?? null;
}

/** The attempt this browser made for `orgId`, if any: the proof that this
 *  person started that team HERE (and so may resume its payment, and be
 *  switched into it without asking). */
export function attemptByOrg(store: AttemptStore | null, orgId: string): SignupAttempt | null {
  return readAttempts(store).find((a) => a.orgId === orgId) ?? null;
}

/** A2's key shape: 8 to 200 characters of `[A-Za-z0-9_.:-]`. */
export function isValidIdempotencyKey(k: string): boolean {
  return k.length >= 8 && k.length <= 200 && /^[A-Za-z0-9_.:-]+$/.test(k);
}

/** The attempt for (name, seats): the stored one for the same name and seat
 *  count (moved to the front), otherwise a new one. Other attempts are kept. */
export function attemptFor(store: AttemptStore | null, name: string, seats: number, mint: () => string): SignupAttempt {
  const n = normalizeOrgName(name);
  const list = readAttempts(store);
  const prev = list.find((a) => a.name === n && a.seats === seats && isValidIdempotencyKey(a.key));
  const next: SignupAttempt = prev ?? { key: mint(), name: n, seats };
  writeAttempts(store, [next, ...list.filter((a) => a !== prev)]);
  return next;
}

/** Remember an attempt (with its org, once known) at the front. */
export function rememberAttempt(store: AttemptStore | null, a: SignupAttempt): void {
  writeAttempts(store, [a, ...readAttempts(store).filter((x) => x.key !== a.key)]);
}

export function recordAttemptOrg(store: AttemptStore | null, key: string, orgId: string): void {
  const list = readAttempts(store);
  if (!list.some((a) => a.key === key)) return;
  writeAttempts(
    store,
    list.map((a) => (a.key === key ? { ...a, orgId } : a)),
  );
}

/** Forget one attempt, by its key. Every other attempt stays. */
export function dropAttempt(store: AttemptStore | null, key: string): void {
  writeAttempts(
    store,
    readAttempts(store).filter((a) => a.key !== key),
  );
}

/** Forget the attempt for `orgId` (it is ready, or being started over). */
export function dropAttemptForOrg(store: AttemptStore | null, orgId: string): void {
  writeAttempts(
    store,
    readAttempts(store).filter((a) => a.orgId !== orgId),
  );
}

// ── Return URLs ─────────────────────────────────────────────────────────────

export const CREATE_PATH = "/onboarding/team";
export const RETURN_PATH = "/onboarding/team/return";
export const CANCELED_PATH = "/onboarding/team/canceled";

/** The query both return pages get. It carries the whole attempt (not just
 *  an org id, which does not exist yet when the URLs are built), so a page
 *  that has lost localStorage (another browser) can replay `POST /v1/orgs` with the same key and get the org
 *  back without creating a second one. */
export function attemptQuery(a: Pick<SignupAttempt, "key" | "name" | "seats">): string {
  const q = new URLSearchParams({ k: a.key, n: a.name, s: String(a.seats) });
  return q.toString();
}

/** `?org=<clerk org id>`: a return page opened for a known team (a
 *  `signup_complete` answer, or the account switcher for a team still being
 *  set up). `null` when absent or not an org id. */
export function orgFromQuery(params: Pick<URLSearchParams, "get">): string | null {
  const org = params.get("org") ?? "";
  return /^org_[A-Za-z0-9]{6,64}$/.test(org) ? org : null;
}

/** The return page for a known team, keeping the attempt when there is one. */
export function statusQuery(orgId: string, a?: Pick<SignupAttempt, "key" | "name" | "seats"> | null): string {
  const q = new URLSearchParams(a ? attemptQuery(a) : "");
  q.set("org", orgId);
  return q.toString();
}

/** The switcher's choice for org `orgId`: the setup page's query when it
 *  is a team still being set up AND sign-up is on, else `null` (switch into
 *  it normally). Known from this browser's attempts, or from bootstrap's
 *  optional `pending_orgs` (#1540) when the backend sends it. */
export function pendingTeamTarget(
  orgId: string,
  known: { selfserve: boolean; stored: readonly SignupAttempt[]; serverPending?: readonly string[] | null },
): string | null {
  if (!known.selfserve) return null;
  const mine = known.stored.find((a) => a.orgId === orgId);
  if (mine) return statusQuery(orgId, mine);
  return known.serverPending?.includes(orgId) ? statusQuery(orgId, null) : null;
}

/** The attempt named by a return page's query, or `null` when it is missing
 *  or malformed. */
export function attemptFromQuery(params: Pick<URLSearchParams, "get">): Omit<SignupAttempt, "orgId"> | null {
  const key = params.get("k") ?? "";
  const name = normalizeOrgName(params.get("n") ?? "");
  const seats = Number(params.get("s") ?? "");
  if (!isValidIdempotencyKey(key) || !name || !Number.isInteger(seats) || seats < MIN_SEATS) return null;
  return { key, name, seats };
}

// ── Waiting ─────────────────────────────────────────────────────────────────

/** "about 4 minutes", "45 seconds". */
export function formatWait(seconds: number): string {
  const s = Math.max(1, Math.ceil(seconds));
  if (s < 60) return `${s} second${s === 1 ? "" : "s"}`;
  const m = Math.ceil(s / 60);
  if (m < 60) return `about ${m} minute${m === 1 ? "" : "s"}`;
  const h = Math.ceil(m / 60);
  return `about ${h} hour${h === 1 ? "" : "s"}`;
}

/** `retry_after_s` from an A2 429 (body first, then nothing). */
export function retryAfterSeconds(err: unknown): number | undefined {
  if (!(err instanceof ApiError)) return undefined;
  const raw = err.body?.retry_after_s;
  return typeof raw === "number" && Number.isFinite(raw) && raw > 0 ? raw : undefined;
}

// ── Create errors ───────────────────────────────────────────────────────────

export interface ErrorView {
  message: string;
  /** Which form field the message belongs under, when it is about one. */
  field?: "name" | "seats";
  /** Seconds until trying again can work. The button stays off until then. */
  retryAfterS?: number;
  /** Keep this attempt's idempotency key. True when the server may have made
   *  the org (a 5xx, a dropped connection): trying again must return that
   *  org, not make a second one. False when the server definitely refused,
   *  or when the key itself is no longer usable. */
  keepAttempt: boolean;
  /** Offer a link to Settings → Account (verify the email there). */
  verifyEmail?: boolean;
  /** Resubmit by itself, with the SAME key, after this many ms (A2's 409
   *  `create_in_progress`: a parallel submit of this attempt is still
   *  running, and the repeat will be answered as its replay). */
  autoRetryMs?: number;
  /** This sign-up can't be continued; offer "Start again" (a new attempt). */
  startOver?: boolean;
  /** The team is already paid for (A2's 409 `signup_complete`): follow its
   *  setup on the return page instead of showing an error. */
  statusOrgId?: string;
  /** Payment already went through: link to the team's setup page. */
  statusLink?: boolean;
}

/** How long to wait before resubmitting after `create_in_progress`, and how
 *  many times, before giving up and showing the message. */
export const CREATE_IN_PROGRESS_RETRY_MS = 2_000;
export const CREATE_IN_PROGRESS_MAX_RETRIES = 5;

const UNREACHABLE = "Couldn't reach CoralSwarm. Check your connection and try again. Trying again won't create a second team.";
const SIGNED_OUT = "You're signed out. Sign in, then try again.";
const BROWSER_ONLY =
  "Creating a team has to be done in the CoralSwarm web app in your browser. Open it there and try again.";
const CHECKOUT_URL_REFUSED = "Couldn't start checkout from this address. Reload the page and try again.";

function codeOf(err: ApiError): string | null {
  return err.code ?? null;
}

/** Plain copy for every `POST /v1/orgs` refusal (A2's table). */
export function createErrorView(err: unknown): ErrorView {
  if (!(err instanceof ApiError)) return { message: UNREACHABLE, keepAttempt: true };
  const wait = retryAfterSeconds(err);
  switch (codeOf(err)) {
    case "invalid_name":
      return { message: NAME_SERVER_ERROR, field: "name", keepAttempt: false };
    case "invalid_seats":
      return { message: SEATS_ERROR, field: "seats", keepAttempt: false };
    case "return_urls_required":
    case "idempotency_key_required":
      return { message: CHECKOUT_URL_REFUSED, keepAttempt: false };
    case "email_unverified":
      return {
        message: "Verify your email address before you create a team. You can do that in Settings › Account.",
        verifyEmail: true,
        keepAttempt: false,
      };
    case "identity_required":
      return { message: "Sign in with your CoralSwarm account to create a team.", keepAttempt: false };
    case "interactive_session_required":
      return { message: BROWSER_ONLY, keepAttempt: false };
    case "idempotency_key_reused":
      return { message: "This sign-up changed after it started. Check the name and seats, then try again.", keepAttempt: false };
    case "create_in_progress":
      // Resend the SAME key after the server's `retry_after_s` (it also
      // sends `Retry-After`); the answer is then the first request's replay.
      return {
        message: "This team is still being created from another click or tab. Wait a moment, then try again. It won't create a second team.",
        keepAttempt: true,
        autoRetryMs: wait ? Math.min(wait, 30) * 1000 : CREATE_IN_PROGRESS_RETRY_MS,
      };
    case "signup_complete": {
      const org = typeof err.body?.org_id === "string" ? err.body.org_id : undefined;
      return {
        message: org
          ? "This team is already paid for. Opening its setup…"
          : "This team is already paid for. It appears in your account switcher as soon as it's set up.",
        keepAttempt: true,
        statusOrgId: org,
      };
    }
    case "checkout_complete":
      return {
        message: "Payment for this team already went through. It appears in your account switcher as soon as it's set up.",
        keepAttempt: true,
        statusLink: true,
      };
    case "checkout_expired":
      return {
        message: "That sign-up wasn't paid within 24 hours, so it was removed. Start again to create the team.",
        keepAttempt: false,
        startOver: true,
      };
    case "org_create_refused":
      return { message: "We couldn't create a team with that name. Try a different name.", field: "name", keepAttempt: false };
    case "too_many_pending_orgs":
      return {
        message: wait
          ? `You already have teams waiting for payment. Finish paying for one of them, or try again in ${formatWait(wait)}.`
          : "You already have teams waiting for payment. Finish paying for one of them, or try again later.",
        retryAfterS: wait,
        keepAttempt: false,
      };
    case "org_create_rate_limited":
      return {
        message: wait
          ? `You've created several teams recently. You can create another in ${formatWait(wait)}.`
          : "You've created several teams recently. Try again later.",
        retryAfterS: wait,
        keepAttempt: false,
      };
    case "org_create_unavailable":
      // #1556: Clerk refused for a reason that isn't the name (our
      // configuration, a plan or quota limit). No other name would work, and
      // the server already logged an alert.
      return {
        message: `We couldn't create your team right now. Try again later. Trying again won't create a second team. If it keeps happening, contact ${SUPPORT_EMAIL}.`,
        keepAttempt: true,
      };
    case "identity_unavailable":
      return {
        message: "Our sign-in service didn't answer. Try again in a moment. Trying again won't create a second team.",
        keepAttempt: true,
      };
  }
  if (err.status === 401) return { message: SIGNED_OUT, keepAttempt: true };
  if (err.status === 404) return { message: "Creating a team isn't open yet.", keepAttempt: false };
  // Any other "gone": this attempt can't be continued (an org being cleaned
  // up, or a repeat more than 24 hours after the first). A new key starts a
  // new sign-up.
  if (err.status === 410) {
    return { message: "This sign-up can't be continued any more. Start again to create the team.", keepAttempt: false, startOver: true };
  }
  // An unrecognised conflict is a refusal of THIS attempt: a new attempt
  // (new key) is the way forward, never a resend of this one.
  if (err.status === 409) {
    return { message: "This sign-up can't be continued. Start again to create the team.", keepAttempt: false, startOver: true };
  }
  // #1521's return-URL check answers a bare 400 (no code). It runs AFTER
  // the org exists (inside the Checkout step), so the key is kept: a retry
  // must get that org back, not make a second one. Same as the resume path.
  if (err.status === 400) return { message: CHECKOUT_URL_REFUSED, keepAttempt: true };
  if (err.status === 429) {
    return {
      message: wait ? `Too many tries. Wait ${formatWait(wait)}, then try again.` : "Too many tries. Wait a minute, then try again.",
      retryAfterS: wait,
      keepAttempt: true,
    };
  }
  if (err.status >= 400 && err.status < 500) {
    return { message: `CoralSwarm couldn't create this team. Try again, or email ${SUPPORT_EMAIL}.`, keepAttempt: false };
  }
  return { message: UNREACHABLE, keepAttempt: true };
}

/** Plain copy for a failed "Resume payment" (`POST /v1/orgs/{org}/trial-checkout`). */
export function resumeErrorView(err: unknown): ErrorView & { startOver?: boolean; alreadyPaid?: boolean; refresh?: boolean } {
  if (!(err instanceof ApiError)) return { message: "Couldn't reach CoralSwarm. Check your connection and try again.", keepAttempt: true };
  switch (codeOf(err)) {
    case "checkout_expired":
      return {
        message: "This sign-up wasn't paid within 24 hours, so payment can't be resumed. Create the team again to start over.",
        keepAttempt: false,
        startOver: true,
      };
    case "checkout_complete":
      return {
        message: "Payment for this team already went through. Setup is under way.",
        keepAttempt: true,
        alreadyPaid: true,
        statusLink: true,
      };
    // The org has no self-serve sign-up record at all (an older team, made
    // before self-serve). NOT "already paid": nothing here says it was.
    case "no_pending_checkout":
      return {
        message: "This team has no sign-up waiting for payment, so there's nothing to resume here. If it's an existing team, open it from the account switcher.",
        keepAttempt: true,
        refresh: true,
      };
    case "not_a_team_org":
      return { message: "This is a personal account, not a team, so there's no team payment to finish.", keepAttempt: false };
    case "org_not_found":
      return { message: "Only the person who created this team can resume its payment.", keepAttempt: false };
    case "invalid_seats":
      return { message: SEATS_ERROR, keepAttempt: false, startOver: true };
    case "interactive_session_required":
      return { message: BROWSER_ONLY, keepAttempt: true };
    case "return_urls_required":
      return { message: CHECKOUT_URL_REFUSED, keepAttempt: true };
  }
  if (err.status === 401) return { message: SIGNED_OUT, keepAttempt: true };
  if (err.status === 400) return { message: CHECKOUT_URL_REFUSED, keepAttempt: true };
  if (err.status === 410) {
    return { message: "This sign-up can't be continued any more. Start again to create the team.", keepAttempt: false, startOver: true };
  }
  return { message: "Couldn't open payment. Try again in a moment.", keepAttempt: true };
}

/** Plain copy for a failed "Retry setup" (`POST …/provisioning/retry`). */
export function retryErrorView(err: unknown): ErrorView & { refresh?: boolean } {
  if (!(err instanceof ApiError)) return { message: "Couldn't reach CoralSwarm. Check your connection and try again.", keepAttempt: true };
  const wait = retryAfterSeconds(err);
  switch (codeOf(err)) {
    case "nothing_to_retry":
      return { message: "Setup isn't stopped, so there's nothing to retry. This page updates as it goes.", keepAttempt: true, refresh: true };
    case "already_set_up":
      return { message: "Your team is already set up.", keepAttempt: true, refresh: true };
    case "not_retryable":
      return {
        message: `Setup can't be restarted from here. Email ${SUPPORT_EMAIL} and we'll finish it for you.`,
        keepAttempt: true,
      };
    case "retry_rate_limited":
      return {
        message: wait
          ? `Setup was restarted a moment ago. You can try again in ${formatWait(wait)}.`
          : "Setup was restarted a moment ago. Wait a few minutes before trying again.",
        retryAfterS: wait,
        keepAttempt: true,
      };
    case "retry_limit_reached":
      return {
        message: `Setup has been restarted as many times as it can be. Email ${SUPPORT_EMAIL} and we'll finish it for you.`,
        keepAttempt: true,
      };
    case "org_not_found":
      return { message: "Only an admin of this team can restart its setup.", keepAttempt: true };
  }
  if (err.status === 401) return { message: SIGNED_OUT, keepAttempt: true };
  return { message: `Couldn't restart setup. Try again, or email ${SUPPORT_EMAIL}.`, keepAttempt: true };
}

// ── The setup state machine ─────────────────────────────────────────────────

/** The three stages the progress rail shows. */
export const SETUP_STAGES = ["Payment", "Setting up", "Ready"] as const;

export type SetupKind =
  /** No answer yet. */
  | "checking"
  /** Back from Stripe; the webhook hasn't landed yet. */
  | "confirming_payment"
  /** A payment is settling (slow card / bank, or a repeat card's first charge). */
  | "payment_settling"
  /** Not paid, and "Resume payment" still works. */
  | "resume"
  /** `awaiting_payment` for a team this browser didn't start: never offer
   *  to pay for it (it may be an older team that simply has no sign-up
   *  record, which reads as `awaiting_payment` too). */
  | "not_ours"
  /** Not paid within 24 hours: start over. */
  | "abandoned"
  | "canceled"
  /** The database is being built. */
  | "provisioning"
  /** Waiting for a newer server build before setup can carry on. */
  | "updating"
  /** Done: switch to the team. */
  | "ready"
  | "failed_retryable"
  | "failed"
  /** 404: not this person's team (or the feature is off). */
  | "not_found";

export interface SetupView {
  kind: SetupKind;
  /** Index into `SETUP_STAGES` of the stage in progress (3 = all done). */
  stage: number;
  title: string;
  body: string;
  canResume: boolean;
  canRetry: boolean;
  canStartOver: boolean;
  /** Offer an "Open <team>" button rather than switching by itself. */
  canOpen: boolean;
  /** Keep polling. */
  poll: boolean;
}

/** Which page is asking: Stripe's success_url, or its cancel_url. */
export type ReturnPage = "return" | "canceled";

export interface SetupInput {
  status: OrgSetupStatus | null;
  /** The status read was refused with a 404 (`org_not_found`). */
  notFound?: boolean;
  page: ReturnPage;
  now: number;
  /** How long this page has been waiting, for the post-Stripe grace. */
  elapsedMs: number;
  /** This browser started this team's sign-up (a stored attempt for it, or
   *  one the person just confirmed). Default `true`. When `false` the page
   *  never offers payment and never switches by itself. */
  owned?: boolean;
}

/** How long the success page waits for Stripe's webhook before it offers
 *  "Resume payment" on an org still `awaiting_payment`. Checkout redirects
 *  to success_url before the webhook lands, so a bare `awaiting_payment` on
 *  arrival almost always means "paid, not recorded yet". */
export const PAYMENT_CONFIRM_GRACE_MS = 45_000;

/** Human labels for A1's job steps (`step` while `provisioning`). An unknown
 *  step reads as plain "setting up" rather than a guess. */
const STEP_LABELS: Record<string, string> = {
  queued: "Getting ready to set up your team",
  verify_org: "Checking your team",
  create_database: "Creating its own private database",
  insert_routing: "Connecting the database to your team",
  migrate: "Setting up the database",
  contract: "Preparing your team's memory",
  fingerprint: "Sealing the database to your team",
  provision_dek: "Creating its encryption key",
  activate: "Opening it to your team",
};

export function stepLabel(step: string | null | undefined): string {
  return (step && STEP_LABELS[step]) || "Setting up your team";
}

function view(kind: SetupKind, stage: number, title: string, body: string, extra: Partial<SetupView> = {}): SetupView {
  return { kind, stage, title, body, canResume: false, canRetry: false, canStartOver: false, canOpen: false, poll: false, ...extra };
}

/** `GET /v1/orgs/{org}/provisioning` → what the return page shows. */
export function setupView(input: SetupInput): SetupView {
  const { status, page, now, elapsedMs } = input;
  const owned = input.owned !== false;
  if (input.notFound) {
    return view(
      "not_found",
      0,
      "We can't show this team's setup",
      "Only the person who created a team, or one of its admins, can see its setup. If that's you, check you're signed in with the same account.",
    );
  }
  if (!status) return view("checking", 0, "Checking on your team", "One moment while we check where your team is.", { poll: true });

  switch (status.status) {
    case "ready":
      return owned
        ? view("ready", 3, "Your team is ready", "Opening it now.")
        : view("ready", 3, "This team is ready", "Open it to start using it.", { canOpen: true });
    case "provisioning":
      if (status.waiting_for === "payment") {
        return view(
          "payment_settling",
          0,
          "Waiting for your payment",
          "Your payment is still going through. Some cards and banks take a few minutes. Setup starts as soon as it clears, and this page updates by itself.",
          { poll: true },
        );
      }
      if (status.waiting_for === "update") {
        return view(
          "updating",
          1,
          "We're updating CoralSwarm",
          "We're updating, this takes a few minutes. Setup carries on by itself as soon as the update is done, so you can leave this page open or come back later.",
          { poll: true },
        );
      }
      return view("provisioning", 1, "Setting up your team", `${stepLabel(status.step)}. This usually takes a minute or two, and you can leave this page; setup keeps going.`, {
        poll: true,
      });
    case "failed":
      return status.retryable
        ? view(
            "failed_retryable",
            1,
            "Setup didn't finish",
            "Something went wrong while we were building your team. Retrying picks up where it stopped, and nothing that already finished is done twice.",
            { canRetry: true },
          )
        : view(
            "failed",
            1,
            "Setup didn't finish",
            `Something went wrong while we were building your team, and it can't be restarted from here. Email ${SUPPORT_EMAIL} and we'll finish it for you.`,
          );
    case "canceled":
      return view(
        "canceled",
        0,
        "This team's subscription was canceled",
        "The subscription was canceled before setup finished, so the team wasn't set up and nothing was charged. Create the team again if you still want it.",
        { canStartOver: true },
      );
    case "payment_abandoned":
      return view(
        "abandoned",
        0,
        "This sign-up has expired",
        "Payment wasn't finished within 24 hours, so this sign-up was closed and nothing was charged. Create the team again to start over.",
        { canStartOver: true },
      );
    case "awaiting_payment": {
      if (status.waiting_for === "payment") {
        return view(
          "payment_settling",
          0,
          "Waiting for your payment",
          "Your payment is still going through. Some cards and banks take a few minutes. Setup starts as soon as it clears, and this page updates by itself.",
          { poll: true },
        );
      }
      const until = status.resume_payment_until ? Date.parse(status.resume_payment_until) : NaN;
      const expired = Number.isFinite(until) && until <= now;
      if (expired) {
        return view(
          "abandoned",
          0,
          "This sign-up has expired",
          "Payment wasn't finished within 24 hours, so this sign-up was closed and nothing was charged. Create the team again to start over.",
          { canStartOver: true },
        );
      }
      if (!owned) {
        return view(
          "not_ours",
          0,
          "This team's payment can't be finished here",
          "Only the browser that started a team's sign-up can finish paying for it. If you started it somewhere else, go back there. If this is a team you already use, open it.",
          { canOpen: true, poll: true },
        );
      }
      if (page === "return" && elapsedMs < PAYMENT_CONFIRM_GRACE_MS) {
        return view("confirming_payment", 0, "Confirming your payment", "Stripe is letting us know your card was added. This usually takes a few seconds.", {
          poll: true,
        });
      }
      const deadline = Number.isFinite(until) ? ` You can finish it until ${formatDeadline(until)}.` : "";
      return view(
        "resume",
        0,
        page === "canceled" ? "Payment wasn't finished" : "We haven't received your payment yet",
        page === "canceled"
          ? `Your team is saved, but it won't be set up until a card is added. Nothing has been charged.${deadline}`
          : `If you closed Stripe's page before finishing, pick up where you left off. Nothing has been charged.${deadline}`,
        { canResume: true, poll: true },
      );
    }
  }
  // An unknown status from a newer backend: say nothing wrong, keep asking.
  return view("checking", 0, "Checking on your team", "One moment while we check where your team is.", { poll: true });
}

/** The seats "Resume payment" buys: the server's own record of this
 *  sign-up (A2's status body `seats`) first; the attempt this browser kept
 *  only for an older backend that omits it. `null`: nothing to resume with. */
export function resumeSeats(status: Pick<OrgSetupStatus, "seats"> | null | undefined, attempt?: { seats: number } | null): number | null {
  const s = status?.seats;
  if (typeof s === "number" && Number.isInteger(s) && s >= MIN_SEATS) return s;
  return attempt?.seats ?? null;
}

/** "Oct 28, 3:15 PM", in the reader's own time zone. */
export function formatDeadline(ms: number): string {
  return new Date(ms).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

// ── Polling ─────────────────────────────────────────────────────────────────

export const POLL_BASE_MS = 1_500;
export const POLL_MAX_MS = 10_000;
/** The slow states (waiting on a person or a deploy) back off further. */
export const POLL_SLOW_MAX_MS = 30_000;

/** Delay before poll number `n` (0-based): 1.5 s growing by 1.5x, capped at
 *  10 s, or 30 s for the states that wait on a person or a deploy. `false`
 *  once the view is terminal. */
export function nextPollDelay(n: number, v: Pick<SetupView, "poll" | "kind">): number | false {
  if (!v.poll) return false;
  const slow = v.kind === "resume" || v.kind === "not_ours" || v.kind === "updating" || v.kind === "payment_settling";
  const cap = slow ? POLL_SLOW_MAX_MS : POLL_MAX_MS;
  return Math.min(cap, Math.round(POLL_BASE_MS * Math.pow(1.5, Math.max(0, n))));
}

/** The binding gate's 503 for an org whose database is still being built
 *  (`{"error":"service unavailable","status":"provisioning"}`), as distinct
 *  from the same 503 for a real outage. */
export function isProvisioningGate(err: unknown): boolean {
  return err instanceof ApiError && err.status === 503 && err.body?.status === "provisioning";
}

// ── Checkout copy (#1521 "Checkout-page copy the web PR (B) must show") ────

/** `2_000` cents → "$20", `1_950` → "$19.50". `null` when unknown. */
export function formatSeatPrice(cents: number | null | undefined): string | null {
  if (cents == null || !Number.isFinite(cents) || cents < 0) return null;
  const whole = cents % 100 === 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: whole ? 0 : 2,
    maximumFractionDigits: 2,
  }).format(cents / 100);
}

/** The trial copy shown before redirecting to Stripe (response `trial: true`). */
export function trialCopy(seats: number, priceCents: number | null | undefined): string[] {
  const price = formatSeatPrice(priceCents);
  const charge = price
    ? `We charge the card you add for ${seats} seats at ${price} per seat per month, and your team gets its full monthly credits right away.`
    : `We charge the card you add for ${seats} seats at the standard per-seat price each month, and your team gets its full monthly credits right away.`;
  return [
    `Start your free trial. Your team gets ${TRIAL_CREDITS.toLocaleString("en-US")} free credits to share, and you won't be charged today.`,
    `When the free credits run out, or after ${TRIAL_DAYS} days, whichever comes first, your subscription starts automatically. ${charge}`,
    `We'll email you when 80% of the free credits are used, when they run out, and 7 days before the ${TRIAL_DAYS} days are up.`,
    "Cancel any time before then in Settings → Billing and you won't be charged.",
    "One free trial per person and per card.",
    "Extra usage turns on after your first payment.",
  ];
}

/** The copy when the response says `trial: false` (the creator already had one). */
export function repeatTrialCopy(seats: number, priceCents: number | null | undefined): string {
  const price = formatSeatPrice(priceCents);
  return price
    ? `You've already used a free trial, so your subscription starts today: we'll charge ${seats} seats × ${price} now.`
    : `You've already used a free trial, so your subscription starts today: we'll charge for ${seats} seats now.`;
}

/** #1521: a card that already had a trial ends the new trial at completion. */
export const CARD_ALREADY_TRIALED = "This card has already been used for a free trial, so your subscription has started.";

// ── Trial banner (GET /v1/billing/summary → trial) ──────────────────────────

export interface TrialBannerView {
  tone: "ok" | "warn";
  text: string;
  /** Show a "Billing" link. */
  billingLink: boolean;
  /** The banner may be hidden for the rest of the session. */
  dismissible: boolean;
}

/** Trial states in which extra usage and auto-reload are refused (409
 *  `trial_active`, #1521). */
export const TRIAL_REFUSES_EXTRA = new Set(["checkout_open", "trialing", "converting"]);

export function trialRefusesExtraUsage(trial: BillingTrial | null | undefined): boolean {
  return !!trial && TRIAL_REFUSES_EXTRA.has(trial.state);
}

export const TRIAL_EXTRA_USAGE_COPY =
  "Extra usage and auto-reload turn on after your free trial ends and your first payment goes through.";

function formatDay(iso: string | null): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric", timeZone: "UTC" });
}

/** What the org app's trial banner says, or `null` for no banner.
 *  `justCreated` is true right after the sign-up switched into this team,
 *  which is the only time a repeat-card conversion needs explaining. */
export function trialBannerView(trial: BillingTrial | null | undefined, justCreated = false): TrialBannerView | null {
  if (!trial) return null;
  const total = Math.max(0, trial.credits_total);
  const used = Math.max(0, trial.credits_used);
  const left = Math.max(0, total - used);
  const fmt = (n: number) => Math.round(n).toLocaleString("en-US");
  switch (trial.state) {
    case "trialing": {
      const end = formatDay(trial.ends_at);
      const nearlyOut = total > 0 && used / total >= 0.8;
      return {
        tone: nearlyOut ? "warn" : "ok",
        text:
          `Free trial: ${fmt(left)} of ${fmt(total)} credits left. ` +
          `When they run out${end ? `, or on ${end}` : ""}, your subscription starts automatically and the card on file is charged for ${trial.seats} seats.`,
        billingLink: true,
        dismissible: !nearlyOut,
      };
    }
    case "converting":
      if (justCreated && !trial.payment_failed) {
        return { tone: "ok", text: CARD_ALREADY_TRIALED, billingLink: true, dismissible: true };
      }
      return trial.payment_failed
        ? {
            tone: "warn",
            text: "Your free trial has ended, but the card on file was declined. Update it in Billing so your team keeps its credits.",
            billingLink: true,
            dismissible: false,
          }
        : {
            tone: "ok",
            text: `Your free trial has ended. We're charging the card on file for ${trial.seats} seats, and your team's monthly credits arrive as soon as the payment goes through.`,
            billingLink: true,
            dismissible: true,
          };
    case "converted":
      return justCreated ? { tone: "ok", text: CARD_ALREADY_TRIALED, billingLink: true, dismissible: true } : null;
    default:
      return null;
  }
}

/** Set by the return page right before it switches into the new team, so
 *  the banner can explain a repeat-card conversion once. */
export const JUST_CREATED_KEY = "cs_team_just_created";
