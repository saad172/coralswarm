// Home launchpad: the pure logic behind app/(app)/home/page.tsx.
//
// Everything here is deterministic and request-free, so it is unit-tested in
// lib/homeLaunchpad.test.ts: which state the page is in, which What's new rows
// get the "new" dot, the prompt starters (templates over data Home already
// has, no LLM), freshness labels, where each row opens, and the setup
// checklist's progress. The page owns the three reads (lib/homeLaunchpadApi.ts)
// and the markup (components/home/Launchpad.tsx).

import type { Hue } from "@/components/ui/hue";
import { formatDayParam } from "./journal";
import { oceanHue } from "./oceanHue";
import { meetingHref } from "./provenance";
import type { AttentionResponse, ContinueItem, WhatsNewItem } from "./homeLaunchpadApi";
import { browserStorage, type SetupState, type StorageLike } from "./homeSetup";

// ── Page state ──────────────────────────────────────────────────────────

/** `empty` is State B (the setup checklist), `populated` is State A, and
 *  `loading` is before Home knows which: the browser memory has not been
 *  read yet, or `/v1/home/attention` has not answered. It draws State A's
 *  skeleton. */
export type HomeState = "loading" | "empty" | "populated";

export function homeState(input: {
  attention: AttentionResponse | undefined;
  /** The attention read failed with no data to show. */
  attentionFailed: boolean;
  /** `onboarded` has been read from this browser's memory. Until it has, an
   *  empty answer cannot be trusted to mean State B: an established user
   *  would see the checklist flash before their onboarded mark loads. */
  memoryLoaded: boolean;
  /** This identity has seen content on Home before (`readOnboarded`). */
  onboarded: boolean;
}): HomeState {
  if (!input.memoryLoaded) return "loading";
  // Once content has existed, the checklist never comes back, even if a
  // later answer says the ocean is empty.
  if (input.onboarded) return "populated";
  if (input.attention) return input.attention.has_content ? "populated" : "empty";
  // A failed read with nothing cached shows State A: the sections carry their
  // own state, and an established user must not be sent back to setup.
  return input.attentionFailed ? "populated" : "loading";
}

// ── "New since your last visit" ─────────────────────────────────────────

function time(iso: string | null | undefined): number {
  if (!iso) return Number.NaN;
  return Date.parse(iso);
}

/** A reef is new when it formed after the last visit. With no last visit
 *  (the first one) nothing is marked new: there is nothing to compare. */
export function isNewSince(formedAt: string, lastVisitAt: string | null): boolean {
  const formed = time(formedAt);
  const last = time(lastVisitAt);
  return Number.isFinite(formed) && Number.isFinite(last) && formed > last;
}

function countNew(items: readonly WhatsNewItem[], lastVisitAt: string | null): number {
  return items.filter((item) => isNewSince(item.formed_at, lastVisitAt)).length;
}

/** "2 new since your last visit", or null on the first visit. */
export function newSinceLabel(items: readonly WhatsNewItem[], lastVisitAt: string | null): string | null {
  if (!Number.isFinite(time(lastVisitAt))) return null;
  return `${countNew(items, lastVisitAt)} new since your last visit`;
}

/** What to store as the last visit once What's new has been seen: the
 *  server's `generated_at` (the same clock as `formed_at`, so client clock
 *  skew cannot hide or invent a dot), else the client's now. Never earlier
 *  than what is already stored: a response served from a cache, or another
 *  tab's newer visit, must not move the baseline back and re-light dots the
 *  user has seen. */
export function nextLastVisit(stored: string | null, generatedAt: string | null | undefined, now: Date): string {
  const seen = Number.isFinite(time(generatedAt)) ? (generatedAt as string) : now.toISOString();
  return Number.isFinite(time(stored)) && time(stored) > time(seen) ? (stored as string) : seen;
}

// ── Per-identity browser memory ─────────────────────────────────────────

/** Keys are per identity (`AppState.queryScope`: the signed-in user and the
 *  active org), because What's new differs per org. */
const lastVisitKey = (scope: string) => `cs.home.lastVisit.${scope}`;
const onboardedKey = (scope: string) => `cs.home.onboarded.${scope}`;

export function readLastVisit(scope: string, storage: StorageLike | null = browserStorage()): string | null {
  try {
    const raw = storage?.getItem(lastVisitKey(scope)) ?? null;
    return raw && Number.isFinite(Date.parse(raw)) ? raw : null;
  } catch {
    return null;
  }
}

export function writeLastVisit(scope: string, iso: string, storage: StorageLike | null = browserStorage()): void {
  try {
    storage?.setItem(lastVisitKey(scope), iso);
  } catch {
    /* storage full or blocked: every visit is then a first visit */
  }
}

export function readOnboarded(scope: string, storage: StorageLike | null = browserStorage()): boolean {
  try {
    return !!storage?.getItem(onboardedKey(scope));
  } catch {
    return false;
  }
}

/** Persist that this identity has content, so the checklist never returns. */
export function writeOnboarded(scope: string, now: Date, storage: StorageLike | null = browserStorage()): void {
  try {
    storage?.setItem(onboardedKey(scope), now.toISOString());
  } catch {
    /* see writeLastVisit */
  }
}

// ── Freshness ───────────────────────────────────────────────────────────

/** When this client received a section's data (TanStack's `dataUpdatedAt`),
 *  or null before the first answer. The "updated N min ago" label is measured
 *  on the client's clock at both ends. The server's `generated_at` is not
 *  used here: a server clock running behind the client's would make a fresh
 *  answer look minutes old. (The new dot does use `generated_at`, because it
 *  compares it with `formed_at`, which comes from the same server clock.) */
export function updatedAt(dataUpdatedAt: number): number | null {
  return dataUpdatedAt > 0 ? dataUpdatedAt : null;
}

/** The quiet mono line in a section header. */
export function freshnessLabel(at: number | null, now: number, refreshing: boolean): string {
  if (refreshing) return "refreshing…";
  if (at === null) return "";
  const mins = Math.max(0, Math.floor((now - at) / 60_000));
  if (mins < 1) return "updated just now";
  if (mins < 60) return `updated ${mins} min ago`;
  return `updated ${Math.floor(mins / 60)} h ago`;
}

// ── Prompt starters (templates, no LLM) ─────────────────────────────────

/** A chip's visible text and the question it asks. They differ only when a
 *  long title is shortened on the chip; the question keeps the whole title. */
export interface PromptStarter {
  label: string;
  question: string;
}

/** How much of a title a chip shows. Chips stay about as short as the mock's
 *  ("Catch me up on cs-home-redesign"); the question is not cut. */
const CHIP_TOPIC_MAX = 32;
/** A guard on the asked question only; titles are rarely this long. */
const QUESTION_TOPIC_MAX = 160;

/** A title fit for a template: whitespace collapsed, trailing punctuation
 *  dropped, and past `max` cut at a word with an ellipsis (never after a
 *  dangling dash or comma). */
function starterTopic(title: string, max = CHIP_TOPIC_MAX): string {
  const clean = title.replace(/\s+/g, " ").trim().replace(/[\s.?!:;,…]+$/, "");
  if (clean.length <= max) return clean;
  const cut = clean.slice(0, max);
  const space = cut.lastIndexOf(" ");
  return `${(space > max / 2 ? cut.slice(0, space) : cut).replace(/[\s,;:·—–-]+$/, "")}…`;
}

/** "coralswarm · cs-home-redesign" → "cs-home-redesign": a grouped session's
 *  title is `<repo> · <branch>`, and the branch is what people call it. Null
 *  when the title is not `<repo> · <branch>`. */
function sessionBranch(title: string): string | null {
  const parts = title.split(" · ");
  return parts.length > 1 ? parts[parts.length - 1] : null;
}

/** "Kim Ng — pricing owner" → "Kim Ng": an entity's name without the
 *  descriptor reef titles carry after a dash. */
function entityName(title: string): string {
  return title.split(/\s[—–-]\s/)[0];
}

/** One template per source. Reef and agent-task titles are statements
 *  ("Usage-based billing starts at each Q1 renewal"), so those are quoted
 *  rather than spliced into a question as if they were a noun. Names
 *  (people, branches, workspaces, meetings) read naturally unquoted. */
const TEMPLATES = {
  decision: (t: string) => `What led to ${t}?`,
  thread: (t: string) => `Where did ${t} land?`,
  entity: (t: string) => `What do we know about ${t}?`,
  reference: (t: string) => `Summarize ${t}`,
  session: (t: string) => `Catch me up on ${t}`,
  workspace: (t: string) => `Who's working on ${t}?`,
  meeting: (t: string) => `What came out of ${t}?`,
} as const;

type StarterTemplate = keyof typeof TEMPLATES;

interface StarterSource {
  template: StarterTemplate;
  /** The subject, uncut. */
  topic: string;
  quoted: boolean;
}

function render(source: StarterSource): PromptStarter | null {
  const fill = (max: number) => {
    const t = starterTopic(source.topic, max);
    return source.quoted ? `“${t}”` : t;
  };
  if (!starterTopic(source.topic)) return null;
  const write = TEMPLATES[source.template];
  return { label: write(fill(CHIP_TOPIC_MAX)), question: write(fill(QUESTION_TOPIC_MAX)) };
}

function whatsNewSource(item: WhatsNewItem): StarterSource {
  switch (item.kind) {
    case "decision":
      return { template: "decision", topic: item.title, quoted: true };
    case "entity":
    case "person":
      return { template: "entity", topic: entityName(item.title), quoted: false };
    case "reference":
      return { template: "reference", topic: item.title, quoted: true };
    // Threads, and any kind added later: a statement-shaped title.
    default:
      return { template: "thread", topic: item.title, quoted: true };
  }
}

function continueSource(item: ContinueItem): StarterSource | null {
  switch (item.kind) {
    case "coding":
    case "agent": {
      const branch = sessionBranch(item.title);
      return branch ? { template: "session", topic: branch, quoted: false } : { template: "session", topic: item.title, quoted: true };
    }
    case "workspace":
      return { template: "workspace", topic: item.title, quoted: false };
    case "meeting":
      return { template: "meeting", topic: item.title, quoted: false };
    // A chat is one click away in Continue; asking about it again adds nothing.
    default:
      return null;
  }
}

/** Used when the data yields fewer than three. */
const FALLBACK_STARTERS = [
  "What did I work on this week?",
  "What decisions were made recently?",
  "Who's working on what?",
] as const;

/** Three starters: What's new and Continue interleaved (a decision, then a
 *  branch, then the next reef...), preferring a different template for each
 *  chip before repeating one, de-duplicated, topped up from the fallbacks.
 *  Built from the two reads Home already makes; adds no request. */
export function promptStarters(
  continueItems: readonly ContinueItem[],
  whatsNew: readonly WhatsNewItem[],
  n = 3,
): PromptStarter[] {
  type Candidate = { starter: PromptStarter; template: string };
  const candidates = (sources: (StarterSource | null)[]): Candidate[] =>
    sources.flatMap((source) => {
      const starter = source ? render(source) : null;
      return starter && source ? [{ starter, template: source.template }] : [];
    });
  const fromNew = candidates(whatsNew.map(whatsNewSource));
  const fromContinue = candidates(continueItems.map(continueSource));
  const ordered: Candidate[] = [];
  for (let i = 0; i < Math.max(fromNew.length, fromContinue.length); i++) {
    if (fromNew[i]) ordered.push(fromNew[i]);
    if (fromContinue[i]) ordered.push(fromContinue[i]);
  }
  const out: PromptStarter[] = [];
  const seen = new Set<string>();
  const templates = new Set<string>();
  const add = (c: Candidate, distinctTemplate: boolean) => {
    const key = c.starter.question.toLowerCase();
    if (out.length >= n || seen.has(key) || (distinctTemplate && templates.has(c.template))) return;
    seen.add(key);
    templates.add(c.template);
    out.push(c.starter);
  };
  ordered.forEach((c) => add(c, true));
  ordered.forEach((c) => add(c, false));
  FALLBACK_STARTERS.forEach((text) => add({ starter: { label: text, question: text }, template: text }, false));
  return out;
}

// ── Rows ────────────────────────────────────────────────────────────────

/** A Continue row's mono context line, as the mock draws it: the kind, then
 *  the server's context ("coding · claude code · last: cache warmup"). A
 *  grouped row's session count is never part of it; the row shows `count`
 *  once, as a pill. */
export function continueMeta(item: ContinueItem): string {
  return item.context ? `${item.kind} · ${item.context}` : item.kind;
}

/** Where a Continue row opens.
 *
 *  Sessions have no page of their own (`/session` and `/sessions` are retired
 *  redirects to /home), so a coding or agent row opens Today on the day the
 *  session was last active, where it is listed. A meeting row's id is always
 *  its transcript's `source_id` (contract), which the meeting page opens in
 *  the row's ocean. */
export function continueHref(item: ContinueItem): string {
  const id = encodeURIComponent(item.id);
  switch (item.kind) {
    case "chat":
      return `/chats?id=${id}`;
    case "workspace":
      return `/workspaces?id=${id}`;
    case "meeting":
      // Without an ocean the meeting page falls back to the current one.
      return item.graph_id ? meetingHref(item.id, item.graph_id) : `/meeting?${new URLSearchParams({ source_id: item.id })}`;
    case "coding":
    case "agent": {
      const at = new Date(item.at);
      return Number.isNaN(at.getTime()) ? "/today" : `/today?d=${formatDayParam(at)}`;
    }
    default:
      return "/home";
  }
}

/** A reef opens its Topic page, in its own ocean. */
export function reefHref(item: WhatsNewItem): string {
  return `/topics?id=${encodeURIComponent(item.reef_id)}&graph_id=${encodeURIComponent(item.ocean.graph_id)}`;
}

/** Reef kind → hue (docs/design/DESIGN_SYSTEM.md): decision coral, thread
 *  amber, entity/person pink, reference plum. */
export function reefHue(kind: string): Hue {
  switch (kind) {
    case "decision":
      return "coral";
    case "thread":
      return "amber";
    case "entity":
    case "person":
      return "pink";
    case "reference":
      return "plum";
    default:
      return "teal";
  }
}

/** The ocean dot. The contract's private ocean is `private`; lib/oceanHue.ts
 *  knows it as `personal`/`member`, which is always pink. */
export function whatsNewOceanHue(ocean: WhatsNewItem["ocean"]): Hue {
  return oceanHue({ graph_id: ocean.graph_id, kind: ocean.kind === "private" ? "personal" : ocean.kind });
}

/** The needs-attention row's mono line: which connector, and what is wrong. */
export function attentionMeta(item: { provider: string; reason: "reauth" | "failing" }): string {
  return `${item.provider} · ${item.reason === "reauth" ? "sign-in expired" : "sync failing"}`;
}

// ── Greeting ────────────────────────────────────────────────────────────
// The salutation itself is `greeting` in lib/chatPrompts.ts, shared with the
// Chats landing so the two never disagree about the time of day.

/** The first name from the auth session, or null (the greeting then has no
 *  name rather than a Clerk id or an email). */
export function firstNameOf(user: { firstName?: string | null; fullName?: string | null } | null | undefined): string | null {
  const first = user?.firstName?.trim();
  if (first) return first;
  const full = user?.fullName?.trim();
  return full ? full.split(/\s+/)[0] : null;
}

// ── State B: setup checklist ────────────────────────────────────────────

export type ChecklistStepId = "connect" | "mcp" | "ask";

export interface ChecklistStep {
  id: ChecklistStepId;
  done: boolean;
  /** Done, but not a success yet: step 1 while a connector is broken. The
   *  needs-attention row above the checklist says what to fix. */
  attention: boolean;
  /** The first step not yet done. */
  current: boolean;
}

/** The three steps of State B and how many are done. `derived` is what the
 *  attention read says (`has_source`, `has_mcp`, `has_asked`, and whether a
 *  connector is broken); marks are the clicks this browser remembers
 *  (lib/homeSetup.ts). Step 1 is data only: following the Connect link is not
 *  a connected source. It still counts as done while a connector is broken
 *  (it IS connected), but carries `attention` so it does not read as a
 *  success. Step 2 is done when an MCP client has connected, or when the URL
 *  was copied here (the client may not have connected yet). Step 3 is done
 *  when the viewer has asked anywhere, or asked from Home in this browser
 *  (the server may not have the question yet). */
export function setupChecklist(
  marks: SetupState,
  derived: { hasSource: boolean; hasMcp: boolean; hasAsked: boolean; needsAttention: boolean },
): {
  steps: ChecklistStep[];
  done: number;
  total: number;
} {
  const doneById: Record<ChecklistStepId, boolean> = {
    connect: derived.hasSource,
    mcp: derived.hasMcp || marks.mcp,
    ask: derived.hasAsked || marks.ask,
  };
  const ids: ChecklistStepId[] = ["connect", "mcp", "ask"];
  const currentId = ids.find((id) => !doneById[id]) ?? null;
  const steps = ids.map((id) => ({
    id,
    done: doneById[id],
    attention: id === "connect" && doneById.connect && derived.needsAttention,
    current: id === currentId,
  }));
  return { steps, done: steps.filter((s) => s.done).length, total: steps.length };
}
