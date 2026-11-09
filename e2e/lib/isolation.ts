// Helpers for the `@isolation` scenarios (tests/isolation-*.spec.ts): real
// users, real orgs and real tenant databases, all built through the API the
// product uses. The backend under test serves as `coralswarm_app` (NOBYPASSRLS;
// `stack/assert-db.mjs` proves that at boot), so every denial these scenarios
// see is the policy working, not a superuser reading around it.

import { expect, type APIRequestContext } from "@playwright/test";
import crypto from "node:crypto";
import { WEB_BASE } from "../stack/config.mjs";
import { Api, completeStripeCheckout } from "./api";
import { adoptE2eUser, clerkApi, createUser, findUserByEmail, primaryEmail, sessionToken, testEmail } from "./clerk";
import type { SignedIn } from "./fixtures";

/** A Clerk session that starts with `orgId` active, and the API client that
 *  uses it. Clerk activates a user's organization on a session created after
 *  they joined it; the claim is checked, so a session that comes up personal
 *  fails here rather than silently testing the personal plane. */
export async function orgApi(request: APIRequestContext, userId: string, orgId: string): Promise<Api> {
  const session = await clerkApi.sessions.createSession({ userId });
  const token = cached(async () => (await clerkApi.sessions.getToken(session.id, undefined as unknown as string)).jwt);
  const claims = JSON.parse(Buffer.from((await token()).split(".")[1], "base64url").toString());
  const active = claims.o?.id ?? claims.org_id;
  expect(active, `a new session for ${userId} did not start with ${orgId} active (claims: ${JSON.stringify(claims)})`).toBe(orgId);
  return Api.withToken(request, token);
}

/**
 * Reuse a session JWT for most of its 60 s life. Every Backend API call
 * counts against the dev instance's rate limit, which every run, shard and
 * laptop share, and these scenarios make dozens of requests each.
 */
export function cached(mint: () => Promise<string>, ttlMs = 40_000): () => Promise<string> {
  let jwt: Promise<string> | undefined;
  let at = 0;
  return () => {
    if (!jwt || Date.now() - at > ttlMs) {
      at = Date.now();
      jwt = mint().catch((e) => {
        jwt = undefined;
        throw e;
      });
    }
    return jwt;
  };
}

/** A fresh Clerk user and an API client on their personal session. */
export async function personalUser(request: APIRequestContext, label: string, name?: string): Promise<SignedIn> {
  const user = await createUser(label, name);
  return {
    user,
    email: primaryEmail(user),
    api: new Api(
      request,
      user.id,
      cached(() => sessionToken(user.id)),
    ),
  };
}

export type Org = {
  orgId: string;
  name: string;
  admin: SignedIn;
  /** The admin, acting in the org (org-active session). */
  adminInOrg: Api;
  /** The org's shared ocean. */
  oceanId: string;
};

/**
 * A self-serve team org, the way a customer gets one: `POST /v1/orgs`, the
 * Stripe Checkout completed through the real webhook, then the backend's own
 * provisioning job building its tenant database (~7 s on the stack).
 */
export async function createReadyOrg(request: APIRequestContext, label: string, seats = 2): Promise<Org> {
  const admin = await personalUser(request, `${label}-admin`, `E2E ${label}`);
  const user = admin.user;
  await admin.api.graphs(); // provision the personal account first (README "Known gaps")
  const name = `E2E ${label} ${crypto.randomUUID().slice(0, 6)}`;
  const created = await admin.api.json(
    "POST",
    "/v1/orgs",
    { name, seats, success_url: `${WEB_BASE}/onboarding/team/return`, cancel_url: `${WEB_BASE}/onboarding/team` },
    { "Idempotency-Key": crypto.randomUUID() },
  );
  await completeStripeCheckout(request, created.session_id);
  await expect
    .poll(async () => (await admin.api.json("GET", `/v1/orgs/${created.org_id}/provisioning`)).status, { timeout: 30_000, intervals: [250] })
    .toBe("ready");
  const adminInOrg = await orgApi(request, user.id, created.org_id);
  const graphs = await adminInOrg.graphs();
  const ocean = graphs.find((g) => g.kind === "org");
  expect(ocean, `org ${name} has no shared ocean: ${JSON.stringify(graphs)}`).toBeTruthy();
  return { orgId: created.org_id, name, admin, adminInOrg, oceanId: ocean!.id };
}

/** Invite a member through the real route (it pre-creates the Clerk user and
 *  the membership), then act as them: personal session and org session. */
export async function addMember(request: APIRequestContext, org: Org, label: string) {
  const email = testEmail(label);
  await org.adminInOrg.json("POST", "/v1/orgs/invites", { email, role: "member", name: `E2E ${label}` });
  const user = await findUserByEmail(email);
  expect(user, `the invite did not create a Clerk user for ${email}`).toBeTruthy();
  adoptE2eUser(user!.id);
  return {
    user: user!,
    email,
    personal: new Api(
      request,
      user!.id,
      cached(() => sessionToken(user!.id)),
    ),
    inOrg: await orgApi(request, user!.id, org.orgId),
  };
}

/** Capture a note through the real ingest route; returns the atom ids it wrote. */
export async function ingestNote(api: Api, graphId: string, content: string) {
  const sourceId = `e2e-iso-${crypto.randomUUID()}`;
  const res = await api.json("POST", "/v1/ingest/atom", {
    graph_id: graphId,
    source_type: "note",
    source_id: sourceId,
    content,
    participants: [],
    timestamp: new Date().toISOString(),
  });
  expect(res.atom_ids?.length, `ingest wrote no atoms: ${JSON.stringify(res)}`).toBeGreaterThan(0);
  return { sourceId, atomIds: res.atom_ids as string[] };
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;

export type Probe = { status: number; body: string };

export async function probe(api: Api, method: "GET" | "POST" | "DELETE", path: string, data?: unknown, headers?: Record<string, string>): Promise<Probe> {
  const res = await api.call(method, path, data, headers);
  // Ids (the probed one, request ids) differ between the two requests being
  // compared; everything else must not.
  return { status: res.status(), body: (await res.text()).replace(UUID, "<uuid>") };
}

/**
 * The isolation contract for one route: what `attacker` gets for the
 * victim's REAL id is exactly what it gets for an id that never existed
 * (same status, same body with ids masked), it is a refusal, and it never
 * contains the victim's content. An answer that differs is an existence
 * oracle even when it is a refusal.
 */
export async function expectIndistinguishable(
  attacker: Api,
  method: "GET" | "POST" | "DELETE",
  pathFor: (id: string) => string,
  realId: string,
  opts: { data?: (id: string) => unknown; secret?: string; label?: string } = {},
) {
  const ghost = crypto.randomUUID();
  const real = await probe(attacker, method, pathFor(realId), opts.data?.(realId));
  const never = await probe(attacker, method, pathFor(ghost), opts.data?.(ghost));
  const what = opts.label ?? `${method} ${pathFor("{id}")}`;
  expect(real.status, `${what}: the victim's real id must be refused (got ${real.status}: ${real.body.slice(0, 300)})`).toBeGreaterThanOrEqual(400);
  expect(real.status, `${what}: a refusal, not a server error`).toBeLessThan(500);
  expect(real, `${what}: the victim's real id must answer exactly like an id that never existed`).toEqual(never);
  if (opts.secret) expect(real.body, `${what}: leaked the victim's content`).not.toContain(opts.secret);
  return real;
}
