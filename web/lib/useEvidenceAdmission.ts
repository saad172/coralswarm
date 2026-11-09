"use client";

import { useCallback, useContext, useRef } from "react";
import { useAuth } from "@clerk/nextjs";
import { useApp } from "./providers";
import { credentialScope, sameCredentialScope } from "./credentialScope";
import { EvidenceAdmissionContext } from "./evidenceAdmissionContext";

/** Live identity is observed during render. Existing app account metadata can
 * lag Clerk by one render, so a result keyed only on accountId is insufficient. */
export function useEvidenceAdmission() {
  const parent = useContext(EvidenceAdmissionContext);
  const live = useAuth();
  const app = useApp();
  const bearer = credentialScope(app.getAuthToken());
  let parentBearer: ReturnType<typeof credentialScope> = null;
  if (parent?.ready) {
    try { parentBearer = credentialScope(parent.getToken()); } catch { /* The parent detail is no longer admitted. */ }
  }
  const localKey = JSON.stringify([live.userId, live.orgId ?? null, app.userId, app.accountId, app.resolvedOrgId, app.currentGraphId, bearer?.issuer]);
  const key = parent ? JSON.stringify([parent.key, localKey]) : localKey;
  // A parent pins the detail address, but cannot admit a fresher child identity
  // or mutable token getter. Both observations must still agree.
  const parentReady = !parent || (parent.ready && parentBearer?.subject === bearer?.subject && parentBearer?.org === bearer?.org && parentBearer?.issuer === bearer?.issuer && parent.currentGraphId === app.currentGraphId);
  const ready = !!(parentReady && live.isLoaded && live.isSignedIn && app.authReady && live.userId && live.userId === app.userId && (live.orgId ?? null) === app.resolvedOrgId && bearer?.subject === live.userId && bearer?.org === (live.orgId ?? null));
  const current = useRef({ key, ready, liveUser: live.userId, liveOrg: live.orgId ?? null, issuer: bearer?.issuer, getAuthToken: app.getAuthToken, parentGetToken: parent?.getToken });
  current.current = { key, ready, liveUser: live.userId, liveOrg: live.orgId ?? null, issuer: bearer?.issuer, getAuthToken: app.getAuthToken, parentGetToken: parent?.getToken };
  const getToken = useCallback(() => {
    const next = current.current;
    const token = next.getAuthToken();
    const scope = credentialScope(token);
    if (!next.ready || next.key !== key || !token || !scope || scope.subject !== next.liveUser || scope.org !== next.liveOrg || scope.issuer !== next.issuer) throw new Error("The Library account changed. Verify the current account and retry.");
    const admitted = next.parentGetToken?.() ?? token;
    if (!sameCredentialScope(token, admitted)) throw new Error("The Library account changed. Verify the current account and retry.");
    return admitted;
  }, [key]);
  // `queryScope` lets an admitted reader share an identity-scoped cache entry
  // with the rest of the app (lib/appQueries.ts); `getToken` still fences it.
  return { key, ready, getToken, graphs: app.graphs, currentGraphId: app.currentGraphId, queryScope: app.queryScope };
}
