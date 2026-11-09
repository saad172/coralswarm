/** Unverified claims are used only to refuse credential substitution. The
 * server remains responsible for signature, issuer and authorization checks. */
export interface CredentialScope { subject: string; org: string | null; issuer: string }
export function credentialScope(token: string | null): CredentialScope | null {
  try {
    if (!token) return null;
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const claims = JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"))) as { sub?: unknown; iss?: unknown; org_id?: unknown; o?: { id?: unknown } };
    const org = claims.org_id ?? claims.o?.id ?? null;
    if (typeof claims.sub !== "string" || !claims.sub || typeof claims.iss !== "string" || !claims.iss || (org !== null && typeof org !== "string")) return null;
    return { subject: claims.sub, org, issuer: claims.iss };
  } catch { return null; }
}
export function sameCredentialScope(original: string, refreshed: string): boolean {
  const a = credentialScope(original), b = credentialScope(refreshed);
  return !!a && !!b && a.subject === b.subject && a.org === b.org && a.issuer === b.issuer;
}
