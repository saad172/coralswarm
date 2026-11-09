import { describe, expect, it } from "vitest";
import { ApiError, type OrgDomain } from "@/lib/api";
import {
  claimFailure,
  confirmationFailure,
  dnsCheckFailure,
  domainsListFailure,
  domainStatus,
  removeFailure,
} from "./domainVerificationModel";

function domain(overrides: Partial<OrgDomain> = {}): OrgDomain {
  return {
    id: "dom_1",
    domain: "acme.example",
    verified: false,
    method: null,
    verified_at: null,
    claimed_at: "2026-09-18T10:00:00Z",
    dns_record_name: "_coralswarm-verification.acme.example",
    dns_record_value: "coralswarm-domain-verification=abc123",
    confirmation_email: null,
    confirmation_sent_at: null,
    ...overrides,
  };
}

describe("domainStatus", () => {
  it("reads a DNS-verified domain as arming auto-add", () => {
    const status = domainStatus(domain({ verified: true, method: "dns_txt" }));
    expect(status.kind).toBe("arms_auto_add");
    expect(status.tag).toBe("DNS");
    expect(status.label).toMatch(/arms auto-add/);
  });

  it("reads an email-verified domain as verified but NOT arming, and says how to switch to DNS", () => {
    const status = domainStatus(domain({ verified: true, method: "email" }));
    expect(status.kind).toBe("verified_other_proof");
    expect(status.tag).toBe("Email");
    expect(status.label).toMatch(/only a DNS-verified domain arms auto-add/);
    expect(status.label).toMatch(/Publish the DNS record below to arm it/);
    // The upgrade happens IN PLACE (CS-450 review round): `verify_dns` no
    // longer short-circuits on an email-verified row, so the old advice to
    // remove and re-claim would now send the admin the long way round.
    expect(status.label).not.toMatch(/remove this domain/i);
  });

  it("reads an unverified domain as neither", () => {
    const status = domainStatus(domain({ verified: false, method: null }));
    expect(status.kind).toBe("unverified");
    expect(status.tag).toBe("Unverified");
  });

  it("fails to the safer 'verified, does not arm' reading for a method this build doesn't recognise", () => {
    // `method`'s TS type only names the two live values — this simulates a
    // future third one arriving over the wire before the client is rebuilt,
    // which the type system cannot catch at compile time.
    const status = domainStatus(domain({ verified: true, method: "carrier_pigeon" as OrgDomain["method"] }));
    expect(status.kind).toBe("verified_other_proof");
  });
});

describe("dnsCheckFailure — 409 (not published) and 502 (resolver unreachable) must never blur", () => {
  it("409 reads as not_published, carrying the backend's own message verbatim", () => {
    const outcome = dnsCheckFailure(
      new ApiError("no matching TXT record at _coralswarm-verification.acme.example yet (0 TXT record(s) found there) — DNS can take a while to propagate", 409),
    );
    expect(outcome.kind).toBe("not_published");
    expect(outcome).toMatchObject({ message: expect.stringContaining("DNS can take a while to propagate") });
  });

  it("409 also covers 'taken by another org', still read straight through — a different fact, same status code", () => {
    const outcome = dnsCheckFailure(
      new ApiError("acme.example is already verified by another organization on CoralSwarm", 409),
    );
    expect(outcome.kind).toBe("not_published");
    expect(outcome).toMatchObject({ message: expect.stringContaining("already verified by another organization") });
  });

  it("502 reads as resolver_unreachable with copy that says nothing about the record itself", () => {
    const outcome = dnsCheckFailure(new ApiError("could not reach a DNS resolver", 502));
    expect(outcome.kind).toBe("resolver_unreachable");
    if (outcome.kind === "resolver_unreachable") {
      expect(outcome.message).not.toMatch(/not published|record.*(missing|absent|wrong)/i);
      expect(outcome.message).toMatch(/couldn't reach a dns resolver/i);
    }
  });

  it("409 and 502 never produce the same message", () => {
    const conflict = dnsCheckFailure(new ApiError("no matching TXT record at x yet (0 found)", 409));
    const gateway = dnsCheckFailure(new ApiError("could not reach a DNS resolver", 502));
    expect(conflict.kind).not.toBe(gateway.kind);
    expect((conflict as { message: string }).message).not.toBe((gateway as { message: string }).message);
  });

  it("404/403/other statuses and non-ApiError errors fall into 'other', never 'not_published' or 'resolver_unreachable'", () => {
    expect(dnsCheckFailure(new ApiError("not_found", 404)).kind).toBe("other");
    expect(dnsCheckFailure(new ApiError("forbidden", 403)).kind).toBe("other");
    expect(dnsCheckFailure(new Error("network blip")).kind).toBe("other");
  });
});

describe("confirmationFailure — a 503 has TWO causes, discriminated by the backend's own `error` field", () => {
  it("400 reads as domain_mismatch, message passed through", () => {
    const outcome = confirmationFailure(
      new ApiError("the confirmation address must be a valid address at acme.example — that is what proves you control it", 400),
    );
    expect(outcome.kind).toBe("domain_mismatch");
  });

  it("409 reads as blocked, message passed through (covers both 'already verified' and cooldown)", () => {
    expect(confirmationFailure(new ApiError("that domain is already verified", 409)).kind).toBe("blocked");
    expect(
      confirmationFailure(new ApiError("a confirmation was sent for that domain very recently — try again shortly", 409))
        .kind,
    ).toBe("blocked");
  });

  it("502 reads as send_failed, explicitly retryable", () => {
    const outcome = confirmationFailure(
      new ApiError("could not send the confirmation email — nothing was consumed, try again", 502),
    );
    expect(outcome.kind).toBe("send_failed");
  });

  /** The PERMANENT 503 — `backend/src/routes/org_domains.rs::
   *  confirmation_not_configured`'s own `error: "email_confirmation_not_configured"`,
   *  read from that source, not guessed (code review on PR #1048: the
   *  story's original brief called EVERY 503 here a deployment fact — that
   *  was the founder's own error, reading the endpoint's `utoipa`
   *  annotation instead of the handler). */
  it("503 with the backend's permanent discriminator reads as not_configured with OWN copy", () => {
    const outcome = confirmationFailure(new ApiError("email_confirmation_not_configured", 503));
    expect(outcome.kind).toBe("not_configured");
    if (outcome.kind === "not_configured") {
      expect(outcome.message).not.toBe("email_confirmation_not_configured");
      expect(outcome.message).toMatch(/set up on this deployment/i);
      expect(outcome.message).toMatch(/verify by dns instead/i);
    }
  });

  it("503 with the permanent discriminator never reads as retryable (send_failed) or as the admin's own mistake (domain_mismatch)", () => {
    const outcome = confirmationFailure(new ApiError("email_confirmation_not_configured", 503));
    expect(outcome.kind).not.toBe("send_failed");
    expect(outcome.kind).not.toBe("domain_mismatch");
  });

  /** The TRANSIENT 503 — the plain `"service unavailable"` every route in
   *  `org_domains.rs` shares for `state.control_db()` being `None`. This is
   *  the exact body the story's original brief mistook for the permanent
   *  case; it MUST keep a retry, reconciled with `dnsCheckFailure`'s own
   *  503 branch below (same status, same `admin_ctx` preamble, same
   *  copy). */
  it("503 with any OTHER body reads as transient — retryable, never the permanent 'not set up' copy", () => {
    const outcome = confirmationFailure(new ApiError("service unavailable", 503));
    expect(outcome.kind).toBe("transient");
    if (outcome.kind === "transient") {
      expect(outcome.message).toMatch(/briefly unavailable/i);
      expect(outcome.message).not.toMatch(/set up on this deployment/i);
    }
  });

  it("dnsCheckFailure's own 503 copy and confirmationFailure's transient 503 copy read identically", () => {
    const dns = dnsCheckFailure(new ApiError("service unavailable", 503));
    const confirmation = confirmationFailure(new ApiError("service unavailable", 503));
    expect((dns as { message: string }).message).toBe((confirmation as { message: string }).message);
  });
});

describe("claimFailure", () => {
  it("400 passes the backend's own DomainRejection message through verbatim", () => {
    const outcome = claimFailure(new ApiError("that is not a domain — enter it as acme.com, with no scheme, path, or @", 400));
    expect(outcome.kind).toBe("rejected");
  });

  it("409 reads as limit_reached, message passed through (it names the cap)", () => {
    const outcome = claimFailure(
      new ApiError("this organisation has already claimed the maximum of 25 domains — remove one first", 409),
    );
    expect(outcome.kind).toBe("limit_reached");
    expect((outcome as { message: string }).message).toMatch(/25 domains/);
  });

  it("non-ApiError errors fall into 'other' with calm fallback copy", () => {
    const outcome = claimFailure(new Error("boom"));
    expect(outcome.kind).toBe("other");
  });
});

describe("removeFailure", () => {
  it("404/403/503 each get their own calm copy", () => {
    expect(removeFailure(new ApiError("not_found", 404))).toMatch(/already gone/);
    expect(removeFailure(new ApiError("forbidden", 403))).toMatch(/only an admin/i);
    expect(removeFailure(new ApiError("service unavailable", 503))).toMatch(/briefly unavailable/i);
  });
});

describe("domainsListFailure — a raw backend body is not for every audience (item 1)", () => {
  it("403 (not an org admin) maps to friendly copy and is marked forbidden — no retry", () => {
    const failure = domainsListFailure(new ApiError("admin only", 403));
    expect(failure.forbidden).toBe(true);
    expect(failure.message).not.toBe("admin only");
    expect(failure.message).toMatch(/only an admin/i);
  });

  it("403 (personal account) reads the same calm way — the raw backend string never leaks through either", () => {
    const failure = domainsListFailure(new ApiError("personal accounts have no org domains", 403));
    expect(failure.forbidden).toBe(true);
    expect(failure.message).not.toBe("personal accounts have no org domains");
  });

  it("every other failure is NOT forbidden — worth retrying", () => {
    expect(domainsListFailure(new ApiError("service unavailable", 503)).forbidden).toBe(false);
    expect(domainsListFailure(new Error("network blip")).forbidden).toBe(false);
  });

  it("a non-ApiError error falls back to calm copy, not a raw message", () => {
    const failure = domainsListFailure(new Error("boom"));
    expect(failure.forbidden).toBe(false);
    expect(failure.message).toMatch(/couldn't load/i);
  });
});
