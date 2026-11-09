import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ApiError } from "./api";
import { UnexpectedEmptyOrgError } from "./oceanBootstrap";
import { bootErrorMessage, GENERIC_BOOT_ERROR, PROVISIONING_BOOT_ERROR, SIGNED_OUT_ERROR } from "./bootErrorCopy";

// `__dirname` is `web/lib`; `backend/` is a sibling of `web/` at the repo root.
const GATE_RS = path.resolve(__dirname, "..", "..", "backend", "src", "tenancy", "gate.rs");

// The exact strings `backend/src/tenancy/gate.rs` returns TODAY. Each one is
// the thing that used to render as "check your connection".
//
// "use your organization's subdomain" was in this list and is not any more:
// claim-based tenancy PR 1.2 rewrote the arm that returned it (the apex now
// binds an org's tenant from its claim), so keeping it would have left the
// case green while testing a string the backend can no longer produce — the
// fixture would have been asserting nothing about the gate, only that an
// arbitrary 403 body passes through. "organization token required" survives
// because `HostTarget::Subdomain` still returns it until the Phase 4 cutover
// deletes that arm.
const GATE_REASONS = [
  "organization token required",
  "token does not match this organization",
  "organization is not provisioned for service",
  "organization is not active",
];

describe("bootErrorMessage", () => {
  // CONTRACT CHECK, not a copy test: `bootErrorMessage` just echoes whatever
  // 403 reason it is handed, so the test below passing proves nothing about
  // whether GATE_REASONS still matches the backend — a renamed or deleted
  // gate reason would leave it green, since any non-empty, non-placeholder
  // string round-trips unchanged. Rust and TypeScript share no source here,
  // so the practical substitute is reading the gate's actual source: if a
  // reason above stops being a literal `GateOutcome::Forbidden(...)` argument
  // in gate.rs, this fails and says so, instead of the fixture quietly
  // drifting from what the backend can produce.
  it("GATE_REASONS are still literal GateOutcome::Forbidden strings in gate.rs", () => {
    const source = readFileSync(GATE_RS, "utf8");
    for (const reason of GATE_REASONS) {
      expect(
        source,
        `backend/src/tenancy/gate.rs no longer contains GateOutcome::Forbidden("${reason}") — update GATE_REASONS in bootErrorCopy.test.ts`,
      ).toContain(`GateOutcome::Forbidden("${reason}")`);
    }
  });

  it("surfaces the tenancy gate's own reason for a 403", () => {
    for (const reason of GATE_REASONS) {
      expect(bootErrorMessage(new ApiError(reason, 403))).toBe(reason);
    }
  });

  it("keeps the generic copy when a 403 carries no usable reason", () => {
    // `lib/api.ts` leaves its `HTTP <status>` placeholder when the body is
    // empty or not JSON; showing that verbatim would be useless to a user.
    expect(bootErrorMessage(new ApiError("HTTP 403", 403))).toBe(GENERIC_BOOT_ERROR);
    expect(bootErrorMessage(new ApiError("", 403))).toBe(GENERIC_BOOT_ERROR);
    expect(bootErrorMessage(new ApiError("   ", 403))).toBe(GENERIC_BOOT_ERROR);
  });

  it("passes through the empty-org error's own message", () => {
    const err = new UnexpectedEmptyOrgError();
    expect(bootErrorMessage(err)).toBe(err.message);
  });

  it("uses the generic copy for other statuses", () => {
    for (const status of [400, 404, 500, 503]) {
      expect(bootErrorMessage(new ApiError("something else", status))).toBe(GENERIC_BOOT_ERROR);
    }
  });

  it("names a 401 as being signed out, never as a connection problem", () => {
    // The real incident: a signed-out client said "check your connection"
    // while the backend was healthy and answering 401 {"error":"missing token"}.
    expect(bootErrorMessage(new ApiError("missing token", 401))).toBe(SIGNED_OUT_ERROR);
    expect(bootErrorMessage(new ApiError("", 401))).toBe(SIGNED_OUT_ERROR);
    expect(bootErrorMessage(new ApiError("HTTP 401", 401))).toBe(SIGNED_OUT_ERROR);
    // and it must not leak the backend's client-facing wording to a person
    expect(bootErrorMessage(new ApiError("missing token", 401))).not.toContain("token");
  });

  it("uses the generic copy for non-ApiError failures (timeouts, network)", () => {
    expect(bootErrorMessage(new Error("boot timed out"))).toBe(GENERIC_BOOT_ERROR);
    expect(bootErrorMessage("not an error at all")).toBe(GENERIC_BOOT_ERROR);
    expect(bootErrorMessage(undefined)).toBe(GENERIC_BOOT_ERROR);
  });
});

describe("the provisioning gate (self-serve teams, A2 #1533)", () => {
  it("names a team mid-setup instead of blaming the connection", () => {
    const gate = new ApiError("service unavailable", 503, null, null, null, { error: "service unavailable", status: "provisioning" });
    expect(bootErrorMessage(gate)).toBe(PROVISIONING_BOOT_ERROR);
    // A real outage keeps the connection copy.
    expect(bootErrorMessage(new ApiError("service unavailable", 503, null, null, null, { error: "service unavailable" }))).toBe(GENERIC_BOOT_ERROR);
  });
});
