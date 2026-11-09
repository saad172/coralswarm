// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { useEffect, useState } from "react";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, type OrgDomain } from "@/lib/api";
import { DomainVerification } from "./DomainVerification";
import { domainsListFailure } from "./domainVerificationModel";

const identity = vi.hoisted(() => ({ app: { authReady: true, getAuthToken: () => "TOKEN" as string | null } }));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));

vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: {
    orgs: {
      domains: {
        list: vi.fn(),
        claim: vi.fn(),
        remove: vi.fn(),
        verify: vi.fn(),
        requestConfirmation: vi.fn(),
      },
    },
  },
}));

const list = vi.mocked(api.orgs.domains.list);
const claim = vi.mocked(api.orgs.domains.claim);
const remove = vi.mocked(api.orgs.domains.remove);
const verify = vi.mocked(api.orgs.domains.verify);
const requestConfirmation = vi.mocked(api.orgs.domains.requestConfirmation);

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

/** BLOCKER 1 fix (code review on PR #1048): `DomainVerification` no longer
 *  fetches its own list — `members/page.tsx` does, passing `domains`/
 *  `loadErr`/`loadForbidden`/`onRetry`/`onDomainsChange` down (the SAME
 *  props `AutoAddSetting` now also receives). This harness re-implements
 *  that page-level plumbing exactly (same `domainsListFailure`, same
 *  "clear the error on retry" discipline as `refetchOrgDomains`) so the
 *  rest of this file can keep exercising `list`/`claim`/`verify`/`remove`/
 *  `requestConfirmation` through the SAME component contract the real page
 *  uses, rather than re-deriving a second, possibly-wrong version of it. */
function Harness() {
  const [domains, setDomains] = useState<OrgDomain[] | null>(null);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [loadForbidden, setLoadForbidden] = useState(false);
  const [generation, setGeneration] = useState(0);

  useEffect(() => {
    let live = true;
    api.orgs.domains
      .list("TOKEN")
      .then((data) => {
        if (!live) return;
        setDomains(data.domains);
        setLoadErr(null);
        setLoadForbidden(false);
      })
      .catch((e) => {
        if (!live) return;
        const failure = domainsListFailure(e);
        setLoadErr(failure.message);
        setLoadForbidden(failure.forbidden);
      });
    return () => {
      live = false;
    };
  }, [generation]);

  return (
    <DomainVerification
      domains={domains}
      loadErr={loadErr}
      loadForbidden={loadForbidden}
      onRetry={() => {
        setLoadErr(null);
        setGeneration((g) => g + 1);
      }}
      onDomainsChange={setDomains}
    />
  );
}

beforeEach(() => {
  identity.app = { authReady: true, getAuthToken: () => "TOKEN" };
  list.mockReset();
  claim.mockReset();
  remove.mockReset();
  verify.mockReset();
  requestConfirmation.mockReset();
});
afterEach(() => {
  cleanup();
});

describe("DomainVerification — loading / error / empty", () => {
  it("shows a loading state, then the empty state with a still-visible claim form", async () => {
    list.mockResolvedValue({ domains: [] });
    render(<Harness />);
    expect(screen.getByText(/verified domains…/i)).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());
    expect(screen.getByPlaceholderText("acme.com")).toBeInTheDocument();
  });

  it("shows a retry action when the list read fails, and the error box clears the instant retry is clicked (item 6)", async () => {
    list.mockRejectedValue(new ApiError("service unavailable", 503));
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("button", { name: /try again/i })).toBeInTheDocument());
    list.mockResolvedValue({ domains: [] });
    fireEvent.click(screen.getByRole("button", { name: /try again/i }));
    // The error box (and its button) disappear immediately — no stale
    // ErrorNote lingering with nothing to indicate a retry is in flight.
    expect(screen.queryByRole("button", { name: /try again/i })).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());
  });

  /** item 1, code review on PR #1048: a raw backend 403 ("admin only",
   *  "personal accounts have no org domains") must never reach the screen
   *  verbatim inside a `role="alert"` note with a retry that can never
   *  succeed. */
  it("a 403 reads as calm, specific copy with NO retry — a dead end, same as the roster's own 403", async () => {
    list.mockRejectedValue(new ApiError("admin only", 403));
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("alert")).toBeInTheDocument());
    expect(screen.queryByText("admin only")).not.toBeInTheDocument();
    expect(screen.getByText(/only an admin/i)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /try again/i })).not.toBeInTheDocument();
  });
});

describe("DomainVerification — the '#domains' section id AutoAddSetting's gate copy links to (item 5)", () => {
  it("the root element carries id=\"domains\"", async () => {
    list.mockResolvedValue({ domains: [] });
    const { container } = render(<Harness />);
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());
    expect(container.querySelector("#domains")).not.toBeNull();
  });
});

describe("DomainVerification — status legibility (DNS arms auto-add, email doesn't)", () => {
  it("a DNS-verified domain reads as arming auto-add", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "dns_txt" })] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());
    expect(screen.getByText(/verified by dns — arms auto-add/i)).toBeInTheDocument();
    expect(screen.getByText("DNS")).toBeInTheDocument();
    // Fully DNS-armed — no TXT panel or email form offered any more.
    expect(screen.queryByText(/check dns now/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/confirmation address/i)).not.toBeInTheDocument();
  });

  it("an email-verified domain reads as verified but explicitly NOT arming auto-add", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "email" })] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());
    expect(screen.getByText(/verified by email.*only a dns-verified domain arms auto-add/i)).toBeInTheDocument();
    expect(screen.getByText("Email")).toBeInTheDocument();
  });

  it("an unverified domain offers BOTH the DNS record and the email-confirmation option, with the auto-add consequence stated for each", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());
    expect(screen.getByText("Not verified yet.")).toBeInTheDocument();
    expect(screen.getByText("dns txt record — arms auto-add")).toBeInTheDocument();
    expect(
      screen.getByText((_content, node) => node?.textContent === "Proves you control acme.example, same as DNS — but auto-add only arms from a DNS-verified domain."),
    ).toBeInTheDocument();
    expect(screen.getByText("_coralswarm-verification.acme.example")).toBeInTheDocument();
    expect(screen.getByText("coralswarm-domain-verification=abc123")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send link/i })).toBeInTheDocument();
  });
});

describe("DomainVerification — BLOCKER 2: an email-verified domain can be upgraded to DNS", () => {
  it("keeps the TXT record panel and 'Check DNS now' available for a verified-but-not-dns_txt domain, and explains the upgrade", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "email", verified_at: "2026-09-20T10:00:00Z" })] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());

    // Still verified-by-email at the row level...
    expect(screen.getByText(/verified by email.*only a dns-verified domain arms auto-add/i)).toBeInTheDocument();
    // ...but the TXT panel and its action are NOT hidden (the bug: gated on
    // `d.verified` alone, which hid this for ANY proof).
    expect(screen.getByText("_coralswarm-verification.acme.example")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument();
    expect(
      screen.getByText(/already proven by email — publishing this record is what arms auto-add/i),
    ).toBeInTheDocument();

    // The email-confirmation panel, unlike the TXT one, stays hidden once
    // ANY proof exists — the backend still refuses a second confirmation
    // with 409 "already verified".
    expect(screen.queryByLabelText(/confirmation address/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /send link/i })).not.toBeInTheDocument();
  });

  it("checking DNS on an email-verified domain can upgrade it to arm auto-add", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "email" })] });
    verify.mockResolvedValue(domain({ verified: true, method: "dns_txt", verified_at: "2026-09-22T10:00:00Z" }));
    render(<Harness />);
    const checkButton = await screen.findByRole("button", { name: /check dns now/i });
    fireEvent.click(checkButton);

    await waitFor(() => expect(verify).toHaveBeenCalledWith("TOKEN", "dom_1"));
    await waitFor(() => expect(screen.getByText(/verified by dns — arms auto-add/i)).toBeInTheDocument());
    // Fully armed now — the upgrade explanation and the TXT panel are gone.
    expect(screen.queryByText(/already proven by email/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /check dns now/i })).not.toBeInTheDocument();
  });

  it("a lone TXT panel (email-verified, upgrade case) does not sit half-width next to nothing", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "email" })] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());
    const panelGrid = screen.getByText("dns txt record — arms auto-add").closest("div.mt-\\[12px\\]");
    expect(panelGrid?.className).not.toMatch(/sm:grid-cols-2/);
  });
});

describe("DomainVerification — claiming a domain", () => {
  it("claims, clears the input, and shows the new row", async () => {
    list.mockResolvedValue({ domains: [] });
    claim.mockResolvedValue(domain({ domain: "beta.example" }));
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());

    const input = screen.getByLabelText("Domain to claim");
    fireEvent.change(input, { target: { value: "beta.example" } });
    fireEvent.click(screen.getByRole("button", { name: /claim domain/i }));

    await waitFor(() => expect(claim).toHaveBeenCalledWith("TOKEN", "beta.example"));
    await waitFor(() => expect(screen.getByText("beta.example")).toBeInTheDocument());
    expect((input as HTMLInputElement).value).toBe("");
  });

  it("shows the backend's own rejection message verbatim on a 400", async () => {
    list.mockResolvedValue({ domains: [] });
    claim.mockRejectedValue(new ApiError("that is not a domain — enter it as acme.com, with no scheme, path, or @", 400));
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText("Domain to claim"), { target: { value: "not a domain" } });
    fireEvent.click(screen.getByRole("button", { name: /claim domain/i }));

    await waitFor(() => expect(screen.getByText(/enter it as acme.com/i)).toBeInTheDocument());
  });

  /** item 7, code review on PR #1048 — same Enter-submit every other add
   *  row in the app binds (`CaptureExclusions.tsx`, `WorkspaceMembers.tsx`,
   *  `NewWorkspace.tsx`, `onboarding/org/invite/page.tsx`). */
  it("Enter in the claim-domain input submits, same as clicking 'Claim domain'", async () => {
    list.mockResolvedValue({ domains: [] });
    claim.mockResolvedValue(domain({ domain: "beta.example" }));
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText("Domain to claim"), { target: { value: "beta.example" } });
    fireEvent.keyDown(screen.getByLabelText("Domain to claim"), { key: "Enter" });

    await waitFor(() => expect(claim).toHaveBeenCalledWith("TOKEN", "beta.example"));
  });
});

describe("DomainVerification — DNS check: 409 and 502 must read differently", () => {
  it("409 (not published) shows the backend's own specific message", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    verify.mockRejectedValue(
      new ApiError("no matching TXT record at _coralswarm-verification.acme.example yet (0 TXT record(s) found there) — DNS can take a while to propagate", 409),
    );
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /check dns now/i }));
    await waitFor(() => expect(screen.getByText(/dns can take a while to propagate/i)).toBeInTheDocument());
  });

  it("502 (resolver unreachable) shows DIFFERENT copy that never claims the record is missing", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    verify.mockRejectedValue(new ApiError("could not reach a DNS resolver", 502));
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /check dns now/i }));
    await waitFor(() => expect(screen.getByText(/couldn't reach a dns resolver/i)).toBeInTheDocument());
    expect(screen.queryByText(/dns can take a while to propagate/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/not published/i)).not.toBeInTheDocument();
  });

  it("a successful check replaces the row with the verified state", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    verify.mockResolvedValue(domain({ verified: true, method: "dns_txt", verified_at: "2026-09-22T10:00:00Z" }));
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /check dns now/i }));
    await waitFor(() => expect(screen.getByText(/verified by dns — arms auto-add/i)).toBeInTheDocument());
  });

  /** item 2, code review on PR #1048: a 404 means the client's OWN list is
   *  stale (this row is already gone) — worth refetching, not just showing
   *  calmer copy for, same "refetch on a stale-list signal" precedent the
   *  page's own invite handlers hold for `not_pending`. */
  it("a 404 triggers a refetch of the whole list — the client's own copy was stale", async () => {
    list.mockResolvedValueOnce({ domains: [domain()] });
    verify.mockRejectedValue(new ApiError("not_found", 404));
    render(<Harness />);
    await waitFor(() => expect(screen.getByRole("button", { name: /check dns now/i })).toBeInTheDocument());

    list.mockResolvedValueOnce({ domains: [] });
    fireEvent.click(screen.getByRole("button", { name: /check dns now/i }));

    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());
  });
});

describe("DomainVerification — per-row in-flight state is independent (item 3)", () => {
  it("checking DNS on one row does not disable or re-enable another row's own button", async () => {
    let resolveFirst: (d: OrgDomain) => void = () => {};
    list.mockResolvedValue({
      domains: [domain({ id: "dom_1", domain: "acme.example" }), domain({ id: "dom_2", domain: "beta.example" })],
    });
    verify.mockImplementation((_token, id) => {
      if (id === "dom_1") {
        return new Promise((resolve) => {
          resolveFirst = resolve;
        });
      }
      return Promise.resolve(domain({ id: "dom_2", domain: "beta.example", verified: true, method: "dns_txt" }));
    });
    render(<Harness />);
    await waitFor(() => expect(screen.getAllByRole("button", { name: /check dns now/i })).toHaveLength(2));

    const [firstButton, secondButton] = screen.getAllByRole("button", { name: /check dns now|checking/i });
    fireEvent.click(firstButton);
    // The first row's button is now disabled/"Checking…" while its request
    // is still in flight...
    await waitFor(() => expect(firstButton).toHaveTextContent(/checking/i));
    // ...but the SECOND row's button is untouched — a single shared
    // `verifyingId` used to disable/relabel BOTH.
    expect(secondButton).not.toHaveTextContent(/checking/i);
    expect(secondButton).toBeEnabled();

    fireEvent.click(secondButton);
    await waitFor(() => expect(verify).toHaveBeenCalledWith("TOKEN", "dom_2"));
    await waitFor(() => expect(screen.getByText("beta.example")).toBeInTheDocument());
    // The still-pending first row is unaffected by the second row's own
    // request completing.
    expect(firstButton).toHaveTextContent(/checking/i);

    resolveFirst(domain({ id: "dom_1", domain: "acme.example", verified: true, method: "dns_txt" }));
    await waitFor(() => expect(screen.getAllByText(/verified by dns — arms auto-add/i)).toHaveLength(2));
  });
});

describe("DomainVerification — email confirmation prefills from the backend's own outstanding send (item 4)", () => {
  it("prefills the address input from confirmation_email, and surfaces confirmation_sent_at", async () => {
    list.mockResolvedValue({
      domains: [
        domain({ confirmation_email: "person@example.com", confirmation_sent_at: "2026-09-20T10:00:00Z" }),
      ],
    });
    render(<Harness />);
    const emailInput = (await screen.findByLabelText(
      "Confirmation address at acme.example",
    )) as HTMLInputElement;
    expect(emailInput.value).toBe("person@example.com");
    expect(screen.getByText(/last sent to person@example.com/i)).toBeInTheDocument();
  });

  it("an edit this session overrides the prefill on submit", async () => {
    list.mockResolvedValue({
      domains: [domain({ confirmation_email: "person@example.com", confirmation_sent_at: "2026-09-20T10:00:00Z" })],
    });
    requestConfirmation.mockResolvedValue({ status: "sent" });
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() => expect(requestConfirmation).toHaveBeenCalledWith("TOKEN", "dom_1", "person@example.com"));
  });

  /** item 7 — the confirmation-email row is the app's SECOND "add row" in
   *  this same file; both get the same Enter-submit binding. */
  it("Enter in the confirmation-email input submits, same as clicking 'Send link'", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    requestConfirmation.mockResolvedValue({ status: "sent" });
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    fireEvent.keyDown(emailInput, { key: "Enter" });

    await waitFor(() =>
      expect(requestConfirmation).toHaveBeenCalledWith("TOKEN", "dom_1", "person@example.com"),
    );
  });
});

describe("DomainVerification — email confirmation: a 503 has TWO causes, only one is permanent (BLOCKER 3)", () => {
  it("sends to the typed address and shows a 'sent' notice with a way to send another", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    requestConfirmation.mockResolvedValue({ status: "sent" });
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() =>
      expect(requestConfirmation).toHaveBeenCalledWith("TOKEN", "dom_1", "person@example.com"),
    );
    await waitFor(() => expect(screen.getByText(/sent — check the inbox/i)).toBeInTheDocument());
    expect(screen.getByRole("button", { name: /send another/i })).toBeInTheDocument();
  });

  /** THE permanent case — `error: "email_confirmation_not_configured"`
   *  (`backend/src/routes/org_domains.rs::confirmation_not_configured`),
   *  read from that exact string, not guessed. */
  it("the backend's permanent discriminator reads as 'not set up on this deployment', pointing back at DNS — not a retryable error", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    requestConfirmation.mockRejectedValue(new ApiError("email_confirmation_not_configured", 503));
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() => expect(screen.getByText(/set up on this deployment/i)).toBeInTheDocument());
    expect(screen.getByText(/verify by dns instead/i)).toBeInTheDocument();
    // The raw, unhelpful backend body must never leak through verbatim.
    expect(screen.queryByText("email_confirmation_not_configured")).not.toBeInTheDocument();
    // And the form must not reappear offering a retry that will only 503 again.
    expect(screen.queryByLabelText("Confirmation address at acme.example")).not.toBeInTheDocument();
  });

  /** THE transient case — the founder's own correction (this story's
   *  original brief called EVERY 503 here permanent; that was wrong, and
   *  it was the founder's error, not the implementer's — see this file's
   *  own history). A plain "service unavailable" (a control-plane blip)
   *  MUST keep the form, with a retryable message, not the permanent one. */
  it("any OTHER 503 body reads as transient — the form STAYS, with a retryable message, never the permanent copy", async () => {
    list.mockResolvedValue({ domains: [domain()] });
    requestConfirmation.mockRejectedValue(new ApiError("service unavailable", 503));
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() => expect(screen.getByText(/briefly unavailable — try again/i)).toBeInTheDocument());
    expect(screen.queryByText(/set up on this deployment/i)).not.toBeInTheDocument();
    // The form is still here — this IS worth retrying.
    expect(screen.getByLabelText("Confirmation address at acme.example")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send link/i })).toBeInTheDocument();
  });

  /** item 2: the confirmation 409 "already verified" means the client's
   *  own list is stale (verified elsewhere since the last read) — worth a
   *  refetch. The cooldown 409 ("sent very recently") is NOT stale and
   *  must NOT trigger one. */
  it("a 409 'already verified' triggers a refetch; the cooldown 409 does not", async () => {
    list.mockResolvedValueOnce({ domains: [domain()] });
    requestConfirmation.mockRejectedValueOnce(new ApiError("that domain is already verified", 409));
    render(<Harness />);
    const emailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(emailInput, { target: { value: "person@example.com" } });
    list.mockResolvedValueOnce({ domains: [domain({ verified: true, method: "dns_txt" })] });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByText(/verified by dns — arms auto-add/i)).toBeInTheDocument());

    cleanup();
    list.mockReset();
    requestConfirmation.mockReset();
    list.mockResolvedValueOnce({ domains: [domain()] });
    requestConfirmation.mockRejectedValueOnce(
      new ApiError("a confirmation was sent for that domain very recently — try again shortly", 409),
    );
    render(<Harness />);
    const secondEmailInput = await screen.findByLabelText("Confirmation address at acme.example");
    fireEvent.change(secondEmailInput, { target: { value: "person@example.com" } });
    fireEvent.click(screen.getByRole("button", { name: /send link/i }));

    await waitFor(() => expect(screen.getByText(/try again shortly/i)).toBeInTheDocument());
    expect(list).toHaveBeenCalledTimes(1);
  });
});

describe("DomainVerification — remove is a two-step confirm, never window.confirm()", () => {
  it("Remove -> Confirm removes the row; Cancel backs out without calling the API", async () => {
    list.mockResolvedValue({ domains: [domain({ verified: true, method: "dns_txt" })] });
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /^remove$/i }));
    fireEvent.click(screen.getByRole("button", { name: /^cancel$/i }));
    expect(remove).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: /^remove$/i })).toBeInTheDocument();

    remove.mockResolvedValue(undefined);
    fireEvent.click(screen.getByRole("button", { name: /^remove$/i }));
    fireEvent.click(screen.getByRole("button", { name: /^confirm$/i }));
    await waitFor(() => expect(remove).toHaveBeenCalledWith("TOKEN", "dom_1"));
    await waitFor(() => expect(screen.queryByText("acme.example")).not.toBeInTheDocument());
  });

  it("a 404 on remove (already gone server-side) triggers a refetch of the whole list", async () => {
    list.mockResolvedValueOnce({ domains: [domain()] });
    remove.mockRejectedValue(new ApiError("not_found", 404));
    render(<Harness />);
    await waitFor(() => expect(screen.getByText("acme.example")).toBeInTheDocument());

    list.mockResolvedValueOnce({ domains: [] });
    fireEvent.click(screen.getByRole("button", { name: /^remove$/i }));
    fireEvent.click(screen.getByRole("button", { name: /^confirm$/i }));

    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByText("No domains claimed yet")).toBeInTheDocument());
  });
});
