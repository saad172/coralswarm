/* The copy decisions on the Gmail Add-on approval page
 * (`app/(app)/addin/approve/page.tsx`), kept framework-free so they get real
 * unit coverage — this project's vitest setup has no DOM and no
 * testing-library (see `vitest.config.ts`), so a page that calls `useUser()`
 * cannot be exercised by a plain `.test.ts` file. Same convention as
 * `lib/orgAdmin.ts` and `components/connectors/slackOnboardingModel.ts`.
 *
 * WHY THIS PAGE NEEDS AN IDENTITY LINE AT ALL. Approving a pairing code binds
 * a long-lived, revocable bearer (`csat_…`) to WHOEVER IS SIGNED IN IN THIS
 * BROWSER — not to whoever is signed into Gmail. Those are routinely
 * different people's accounts on a shared machine, and routinely two
 * different accounts of the SAME person (work Google, personal CoralSwarm).
 * Nothing on the old page said which one was about to be bound, so the
 * failure mode was silent: the add-on works, it just files everything into a
 * stranger's ocean. Naming the account before the button is the whole fix,
 * and it has to survive into the CONFIRMATION too — that is the only moment
 * someone re-reads the page carefully, and "approved for the wrong account"
 * is recoverable (Connectors → Gmail Add-on → Revoke) only if they notice. */

/** The shape this module needs out of Clerk's `useUser().user`. Structural,
 *  not Clerk's own type: the pure module must stay importable by a test that
 *  never loads the Clerk SDK. */
export interface EmailBearingUser {
  primaryEmailAddress?: { emailAddress?: string | null } | null;
  emailAddresses?: ReadonlyArray<{ emailAddress?: string | null }> | null;
}

/** The address to NAME on this page, or `null` when none is known yet.
 *
 *  Falls back to the first address on the account when `primaryEmailAddress`
 *  is unset — a real Clerk state (an account created through an OAuth
 *  provider that returned an unverified address can carry emails with no
 *  primary), and one where showing nothing would be worse than showing the
 *  address the user actually recognizes. Never invents a placeholder: `null`
 *  means "not resolved yet", which the caller renders as its own sentence
 *  rather than as a name. */
export function primaryEmailOf(user: EmailBearingUser | null | undefined): string | null {
  if (!user) return null;
  const primary = user.primaryEmailAddress?.emailAddress;
  if (typeof primary === "string" && primary.trim() !== "") return primary.trim();
  for (const entry of user.emailAddresses ?? []) {
    const addr = entry?.emailAddress;
    if (typeof addr === "string" && addr.trim() !== "") return addr.trim();
  }
  return null;
}

/** The line rendered directly above the Approve button.
 *
 *  With no address resolved this still says something true and still warns —
 *  it must never silently collapse to nothing, because the unresolved case
 *  is precisely when someone can't tell which account they're in. */
export function approvingAsLine(email: string | null): string {
  return email === null
    ? "Approving as the account you're signed in to here — not the Google account Gmail is open in."
    : `Approving as ${email}`;
}

/** The confirmation, after `POST /v1/addin/pair/approve` returns 200.
 *
 *  Names the account a second time (see the module doc) and hands back the
 *  ONE next action, quoted BYTE-FOR-BYTE as the add-on's own button renders
 *  it (`extensions/gmail-addin/Card.gs`: `.setText("I've approved —
 *  continue")`). A paraphrase here would send people hunting for a button
 *  that doesn't exist; the em dash and the curly apostrophe are part of the
 *  string they are looking at in the other window. */
export const ADDON_CONTINUE_BUTTON = "I've approved — continue";

export function approvedLine(email: string | null): string {
  const who = email === null ? "your CoralSwarm account" : email;
  return `Approved for ${who}. Go back to Gmail and tap “${ADDON_CONTINUE_BUTTON}” on the add-on card.`;
}

/* ── Microsoft add-ins (Teams + Outlook, `?kind=microsoft`) ────────────────
 * One approval connects BOTH apps, and the person picks the workspace here, in
 * the browser, because the panes themselves have no sign-in. The same rule as
 * Gmail applies: name the account (and now the workspace) before the click that
 * binds the credential, and again in the confirmation. */

export const MICROSOFT_PAGE_TITLE = "Connect CoralSwarm in Teams and Outlook";
export const MICROSOFT_PAGE_INTRO =
  "One approval connects the Teams meeting pane and the Outlook add-in — no tokens to copy, nothing to type.";

export function microsoftApprovingLine(email: string | null, workspace: string | null): string {
  const who = email === null ? "the account you're signed in to here" : email;
  return workspace === null ? `Approving as ${who}` : `Approving as ${who} · ${workspace}`;
}

/** After approval: the pane connects on its own, and the button is the manual path. */
export function microsoftApprovedLine(email: string | null, workspace: string): string {
  const who = email === null ? "your CoralSwarm account" : email;
  return `Approved for ${who} in ${workspace}. Go back to Teams or Outlook — it connects on its own, or tap “${ADDON_CONTINUE_BUTTON}”.`;
}

/** The kind query parameter: only the exact value `microsoft` selects the Microsoft flow. */
export function approvalKind(raw: string | null): "microsoft" | "gmail" {
  return raw === "microsoft" ? "microsoft" : "gmail";
}
