"use client";

// 6g — Joining as a member. What an invited member sees when they open the
// invite: NOT part of the admin's 6a–6e wizard (a different person, a
// different entry point — reached from an email link, not from /onboarding).
// The callout below is the one place in this task that has to say, in plain
// language, what "private" means inside an org versus on a personal account.
//
// Real sign-in flow (org invites PR 2 web): a custom Clerk email-code flow via
// `useSignIn`, not `<SignIn>` — this page owns its own three-step state
// machine (landing → code → name) instead of routing/hash-based Clerk UI, so
// it can carry the invite's org/inviter framing through every step. There is
// no backend endpoint for the display name typed in the last step: it's
// written straight to Clerk (`user.update`), and the control-plane
// `users.name` picks it up passively off the JWT's name claim on the next
// authenticated request — see the comment on `finishNameStep` below.

import { Suspense, useEffect, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useAuth, useOrganization, useSignIn, useUser } from "@clerk/nextjs";
import { Button, Input, Loading, Mono } from "@/components/ui";
import { IconLibrary, IconLock } from "@/components/icons";
import { OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";

const WEB_APEX = process.env.NEXT_PUBLIC_WEB_APEX || "coralswarm.example";

function roleLabel(role: string | null | undefined): string {
  return (role ?? "member").replace(/^org:/, "");
}

/** `org`/`inviter` arrive as unsigned query-string params and render into
 *  page copy on a trusted origin — passed through raw they're an injection
 *  and layout hazard. Strip anything outside a conservative, unicode-aware
 *  allowlist, trim, and cap length; empty after stripping reads as absent
 *  (`null`) rather than an empty string.
 *
 *  WHAT THIS DOES NOT DO: it bounds the CHARACTERS a label may contain, never
 *  its truthfulness. `?org=Payroll%20Portal&inviter=IT%20Security` survives
 *  this function intact, because every character in it is ordinary. Authority
 *  is a separate question, settled in `JoinInner`: a sanitized label is only
 *  ever shown as something the LINK claims, never as something we assert.
 *
 *  Kept in lockstep with `safe_label` in `backend/src/routes/org_invites.rs`
 *  (which builds these params for the invite email and has a test asserting it
 *  mirrors this one) — change the allowlist or the cap in one, change both. */
function safeLabel(raw: string | null): string | null {
  if (!raw) return null;
  const cleaned = raw
    .replace(/[^\p{L}\p{N} .,'&-]/gu, "")
    .trim()
    .slice(0, 48)
    .trim();
  return cleaned.length === 0 ? null : cleaned;
}

/** Maps the `{ error }` returned by a `SignInFutureResource` method (this
 *  Clerk version's `useSignIn()` returns the new signals-based "future" API —
 *  `signIn.emailCode.sendCode/verifyCode/finalize`, each resolving
 *  `{ error: ClerkError | null }` rather than throwing) to calm, specific
 *  copy. Duck-typed rather than importing `ClerkError` — `@clerk/shared` isn't
 *  a direct dependency here, only `@clerk/nextjs` is. `form_identifier_not_found`
 *  is Clerk's code for "no account with this identifier", which for an invite
 *  link means the invite doesn't exist or was revoked.
 *
 *  This page is reached unauthenticated, straight off an email link — the
 *  address typed into the landing-step Input is never verified as belonging
 *  to whoever is looking at the screen. A message that says "this address
 *  doesn't have a slot" would turn /join into a member-existence oracle: an
 *  attacker submits arbitrary addresses and learns who holds a slot here (and
 *  by extension, who works here). Keep the copy neutral for this code
 *  regardless of how the page got its email — the query-string `email` param
 *  is unsigned and proves nothing either way, so there's no "trusted" case to
 *  special-case back in. */
function clerkErrorMessage(err: { code?: string; message?: string; longMessage?: string } | null | undefined): string {
  if (!err) return "Something went wrong — try again.";
  if (err.code === "form_identifier_not_found") {
    return "We couldn't start sign-in for that address. Check it, or ask your admin for a new invite.";
  }
  return err.longMessage || err.message || "Something went wrong — try again.";
}

const MEMBER_GETS = [
  {
    title: "A workspace of your own",
    icon: IconLock,
    hue: "coral" as const,
  },
  {
    title: "The team's shared knowledge",
    icon: IconLibrary,
    hue: "teal" as const,
  },
];

const DAY_ONE = [
  { title: "The org's shared workspace", meta: "everyone starts here", hue: "teal" as const },
  { title: "Connect your own Slack account", meta: "set up from Connectors", hue: "amber" as const },
  { title: "A workspace of your own", meta: "private · empty until you fill it", hue: "coral" as const },
];

type Step = "landing" | "code" | "name";

/** The hostname this invite was opened on, for the "you'll sign in at X" copy.
 *
 *  Resolved post-mount only — reading `window.location.hostname` during the
 *  first render would disagree with the server-rendered markup (this page is
 *  prerendered at build time, without a `window`). Starts at the env-configured apex, which
 *  is exactly what both the server and the first client render already agree
 *  on, then refines once mounted.
 *
 *  It used to ALSO return an org name derived from the host's slug
 *  (`activeOrgSlugFromHost()`), preferred over the `?org=` query param because
 *  a hostname can't be forged by a link. Claim-based tenancy (PR 2.1) removed
 *  org hostnames, so there is no host-derived name left to prefer and this is
 *  now a display string only. The gap that left was NOT filled by promoting
 *  the unsigned query param — see `verifiedOrg` in `JoinInner`. */
function useJoinHost(): string {
  const [host, setHost] = useState(WEB_APEX);
  useEffect(() => {
    setHost(window.location.hostname);
  }, []);
  return host;
}

function JoinInner() {
  const params = useSearchParams();
  const router = useRouter();
  const emailParam = params.get("email") ?? "";
  const orgParam = safeLabel(params.get("org"));
  const inviterParam = safeLabel(params.get("inviter"));

  const host = useJoinHost();

  const { isLoaded: authLoaded, isSignedIn, orgRole } = useAuth();
  const { organization } = useOrganization();
  const { user, isLoaded: userLoaded } = useUser();
  // This Clerk version's `useSignIn()` returns the signals-based "future" API
  // (`{ signIn, errors, fetchStatus }`, not the classic `{ isLoaded, signIn,
  // setActive }`): `signIn` is never null, and its factor-specific methods
  // (`emailCode.sendCode` / `.verifyCode` / `finalize`) resolve `{ error }`
  // instead of throwing. See `SignInFutureResource` in
  // `@clerk/shared/dist/types/signInFuture.d.ts`.
  const { signIn } = useSignIn();

  // THE ONLY AUTHORITATIVE ORG NAME ON THIS PAGE: the one Clerk put on the
  // session. Null until the visitor is actually signed in.
  //
  // `orgParam` is NOT a substitute for it. Our own invite emails set `?org=`
  // and `?inviter=` (backend `org_invites.rs`), but so can anyone who retypes
  // the URL, and `safeLabel` bounds only which characters they may contain,
  // never whether they are true. While each org had its own hostname the
  // forged case was covered — the host-derived slug was preferred and a link
  // can't forge a hostname. Claim-based tenancy (PR 2.1) removed org
  // hostnames, which would have left the param as the sole source: an
  // attacker-supplied name rendered as first-party fact ("IT Security invited
  // you to Payroll Portal"), on our domain, above a sign-in form. That is a
  // phishing pretext we would be hosting, so the param does not get promoted
  // into the gap.
  //
  // Instead: nothing on this page asserts an organization's identity before
  // Clerk has issued a session carrying one. Pre-auth copy is generic, and
  // the link's own labels appear exactly once, attributed to the link (see
  // the "your invite link says" line below) rather than stated as ours.
  // Which org the visitor actually joins was never decided here anyway — the
  // Clerk invitation the email code redeems decides it.
  const verifiedOrg = isSignedIn ? (organization?.name ?? null) : null;
  const org = verifiedOrg ?? "this organization";

  const [step, setStep] = useState<Step>("landing");
  const [email, setEmail] = useState(emailParam);
  const [code, setCode] = useState("");
  const [codeErr, setCodeErr] = useState<string | null>(null);
  const [sendingCode, setSendingCode] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [name, setName] = useState("");
  const [savingName, setSavingName] = useState(false);
  const [nameStepErr, setNameStepErr] = useState<string | null>(null);

  // Prefill the name step from whatever Clerk already knows once the sign-in
  // completes and `useUser` resolves — but only if the field is still exactly
  // what the user left it (never clobber in-progress typing if `useUser`
  // resolves late, after they've already started editing).
  const nameDirty = useRef(false);
  useEffect(() => {
    if (!nameDirty.current && user?.fullName) setName(user.fullName);
  }, [user?.fullName]);

  // `emailCode.verifyCode` only reports whether the *code* was accepted, not
  // whether the sign-in is fully complete — that lands as a fresh `signIn`
  // snapshot on the next render once the underlying signal updates (reading
  // `signIn.status` synchronously right after the `await` in `verifyCode`
  // below would still see the pre-verify snapshot). React to the transition
  // here instead, then finalize (converts the completed attempt into an
  // active session) and move on to the name step.
  //
  // Depend on `signIn.status` itself, not `signIn` — this Clerk version's
  // `signIn` object is referentially STABLE across renders (same object
  // identity every time; only its properties change), so `[step, signIn]`
  // never re-runs the effect once the object reference is already captured.
  // `signIn.status` is the changing primitive that actually drives a
  // re-render (useSignIn subscribes to the underlying signal), so it's the
  // dependency that fires here. `finalizedRef` is a one-shot guard: this
  // effect can otherwise re-run — same `status === "complete"` — on any
  // unrelated re-render (e.g. `step`/`user` changing) before `setStep("name")`
  // commits, which would call `finalize()` a second time on an
  // already-finalized (and per `canBeDiscarded`, effectively spent) attempt.
  const finalizedRef = useRef(false);
  useEffect(() => {
    if (step !== "code" || signIn.status !== "complete" || finalizedRef.current) return;
    finalizedRef.current = true;
    void (async () => {
      const { error } = await signIn.finalize();
      if (error) {
        finalizedRef.current = false;
        setCodeErr(clerkErrorMessage(error));
        return;
      }
      setStep("name");
    })();
  }, [step, signIn, signIn.status]);

  async function startSignIn() {
    const identifier = email.trim();
    if (!identifier) return;
    setCodeErr(null);
    setSendingCode(true);
    const { error } = await signIn.emailCode.sendCode({ emailAddress: identifier });
    setSendingCode(false);
    if (error) {
      setCodeErr(clerkErrorMessage(error));
      return;
    }
    setStep("code");
  }

  async function resendCode() {
    setCodeErr(null);
    setSendingCode(true);
    // No params: reuses the identifier already on this sign-in attempt.
    const { error } = await signIn.emailCode.sendCode();
    setSendingCode(false);
    if (error) setCodeErr(clerkErrorMessage(error));
  }

  async function verifyCode() {
    const trimmed = code.trim();
    if (trimmed.length === 0) return;
    setCodeErr(null);
    setVerifying(true);
    const { error } = await signIn.emailCode.verifyCode({ code: trimmed });
    setVerifying(false);
    if (error) setCodeErr(clerkErrorMessage(error));
    // On success, the completion effect above takes it from here.
  }

  function handlePrimaryCta() {
    if (authLoaded && isSignedIn) {
      if (!user?.fullName) setStep("name");
      else router.push("/home");
      return;
    }
    void startSignIn();
  }

  async function finishNameStep(skip: boolean) {
    if (skip) {
      router.push("/home");
      return;
    }
    // The button that calls this with `skip: false` is disabled until
    // `userLoaded` — this guard is a backstop, not the primary gate.
    if (!userLoaded) return;
    if (!user) {
      // Shouldn't happen post-sign-in, but `useUser` resolving to a null user
      // here would otherwise silently drop the typed name and navigate on
      // anyway — surface it instead of pretending nothing went wrong.
      setNameStepErr("Couldn't load your account just now — try again in a moment.");
      return;
    }
    setNameStepErr(null);
    const trimmed = name.trim();
    if (trimmed) {
      setSavingName(true);
      try {
        const spaceIdx = trimmed.indexOf(" ");
        const firstName = spaceIdx === -1 ? trimmed : trimmed.slice(0, spaceIdx);
        const lastName = spaceIdx === -1 ? undefined : trimmed.slice(spaceIdx + 1).trim() || undefined;
        // Clerk write — there is no backend name endpoint. Control-plane
        // `users.name` syncs passively off the JWT's name claim on the next
        // authenticated request, which is what actually propagates this
        // server-side (not this call itself).
        await user.update({ firstName, lastName });
      } catch {
        // The name is optional and editable later in Settings — a failed
        // write here must never block entry.
      } finally {
        setSavingName(false);
      }
    }
    router.push("/home");
  }

  const knownEmail = email.trim();
  // The signed-in branch names the org only when Clerk has one for us; it
  // falls back to the product name rather than to the link's claim (the same
  // "Enter Coralswarm" the name step's button already uses).
  const ctaLabel =
    authLoaded && isSignedIn
      ? verifiedOrg
        ? `Enter ${verifiedOrg}`
        : "Enter Coralswarm"
      : knownEmail
        ? `Continue as ${knownEmail}`
        : "Continue";
  const ctaDisabled = !authLoaded || (!isSignedIn && (sendingCode || !knownEmail));

  return (
    <OnboardingChrome right={<HeaderPill tone="ok">{host}</HeaderPill>}>
      <div className="w-full" style={{ maxWidth: 560 }}>
        {step === "landing" && (
          <>
            <StepCard
              eyebrow="you were invited"
              title={
                verifiedOrg ? (
                  <>
                    You&rsquo;ve been invited to <em className="italic text-brand-2">{verifiedOrg}</em>
                  </>
                ) : (
                  <>
                    You&rsquo;ve been invited to a team on{" "}
                    <em className="italic text-brand-2">Coralswarm</em>
                  </>
                )
              }
              width={560}
              className="mb-[14px]"
            >
              <div className="mb-[18px] -mt-3 flex flex-col gap-[10px]">
                <p className="text-[13.5px] leading-[1.6] text-ink-2">
                  You&rsquo;ll sign in at <span className="font-mono text-[12.5px] text-ink">{host}</span>{" "}
                  from now on. If you already have a personal account, it stays separate and nothing moves.
                </p>
                {/* The one place the unsigned `?org=`/`?inviter=` labels are
                    shown, and they are shown as the LINK's claim — the page
                    never repeats them as its own. Suppressed entirely once
                    `verifiedOrg` exists, so the real name replaces the claim
                    rather than sitting beside it. */}
                {!verifiedOrg && (orgParam || inviterParam) && (
                  <p className="text-[13.5px] leading-[1.6] text-ink-2">
                    Your invite link says{" "}
                    {inviterParam ? (
                      <>
                        <span className="text-ink">{inviterParam}</span> invited you
                      </>
                    ) : (
                      <>you were invited</>
                    )}
                    {orgParam && (
                      <>
                        {" "}
                        to <span className="text-ink">{orgParam}</span>
                      </>
                    )}
                    . A link can say anything, so we don&rsquo;t treat that as proof — sign in and
                    we&rsquo;ll show you the organization you actually joined.
                  </p>
                )}
              </div>

              <div className="mb-4 grid grid-cols-1 gap-[10px] sm:grid-cols-2">
                <div className="rounded-card border border-line bg-bg-subtle p-[14px]">
                  <div className="mb-[8px] flex items-center gap-[9px]">
                    <span
                      className="grid h-[26px] w-[26px] flex-none place-items-center rounded-row"
                      style={{
                        background: `color-mix(in srgb, var(--hue-${MEMBER_GETS[0].hue}) 16%, transparent)`,
                        color: `var(--hue-${MEMBER_GETS[0].hue})`,
                      }}
                    >
                      <IconLock width={14} height={14} />
                    </span>
                    <span className="text-[13.5px] font-semibold text-ink">{MEMBER_GETS[0].title}</span>
                  </div>
                  <div className="text-[12.5px] leading-[1.55] text-ink-2">
                    Yours alone inside {org}. Coworkers can&rsquo;t see it unless you promote something out of it.
                  </div>
                </div>
                <div className="rounded-card border border-line bg-bg-subtle p-[14px]">
                  <div className="mb-[8px] flex items-center gap-[9px]">
                    <span
                      className="grid h-[26px] w-[26px] flex-none place-items-center rounded-row"
                      style={{
                        background: `color-mix(in srgb, var(--hue-${MEMBER_GETS[1].hue}) 16%, transparent)`,
                        color: `var(--hue-${MEMBER_GETS[1].hue})`,
                      }}
                    >
                      <IconLibrary width={14} height={14} />
                    </span>
                    <span className="text-[13.5px] font-semibold text-ink">{MEMBER_GETS[1].title}</span>
                  </div>
                  <div className="text-[12.5px] leading-[1.55] text-ink-2">
                    Every topic and source your colleagues have already published, searchable from your first
                    minute.
                  </div>
                </div>
              </div>

              <div className="mb-[18px] rounded-control border border-warn/50 bg-bg-subtle-2 p-[13px_15px]">
                <p className="text-[12.5px] leading-[1.6] text-ink-2">
                  Your workspace here is private <em className="italic">from your coworkers</em> — nobody at{" "}
                  {org} can read it while you&rsquo;re a member. It is not private{" "}
                  <em className="italic">from {org}</em> in the way a personal account is: the org owns the data.
                </p>
              </div>

              {!isSignedIn && (!emailParam || codeErr) && (
                <div className="mb-[12px]">
                  <Input
                    type="email"
                    placeholder="you@company.com"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                  />
                </div>
              )}

              {codeErr && <p className="mb-[12px] text-[12.5px] leading-[1.5] text-bad">{codeErr}</p>}

              <WizardActions trailing="email code · no password needed">
                <Button variant="brand" disabled={ctaDisabled} onClick={handlePrimaryCta}>
                  {sendingCode ? "Sending code…" : ctaLabel}
                </Button>
                <Button variant="secondary" onClick={() => router.push("/login")}>
                  This isn&rsquo;t me
                </Button>
              </WizardActions>
            </StepCard>

            <div className="rounded-frame border border-line bg-bg p-[18px_20px] shadow-pop">
              <div className="mb-3 flex items-center gap-[9px]">
                <span className="font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">
                  what you get on day one
                </span>
                <span className="ml-auto font-mono text-[9.5px] text-ink-4">already set up by your admins</span>
              </div>
              <div className="flex flex-col gap-[6px]">
                {DAY_ONE.map((d) => (
                  <div
                    key={d.title}
                    className="flex items-center gap-[11px] rounded-row border border-line-2 p-[10px_12px]"
                  >
                    <span
                      className="h-[7px] w-[7px] flex-none rounded-pill"
                      style={{ background: `var(--hue-${d.hue})` }}
                    />
                    <span className="min-w-0 flex-1 text-[13px] text-ink">{d.title}</span>
                    <span className="flex-none font-mono text-[10px] text-ink-4">{d.meta}</span>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}

        {step === "code" && (
          <StepCard eyebrow="check your email" title="Enter your code" width={460}>
            <p className="mb-[16px] -mt-2 text-[13.5px] leading-[1.6] text-ink-2">
              We sent a six-digit code to{" "}
              <span className="font-mono text-[12.5px] text-ink">{email.trim()}</span>.
            </p>
            <div className="mb-[18px]">
              <Input
                label="code"
                inputMode="numeric"
                autoFocus
                maxLength={6}
                placeholder="123456"
                value={code}
                onChange={(e) => setCode(e.target.value.replace(/\D/g, ""))}
                error={codeErr ?? undefined}
              />
            </div>
            <WizardActions>
              <Button variant="brand" disabled={verifying || code.trim().length === 0} onClick={() => void verifyCode()}>
                {verifying ? "Verifying…" : "Verify"}
              </Button>
              <Button variant="ghost" disabled={sendingCode} onClick={() => void resendCode()}>
                {sendingCode ? "Sending…" : "Send a new code"}
              </Button>
            </WizardActions>
          </StepCard>
        )}

        {step === "name" && (
          <StepCard
            eyebrow="one last thing"
            eyebrowTrailing={<Mono className="ml-auto">after your first sign-in</Mono>}
            title={
              <>
                What should coworkers <em className="italic text-brand-2">call</em> you?
              </>
            }
            description="Your name sits beside everything you add to the shared workspace — nothing in a topic is anonymous."
            width={460}
          >
            <div className="mb-[18px]">
              <Input
                label="name"
                hint="Optional — you can add or change it any time in Settings."
                placeholder="Your name"
                autoFocus
                value={name}
                onChange={(e) => {
                  nameDirty.current = true;
                  setName(e.target.value);
                }}
              />
            </div>
            {nameStepErr && <p className="mb-[16px] -mt-2 text-[12.5px] leading-[1.5] text-bad">{nameStepErr}</p>}
            <WizardActions trailing={orgRole ? `joining as ${roleLabel(orgRole)}` : undefined}>
              <Button variant="brand" disabled={savingName || !userLoaded} onClick={() => void finishNameStep(false)}>
                {savingName ? "Saving…" : !userLoaded ? "One moment…" : "Enter Coralswarm"}
              </Button>
              <Button variant="ghost" disabled={savingName} onClick={() => void finishNameStep(true)}>
                Skip
              </Button>
            </WizardActions>
          </StepCard>
        )}
      </div>
    </OnboardingChrome>
  );
}

export default function JoinOrgPage() {
  return (
    <Suspense fallback={<Loading what="loading your invite" />}>
      <JoinInner />
    </Suspense>
  );
}
