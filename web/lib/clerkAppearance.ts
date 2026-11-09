// Clerk appearance for <SignIn> / <SignUp>, applied once on <ClerkProvider>
// (app/layout.tsx) so every Clerk surface shares it.
//
// Two halves, in ascending order of brittleness:
//
//   variables — Clerk's documented theming API. Prefer this. Clerk derives whole
//     colour scales from these, which is why they must be concrete hex values,
//     not var(--brand): the colour maths can't run on an unresolved custom
//     property. That means the hex below duplicates app/globals.css :root, so
//     the two have to be changed together. Non-colour values (fonts) are safe as
//     vars because Clerk passes them through untouched.
//
//   elements — per-element class overrides keyed by Clerk's element descriptors.
//     Not fully stable across clerk-js majors, so keep this list as short as the
//     design allows and re-check it on @clerk/nextjs upgrades. Tailwind's `!`
//     prefix is load-bearing wherever clerk-js sets the same property from its
//     own injected stylesheet — without it their rule wins on specificity. This
//     is the same fight globals.css already picks for the social logos.
//
// Instance shape this is designed against (from `clerk config pull`): there is
// NO password strategy. Sign-in first factors are email_code and email_link,
// plus passkey; Apple/Google/X are the configured OAuth providers; MFA is off.
// So the descriptors that matter are the OTP field, the identity preview, the
// resend link and the alternative-methods (passkey) buttons — not password
// fields. Revisit if strategies change.

// Cool-neutral values, kept in lockstep with app/globals.css :root and
// tailwind.config.ts (the 2026-07 redesign — see that file's header comment
// for the full before/after table). Must stay literal hex, not var(--x): see
// the file banner above on why Clerk's colour maths can't run on an
// unresolved custom property.
const tokens = {
  bg: "#ffffff",
  bg2: "#f5f6f8",
  ink: "#14161a",
  ink2: "#4b5058",
  ink3: "#8a9099",
  line: "#e4e6ea",
  brand: "#f77764",
  brand2: "#d14a38",
  ok: "#3f8a5b",
  warn: "#d89a3a",
  bad: "#c24a3a",
} as const;

export const clerkAppearance = {
  variables: {
    colorPrimary: tokens.brand,
    colorBackground: tokens.bg,
    colorText: tokens.ink,
    colorTextSecondary: tokens.ink3,
    colorTextOnPrimaryBackground: tokens.bg,
    colorInputBackground: tokens.bg,
    colorInputText: tokens.ink,
    colorNeutral: tokens.ink2,
    colorDanger: tokens.bad,
    colorSuccess: tokens.ok,
    colorWarning: tokens.warn,
    borderRadius: "9px", // --r-field
    fontFamily: "var(--sans)",
    fontFamilyButtons: "var(--sans)",
  },

  elements: {
    // Container — the page shell (components/auth/AuthShell.tsx) already
    // supplies the centring, reef-bg and wordmark, so the card only needs to
    // not fight it. Radius/shadow match 4a/4b/4c's card (16px in the mock;
    // --r-frame at 14px is the nearest token — see the redesign's radius
    // scale, which tops out there).
    rootBox: "mx-auto w-full",
    cardBox: "!shadow-none",
    card: "!bg-bg !border !border-line !shadow-pop !rounded-frame !px-6 !py-7",

    // Header — display serif, matching the wordmark above it.
    header: "!gap-1",
    headerTitle: "!font-display !text-[1.375rem] !font-normal !text-ink",
    headerSubtitle: "!text-ink-3 !text-sm",

    // OAuth (Apple / Google / X). Icon glyphs themselves are re-pointed in
    // globals.css — don't restyle socialButtonsProviderIcon here or the two
    // rules will fight.
    socialButtons: "!gap-2",
    socialButtonsBlockButton:
      "!bg-bg !border !border-line !text-ink hover:!bg-bg-hover !rounded-field !shadow-none",
    socialButtonsBlockButtonText: "!text-ink !font-normal",

    dividerRow: "!my-5",
    dividerLine: "!bg-line-2",
    dividerText: "!text-ink-4 !text-xs !uppercase !tracking-wide",

    // Email field + the email_code OTP step.
    formFieldLabel: "!text-ink-2 !text-xs !font-medium",
    formFieldInput:
      "!bg-bg !border !border-line !text-ink !rounded-field focus:!border-brand focus:!ring-2 focus:!ring-brand/25",
    formFieldErrorText: "!text-bad !text-xs",
    formFieldHintText: "!text-ink-3 !text-xs",
    otpCodeFieldInput: "!border-line !text-ink !rounded-field focus:!border-brand",
    formResendCodeLink: "!text-brand-2 hover:!text-brand",
    identityPreview: "!bg-bg-2 !border !border-line !rounded-field",
    identityPreviewText: "!text-ink-2",
    identityPreviewEditButton: "!text-brand-2 hover:!text-brand",

    // Sign-in's primary CTA is ink (4a/4b, and every other Clerk surface in
    // Settings) — 4c's coral sign-up CTA is a scoped override, not a second
    // appearance object; see .cs-signup-cta in app/globals.css.
    formButtonPrimary:
      "!bg-ink hover:!bg-ink-2 !text-white !rounded-field !shadow-none !normal-case !text-sm !font-medium !tracking-normal",

    // Passkey / "use another method".
    alternativeMethodsBlockButton:
      "!bg-transparent !border !border-line !text-ink hover:!bg-bg-2 !rounded-field",
    alternativeMethodsBlockButtonText: "!text-ink !font-normal",
    backLink: "!text-brand-2 hover:!text-brand",

    footer: "!bg-transparent !border-none",
    footerActionText: "!text-ink-3 !text-sm",
    footerActionLink: "!text-brand-2 hover:!text-brand !font-medium",

    alert: "!bg-bg-2 !border !border-line !rounded-field",
    alertText: "!text-ink-2 !text-sm",
    spinner: "!text-brand",
  },
} as const;
