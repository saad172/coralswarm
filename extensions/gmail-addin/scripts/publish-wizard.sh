#!/usr/bin/env bash
#
# CoralSwarm Gmail Add-on — publishing wizard.
#
# Walks a human through the dashboard-only steps of getting the add-on from
# "Test deployments on one account" onto real users' Gmail. Every step here is
# one that NO script and NO agent can perform: they all require a browser and a
# signed-in Google account.
#
# This script is READ-ONLY. It creates nothing, deletes nothing, calls no API,
# opens no browser, and stores no credentials. It prints URLs and instructions
# and waits for you to confirm each step. Quitting at any point loses nothing.
#
# The reasoning and the citation for every instruction lives in
# docs/connectors/gmail-addon-publishing.md — this script is the checklist, that
# doc is the argument.
#
# Usage:  ./extensions/gmail-addin/scripts/publish-wizard.sh
#
set -euo pipefail

# ---------------------------------------------------------------- presentation

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
  CORAL=$'\033[38;5;209m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BLUE=$'\033[36m'
else
  BOLD=''; DIM=''; RESET=''; CORAL=''; GREEN=''; YELLOW=''; BLUE=''
fi

DOC='docs/connectors/gmail-addon-publishing.md'

rule() { printf '%s\n' "${DIM}────────────────────────────────────────────────────────────────────${RESET}"; }

banner() {
  printf '\n%s\n' "${CORAL}${BOLD}  CoralSwarm Gmail Add-on — publishing wizard${RESET}"
  printf '%s\n\n' "${DIM}  Read-only. Nothing here changes any system. Ctrl-C is always safe.${RESET}"
}

# step <id> <who> <title>
#
# <id> is PHASE-LOCAL and matches the runbook table in the doc ("1.7" here is
# step 1.7 there), so a step means the same thing however you entered the
# wizard. A global counter would renumber everything when Phase 0 is skipped.
step() {
  printf '\n'; rule
  printf '%s\n' "${BOLD}Step $1 ${DIM}[$2]${RESET}${BOLD} — $3${RESET}"
  rule
}

url()   { printf '  %s %s\n' "${BLUE}open →${RESET}" "${BOLD}$1${RESET}"; }
info()  { printf '  %s\n' "$1"; }
field() { printf '  %s %-34s %s\n' "${GREEN}·${RESET}" "$1" "$2"; }
warn()  { printf '  %s %s\n' "${YELLOW}!${RESET}" "$1"; }
ref()   { printf '  %s\n' "${DIM}why: ${DOC} §$1${RESET}"; }

# Pause until the human confirms. Never proceeds silently; never assumes.
confirm() {
  local prompt="${1:-Done with this step?}" reply
  while true; do
    printf '\n  %s [%s/%s/%s] ' "${BOLD}${prompt}${RESET}" "y=done" "s=skip" "q=quit"
    if ! IFS= read -r reply; then printf '\n'; bail; fi
    case "${reply}" in
      y|Y|yes|YES) printf '  %s\n' "${GREEN}✓ done${RESET}"; return 0 ;;
      s|S|skip)    printf '  %s\n' "${YELLOW}→ skipped${RESET}"; return 0 ;;
      q|Q|quit)    bail ;;
      # Anything else — INCLUDING an empty line — re-prompts. Bare Enter must
      # never tick off a blocker check the human did not actually read.
      *)           printf '  %s\n' "${DIM}please answer y, s, or q${RESET}" ;;
    esac
  done
}

# Ask a yes/no question; returns 0 for yes, 1 for no.
ask() {
  local prompt="$1" reply
  while true; do
    printf '\n  %s [y/n] ' "${BOLD}${prompt}${RESET}"
    if ! IFS= read -r reply; then printf '\n'; bail; fi
    case "${reply}" in
      y|Y|yes|YES) return 0 ;;
      n|N|no|NO)   return 1 ;;
      *)           printf '  %s\n' "${DIM}please answer y or n${RESET}" ;;
    esac
  done
}

bail() {
  printf '\n  %s\n' "${DIM}Stopped. Nothing was changed. Re-run any time — the wizard has no state.${RESET}"
  exit 0
}

# ---------------------------------------------------------------------- phases

phase_header() {
  printf '\n\n%s\n' "${CORAL}${BOLD}═══ $1 ═══${RESET}"
  printf '%s\n' "${DIM}$2${RESET}"
}

phase_zero() {
  phase_header "PHASE 0 — blockers" \
    "Four things that must be true before any dashboard step is worth doing."

  step "0.1" "REPO" "Publish the add-on logo"
  info "appsscript.json's addOns.common.logoUrl points at:"
  info "    ${BOLD}https://app.coralswarm.example/addin/gmail-icon.png${RESET}"
  warn "As of 2026-09-13 that URL returns 404."
  info "${BOLD}It goes live with the web deploy of PR #906${RESET}, which ships the PNG"
  info "and merges ahead of this manifest being used — so this is a"
  info "merge-ordering dependency, not work for you. Just confirm the URL"
  info "actually serves an image before step 1.7 cuts the versioned deployment."
  info ""
  info "logoUrl is a REQUIRED manifest field and must be a publicly fetchable"
  info "HTTPS image. The master is 128x128, square, transparent PNG — that same"
  info "file also satisfies the store listing's 128x128 icon requirement."
  ref "1.3"
  confirm "Does that URL serve a real PNG now?"

  step "0.2" "REPO" "Produce the remaining store-listing artwork"
  field "Application icon"   "32x32 px (in addition to the 128x128 above)"
  field "App card banner"    "220x140 px"
  field "Screenshots"        "1-10; 1280x800 recommended, full bleed, square corners"
  info "Required for the store listing — which a PRIVATE app still needs."
  ref "2.a"
  confirm "Artwork ready?"

  step "0.3" "HUMAN" "Confirm the legal URLs are live"
  field "Terms of service URL" "required"
  field "Privacy policy URL"   "required (also a verification prerequisite)"
  field "Support URL"          "required"
  info "All three are hard requirements of the Marketplace store listing,"
  info "even for a private, internal-only app."
  ref "2.a"
  confirm "All three URLs live and public?"

  step "0.4" "HUMAN" "Pick the publishing Google account"
  warn "It MUST be a coralswarm.example Google Workspace account."
  info "Publishing privately requires that the app was built using a Google"
  info "Workspace account, and that the Cloud project is owned by the org."
  info "A personal @gmail.com account cannot publish a private listing."
  info ""
  info "coralswarm.example IS a Workspace domain (MX = smtp.google.com), so this"
  info "path is open — but if PR #835's test deployment currently lives on a"
  info "personal account, the Apps Script project must be recreated or"
  info "transferred to a coralswarm.example account first."
  ref "2.b"
  confirm "Signed in as a coralswarm.example account?"
}

phase_one() {
  phase_header "PHASE 1 — private listing for coralswarm.example" \
    "No Google review. No OAuth verification. Live the moment you publish."

  step "1.1" "HUMAN" "Create a NEW standard Cloud project"
  url "https://console.cloud.google.com/projectcreate"
  field "Suggested name" "coralswarm-gmail-addon-internal"
  field "Organization"   "coralswarm.example  (NOT 'No organization')"
  warn "Do NOT reuse the Google Workspace connector's Cloud project."
  info "That project's consent screen is External (it serves customers'"
  info "Google accounts). A private add-on listing needs an Internal consent"
  info "screen, and audience type is per project — one project cannot be both."
  info ""
  info "${BOLD}Record the PROJECT NUMBER${RESET} (not the ID) — step 1.4 needs it."
  ref "3"
  confirm "Project created and project number written down?"

  step "1.2" "HUMAN" "Create (or open) the Apps Script project"
  url "https://script.google.com"
  info "Owned by the coralswarm.example account from Phase 0."
  info "In Project Settings, enable ${BOLD}\"Show appsscript.json manifest file\"${RESET}"
  info "so the manifest is editable."
  confirm "Apps Script project open?"

  step "1.3" "REPO" "Push the add-on code"
  info "From this repo — an agent or you can run these:"
  printf '\n'
  info "    ${DIM}# Node >= 22 required${RESET}"
  info "    npm install -g @google/clasp"
  info "    ${DIM}# enable the Apps Script API first:${RESET}"
  info "    ${DIM}#   https://script.google.com/home/usersettings${RESET}"
  info "    clasp login"
  info "    clasp clone-script <scriptId>   ${DIM}# into a scratch dir${RESET}"
  info "    ${DIM}# copy every .gs file and appsscript.json from extensions/gmail-addin/ over it${RESET}"
  info "    clasp push"
  printf '\n'
  info "Or paste every .gs file and appsscript.json into the editor by hand."
  warn "clasp v3 renamed the v2 commands (clone -> clone-script, etc.);"
  warn "most third-party tutorials still show the older v2 spelling."
  ref "4"
  confirm "Code is in the Apps Script project?"

  step "1.4" "HUMAN" "Link Apps Script to the standard Cloud project"
  info "In the Apps Script editor:"
  field "1." "Project Settings (left panel)"
  field "2." "Google Cloud Project → Change project"
  field "3." "Enter the PROJECT NUMBER from step 1.1 → Set project"
  warn "This is IRREVERSIBLE and forces every user to re-authorize."
  warn "The auto-created default project is deleted. APIs must be re-enabled"
  warn "manually in the new project."
  info "A standard project is mandatory: you cannot publish from the default one."
  ref "3"
  confirm "Apps Script now linked to the standard project?"

  step "1.5" "HUMAN" "Configure the consent screen (Google Auth platform)"
  url "https://console.cloud.google.com/auth/branding"
  info "Cloud console → Menu → Google Auth platform → Branding"
  field "App name"              "CoralSwarm"
  field "User support email"    "a monitored coralswarm.example address"
  field "Audience"              "${BOLD}Internal${RESET}  ← this is what skips verification"
  field "Developer contact"     "a monitored coralswarm.example address"
  printf '\n'
  info "Then: ${BOLD}Data Access → Add or Remove Scopes${RESET}, and add all three:"
  field "" "https://www.googleapis.com/auth/script.external_request"
  field "" "https://www.googleapis.com/auth/gmail.addons.current.message.metadata"
  field "" "https://www.googleapis.com/auth/gmail.addons.execute"
  printf '\n'
  warn "${BOLD}Read the sensitivity label the console shows beside each scope.${RESET}"
  info "Expected: message.metadata = Sensitive; the other two = Non-sensitive;"
  info "and critically, ${BOLD}none Restricted${RESET} (Restricted would mean a paid,"
  info "annually-renewed third-party CASA security assessment for the public"
  info "listing). The console is authoritative here — Google's published scope"
  info "pages omit gmail.addons.execute entirely. If anything reads Restricted,"
  info "${BOLD}stop and re-read ${DOC} §5${RESET} before going further."
  ref "5"
  confirm "Consent screen saved, and scope labels checked?"

  step "1.6" "HUMAN" "Enable the Google Workspace Marketplace SDK"
  url "https://console.cloud.google.com/apis/library/appsmarket-component.googleapis.com"
  warn "Enable the ${BOLD}SDK${RESET}, not the similarly-named API. They differ."
  info "Make sure the console is on the project from step 1.1."
  ref "2"
  confirm "Marketplace SDK enabled?"

  step "1.7" "REPO or HUMAN" "Create the versioned deployment and copy its ID"
  info "The Marketplace SDK needs an Apps Script ${BOLD}deployment ID${RESET}."
  printf '\n'
  info "${BOLD}From the CLI:${RESET}"
  info "    clasp create-deployment -d \"v1\""
  info "    clasp list-deployments"
  printf '\n'
  info "${BOLD}Or from the editor:${RESET}"
  field "1." "Deploy → New deployment"
  field "2." "Select type → ${BOLD}Add-on${RESET} → Deploy"
  field "3." "Deploy → Manage deployments → select the active one → copy ID"
  printf '\n'
  warn "Deployment IDs only appear on ACTIVE deployments."
  warn "A versioned deployment (unlike a test deployment) ENFORCES the manifest"
  warn "allowlists: urlFetchWhitelist and openLinkUrlPrefixes must both be"
  warn "right, or backend calls and the Clerk token exchange fail silently."
  ref "4"
  confirm "Deployment ID copied?"

  step "1.8" "HUMAN" "Marketplace SDK → App Configuration"
  url "https://console.cloud.google.com/apis/api/appsmarket-component.googleapis.com/googleapps_sdk"
  field "App visibility"     "${BOLD}Private${RESET}"
  field "Installation"       "admin-only, or individual+admin — your call"
  field "App integration"    "Google Workspace add-on"
  field "  → Deployment ID"  "paste from step 1.7"
  field "OAuth scopes"       "the same three from step 1.5"
  field "Developer name"     "CoralSwarm"
  field "Developer email"    "not shown publicly"
  field "Developer website"  "https://coralswarm.example"
  field "Trader status"      "EEA requirement — a legal answer, not technical"
  printf '\n'
  warn "${BOLD}App visibility is PERMANENT once set.${RESET} Private can never become"
  warn "Public later. The public listing (Phase 2) will be an entirely separate"
  warn "Cloud project — listings and reviews do not transfer between projects."
  info "That is a deliberate, accepted cost of shipping internally this week."
  ref "2.c"
  confirm "App Configuration saved?"

  step "1.9" "HUMAN" "Marketplace SDK → Store Listing"
  field "Application name"      "<= 50 chars; match the consent screen name"
  field "Short description"     "<= 200 chars"
  field "Detailed description"  "< 16,000 chars"
  field "Category + language"   "required"
  field "Icons"                 "32x32 and 128x128 (Phase 0)"
  field "Card banner"           "220x140 (Phase 0)"
  field "Screenshots"           "1-10 (Phase 0)"
  field "Terms / Privacy / Support URLs" "all three required (Phase 0)"
  info "A private listing still requires a full store listing."
  ref "2.a"
  confirm "Store listing complete?"

  step "1.10" "HUMAN" "Publish"
  info "Hit Publish in the Marketplace SDK."
  info "${GREEN}No Google review.${RESET} The listing is available immediately to"
  info "everyone in the coralswarm.example organization, under ${BOLD}Internal apps${RESET}."
  ref "2.b"
  confirm "Published?"

  step "1.11" "HUMAN" "Admin-install it for the organization"
  url "https://admin.google.com"
  info "Admin console → Menu → Apps → Google Workspace Marketplace apps"
  info "                → Apps list → ${BOLD}Install app${RESET}"
  field "1." "Find the internal CoralSwarm app"
  field "2." "Admin install → Continue"
  field "3." "Review the data access it requests"
  field "4." "Everyone at your organization (or specific groups/OUs)"
  field "5." "Finish"
  warn "Requires the Google Workspace Marketplace administrator privilege."
  warn "Allow up to ~24 hours to propagate."
  ref "2.b"
  confirm "Installed for the org?"

  step "1.12" "HUMAN" "Verify it end to end in real Gmail"
  info "As a coralswarm.example user, in Gmail:"
  field "1." "Open any message — the CoralSwarm card should render"
  field "2." "Tap ${BOLD}Sign in to CoralSwarm${RESET} (Clerk popup: Google sign-in, workspace picker)"
  field "3." "Pick a workspace; the popup closes and the card shows it"
  field "4." "Tap ${BOLD}Save to CoralSwarm${RESET} and confirm the thread ingests"
  printf '\n'
  warn "Sign-in only works if this Apps Script project's redirect URI"
  warn "(https://script.google.com/macros/d/<SCRIPT_ID>/usercallback) is"
  warn "registered on the Clerk OAuth app, and CLERK_CLIENT_ID is filled in."
  ref "6.1"
  confirm "Full sign-in + save round-trip works?"

  printf '\n'; rule
  printf '%s\n' "${GREEN}${BOLD}  Phase 1 complete — the add-on is live for coralswarm.example.${RESET}"
  rule
}

phase_two() {
  phase_header "PHASE 2 — public Marketplace listing" \
    "A SEPARATE Cloud project, from scratch. Do this when you actually want it."

  step "2.0" "HUMAN" "Start over in a new project"
  info "Repeat Phase 1 steps 1.1-1.9 in a brand-new Cloud project"
  field "Suggested name"  "coralswarm-gmail-addon-public"
  field "Audience"        "${BOLD}External${RESET} (not Internal)"
  field "App visibility"  "${BOLD}Public${RESET}"
  warn "Nothing transfers from the private project — not the listing, not"
  warn "reviews, not the configuration. This is a genuine second setup."
  ref "2.c"
  confirm "New public project configured up to the store listing?"

  step "2.1" "HUMAN" "Verify domain ownership and brand details"
  url "https://console.cloud.google.com/auth/branding"
  field "App logo"          "the 128x128 PNG"
  field "App homepage URL"  "https://coralswarm.example"
  field "Privacy policy URL" "must be publicly accessible"
  field "Authorized domains" "coralswarm.example"
  info "Domain ownership must be verified (Search Console) and the Cloud"
  info "project must be standard — both already true from Phase 1's pattern."
  ref "2.a"
  confirm "Branding complete and domain verified?"

  step "2.2" "HUMAN" "Submit for OAuth verification"
  info "Google Auth platform → Submit for verification."
  info "Triggered by ${BOLD}gmail.addons.current.message.metadata${RESET}, which is a"
  info "${BOLD}Sensitive${RESET} scope. Prepare a demo video of the add-on in use."
  info "${GREEN}Most verification requests get a response within 24-72 hours.${RESET}"
  printf '\n'
  info "The good news: none of our three scopes is ${BOLD}Restricted${RESET}, so there is"
  info "${GREEN}no CASA third-party security assessment${RESET} and no annual"
  info "recertification. That is only true because the add-on asks for"
  info "gmail.addons.current.message.metadata rather than gmail.readonly or"
  info "gmail.metadata — both of which ARE restricted."
  warn "Never widen these scopes casually: gmail.readonly would move this from"
  warn "a ~72-hour review to a paid, annually-renewed security assessment."
  ref "5"
  confirm "Submitted for verification?"

  step "2.3" "HUMAN" "Submit the listing for Marketplace review"
  info "Only AFTER verification completes — \"the app didn't complete the OAuth"
  info "verification\" is a documented review-failure reason."
  info "Review typically takes several days."
  printf '\n'
  info "Other common rejection reasons to pre-empt:"
  field "·" "wrong consent-screen user type or publishing status"
  field "·" "low-quality or inaccurate screenshots"
  field "·" "broken links in the listing"
  field "·" "bugs or incomplete functionality"
  field "·" "trademark misuse in the name or description"
  ref "2.a"
  confirm "Submitted for review?"

  step "2.4" "HUMAN" "Optional — stay Unlisted during a beta"
  info "In App Configuration, tick ${BOLD}Unlisted${RESET}: the listing stops appearing"
  info "in browse and search, but anyone with the direct store URL can still"
  info "install. Reversible — untick to go fully public."
  warn "Unlisted is NOT a verification bypass. It hides the listing; it does"
  warn "not change who the users are. An unlisted public app still has an"
  warn "External consent screen, so verification still applies."
  ref "2.c"
  confirm "Noted?"

  printf '\n'; rule
  printf '%s\n' "${GREEN}${BOLD}  Phase 2 submitted.${RESET}"
  rule
}

# ------------------------------------------------------------------------ main

main() {
  banner

  info "This wizard covers the ${BOLD}dashboard-only${RESET} steps — the ones no script"
  info "can do for you. Steps marked [REPO] can be handed to an agent instead."
  printf '\n'
  info "Full reasoning, with a citation for every claim:"
  info "    ${BOLD}${DOC}${RESET}"
  printf '\n'
  info "Recommended path: ${BOLD}private listing now${RESET} (no review, no OAuth"
  info "verification — coralswarm.example is a Google Workspace domain), then a"
  info "${BOLD}separate project${RESET} for a public listing later."

  if ask "Run Phase 0 (blockers)?"; then phase_zero; fi
  if ask "Run Phase 1 (private listing for coralswarm.example)?"; then phase_one; fi
  if ask "Run Phase 2 (public Marketplace listing)?"; then phase_two; fi

  printf '\n'
  info "${DIM}Nothing was changed by this script. Re-run it any time.${RESET}"
  printf '\n'
}

main "$@"
