//! "Connect Google Workspace" — the one-consent-screen flow for all four
//! Google surfaces (Calendar, Gmail, Chat, Meet). Pure logic only: claim
//! encoding, scope union, granted-scope mapping and the redirect result
//! query. The route-level orchestration (state minting, callback, persistence,
//! post-connect side effects) lives in `routes::google_workspace_connect`.
//!
//! Nothing about capture semantics changes. Connecting stores a credential
//! row per surface; Gmail stays opt-in per thread, Meet per meeting, Chat per
//! space. No connect path in this module (or its caller) starts ingesting
//! anything on its own beyond what an individual per-surface connect already
//! does (a Calendar/Gmail watch channel, a Chat subscription, a Meet sync-state
//! row).
//!
//! # Why reading `scope` from the token response is acceptable HERE
//!
//! `connectors::google`'s module doc (the incremental-consent paragraph; the
//! "§1.1" `callback` and `post_store` refer to) and `routes::oauth::callback`
//! deliberately refuse to derive WHICH surface a per-surface connect was for
//! from `bundle.scope`. The reason: every Google request carries
//! `include_granted_scopes=true`, so the token response's `scope` is the UNION
//! of everything this client has ever been granted by this user. A member who
//! connected Calendar last week and now connects Gmail gets a `scope`
//! containing BOTH, so "the scope contains calendar.readonly" cannot tell you
//! the request was a Calendar one. Deriving the surface from it would write the
//! wrong row (the original CodeRabbit #669 hazard).
//!
//! The all-in-one flow is a different question. It never asks "which surface
//! was this request for"; the answer is fixed up front, by the signed state:
//!
//! 1. The set of surfaces REQUESTED is embedded in the signed `sfc` claim at
//!    mint time ([`encode_claim`]) and re-intersected with the flags in force
//!    at callback time. It is never read from the token response or from any
//!    client parameter.
//! 2. The token response's `scope` is used only to SUBTRACT from that set
//!    ([`plan_from_grant`]): a requested surface is connected iff every one of
//!    its data scopes is in the granted list (Google's granular consent lets a
//!    user untick boxes, so a partial grant is normal).
//! 3. Why a union-scope `scope` cannot mislead that subtraction: a scope Google
//!    reports as granted IS granted to this client for this user, regardless of
//!    which earlier consent granted it, and the access/refresh token we hold is
//!    valid for it. Creating the row for a requested-and-granted surface is
//!    therefore correct even if part of the grant is older than this flow. The
//!    hazard in the per-surface path was inferring a surface from the grant;
//!    here the grant can only remove surfaces, never add one.
//! 4. A scope Google returns because of `include_granted_scopes` for a surface
//!    that was NOT requested in this flow (a disabled org flag, a surface
//!    outside the claim) never creates a row: it is not in `requested`, so
//!    [`plan_from_grant`] reports it as skipped (`Disabled`) at most.

use super::google::{scopes_for, Surface};

/// The `?surface=` value / `sfc` claim prefix of the all-in-one flow.
pub(crate) const WORKSPACE_SURFACE: &str = "workspace";

/// True when `sfc` (a verified state's surface claim, or the `?surface=`
/// query param) names the all-in-one flow, in either the bare
/// (`workspace`, as sent by the web client) or the encoded
/// (`workspace:calendar,gmail`, as signed into a state) form.
pub(crate) fn is_workspace_surface(sfc: Option<&str>) -> bool {
    match sfc {
        Some(s) => s == WORKSPACE_SURFACE || s.starts_with("workspace:"),
        None => false,
    }
}

/// Encode the requested surface set into the signed `sfc` claim:
/// `workspace:calendar,gmail`. Stable order (`Surface::ALL`), de-duplicated.
pub(crate) fn encode_claim(requested: &[Surface]) -> String {
    let names: Vec<&str> = ordered(requested).iter().map(|s| s.as_str()).collect();
    format!("{WORKSPACE_SURFACE}:{}", names.join(","))
}

/// Parse a verified `sfc` claim back into the requested surface set.
///
/// `None` when the claim is not a workspace claim at all. `Some(vec![])` for
/// the bare `workspace` literal (a claim that carries no set refuses to
/// request anything, rather than defaulting to "everything") and for a claim
/// whose names are all unknown. Unknown names are dropped, never guessed.
pub(crate) fn parse_claim(sfc: &str) -> Option<Vec<Surface>> {
    let rest = if sfc == WORKSPACE_SURFACE {
        ""
    } else {
        sfc.strip_prefix("workspace:")?
    };
    let parsed: Vec<Surface> = rest.split(',').filter_map(Surface::parse).collect();
    Some(ordered(&parsed))
}

/// `surfaces` de-duplicated and in the stable `Surface::ALL` order.
fn ordered(surfaces: &[Surface]) -> Vec<Surface> {
    Surface::ALL
        .iter()
        .copied()
        .filter(|s| surfaces.contains(s))
        .collect()
}

/// The consent-screen scope string for `surfaces`: `openid email` plus the
/// UNION of each surface's own frozen [`Surface::data_scope`], de-duplicated.
/// Built from [`scopes_for`] per surface so the per-surface scope lists are
/// never duplicated here.
pub(crate) fn union_scopes(surfaces: &[Surface]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for surface in ordered(surfaces) {
        for tok in scopes_for(surface).split_whitespace() {
            if !seen.iter().any(|t| t == tok) {
                seen.push(tok.to_string());
            }
        }
    }
    seen.join(" ")
}

/// Whether every data scope of `surface` appears in `granted_scope` (the
/// space-delimited `scope` of the token response). All-or-nothing per surface:
/// a surface whose scopes were only partly ticked would be half-functional
/// (Chat without `chat.messages.readonly`, say), so it is reported as not
/// granted and the member can reconnect that one tile.
pub(crate) fn surface_fully_granted(surface: Surface, granted_scope: &str) -> bool {
    let granted: Vec<&str> = granted_scope.split_whitespace().collect();
    surface
        .data_scope()
        .split_whitespace()
        .all(|needed| granted.contains(&needed))
}

/// Why a surface did not get a row from an all-in-one connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Requested, but the member did not tick it on Google's consent screen.
    Unticked,
    /// Not requested in this flow: its org flag (or env default) is off.
    Disabled,
}

impl SkipReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            SkipReason::Unticked => "unticked",
            SkipReason::Disabled => "disabled",
        }
    }
}

/// Which surfaces an all-in-one callback connects, and why the rest are not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrantPlan {
    /// Requested in this flow AND granted: these get a row.
    pub(crate) connect: Vec<Surface>,
    /// Every other surface, with the reason.
    pub(crate) skipped: Vec<(Surface, SkipReason)>,
}

/// Map a token response's granted scope onto surfaces.
///
/// `requested` is the verified state's claim already intersected with the
/// flags in force now. A surface becomes a row only if it is in `requested`
/// AND [`surface_fully_granted`]. A scope present in `granted_scope` for a
/// surface outside `requested` is ignored (it can be there purely because of
/// `include_granted_scopes`); that surface is reported `Disabled`.
pub(crate) fn plan_from_grant(requested: &[Surface], granted_scope: &str) -> GrantPlan {
    let mut connect = Vec::new();
    let mut skipped = Vec::new();
    for surface in Surface::ALL {
        if !requested.contains(&surface) {
            skipped.push((surface, SkipReason::Disabled));
        } else if surface_fully_granted(surface, granted_scope) {
            connect.push(surface);
        } else {
            skipped.push((surface, SkipReason::Unticked));
        }
    }
    GrantPlan { connect, skipped }
}

/// Per-surface outcome of an all-in-one connect, as reported to the web app.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WorkspaceOutcome {
    pub(crate) connected: Vec<Surface>,
    pub(crate) skipped: Vec<(Surface, SkipReason)>,
    /// Granted and requested, but persisting the row failed.
    pub(crate) failed: Vec<Surface>,
}

impl WorkspaceOutcome {
    /// Query string appended to the `/connectors` redirect:
    /// `google_workspace=1&gws_connected=calendar,gmail&gws_skipped=chat:unticked,meet:disabled&gws_failed=`
    /// (empty lists are omitted).
    pub(crate) fn to_query(&self) -> String {
        let mut q = String::from("google_workspace=1");
        let join = |v: Vec<String>| v.join(",");
        if !self.connected.is_empty() {
            q.push_str("&gws_connected=");
            q.push_str(&join(self.connected.iter().map(|s| s.as_str().to_string()).collect()));
        }
        if !self.skipped.is_empty() {
            q.push_str("&gws_skipped=");
            q.push_str(&join(
                self.skipped
                    .iter()
                    .map(|(s, r)| format!("{}:{}", s.as_str(), r.as_str()))
                    .collect(),
            ));
        }
        if !self.failed.is_empty() {
            q.push_str("&gws_failed=");
            q.push_str(&join(self.failed.iter().map(|s| s.as_str().to_string()).collect()));
        }
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAL: &str = "https://www.googleapis.com/auth/calendar.readonly";
    const GMAIL: &str = "https://www.googleapis.com/auth/gmail.readonly";
    const MEET: &str = "https://www.googleapis.com/auth/meetings.space.readonly";

    fn chat_scopes() -> String {
        Surface::Chat.data_scope().to_string()
    }

    #[test]
    fn union_is_identity_openid_email_plus_each_surfaces_own_scopes_once() {
        let all = union_scopes(&Surface::ALL);
        let toks: Vec<&str> = all.split_whitespace().collect();
        assert_eq!(&toks[..2], &["openid", "email"]);
        // openid/email appear once even though scopes_for adds them per surface.
        assert_eq!(toks.iter().filter(|t| **t == "openid").count(), 1);
        assert_eq!(toks.iter().filter(|t| **t == "email").count(), 1);
        for s in Surface::ALL {
            for need in s.data_scope().split_whitespace() {
                assert!(toks.contains(&need), "{need} missing from the union");
            }
        }
        // 2 identity + 1 calendar + 1 gmail + 4 chat + 1 meet, no extras.
        assert_eq!(toks.len(), 2 + 1 + 1 + 4 + 1);
    }

    #[test]
    fn union_of_a_subset_contains_only_that_subsets_scopes() {
        let s = union_scopes(&[Surface::Calendar, Surface::Meet]);
        assert!(s.contains(CAL) && s.contains(MEET));
        assert!(!s.contains(GMAIL));
        assert!(!s.contains("chat."));
    }

    #[test]
    fn union_scopes_reuses_the_per_surface_scope_lists() {
        // A single-surface union is exactly that surface's own scopes_for.
        for s in Surface::ALL {
            assert_eq!(union_scopes(&[s]), scopes_for(s));
        }
    }

    #[test]
    fn claim_round_trips_in_stable_order_and_dedupes() {
        let c = encode_claim(&[Surface::Meet, Surface::Calendar, Surface::Meet]);
        assert_eq!(c, "workspace:calendar,meet");
        assert_eq!(parse_claim(&c), Some(vec![Surface::Calendar, Surface::Meet]));
    }

    #[test]
    fn bare_workspace_claim_requests_nothing_and_non_workspace_is_none() {
        assert_eq!(parse_claim("workspace"), Some(vec![]));
        assert_eq!(parse_claim("workspace:"), Some(vec![]));
        assert_eq!(parse_claim("workspace:bogus,gmail"), Some(vec![Surface::Gmail]));
        assert_eq!(parse_claim("gmail"), None);
        assert_eq!(parse_claim("workspaces:gmail"), None);
    }

    #[test]
    fn is_workspace_surface_accepts_bare_and_encoded_only() {
        assert!(is_workspace_surface(Some("workspace")));
        assert!(is_workspace_surface(Some("workspace:gmail")));
        assert!(!is_workspace_surface(Some("gmail")));
        assert!(!is_workspace_surface(Some("workspacey")));
        assert!(!is_workspace_surface(None));
    }

    #[test]
    fn full_grant_connects_every_requested_surface() {
        let granted = format!("openid email {CAL} {GMAIL} {} {MEET}", chat_scopes());
        let plan = plan_from_grant(&Surface::ALL, &granted);
        assert_eq!(plan.connect, Surface::ALL.to_vec());
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn partial_grant_connects_only_the_ticked_surfaces() {
        // User unticked Gmail and Meet.
        let granted = format!("openid email {CAL} {}", chat_scopes());
        let plan = plan_from_grant(&Surface::ALL, &granted);
        assert_eq!(plan.connect, vec![Surface::Calendar, Surface::Chat]);
        assert_eq!(
            plan.skipped,
            vec![(Surface::Gmail, SkipReason::Unticked), (Surface::Meet, SkipReason::Unticked)]
        );
    }

    #[test]
    fn partly_ticked_chat_is_not_connected() {
        // Chat needs all four of its scopes; messages.readonly was unticked.
        let granted = format!(
            "openid email {CAL} https://www.googleapis.com/auth/chat.spaces.readonly \
             https://www.googleapis.com/auth/chat.memberships.readonly \
             https://www.googleapis.com/auth/directory.readonly"
        );
        let plan = plan_from_grant(&[Surface::Calendar, Surface::Chat], &granted);
        assert_eq!(plan.connect, vec![Surface::Calendar]);
        assert_eq!(plan.skipped.iter().find(|(s, _)| *s == Surface::Chat).unwrap().1, SkipReason::Unticked);
    }

    #[test]
    fn unrequested_scope_returned_via_include_granted_scopes_creates_no_row() {
        // Only Calendar and Gmail were requested here (Meet/Chat flag off), but
        // Google returned Meet's scope too because the member granted it in an
        // earlier consent and every request sends include_granted_scopes=true.
        let granted = format!("openid email {CAL} {GMAIL} {MEET}");
        let plan = plan_from_grant(&[Surface::Calendar, Surface::Gmail], &granted);
        assert_eq!(plan.connect, vec![Surface::Calendar, Surface::Gmail]);
        assert!(!plan.connect.contains(&Surface::Meet));
        assert!(plan.skipped.contains(&(Surface::Meet, SkipReason::Disabled)));
        assert!(plan.skipped.contains(&(Surface::Chat, SkipReason::Disabled)));
    }

    #[test]
    fn empty_or_identity_only_grant_connects_nothing() {
        let plan = plan_from_grant(&Surface::ALL, "openid email");
        assert!(plan.connect.is_empty());
        assert_eq!(plan.skipped.len(), 4);
        assert!(plan.skipped.iter().all(|(_, r)| *r == SkipReason::Unticked));
        assert!(plan_from_grant(&Surface::ALL, "").connect.is_empty());
    }

    #[test]
    fn a_scope_substring_is_not_a_grant() {
        // Token match is exact, not substring: a longer scope name that merely
        // contains the needed one must not count.
        let granted = format!("openid email {CAL}.extra");
        assert!(plan_from_grant(&[Surface::Calendar], &granted).connect.is_empty());
    }

    #[test]
    fn outcome_query_lists_connected_skipped_and_failed() {
        let o = WorkspaceOutcome {
            connected: vec![Surface::Calendar, Surface::Gmail],
            skipped: vec![(Surface::Chat, SkipReason::Unticked), (Surface::Meet, SkipReason::Disabled)],
            failed: vec![],
        };
        assert_eq!(
            o.to_query(),
            "google_workspace=1&gws_connected=calendar,gmail&gws_skipped=chat:unticked,meet:disabled"
        );
        let f = WorkspaceOutcome { failed: vec![Surface::Gmail], ..Default::default() };
        assert_eq!(f.to_query(), "google_workspace=1&gws_failed=gmail");
    }
}
