//! Today's date, in the asking user's timezone, for the answer model.
//!
//! Asked "What was decided this week?" on Thursday 2026-10-22, the answer
//! model called "this week" ambiguous, picked 2026-10-16..2026-10-22, and in
//! the same answer called 2026-10-18 "just before that window" — it was never
//! told what day it was. Every Ask surface (scoped chat, MCP `ask_ocean`, the
//! legacy stream) appends [`answer_date_context`] to its system prompt so the
//! model resolves relative periods from one stated date, with the same
//! Monday-start week the retrieval window (`routes::queries::week_range`)
//! already uses.
//!
//! `now` is a parameter, never read inside: the scoped path passes its
//! injectable `recap_clock`, and tests pin an instant.
use chrono::{DateTime, Datelike, Duration, Utc};
use chrono_tz::Tz;

/// One paragraph for the system prompt: weekday + ISO date + zone, the
/// resolved bounds of "this week" (Monday through today) and "last week"
/// (previous Monday–Sunday), and the instruction to state the range used.
pub fn answer_date_context(now: DateTime<Utc>, tz: Tz) -> String {
    let today = now.with_timezone(&tz).date_naive();
    let this_monday = today - Duration::days(today.weekday().num_days_from_monday() as i64);
    let last_monday = this_monday - Duration::days(7);
    let last_sunday = this_monday - Duration::days(1);
    format!(
        "Today is {weekday} {today} in {zone}. Weeks start on Monday: \"this week\" is Monday {this_monday} through today, and \"last week\" is Monday {last_monday} through Sunday {last_sunday}; resolve \"today\", \"yesterday\" and \"this month\" from today's date the same way. When the question names a relative period, state the concrete date range you used and keep every date in the answer consistent with it.",
        weekday = today.format("%A"),
        zone = tz.name(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn thursday_in_chicago_resolves_this_and_last_week() {
        let ctx = answer_date_context(at("2026-10-22T20:00:00Z"), chrono_tz::America::Chicago);
        assert!(ctx.starts_with("Today is Thursday 2026-10-22 in America/Chicago."), "{ctx}");
        assert!(ctx.contains("\"this week\" is Monday 2026-10-19 through today"), "{ctx}");
        assert!(ctx.contains("\"last week\" is Monday 2026-10-12 through Sunday 2026-10-18"), "{ctx}");
        assert!(ctx.contains("state the concrete date range you used"), "{ctx}");
    }

    #[test]
    fn utc_and_local_date_differ_west_of_utc() {
        // 03:30Z Friday is still Thursday evening in Chicago (UTC-5 on this date).
        let now = at("2026-10-23T03:30:00Z");
        let utc = answer_date_context(now, chrono_tz::UTC);
        let chicago = answer_date_context(now, chrono_tz::America::Chicago);
        assert!(utc.starts_with("Today is Friday 2026-10-23 in UTC."), "{utc}");
        assert!(chicago.starts_with("Today is Thursday 2026-10-22 in America/Chicago."), "{chicago}");
        // Same ISO week either way, so the Monday anchors agree.
        for ctx in [&utc, &chicago] {
            assert!(ctx.contains("Monday 2026-10-19 through today"), "{ctx}");
        }
    }

    #[test]
    fn utc_and_local_date_differ_east_of_utc_across_a_week_boundary() {
        // 16:00Z Sunday is already Monday 01:00 in Tokyo, so the local week
        // has rolled: "this week" is Monday-through-Monday and "last week" is
        // the week UTC still calls current.
        let now = at("2026-10-25T16:00:00Z");
        let utc = answer_date_context(now, chrono_tz::UTC);
        let tokyo = answer_date_context(now, chrono_tz::Asia::Tokyo);
        assert!(utc.starts_with("Today is Sunday 2026-10-25 in UTC."), "{utc}");
        assert!(utc.contains("\"this week\" is Monday 2026-10-19 through today"), "{utc}");
        assert!(utc.contains("\"last week\" is Monday 2026-10-12 through Sunday 2026-10-18"), "{utc}");
        assert!(tokyo.starts_with("Today is Monday 2026-10-26 in Asia/Tokyo."), "{tokyo}");
        assert!(tokyo.contains("\"this week\" is Monday 2026-10-26 through today"), "{tokyo}");
        assert!(tokyo.contains("\"last week\" is Monday 2026-10-19 through Sunday 2026-10-25"), "{tokyo}");
    }
}
