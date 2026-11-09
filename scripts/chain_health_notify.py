#!/usr/bin/env python3
"""Decide whether one chain-health check should fail its workflow run.

`.github/workflows/chain-health-check.yml` polls `/internal/admin/chain-health`
hourly, and a failed run emails the founder. Three outcomes:

* ``page``: the response says ``page: true``, meaning an invariant is broken
  right now (a capture nothing is moving, an unmeasured plane, the
  conservation or outcome checks). Every run fails until it clears.
* ``notify``: only the capture SLO is breached (a backlog draining slower than
  the SLO). The run fails when the breach starts and then once every
  ``--remind-hours`` while it lasts, so a day-long backlog sends four emails,
  not twenty-four.
* ``healthy`` / ``quiet``: the run passes. ``quiet`` is an SLO breach that was
  already reported; it is logged as a warning annotation.

A response without ``page`` (a backend from before the two tiers) treats any
unhealthy response as a page, the old behaviour.

State is a small JSON file the workflow carries between runs in the Actions
cache: ``{"slo_since": iso, "slo_notified_at": iso}``. A missing or unreadable
state file means no breach has been reported yet, so the first run of a breach
always notifies.

Usage: chain_health_notify.py RESPONSE_JSON STATE_IN STATE_OUT [--remind-hours N]
Exit status: 1 for page or notify, 0 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

DEFAULT_REMIND_HOURS = 6.0


def _parse(ts: object) -> datetime | None:
    if not isinstance(ts, str):
        return None
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def decide(response: dict, state: dict, now: datetime, remind: timedelta) -> tuple[str, dict]:
    """Returns (outcome, new_state)."""
    healthy = response.get("healthy") is True
    page = response.get("page")
    if not isinstance(page, bool):
        page = not healthy
    if healthy:
        return "healthy", {}
    if page:
        # A page fails every run on its own. Keep any SLO bookkeeping so the
        # breach is not re-announced the moment the page clears.
        return "page", dict(state)
    since = _parse(state.get("slo_since")) or now
    notified = _parse(state.get("slo_notified_at"))
    if notified is None or now - notified >= remind:
        return "notify", {"slo_since": since.isoformat(), "slo_notified_at": now.isoformat()}
    return "quiet", {"slo_since": since.isoformat(), "slo_notified_at": notified.isoformat()}


def _summary(response: dict) -> str:
    capture = response.get("capture_admission") or {}
    lag = [
        f"{row.get('lag', {}).get('graph_id', '?')[:8]} max {int(row.get('lag', {}).get('max_secs', 0))}s"
        for row in capture.get("lag_violations") or []
    ]
    queued = [
        f"{row.get('captures', {}).get('graph_id', '?')[:8]} {row.get('captures', {}).get('over_lag_age', 0)} queued >1h"
        for row in capture.get("queued_lag_violations") or []
    ]
    parts = []
    if lag:
        parts.append("lag: " + ", ".join(lag))
    if queued:
        parts.append("queued: " + ", ".join(queued))
    if capture.get("unknown_clock"):
        parts.append(f"unknown clock groups: {len(capture['unknown_clock'])}")
    return "; ".join(parts) or "see the JSON above"


def _http_5xx_summary(response: dict) -> str:
    rows = (response.get("http_5xx") or {}).get("violations") or []
    return ", ".join(
        f"{row.get('route', '?')} {row.get('errors_5xx', 0)}/{row.get('requests', 0)} 5xx"
        for row in rows
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("response")
    parser.add_argument("state_in")
    parser.add_argument("state_out")
    parser.add_argument("--remind-hours", type=float, default=DEFAULT_REMIND_HOURS)
    parser.add_argument("--now", help="ISO timestamp, for tests")
    args = parser.parse_args(argv)

    with open(args.response, encoding="utf-8") as f:
        response = json.load(f)
    try:
        with open(args.state_in, encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            state = {}
    except (OSError, ValueError):
        state = {}
    now = _parse(args.now) if args.now else datetime.now(timezone.utc)
    if now is None:
        parser.error("--now must be an ISO timestamp")

    outcome, new_state = decide(response, state, now, timedelta(hours=args.remind_hours))
    with open(args.state_out, "w", encoding="utf-8") as f:
        json.dump(new_state, f)

    summary = _summary(response)
    if outcome == "page":
        print(
            "::error::chain-health page: an invariant is broken now (capture_admission.stalled / "
            "stale_planes / missing_planes, conservation, outcome_distribution, structural_dead_letters, "
            "read_only_rejections, unsure_adjudication_rate or http_5xx). See the JSON above."
        )
        failing_routes = _http_5xx_summary(response)
        if failing_routes:
            print(f"::error::routes failing in the last hour: {failing_routes}")
    elif outcome == "notify":
        print(
            f"::error::capture SLO breached since {new_state['slo_since']} ({summary}). Captures are "
            f"moving but slower than the SLO. The next reminder is in {args.remind_hours:g} h unless it clears."
        )
    elif outcome == "quiet":
        print(
            f"::warning::capture SLO still breached since {new_state['slo_since']} ({summary}); "
            f"already reported at {new_state['slo_notified_at']}."
        )
    else:
        print("chain-health: healthy")
    print(f"outcome={outcome}")
    return 1 if outcome in ("page", "notify") else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
