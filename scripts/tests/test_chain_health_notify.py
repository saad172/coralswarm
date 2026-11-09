import importlib.util
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

_SPEC = importlib.util.spec_from_file_location(
    "chain_health_notify",
    os.path.join(os.path.dirname(__file__), "..", "chain_health_notify.py"),
)
notify = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(notify)

NOW = datetime(2026, 10, 21, 4, 0, tzinfo=timezone.utc)
REMIND = timedelta(hours=6)
SLO_ONLY = {"healthy": False, "page": False}
PAGE = {"healthy": False, "page": True}
HEALTHY = {"healthy": True, "page": False}


def hourly(responses, state=None):
    """Runs the hourly check over `responses`; returns each run's outcome."""
    state = state or {}
    outcomes = []
    for hour, response in enumerate(responses):
        outcome, state = notify.decide(response, state, NOW + timedelta(hours=hour), REMIND)
        outcomes.append(outcome)
    return outcomes


class ChainHealthNotifyTest(unittest.TestCase):
    def test_a_day_of_slo_breach_fails_once_per_reminder_not_every_hour(self):
        outcomes = hourly([SLO_ONLY] * 24)
        self.assertEqual(outcomes.count("notify"), 4)
        self.assertEqual(outcomes[0], "notify", "the breach is reported when it starts")
        self.assertEqual(outcomes[1:6], ["quiet"] * 5)
        self.assertEqual(outcomes[6], "notify")

    def test_http_5xx_summary_names_each_failing_route(self):
        response = {
            "http_5xx": {
                "violations": [
                    {"route": "/v1/library", "requests": 4, "errors_5xx": 4, "status_503": 4},
                    {"route": "/v1/me", "requests": 10, "errors_5xx": 3, "status_503": 0},
                ]
            }
        }
        self.assertEqual(
            notify._http_5xx_summary(response), "/v1/library 4/4 5xx, /v1/me 3/10 5xx"
        )
        self.assertEqual(notify._http_5xx_summary(PAGE), "")

    def test_a_page_fails_every_run(self):
        self.assertEqual(hourly([PAGE] * 5), ["page"] * 5)

    def test_recovery_then_a_new_breach_notifies_again(self):
        self.assertEqual(
            hourly([SLO_ONLY, SLO_ONLY, HEALTHY, SLO_ONLY]),
            ["notify", "quiet", "healthy", "notify"],
        )

    def test_a_page_inside_a_breach_does_not_reset_the_reminder(self):
        self.assertEqual(
            hourly([SLO_ONLY, PAGE, SLO_ONLY]),
            ["notify", "page", "quiet"],
        )

    def test_a_backend_without_page_treats_any_unhealthy_response_as_a_page(self):
        self.assertEqual(hourly([{"healthy": False}, {"healthy": True}]), ["page", "healthy"])

    def test_cli_round_trip_and_unreadable_state_notifies(self):
        with tempfile.TemporaryDirectory() as d:
            response = os.path.join(d, "response.json")
            state_out = os.path.join(d, "state.json")
            with open(response, "w") as f:
                json.dump({**SLO_ONLY, "capture_admission": {"lag_violations": [
                    {"tenant": "t", "lag": {"graph_id": "ac9befa1-x", "max_secs": 25243.2}}]}}, f)
            code = notify.main([response, os.path.join(d, "missing.json"), state_out, "--now", NOW.isoformat()])
            self.assertEqual(code, 1)
            code = notify.main([response, state_out, state_out, "--now", (NOW + timedelta(hours=1)).isoformat()])
            self.assertEqual(code, 0, "the second hour of the same breach passes")
            with open(state_out) as f:
                self.assertEqual(json.load(f)["slo_notified_at"], NOW.isoformat())


if __name__ == "__main__":
    unittest.main()
