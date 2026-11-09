#!/usr/bin/env python3
"""Judge one machine's page_read ledger, never infer fleet-wide health.

Latency alerts require >=20 samples and p95 >250ms. Either timeout counter
alerts at any sample count. 5xx counts are evidence, not an additional gate.
Malformed/unavailable evidence is a checker failure, distinct from low traffic.
"""

import argparse
import copy
import json
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path

COUNTERS = ("samples", "errors_5xx", "statement_timeouts", "acquire_timeouts")
U32_MAX = 2**32 - 1


def validate_row(row, label):
    if not isinstance(row, dict):
        raise ValueError(f"{label}: expected an object")
    for key in COUNTERS:
        value = row.get(key)
        if type(value) is not int or not 0 <= value <= U32_MAX:
            raise ValueError(f"{label}.{key}: expected a nonnegative u32 integer")
    for key in ("p50_ms", "p95_ms"):
        if key not in row:
            raise ValueError(f"{label}.{key}: missing")
        value = row[key]
        if row["samples"] == 0:
            if value is not None:
                raise ValueError(f"{label}.{key}: must be null without samples")
        elif type(value) is not int or not 0 <= value <= 2**64 - 1:
            raise ValueError(f"{label}.{key}: expected a nonnegative percentile with samples")
    if row["samples"] and row["p50_ms"] > row["p95_ms"]:
        raise ValueError(f"{label}: p50 exceeds p95")
    if row["errors_5xx"] > row["samples"]:
        raise ValueError(f"{label}: 5xx exceeds request samples")


def judge(health):
    validate_row(health, "aggregate")
    if type(health.get("window_minutes")) is not int or health["window_minutes"] != 60:
        raise ValueError("window_minutes: expected the backend's 60-minute window")
    if not isinstance(health.get("per_route"), list):
        raise ValueError("per_route: expected an array")
    routes = set()
    for row in health["per_route"]:
        validate_row(row, "route")
        route = row.get("route")
        if not isinstance(route, str) or not route.startswith("/") or any(ord(c) < 32 for c in route):
            raise ValueError("per_route.route: expected a route path")
        if route in routes:
            raise ValueError(f"duplicate route: {route}")
        routes.add(route)
    for key in COUNTERS:
        # The backend sums with saturating u32 arithmetic.
        if health[key] != min(U32_MAX, sum(row[key] for row in health["per_route"])):
            raise ValueError(f"aggregate.{key}: does not agree with per_route")
    evidence, violations = [], []
    for label, row in [("aggregate", health), *((r["route"], r) for r in health["per_route"])]:
        sufficient = row["samples"] >= 20
        row_alert = (sufficient and row["p95_ms"] > 250) or any(row[k] > 0 for k in ("statement_timeouts", "acquire_timeouts"))
        evidence.append({"route": label, **{k: row[k] for k in (*COUNTERS, "p50_ms", "p95_ms")},
                         "latency_samples_sufficient": sufficient,
                         "status": "unhealthy" if row_alert else "healthy" if sufficient else "insufficient"})
        if sufficient and row["p95_ms"] > 250:
            violations.append(f"{label}: p95={row['p95_ms']}ms >250ms ({row['samples']} samples)")
        for key in ("statement_timeouts", "acquire_timeouts"):
            if row[key] > 0:
                violations.append(f"{label}: {key}={row[key]}")
    status = "unhealthy" if violations else "insufficient" if any(r["status"] == "insufficient" for r in evidence) else "healthy"
    return {"scope": "single machine; no fleet aggregation", "window_minutes": 60,
            "status": status, "alert": bool(violations),
            "healthy": None if status == "insufficient" else status == "healthy", "evidence": evidence, "violations": violations,
            "notes": "5xx informational; low samples are insufficient latency evidence, not an SLO pass"}


def load_response(path):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"invalid JSON constant: {value}")))


class HealthTests(unittest.TestCase):
    def fixture(self, n=20, p95=250, statement=0, acquire=0, errors=0):
        row = dict(samples=n, p50_ms=10 if n else None, p95_ms=p95 if n else None,
                   errors_5xx=errors, statement_timeouts=statement, acquire_timeouts=acquire)
        return dict(window_minutes=60, **row, per_route=[dict(route="/v1/home/metrics", **row)] if n else [])

    def test_threshold_boundaries(self):
        for n, p95, expected in [(19, 500, None), (20, 250, True), (20, 251, False), (21, 251, False)]:
            with self.subTest(n=n, p95=p95):
                self.assertEqual(judge(self.fixture(n, p95))["healthy"], expected)

    def test_timeouts_without_latency_sample_floor(self):
        for key in ("statement", "acquire"):
            with self.subTest(key=key):
                self.assertFalse(judge(self.fixture(1, 10, **{key: 1}))["healthy"])

    def test_idle_and_low_samples_report_insufficiency(self):
        for n in (0, 1, 19):
            result = judge(self.fixture(n))
            self.assertIsNone(result["healthy"])
            self.assertEqual(result["status"], "insufficient")
            self.assertFalse(result["alert"])
            self.assertFalse(result["evidence"][0]["latency_samples_sufficient"])

    def test_5xx_informational(self):
        result = judge(self.fixture(errors=3))
        self.assertTrue(result["healthy"])
        self.assertEqual(result["evidence"][1]["errors_5xx"], 3)

    def test_route_slow_hidden_by_aggregate(self):
        value = self.fixture(100, 100)
        value["per_route"] = [self.fixture(20, 500)["per_route"][0],
                              dict(self.fixture(80, 100)["per_route"][0], route="/v1/bootstrap")]
        result = judge(value)
        self.assertFalse(result["healthy"])
        self.assertEqual(len(result["violations"]), 1)
        self.assertIn("/v1/home/metrics", result["violations"][0])

    def test_missing_and_malformed_schema(self):
        cases = [None, [], {}, {"error": "unauthorized"}]
        for key in ("window_minutes", "per_route", *COUNTERS, "p50_ms", "p95_ms"):
            value = self.fixture(); del value[key]; cases.append(value)
        for key, invalids in {"samples": [-1, True, "20", 1.1, U32_MAX + 1],
                              "p95_ms": [None, False, "250", -1],
                              "window_minutes": [59, True, "60"], "per_route": [None, {}]}.items():
            for invalid in invalids:
                value = self.fixture(); value[key] = invalid; cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError): judge(value)

    def test_inconsistent_routes_and_percentiles(self):
        cases = []
        value = self.fixture(); value["per_route"] = []; cases.append(value)
        value = self.fixture(); value["per_route"] *= 2; cases.append(value)
        value = self.fixture(); value["per_route"][0]["p50_ms"] = 501; cases.append(value)
        value = self.fixture(); value["per_route"][0]["route"] = ""; cases.append(value)
        value = self.fixture(); value["per_route"][0]["p95_ms"] = None; cases.append(value)
        value = self.fixture(0); value["p95_ms"] = 0; cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError): judge(value)


    def test_cli_fail_closed_and_exit_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "response.json"
            def invoke(body):
                source.write_text(body)
                return subprocess.run([sys.executable, __file__, str(source)], capture_output=True, text=True)
            for body in ('not-json', '{"samples":0,"samples":20}', '{"p95_ms":NaN}', '{"error":"unauthorized"}'):
                self.assertEqual(invoke(body).returncode, 2)
            self.assertEqual(invoke(json.dumps(self.fixture())).returncode, 0)
            for n in (0, 19):
                result = invoke(json.dumps(self.fixture(n, 500)))
                self.assertEqual(result.returncode, 0)
                self.assertEqual(json.loads(result.stdout)["status"], "insufficient")
            self.assertEqual(invoke(json.dumps(self.fixture(20, 251))).returncode, 1)
            source.unlink()
            self.assertEqual(subprocess.run([sys.executable, __file__, str(source)], capture_output=True).returncode, 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("response", nargs="?")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(HealthTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.response:
        parser.error("response JSON path is required")
    try:
        result = judge(load_response(args.response))
    except (OSError, ValueError) as error:
        print(f"page-read-health checker failed: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 1 if result["alert"] else 0


if __name__ == "__main__":
    sys.exit(main())
