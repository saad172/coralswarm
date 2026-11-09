#!/usr/bin/env python3
"""Latency baseline for the web app's load-path endpoints. Read-only: GET only.

Hits each endpoint N times, interleaved (one request per route per round, so a
slow minute lands on every route rather than on whichever ran then), and
prints p50/p95 of wall time per route plus the median of each phase in the
backend's Server-Timing header (auth, txopen, app, total). Run it before and
after a perf change and diff the tables.

Usage:
  CORALSWARM_TOKEN=<bearer> scripts/perf/probe_api_latency.py [-n 20] \
      [--base https://api.coralswarm.example] [--graph-id <uuid>]

The token is a Clerk session JWT (short-lived) or any bearer `require_auth`
accepts; the org plane comes from the token's claims.
Without --graph-id, the first ocean `GET /v1/graphs` returns is used.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def parse_server_timing(header: str | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for metric in (header or "").split(","):
        parts = [p.strip() for p in metric.split(";")]
        if not parts or not parts[0]:
            continue
        for param in parts[1:]:
            k, _, v = param.partition("=")
            if k == "dur":
                try:
                    out[parts[0]] = float(v)
                except ValueError:
                    pass
    return out


class Client:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def get(self, path: str) -> tuple[int, float, dict[str, float], bytes]:
        req = urllib.request.Request(self.base + path, headers=self.headers, method="GET")
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as res:
                body = res.read()
                status, timing = res.status, res.headers.get("server-timing")
        except urllib.error.HTTPError as e:
            body = e.read()
            status, timing = e.code, e.headers.get("server-timing")
        except (urllib.error.URLError, TimeoutError) as e:
            return 0, (time.perf_counter() - started) * 1000, {}, str(e).encode()
        return status, (time.perf_counter() - started) * 1000, parse_server_timing(timing), body


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("-n", type=int, default=20, help="requests per route (default 20)")
    ap.add_argument("--base", default=os.environ.get("CORALSWARM_API", "https://api.coralswarm.example"))
    ap.add_argument("--graph-id", default=os.environ.get("GRAPH_ID"))
    args = ap.parse_args(argv)

    token = os.environ.get("CORALSWARM_TOKEN")
    if not token:
        print("CORALSWARM_TOKEN is not set", file=sys.stderr)
        return 2
    client = Client(args.base, token)

    graph_id = args.graph_id
    if not graph_id:
        status, _, _, body = client.get("/v1/graphs")
        if status != 200:
            print(f"GET /v1/graphs -> {status}: {body[:200]!r}", file=sys.stderr)
            return 1
        graphs = json.loads(body)
        if not graphs:
            print("no oceans for this token; pass --graph-id", file=sys.stderr)
            return 1
        graph_id = graphs[0]["id"]

    now = datetime.now(timezone.utc).replace(microsecond=0)
    frm = (now - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    to = now.isoformat().replace("+00:00", "Z")
    routes = [
        ("/v1/me", "/v1/me"),
        ("/v1/graphs", "/v1/graphs"),
        ("/v1/graphs/{id}/stats", f"/v1/graphs/{graph_id}/stats"),
        ("/v1/conversations", "/v1/conversations"),
        ("/v1/journal", f"/v1/journal?from={frm}&to={to}"),
        ("/v1/library", f"/v1/library?graph_id={graph_id}"),
    ]

    samples: dict[str, list[tuple[int, float, dict[str, float]]]] = {r: [] for r, _ in routes}
    for i in range(args.n):
        for label, path in routes:
            status, ms, timing, _ = client.get(path)
            samples[label].append((status, ms, timing))
        print(f"\rround {i + 1}/{args.n}", end="", file=sys.stderr, flush=True)
    print(file=sys.stderr)

    phases = ["auth", "txopen", "app", "total"]
    print(f"base={args.base} graph={graph_id[:8]} n={args.n}")
    header = f"{'route':<24} {'p50':>8} {'p95':>8} {'max':>8}  " + " ".join(f"{p + '~':>8}" for p in phases) + "  statuses"
    print(header)
    print("-" * len(header))
    for label, _ in routes:
        rows = samples[label]
        wall = [ms for _, ms, _ in rows]
        med = []
        for p in phases:
            vals = [t[p] for _, _, t in rows if p in t]
            med.append(f"{statistics.median(vals):8.1f}" if vals else f"{'-':>8}")
        statuses: dict[int, int] = {}
        for status, _, _ in rows:
            statuses[status] = statuses.get(status, 0) + 1
        st = " ".join(f"{k}x{v}" for k, v in sorted(statuses.items()))
        print(
            f"{label:<24} {percentile(wall, .5):8.1f} {percentile(wall, .95):8.1f} {max(wall):8.1f}  "
            + " ".join(med)
            + f"  {st}"
        )
    print("\nms. p50/p95/max are client wall time; phase columns are medians of Server-Timing.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
