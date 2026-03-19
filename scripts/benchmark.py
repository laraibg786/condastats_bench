#!/usr/bin/env python
"""condastats benchmark runner — raw JSON output only.

Every run of every case is stored as a separate record so nothing is
pre-aggregated and the full dataset is available for offline analysis.

Usage (local):
    uv run scripts/benchmark.py
    uv run scripts/benchmark.py --runs 5 --cases overall_1mo_1pkg,niche_12mo
    uv run scripts/benchmark.py --list

Usage (CI):
    .venv/bin/python scripts/benchmark.py \
        --runs   5         \
        --cases  all       \
        --label  current   \
        --outdir results/
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Case definitions
# ---------------------------------------------------------------------------

CASES: list[dict[str, Any]] = [
    dict(id="overall_1mo_1pkg", fn="overall", month="2024-01", packages=["pandas"]),
    dict(
        id="overall_1mo_5pkg",
        fn="overall",
        month="2024-01",
        packages=["pandas", "numpy", "scipy", "dask", "requests"],
    ),
    dict(
        id="overall_3mo_1pkg",
        fn="overall",
        start="2024-01",
        end="2024-03",
        packages=["pandas"],
    ),
    dict(
        id="overall_6mo_1pkg",
        fn="overall",
        start="2024-01",
        end="2024-06",
        packages=["pandas"],
    ),
    dict(
        id="overall_12mo_1pkg",
        fn="overall",
        start="2024-01",
        end="2024-12",
        packages=["pandas"],
    ),
    dict(
        id="overall_12mo_5pkg",
        fn="overall",
        start="2024-01",
        end="2024-12",
        packages=["pandas", "numpy", "scipy", "dask", "requests"],
    ),
    dict(
        id="monthly_6mo",
        fn="overall",
        start="2024-01",
        end="2024-06",
        packages=["pandas"],
        monthly=True,
    ),
    dict(
        id="monthly_12mo",
        fn="overall",
        start="2024-01",
        end="2024-12",
        packages=["pandas"],
        monthly=True,
    ),
    dict(id="platform_1mo", fn="pkg_platform", month="2024-01", packages=["pandas"]),
    dict(
        id="platform_12mo",
        fn="pkg_platform",
        start="2024-01",
        end="2024-12",
        packages=["pandas"],
    ),
    dict(id="data_source_1mo", fn="data_source", month="2024-01", packages=["numpy"]),
    dict(
        id="data_source_12mo",
        fn="data_source",
        start="2024-01",
        end="2024-12",
        packages=["numpy"],
    ),
    dict(id="pkg_version_1mo", fn="pkg_version", month="2024-01", packages=["numpy"]),
    dict(id="pkg_python_1mo", fn="pkg_python", month="2024-01", packages=["numpy"]),
    dict(id="niche_1mo", fn="overall", month="2024-01", packages=["pixi"]),
    dict(
        id="niche_12mo", fn="overall", start="2024-01", end="2024-12", packages=["pixi"]
    ),
    dict(
        id="niche_multi_12mo",
        fn="overall",
        start="2024-01",
        end="2024-12",
        packages=["pixi", "rattler", "uv"],
    ),
]

CASE_INDEX = {c["id"]: c for c in CASES}

# ---------------------------------------------------------------------------
# Worker — executes inside each cold-boot subprocess
# ---------------------------------------------------------------------------

WORKER = """
import json, sys, time, os, platform
import psutil

case  = json.loads(sys.argv[1])
label = sys.argv[2]           # "current" or "compare"
proc  = psutil.Process(os.getpid())

# --- import timing ---
t_import_start = time.perf_counter()
import condastats
import_ms = (time.perf_counter() - t_import_start) * 1000

# resolve condastats version from metadata, fall back gracefully
try:
    from importlib.metadata import version as _v
    pkg_version = _v("condastats")
except Exception:
    pkg_version = "unknown"

fn     = getattr(condastats, case["fn"])
kwargs = {}
if "month" in case: kwargs["month"]       = case["month"]
if "start" in case: kwargs["start_month"] = case["start"]
if "end"   in case: kwargs["end_month"]   = case["end"]
if case.get("monthly"): kwargs["monthly"] = True

packages = case["packages"]
pkg_arg  = packages[0] if len(packages) == 1 else packages

# --- baselines before the call ---
net_before  = psutil.net_io_counters()
mem_before  = proc.memory_info().rss
cpu_before  = proc.cpu_times()
t_call_start = time.perf_counter()

result = fn(pkg_arg, **kwargs)

call_ms = (time.perf_counter() - t_call_start) * 1000

# --- deltas after the call ---
net_after  = psutil.net_io_counters()
mem_after  = proc.memory_info().rss
cpu_after  = proc.cpu_times()

rows = result.shape[0] if hasattr(result, "shape") else len(result) if hasattr(result, "__len__") else 1

print(json.dumps(dict(
    pkg_version     = pkg_version,
    import_ms       = round(import_ms,                                          3),
    call_ms         = round(call_ms,                                            3),
    bytes_recv      = net_after.bytes_recv   - net_before.bytes_recv,
    bytes_sent      = net_after.bytes_sent   - net_before.bytes_sent,
    packets_recv    = net_after.packets_recv - net_before.packets_recv,
    packets_sent    = net_after.packets_sent - net_before.packets_sent,
    rss_before_mb   = round(mem_before / 1024 / 1024,                          3),
    rss_after_mb    = round(mem_after  / 1024 / 1024,                          3),
    rss_delta_mb    = round((mem_after - mem_before) / 1024 / 1024,            3),
    cpu_user_ms     = round((cpu_after.user   - cpu_before.user)   * 1000,     3),
    cpu_system_ms   = round((cpu_after.system - cpu_before.system) * 1000,     3),
    rows            = rows,
    python_version  = platform.python_version(),
)))
"""

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class RunRecord:
    """One raw record per subprocess invocation — nothing pre-aggregated."""

    # identity
    label: str
    case_id: str
    fn: str
    packages: list[str]
    month: str | None
    start_month: str | None
    end_month: str | None
    monthly: bool
    run_index: int  # 0-based index within this case
    started_at: str  # ISO-8601 UTC timestamp of subprocess spawn

    # timing
    wall_ms: float  # outer stopwatch: spawn → exit
    import_ms: float  # time to `import condastats`
    call_ms: float  # time for the API call only

    # network (raw bytes — convert to KB/MB in the Excel script)
    bytes_recv: int
    bytes_sent: int
    packets_recv: int
    packets_sent: int

    # memory
    rss_before_mb: float
    rss_after_mb: float
    rss_delta_mb: float

    # cpu
    cpu_user_ms: float
    cpu_system_ms: float

    # result shape
    rows: int

    # versions
    pkg_version: str
    python_version: str

    # error — None on success, last traceback line on failure
    error: str | None = None


@dataclass
class BenchmarkRun:
    """Top-level envelope written to the JSON file."""

    schema_version: str  # bump when RunRecord fields change
    label: str
    started_at: str
    finished_at: str
    hostname: str
    os: str
    cpu_count: int | None
    python_version: str
    records: list[RunRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_once(case: dict, label: str, run_index: int) -> RunRecord:
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()

    proc = subprocess.run(
        [sys.executable, "-c", WORKER, json.dumps(case), label],
        capture_output=True,
        text=True,
    )
    wall_ms = (time.perf_counter() - t0) * 1000

    base = dict(
        label=label,
        case_id=case["id"],
        fn=case["fn"],
        packages=case["packages"],
        month=case.get("month"),
        start_month=case.get("start"),
        end_month=case.get("end"),
        monthly=bool(case.get("monthly")),
        run_index=run_index,
        started_at=started_at,
        wall_ms=round(wall_ms, 3),
    )

    if proc.returncode != 0:
        err = proc.stderr.strip().splitlines()[-1] if proc.stderr else "unknown error"
        return RunRecord(
            **base,
            import_ms=0,
            call_ms=0,
            bytes_recv=0,
            bytes_sent=0,
            packets_recv=0,
            packets_sent=0,
            rss_before_mb=0,
            rss_after_mb=0,
            rss_delta_mb=0,
            cpu_user_ms=0,
            cpu_system_ms=0,
            rows=0,
            pkg_version="unknown",
            python_version="unknown",
            error=err,
        )

    data = json.loads(proc.stdout.strip())
    return RunRecord(**base, **data)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def resolve_cases(spec: str) -> list[dict]:
    if spec.strip() == "all":
        return CASES
    ids = [s.strip() for s in spec.split(",") if s.strip()]
    missing = [i for i in ids if i not in CASE_INDEX]
    if missing:
        print(f"unknown case ids: {missing}", file=sys.stderr)
        print(f"available: {list(CASE_INDEX)}", file=sys.stderr)
        sys.exit(1)
    return [CASE_INDEX[i] for i in ids]


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--cases", default="all")
    p.add_argument("--label", default="results")
    p.add_argument("--outdir", default=".", type=Path)
    p.add_argument("--list", action="store_true")
    args = p.parse_args()

    if args.list:
        for c in CASES:
            print(c["id"])
        return

    cases = resolve_cases(args.cases)
    args.outdir.mkdir(parents=True, exist_ok=True)

    run = BenchmarkRun(
        schema_version="1.0",
        label=args.label,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at="",
        hostname=socket.gethostname(),
        os=platform.platform(),
        cpu_count=os.cpu_count(),
        python_version=platform.python_version(),
    )

    for case in cases:
        print(f"  {case['id']} ", end="", flush=True)
        for i in range(args.runs):
            rec = run_once(case, args.label, i)
            run.records.append(rec)
            print("." if not rec.error else "E", end="", flush=True)
        print()

    run.finished_at = datetime.now(timezone.utc).isoformat()

    out = args.outdir / f"results_{args.label}.json"
    payload = {
        "schema_version": run.schema_version,
        "label": run.label,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "hostname": run.hostname,
        "os": run.os,
        "cpu_count": run.cpu_count,
        "python_version": run.python_version,
        "records": [asdict(r) for r in run.records],
    }
    out.write_text(json.dumps(payload, indent=2))
    print(f"\n  wrote {out}  ({len(run.records)} records)")


if __name__ == "__main__":
    main()
