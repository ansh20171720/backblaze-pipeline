"""
Orchestrator: runs the full pipeline end to end in one command.

    ingest -> transform -> aggregate -> analytics

Each stage is independently incremental (see their own manifest files),
so running this multiple times is safe -- already-processed dates are
skipped automatically at every stage, not just the first one.
"""

import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from ingest import process_csv_files
from transform import run_transform
from aggregate import run_aggregate
from analytics import run_analytics


def run_stage(name: str, func):
    print(f"\n{'=' * 60}")
    print(f"STAGE: {name}")
    print("=" * 60)
    start = time.time()
    func()
    elapsed = time.time() - start
    print(f"\n[{name}] finished in {elapsed:.1f}s")
    return elapsed


def run_pipeline():
    overall_start = time.time()
    timings = {}

    timings["ingest"] = run_stage("Ingest (raw CSV -> partitioned Parquet)", process_csv_files)
    timings["transform"] = run_stage("Transform (data quality layer)", run_transform)
    timings["aggregate"] = run_stage("Aggregate (incremental summary tables)", run_aggregate)
    timings["analytics"] = run_stage("Analytics (Part 2 outputs)", run_analytics)

    total = time.time() - overall_start

    print(f"\n{'=' * 60}")
    print("PIPELINE COMPLETE")
    print("=" * 60)
    for stage, t in timings.items():
        print(f"  {stage:12s}: {t:6.1f}s")
    print(f"  {'total':12s}: {total:6.1f}s")


if __name__ == "__main__":
    run_pipeline()