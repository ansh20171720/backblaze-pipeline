import os
import glob
import json

import pandas as pd

CLEAN_DIR = "data/clean"
AGG_MANIFEST_PATH = "data/aggregate_manifest.json"
MODEL_MONTHLY_PATH = "data/model_monthly_summary.json"


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def aggregate_partition(date: str, summary: dict):
    """Roll up a single day's cleaned partition into running per-model,
    per-month totals. Only this one day's rows are ever read -- prior
    months already reflected in `summary` are never recomputed, which is
    what keeps a pipeline run O(new_rows) rather than O(total_history)."""
    path = os.path.join(CLEAN_DIR, f"date={date}", "data.parquet")
    df = pd.read_parquet(
        path,
        columns=["model", "failure", "capacity_bytes_clean", "missing_smart_pct"],
    )

    month = date[:7]  # YYYY-MM

    grouped = df.groupby("model").agg(
        drive_days=("failure", "size"),
        failures=("failure", "sum"),
        missing_smart_row_sum=("missing_smart_pct", lambda s: (s > 0).sum()),
    )

    for model, row in grouped.iterrows():
        summary.setdefault(model, {})
        summary[model].setdefault(
            month, {"drive_days": 0, "failures": 0, "rows_with_missing_smart": 0}
        )
        bucket = summary[model][month]
        bucket["drive_days"] += int(row["drive_days"])
        bucket["failures"] += int(row["failures"])
        bucket["rows_with_missing_smart"] += int(row["missing_smart_row_sum"])

    return len(df)


def run_aggregate():
    agg_manifest = load_json(AGG_MANIFEST_PATH, {"aggregated_dates": {}})
    summary = load_json(MODEL_MONTHLY_PATH, {})

    partitions = sorted(glob.glob(os.path.join(CLEAN_DIR, "date=*")))
    dates = [os.path.basename(p).replace("date=", "") for p in partitions]

    print(f"Found {len(dates)} cleaned date partition(s) in {CLEAN_DIR}.")

    newly_aggregated = 0
    skipped = 0
    failed = []

    for date in dates:
        if date in agg_manifest["aggregated_dates"]:
            skipped += 1
            continue
        try:
            rows = aggregate_partition(date, summary)
            agg_manifest["aggregated_dates"][date] = {"rows": rows}
            newly_aggregated += 1
        except Exception as e:
            print(f"  [ERROR] Failed to aggregate {date}: {e}")
            failed.append({"date": date, "error": str(e)})

    save_json(AGG_MANIFEST_PATH, agg_manifest)
    save_json(MODEL_MONTHLY_PATH, summary)

    total_drive_days = sum(
        bucket["drive_days"] for months in summary.values() for bucket in months.values()
    )
    total_failures = sum(
        bucket["failures"] for months in summary.values() for bucket in months.values()
    )

    print("\n--- Aggregate summary ---")
    print(f"Newly aggregated dates: {newly_aggregated}")
    print(f"Skipped (already aggregated): {skipped}")
    print(f"Failed: {len(failed)}")
    if failed:
        for f in failed:
            print(f"  - {f['date']}: {f['error']}")
    print(f"Distinct models in summary: {len(summary)}")
    print(f"Total drive-days across all models: {total_drive_days}")
    print(f"Total failures across all models: {total_failures}")
    print("Aggregation complete!")


if __name__ == "__main__":
    run_aggregate()