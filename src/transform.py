import os
import glob
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROCESSED_DIR = "data/processed"          # bronze layer (from ingest.py)
CLEAN_DIR = "data/clean"                  # silver layer (this script's output)
CLEAN_MANIFEST_PATH = "data/clean_manifest.json"
DRIVE_REGISTRY_PATH = "data/drive_registry.json"
MODEL_CAPACITY_PATH = "data/model_capacity_reference.json"

SMART_COL_PREFIX = "smart_"


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def get_smart_columns(df: pd.DataFrame) -> list:
    return [c for c in df.columns if c.startswith(SMART_COL_PREFIX)]


def flag_missing_smart(df: pd.DataFrame) -> pd.DataFrame:
    """Policy: don't drop rows with missing SMART values -- they still
    represent a valid drive-day for AFR purposes. Instead, record what
    fraction of SMART columns were null for each row, so downstream
    analytics (Part 2e) can report on this directly."""
    smart_cols = get_smart_columns(df)
    if not smart_cols:
        df["missing_smart_pct"] = 0.0
        return df
    df["missing_smart_pct"] = df[smart_cols].isna().sum(axis=1) / len(smart_cols)
    return df


def update_drive_registry(registry: dict, df: pd.DataFrame, date: str):
    """Policy: track first/last-seen date per serial number so a drive
    disappearing from the dataset can be distinguished from a drive that
    was explicitly marked as failed (failure == 1). Disappearance without
    a failure flag is treated as right-censored, not as a failure -- this
    is a known limitation of AFR calculated from this kind of panel data,
    and should be documented as such."""
    grouped = df.groupby("serial_number").agg(
        model=("model", "first"),
        any_failure=("failure", "max"),
    )
    for serial, row in grouped.iterrows():
        if serial not in registry:
            registry[serial] = {
                "model": row["model"],
                "first_seen": date,
                "last_seen": date,
                "failed": bool(row["any_failure"]),
            }
        else:
            registry[serial]["last_seen"] = date
            if row["any_failure"]:
                registry[serial]["failed"] = True


def normalize_capacity(df: pd.DataFrame, capacity_reference: dict) -> pd.DataFrame:
    """Policy: normalize capacity_bytes per model to that model's most
    common (mode) reported value across the dataset seen so far. Rows
    that disagree with the reference are flagged, not silently overwritten,
    so the original value is preserved for audit while a clean value is
    available for aggregation."""
    df["capacity_bytes_flagged"] = False
    df["capacity_bytes_clean"] = df["capacity_bytes"]

    for model in df["model"].dropna().unique():
        mask = df["model"] == model
        model_values = df.loc[mask, "capacity_bytes"].dropna()
        if model_values.empty:
            continue

        if model in capacity_reference:
            reference_value = capacity_reference[model]
        else:
            reference_value = int(model_values.mode().iloc[0])
            capacity_reference[model] = reference_value

        mismatch = mask & (df["capacity_bytes"] != reference_value)
        df.loc[mismatch, "capacity_bytes_flagged"] = True
        df.loc[mismatch, "capacity_bytes_clean"] = reference_value

    return df


def clean_partition(date: str, capacity_reference: dict, drive_registry: dict):
    src_path = os.path.join(PROCESSED_DIR, f"date={date}", "data.parquet")
    df = pd.read_parquet(src_path)

    df = flag_missing_smart(df)
    df = normalize_capacity(df, capacity_reference)
    update_drive_registry(drive_registry, df, date)

    out_dir = os.path.join(CLEAN_DIR, f"date={date}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "data.parquet")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)

    return len(df), df["missing_smart_pct"].mean(), df["capacity_bytes_flagged"].sum()


def run_transform():
    os.makedirs(CLEAN_DIR, exist_ok=True)

    clean_manifest = load_json(CLEAN_MANIFEST_PATH, {"cleaned_dates": {}})
    drive_registry = load_json(DRIVE_REGISTRY_PATH, {})
    capacity_reference = load_json(MODEL_CAPACITY_PATH, {})

    partitions = sorted(glob.glob(os.path.join(PROCESSED_DIR, "date=*")))
    dates = [os.path.basename(p).replace("date=", "") for p in partitions]

    print(f"Found {len(dates)} processed date partition(s) in {PROCESSED_DIR}.")

    newly_cleaned = 0
    skipped = 0
    failed = []

    for date in dates:
        if date in clean_manifest["cleaned_dates"]:
            skipped += 1
            continue
        try:
            rows, avg_missing_pct, flagged_capacity = clean_partition(
                date, capacity_reference, drive_registry
            )
            clean_manifest["cleaned_dates"][date] = {
                "rows": rows,
                "avg_missing_smart_pct": round(float(avg_missing_pct), 4),
                "capacity_flagged_rows": int(flagged_capacity),
            }
            newly_cleaned += 1
        except Exception as e:
            print(f"  [ERROR] Failed to clean {date}: {e}")
            failed.append({"date": date, "error": str(e)})

    save_json(CLEAN_MANIFEST_PATH, clean_manifest)
    save_json(DRIVE_REGISTRY_PATH, drive_registry)
    save_json(MODEL_CAPACITY_PATH, capacity_reference)

    print("\n--- Transform summary ---")
    print(f"Newly cleaned dates: {newly_cleaned}")
    print(f"Skipped (already cleaned): {skipped}")
    print(f"Failed: {len(failed)}")
    if failed:
        for f in failed:
            print(f"  - {f['date']}: {f['error']}")
    print(f"Distinct drives tracked in registry: {len(drive_registry)}")
    print(f"Distinct models with a capacity reference: {len(capacity_reference)}")
    print("Transform complete!")


if __name__ == "__main__":
    run_transform()