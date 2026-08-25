import os
import re
import glob
import json
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
MANIFEST_PATH = "data/processed_manifest.json"
SCHEMA_REGISTRY_PATH = "data/schema_registry.json"

DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")
SMART_COL_PATTERN = re.compile(r"^smart_\d+_(raw|normalized)$")


INT_ID_COLUMNS = {"cluster_id", "vault_id", "pod_id", "capacity_bytes", "failure"}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Force numeric columns (SMART attributes + any other id/counter column
    not explicitly known to be a clean integer) to a consistent float64
    dtype. This prevents schema drift caused by pandas promoting a column
    from int64 to float64 the moment it encounters a null value -- which
    otherwise produces a different Arrow schema per file/chunk and breaks
    ParquetWriter's fixed-schema requirement. Column selection is driven by
    a naming pattern (smart_*) plus dtype inspection, not a hardcoded list
    of specific column names."""
    smart_cols = [c for c in df.columns if SMART_COL_PATTERN.match(c)]
    for col in smart_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    
    for col in df.columns:
        if col in INT_ID_COLUMNS or col in smart_cols:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def extract_date(file_path: str) -> Optional[str]:
    match = DATE_PATTERN.search(os.path.basename(file_path))
    return match.group(1) if match else None


def update_schema_registry(registry: dict, columns: list, date: str):
    for col in columns:
        if col not in registry:
            registry[col] = {"first_seen": date, "last_seen": date}
        else:
            registry[col]["last_seen"] = date
            if date < registry[col]["first_seen"]:
                registry[col]["first_seen"] = date


def process_csv_files():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    manifest = load_json(MANIFEST_PATH, {"processed_dates": {}})
    schema_registry = load_json(SCHEMA_REGISTRY_PATH, {})

    csv_files = glob.glob(f"{RAW_DIR}/**/*.csv", recursive=True)
    if not csv_files:
        print(f"No CSV files found under {RAW_DIR}")
        return

    print(f"Found {len(csv_files)} CSV file(s) on disk.")

    skipped_already_done = 0
    skipped_bad_date = 0
    failed_files = []
    newly_processed = 0

    for file_path in sorted(csv_files):
        date = extract_date(file_path)
        if date is None:
            print(f"  [WARN] Could not extract date from filename, skipping: {file_path}")
            skipped_bad_date += 1
            continue

        if date in manifest["processed_dates"]:
            skipped_already_done += 1
            continue

        try:
         
            df = pd.read_csv(file_path, low_memory=False)
            df = normalize_columns(df)
            df = coerce_dtypes(df)

            partition_dir = os.path.join(PROCESSED_DIR, f"date={date}")
            os.makedirs(partition_dir, exist_ok=True)
            output_path = os.path.join(partition_dir, "data.parquet")

            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, output_path)

            update_schema_registry(schema_registry, sorted(df.columns), date)

            manifest["processed_dates"][date] = {
                "source_file": file_path,
                "rows": len(df),
                "columns": len(df.columns),
            }
            newly_processed += 1

            
            del df, table

        except Exception as e:
            print(f"  [ERROR] Failed to process {file_path}: {e}")
            failed_files.append({"file": file_path, "error": str(e)})
            continue

    save_json(MANIFEST_PATH, manifest)
    save_json(SCHEMA_REGISTRY_PATH, schema_registry)

    print("\n--- Ingestion summary ---")
    print(f"Newly processed dates: {newly_processed}")
    print(f"Skipped (already processed): {skipped_already_done}")
    print(f"Skipped (bad/missing date in filename): {skipped_bad_date}")
    print(f"Failed files: {len(failed_files)}")
    if failed_files:
        print("See failures below:")
        for f in failed_files:
            print(f"  - {f['file']}: {f['error']}")
    print(f"Total distinct columns ever seen: {len(schema_registry)}")
    print("Ingestion & normalization complete!")


if __name__ == "__main__":
    process_csv_files()