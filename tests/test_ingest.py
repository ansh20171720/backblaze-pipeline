import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingest import normalize_columns, coerce_dtypes, extract_date, update_schema_registry


def test_normalize_columns_lowercases_and_strips():
    df = pd.DataFrame(columns=["  Date ", "Serial_Number", "SMART_1_RAW"])
    df = normalize_columns(df)
    assert list(df.columns) == ["date", "serial_number", "smart_1_raw"]


def test_coerce_dtypes_forces_smart_columns_to_float():
    df = pd.DataFrame({
        "smart_1_raw": [1, 2, None],
        "smart_1_normalized": [100, None, 90],
        "cluster_id": [1, 2, 3],
    })
    df = coerce_dtypes(df)
    assert df["smart_1_raw"].dtype == "float64"
    assert df["smart_1_normalized"].dtype == "float64"
    assert df["cluster_id"].dtype != "float64" or df["cluster_id"].dtype == "int64"


def test_coerce_dtypes_handles_unexpected_numeric_column_with_nulls():
    df = pd.DataFrame({"pod_slot_num": [1, 2, None]})
    df = coerce_dtypes(df)
    assert df["pod_slot_num"].dtype == "float64"


def test_extract_date_from_standard_filename():
    assert extract_date("data/raw/data_Q1_2024/2024-01-15.csv") == "2024-01-15"


def test_extract_date_returns_none_when_missing():
    assert extract_date("data/raw/readme.txt") is None


def test_schema_registry_tracks_first_and_last_seen():
    registry = {}
    update_schema_registry(registry, ["date", "smart_1_raw"], "2024-01-01")
    update_schema_registry(registry, ["date", "smart_2_raw"], "2024-02-01")

    assert registry["date"]["first_seen"] == "2024-01-01"
    assert registry["date"]["last_seen"] == "2024-02-01"
    assert registry["smart_2_raw"]["first_seen"] == "2024-02-01"
    assert registry["smart_1_raw"]["last_seen"] == "2024-01-01"