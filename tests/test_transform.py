import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from transform import flag_missing_smart, normalize_capacity, update_drive_registry


def test_flag_missing_smart_computes_correct_fraction():
    df = pd.DataFrame({
        "smart_1_raw": [1.0, None],
        "smart_2_raw": [None, None],
    })
    df = flag_missing_smart(df)
    assert df["missing_smart_pct"].tolist() == [0.5, 1.0]


def test_flag_missing_smart_handles_no_smart_columns():
    df = pd.DataFrame({"date": ["2024-01-01"], "model": ["ST4000DM000"]})
    df = flag_missing_smart(df)
    assert (df["missing_smart_pct"] == 0.0).all()


def test_normalize_capacity_flags_deviating_rows():
    df = pd.DataFrame({
        "model": ["A", "A", "A", "B"],
        "capacity_bytes": [1000, 1000, 999, 500],
    })
    reference = {}
    df = normalize_capacity(df, reference)

    assert reference["A"] == 1000
    assert df.loc[2, "capacity_bytes_flagged"] == True
    assert df.loc[2, "capacity_bytes_clean"] == 1000
    assert df.loc[0, "capacity_bytes_flagged"] == False


def test_normalize_capacity_reuses_existing_reference_across_calls():
    reference = {"A": 1000}
    df = pd.DataFrame({"model": ["A"], "capacity_bytes": [900]})
    df = normalize_capacity(df, reference)
    assert df.loc[0, "capacity_bytes_flagged"] == True
    assert df.loc[0, "capacity_bytes_clean"] == 1000


def test_drive_registry_tracks_failure_and_seen_dates():
    registry = {}
    day1 = pd.DataFrame({
        "serial_number": ["S1", "S2"],
        "model": ["A", "B"],
        "failure": [0, 0],
    })
    day2 = pd.DataFrame({
        "serial_number": ["S1"],
        "model": ["A"],
        "failure": [1],
    })
    update_drive_registry(registry, day1, "2024-01-01")
    update_drive_registry(registry, day2, "2024-01-02")

    assert registry["S1"]["first_seen"] == "2024-01-01"
    assert registry["S1"]["last_seen"] == "2024-01-02"
    assert registry["S1"]["failed"] is True
    assert registry["S2"]["last_seen"] == "2024-01-01"
    assert registry["S2"]["failed"] is False