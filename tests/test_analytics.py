import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from analytics import (
    compute_afr,
    afr_by_model,
    top_reliable_models,
    manufacturer_rollup,
    monthly_failure_trend,
    guess_manufacturer,
)


def test_compute_afr_matches_hand_calculation():
    assert round(compute_afr(100_000, 10), 2) == 3.65


def test_compute_afr_handles_zero_drive_days():
    assert compute_afr(0, 0) == 0.0


def test_afr_by_model_aggregates_across_months():
    summary = {
        "ModelA": {
            "2024-01": {"drive_days": 1000, "failures": 1, "rows_with_missing_smart": 0},
            "2024-02": {"drive_days": 1000, "failures": 1, "rows_with_missing_smart": 0},
        }
    }
    result = afr_by_model(summary)
    assert result["ModelA"] == 36.5


def test_top_reliable_models_excludes_low_sample_models():
    summary = {
        "BigSample": {"2024-01": {"drive_days": 50_000, "failures": 1, "rows_with_missing_smart": 0}},
        "TinySample": {"2024-01": {"drive_days": 5, "failures": 0, "rows_with_missing_smart": 0}},
    }
    most, least = top_reliable_models(summary, n=5)
    model_names = [m for m, _ in most] + [m for m, _ in least]
    assert "TinySample" not in model_names
    assert "BigSample" in model_names


def test_guess_manufacturer_matches_known_prefixes():
    assert guess_manufacturer("ST4000DM000") == "Seagate"
    assert guess_manufacturer("WDC WD40EFRX") == "Western Digital"
    assert guess_manufacturer("TOSHIBA MG08ACA16TEY") == "Toshiba"
    assert guess_manufacturer("HGST HUH721010ALE600") == "HGST"


def test_guess_manufacturer_falls_back_to_unknown():
    assert guess_manufacturer("SOME_WEIRD_MODEL") == "Unknown"


def test_manufacturer_rollup_sums_correctly_across_models():
    summary = {
        "ST4000DM000": {"2024-01": {"drive_days": 1000, "failures": 2, "rows_with_missing_smart": 0}},
        "ST16000NM002J": {"2024-01": {"drive_days": 500, "failures": 1, "rows_with_missing_smart": 0}},
    }
    rollup = manufacturer_rollup(summary)
    assert rollup["Seagate"]["drive_days"] == 1500
    assert rollup["Seagate"]["failures"] == 3


def test_monthly_failure_trend_sorted_chronologically():
    summary = {
        "ModelA": {
            "2024-03": {"drive_days": 100, "failures": 0, "rows_with_missing_smart": 0},
            "2024-01": {"drive_days": 100, "failures": 0, "rows_with_missing_smart": 0},
        }
    }
    trend = monthly_failure_trend(summary)
    assert list(trend.keys()) == ["2024-01", "2024-03"]